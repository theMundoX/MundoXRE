#!/usr/bin/env tsx
/**
 * MXRE — Warren County, OH Assessor Parcel Ingest
 *
 * Source: Warren County GIS — Dynamic/ParcelLabelsAddr MapServer
 *   https://maps.co.warren.oh.us/arcgis/rest/services/Dynamic/ParcelLabelsAddr/MapServer/0
 *   ~100K+ parcels, offset-based pagination
 *
 * NOTE: No market value available in public API (CAMA data is auth-gated).
 *       Ingests owner + address + parcel ID for mortgage record linking.
 *
 * Fields: MACCT (parcel ID), OWNER_NAME, ADDRESS_LINE_1, ADDRESS_LINE_2,
 *         MUNICIPALITY_NAME, TOWNSHIP_NAME, CLASS_CODE, USE_CODE_DSC
 *
 * Usage:
 *   npx tsx scripts/ingest-warren-oh.ts
 *   npx tsx scripts/ingest-warren-oh.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://maps.co.warren.oh.us/arcgis/rest/services/Dynamic/ParcelLabelsAddr/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "OH";

// Warren County municipality → primary ZIP
const CITY_ZIP: Record<string, string> = {
  MASON: "45040",
  LEBANON: "45036",
  FRANKLIN: "45005",
  SPRINGBORO: "45066",
  WAYNESVILLE: "45068",
  MORROW: "45152",
  LOVELAND: "45140",
  "KINGS MILLS": "45034",
  MAINEVILLE: "45039",
  "SOUTH LEBANON": "45065",
  HARVEYSBURG: "45032",
  "BLANCHESTER": "45107",
  CARLISLE: "45005",
  "HAMILTON TWP": "45011",
  "CLEARCREEK TWP": "45066",
  "DEERFIELD TWP": "45040",
  "TURTLE CREEK TWP": "45036",
  "WAYNE TWP": "45068",
  "HARLAN TWP": "45036",
  "MASSIE TWP": "45036",
  "SALEM TWP": "45152",
  "TURTLECREEK TWP": "45036",
  "UNION TWP": "45036",
  "WASHINGTON TWP": "45036",
  "HAMILTON": "45011",
  "MIDDLETOWN": "45044",
  "MONROE": "45050",
  "TRENTON": "45067",
  "OXFORD": "45056",
  "WEST CHESTER": "45069",
};

function cleanMuniName(raw: string | null): string {
  if (!raw) return "";
  return raw
    .replace(/\s+(TWP|TOWNSHIP|CORP|CITY|CORPORATION|VILLAGE|VLG)$/i, "")
    .toUpperCase()
    .trim();
}

function getZip(munName: string | null, townshipName: string | null): string {
  const city = cleanMuniName(munName);
  const twp = cleanMuniName(townshipName);
  return CITY_ZIP[city] || CITY_ZIP[twp] || CITY_ZIP[munName?.toUpperCase().trim() || ""] || "45036"; // Lebanon = county seat
}

function classifyCode(cls: string | null): string {
  if (!cls) return "residential";
  const c = cls.toUpperCase();
  if (c.startsWith("1") || c.includes("AG") || c.includes("FARM")) return "agricultural";
  if (c.startsWith("4") || c.includes("COMMERCIAL")) return "commercial";
  if (c.startsWith("5") || c.includes("INDUSTRIAL")) return "industrial";
  if (c.startsWith("6") || c.includes("EXEMPT")) return "exempt";
  if (c.startsWith("3") || c.includes("MULTI")) return "multifamily";
  return "residential";
}

const FIELDS = [
  "OBJECTID",
  "MACCT",
  "PARNUM",
  "OWNER_NAME",
  "ADDRESS_LINE_1",
  "ADDRESS_LINE_2",
  "MUNICIPALITY_NAME",
  "TOWNSHIP_NAME",
  "CLASS_CODE",
  "USE_CODE_DSC",
  "ACREAGE",
].join(",");

async function fetchPage(offset: number): Promise<{ features: Record<string, unknown>[]; count: number }> {
  const url =
    `${PARCELS_URL}/query?where=1%3D1` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = ((json.features as Array<{ attributes: Record<string, unknown> }>) || []).map(
        (f) => f.attributes,
      );

      return { features, count: features.length };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0 };
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOffset = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Warren County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));
  console.log("NOTE: No market value in public API — ingesting address/owner/parcel only\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Warren").eq("state_code", "OH").single();
  if (!county) { console.error("Warren County not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs
  const existing = new Set<string>();
  let exOffset = 0;
  while (true) {
    const { data } = await db.from("properties").select("parcel_id")
      .eq("county_id", COUNTY_ID).not("parcel_id", "is", null)
      .range(exOffset, exOffset + 999);
    if (!data || data.length === 0) break;
    for (const r of data) if (r.parcel_id) existing.add(r.parcel_id);
    if (data.length < 1000) break;
    exOffset += 1000;
  }
  console.log(`  ${existing.size.toLocaleString()} parcels already in DB\n`);

  let inserted = 0, dupes = 0, errors = 0, skipped = 0;
  let offset = skipOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.MACCT || f.PARNUM || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const addrLine1 = String(f.ADDRESS_LINE_1 || "").trim().toUpperCase();
      const addrLine2 = String(f.ADDRESS_LINE_2 || "").trim().toUpperCase();
      const address = addrLine2 ? `${addrLine1} ${addrLine2}`.trim() : addrLine1;
      if (!address) { skipped++; continue; }

      const munName = f.MUNICIPALITY_NAME as string | null;
      const twnName = f.TOWNSHIP_NAME as string | null;
      const city = cleanMuniName(munName) || cleanMuniName(twnName) || "LEBANON";
      const zip = getZip(munName, twnName);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER_NAME || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: null, // not available in public API
        assessed_value: null,
        property_type: classifyCode(f.CLASS_CODE as string | null),
        source: "warren_oh_gis",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(chunk, { onConflict: "county_id,parcel_id" });
      if (error) {
        for (const record of chunk) {
          const { error: e2 } = await db.from("properties").upsert(record, { onConflict: "county_id,parcel_id" });
          if (e2) {
            if (errors < 5) console.error(`\n  Error: ${JSON.stringify(e2).slice(0, 120)}`);
            errors++;
          } else {
            inserted++;
          }
        }
      } else {
        inserted += chunk.length;
      }
    }

    process.stdout.write(
      `\r  offset ${offset.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skipped ${skipped} | errs ${errors}   `,
    );

    offset += count;
    if (count < PAGE_SIZE) break; // last page
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
