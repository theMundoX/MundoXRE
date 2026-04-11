#!/usr/bin/env tsx
/**
 * MXRE — Fulton County, GA Assessor Parcel Ingest (Atlanta)
 *
 * Source: Fulton County GIS — Property Map Viewer MapServer
 *   https://gismaps.fultoncountyga.gov/arcgispub2/rest/services/PropertyMapViewer/PropertyMapViewer/MapServer/11
 *   ~400K+ parcels, offset-based pagination, MaxRecordCount=2000
 *
 * Fields: ParcelID, Owner, Address, TotAppr, TotAssess, LandAppr, LandAssess,
 *         ImprAppr, LUCode, TaxDist
 *
 * GA assessed value = 40% of appraised (fair market value)
 *
 * Usage:
 *   npx tsx scripts/ingest-fulton-ga.ts
 *   npx tsx scripts/ingest-fulton-ga.ts --skip=10000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gismaps.fultoncountyga.gov/arcgispub2/rest/services/PropertyMapViewer/PropertyMapViewer/MapServer/11";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "GA";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function classifyLU(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).toUpperCase().trim();
  // GA land use codes: R=residential, C=commercial, I=industrial, A=agricultural, E=exempt
  if (c.startsWith("A") || c === "AG") return "agricultural";
  if (c.startsWith("C") || c.includes("COMM")) return "commercial";
  if (c.startsWith("I") || c.includes("IND")) return "industrial";
  if (c.startsWith("E") || c.includes("EXE")) return "exempt";
  if (c.includes("MF") || c.includes("APT") || c.includes("MULTI")) return "multifamily";
  if (c.includes("CONDO")) return "condo";
  return "residential";
}

// Fulton County covers several cities; parse ZIP from address if not in separate field
function extractZip(address: string): string | null {
  const m = address.match(/\b(\d{5})(?:-\d{4})?\s*$/);
  return m ? m[1] : null;
}

const FIELDS = [
  "OBJECTID",
  "ParcelID",
  "Owner",
  "Address",
  "TotAppr",
  "TotAssess",
  "LandAppr",
  "LandAssess",
  "ImprAppr",
  "LUCode",
  "TaxDist",
  "NbrHood",
  "LandAcres",
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

  console.log("MXRE — Fulton County, GA Assessor Parcel Ingest (Atlanta)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fulton").eq("state_code", "GA").single();
  if (!county) { console.error("Fulton County, GA not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

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
      const pin = String(f.ParcelID || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.Address || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      // Fulton County layer has no City/ZipCode fields; default city to ATLANTA
      const city = "ATLANTA";
      const zip = extractZip(address) || "";

      const marketValue = parseNum(f.TotAppr);
      // GA: assessed value = 40% of appraised
      const assessedValue = parseNum(f.TotAssess) ?? (marketValue ? Math.round(marketValue * 0.40) : null);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.Owner || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip: zip || "30301",
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.LandAppr),
        property_type: classifyLU(f.LUCode as string | null),
        source: "fulton_ga_gis",
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
          } else { inserted++; }
        }
      } else {
        inserted += chunk.length;
      }
    }

    process.stdout.write(
      `\r  offset ${offset.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skip ${skipped} | errs ${errors}   `,
    );

    offset += count;
    if (count < PAGE_SIZE) break;
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
