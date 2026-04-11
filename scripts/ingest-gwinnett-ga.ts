#!/usr/bin/env tsx
/**
 * MXRE — Gwinnett County, GA Assessor Parcel Ingest (Atlanta suburb)
 *
 * Source: Gwinnett County GIS — Property and Tax FeatureServer
 *   Tax Master Table (non-geographic): FeatureServer/3
 *   https://services3.arcgis.com/RfpmnkSAQleRbndX/arcgis/rest/services/Property_and_Tax/FeatureServer/3
 *   ~300K+ records, offset-based pagination, MaxRecordCount=2000
 *
 * Fields: RPIN/PIN (parcel ID), OWNER1, OWNER2, LOCADDR, LOCCITY, LOCSTATE,
 *         LOCZIP, TOTVAL1 (total value), LANDVAL1 (land value), TAXTOT1
 *
 * GA assessed value = 40% of appraised
 *
 * Usage:
 *   npx tsx scripts/ingest-gwinnett-ga.ts
 *   npx tsx scripts/ingest-gwinnett-ga.ts --skip=10000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://services3.arcgis.com/RfpmnkSAQleRbndX/arcgis/rest/services/Property_and_Tax/FeatureServer/3";
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

const FIELDS = [
  "OBJECTID",
  "RPIN",
  "PIN",
  "LRSN",
  "OWNER1",
  "OWNER2",
  "LOCADDR",
  "LOCCITY",
  "LOCSTATE",
  "LOCZIP",
  "TOTVAL1",
  "LANDVAL1",
  "DWLGVAL1",
  "TAXTOT1",
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

  console.log("MXRE — Gwinnett County, GA Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Gwinnett").eq("state_code", "GA").single();
  if (!county) { console.error("Gwinnett County, GA not in DB"); process.exit(1); }
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
      // RPIN is the primary parcel ID; PIN is alternate format
      const pin = String(f.RPIN || f.PIN || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.LOCADDR || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.LOCCITY || "LAWRENCEVILLE").trim().toUpperCase();
      const zip = String(f.LOCZIP || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.TOTVAL1);
      // GA: assessed value = 40% of appraised
      const assessedValue = marketValue ? Math.round(marketValue * 0.40) : null;

      const own1 = String(f.OWNER1 || "").trim();
      const own2 = String(f.OWNER2 || "").trim();
      const ownerName = own2 && own2 !== own1 ? `${own1}; ${own2}` : own1;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.LANDVAL1),
        property_tax: parseNum(f.TAXTOT1),
        property_type: "residential",
        source: "gwinnett_ga_gis",
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
