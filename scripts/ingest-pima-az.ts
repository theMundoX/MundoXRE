#!/usr/bin/env tsx
/**
 * MXRE — Pima County, AZ Assessor Parcel Ingest (Tucson)
 *
 * Source: Pima County GIS Open Data — Land Records MapServer Layer 12
 *   https://gisdata.pima.gov/arcgis1/rest/services/GISOpenData/LandRecords/MapServer/12
 *   ~450K parcels, offset-based pagination, MaxRecordCount=2000
 *
 * Note: Owner name NOT available in public layer (Pima strips PII from GIS).
 *       Ingests address + ZIP + FCV (Full Cash Value) for mortgage record linking.
 *
 * Fields: PARCEL, ADDRESS_OL, ZIP, FCV, LIMNET, PARCEL_USE, TAXYR, LON, LAT
 *
 * AZ residential assessed value ≈ 10% of FCV
 *
 * Usage:
 *   npx tsx scripts/ingest-pima-az.ts
 *   npx tsx scripts/ingest-pima-az.ts --skip=50000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gisdata.pima.gov/arcgis1/rest/services/GISOpenData/LandRecords/MapServer/12";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "AZ";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

// Extract city from ADDRESS_OL: "2150 W ORANGE GROVE RD, TUCSON AZ 85742"
function extractCity(address: string): string {
  const m = address.match(/,\s*([A-Z\s]+)\s+AZ\s+\d{5}/i);
  if (m) return m[1].trim().toUpperCase();
  return "TUCSON";
}

const FIELDS = [
  "OBJECTID",
  "PARCEL",
  "ADDRESS_OL",
  "ZIP",
  "ZIP4",
  "FCV",
  "LIMNET",
  "PARCEL_USE",
  "TAXYR",
  "LON",
  "LAT",
  "GISACRES",
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

  console.log("MXRE — Pima County, AZ Assessor Parcel Ingest (Tucson)");
  console.log("═".repeat(60));
  console.log("NOTE: Owner name not in public API — address + FCV only\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Pima").eq("state_code", "AZ").single();
  if (!county) { console.error("Pima County, AZ not in DB"); process.exit(1); }
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
      const pin = String(f.PARCEL || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // ADDRESS_OL may look like "2150 W ORANGE GROVE RD" or full "..., TUCSON AZ 85742"
      const rawAddr = String(f.ADDRESS_OL || "").trim().toUpperCase();
      if (!rawAddr) { skipped++; continue; }

      // Strip city/state/zip portion if present to get clean street address
      const address = rawAddr.replace(/,\s*[A-Z\s]+\s+AZ\s+\d{5}.*$/i, "").trim() || rawAddr;

      const city = extractCity(rawAddr);
      const zip = String(f.ZIP || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.FCV);
      const assessedValue = marketValue ? Math.round(marketValue * 0.10) : null;

      const lat = typeof f.LAT === "number" && f.LAT > 30 ? f.LAT as number : null;
      const lng = typeof f.LON === "number" && f.LON < -100 ? f.LON as number : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: null, // not available in public layer
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        lat,
        lng,
        property_type: "residential",
        source: "pima_az_gis",
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
