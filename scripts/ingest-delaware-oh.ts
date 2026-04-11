#!/usr/bin/env tsx
/**
 * MXRE — Delaware County, OH Assessor Parcel Ingest
 *
 * Source: Delaware County GIS — Auditor Current Year Parcels
 *   https://maps.delco-gis.org/arcgiswebadaptor/rest/services/AuditorGISWebsite/AuditorMap_CurrentYearParcels_WM/MapServer/0
 *   ~115K parcels, MaxRecordCount: 1000 (offset-based pagination)
 *
 * Fields: OBJECTID, OWNPARCELID, OWNERNME1, LSN (full situs address),
 *         MKT_Tot_Total, Acres
 *
 * NOTE: LSN format is "123 MAIN ST, CITYNAME 43215" — parsed for address/city/zip
 *
 * Usage:
 *   npx tsx scripts/ingest-delaware-oh.ts
 *   npx tsx scripts/ingest-delaware-oh.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://maps.delco-gis.org/arcgiswebadaptor/rest/services/AuditorGISWebsite/AuditorMap_CurrentYearParcels_WM/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "OH";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

/**
 * Parse Delaware County LSN field: "123 MAIN ST, CITYNAME 43215"
 * Returns { address, city, zip } or null if unparseable
 */
function parseLSN(lsn: string): { address: string; city: string; zip: string } | null {
  const s = lsn.trim();
  // Match: everything up to last comma, then city words, then 5-digit zip
  const m = s.match(/^(.+),\s*(.+?)\s+(\d{5})\s*$/);
  if (!m) return null;
  const address = m[1].trim().toUpperCase();
  const city = m[2].trim().toUpperCase();
  const zip = m[3].trim();
  if (!address || !city || !zip) return null;
  return { address, city, zip };
}

const FIELDS = [
  "OBJECTID",
  "OWNPARCELID",
  "ALTPARCELID",
  "OWNERNME1",
  "LSN",
  "MKT_Tot_Total",
  "Acres",
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

  console.log("MXRE — Delaware County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Delaware").eq("state_code", "OH").single();
  if (!county) { console.error("Delaware County not in DB"); process.exit(1); }
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
      const pin = String(f.OWNPARCELID || f.ALTPARCELID || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const lsnRaw = String(f.LSN || "").trim();
      if (!lsnRaw) { skipped++; continue; }

      const parsed = parseLSN(lsnRaw);
      if (!parsed) { skipped++; continue; }

      const { address, city, zip } = parsed;

      const marketValue = parseNum(f.MKT_Tot_Total);
      const assessedValue = marketValue ? Math.round(marketValue * 0.35) : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNERNME1 || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        property_type: "residential",
        source: "delaware_oh_auditor_gis",
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
