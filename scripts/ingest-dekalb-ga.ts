#!/usr/bin/env tsx
/**
 * MXRE — DeKalb County, GA Assessor Parcel Ingest (Atlanta metro)
 *
 * Source: DeKalb County GIS — Parcels MapServer
 *   https://dcgis.dekalbcountyga.gov/hosted/rest/services/Parcels/MapServer/0
 *   ~280K+ parcels, offset-based pagination, MaxRecordCount=2000
 *
 * Fields: PARCELID, OWNERNME1, OWNERNME2, SITEADDRESS, PSTLZIP5,
 *         CNTASSDVAL (current assessed), CNTTXBLVAL, PRVASSDVAL
 *
 * GA appraised value = assessed value / 0.40 (40% assessment ratio)
 *
 * Usage:
 *   npx tsx scripts/ingest-dekalb-ga.ts
 *   npx tsx scripts/ingest-dekalb-ga.ts --skip=10000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://dcgis.dekalbcountyga.gov/hosted/rest/services/Parcels/MapServer/0";
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

// Parse city from SITEADDRESS: "123 MAIN ST, DECATUR, GA 30030"
function parseAddressCity(siteAddress: string): { address: string; city: string } {
  const parts = siteAddress.split(",").map(s => s.trim());
  if (parts.length >= 2) {
    const stateZipPart = parts[parts.length - 1]; // "GA 30030"
    const cityPart = parts[parts.length - 2]; // "DECATUR"
    const addressPart = parts.slice(0, parts.length - 2).join(", ");
    return {
      address: (addressPart || parts[0]).toUpperCase(),
      city: cityPart.replace(/\s+(GA|TN|AL|SC)$/i, "").toUpperCase().trim(),
    };
  }
  return { address: siteAddress.toUpperCase(), city: "DECATUR" };
}

const FIELDS = [
  "OBJECTID",
  "PARCELID",
  "LOWPARCELID",
  "OWNERNME1",
  "OWNERNME2",
  "SITEADDRESS",
  "PSTLZIP5",
  "CNTASSDVAL",
  "CNTTXBLVAL",
  "PRVASSDVAL",
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

  console.log("MXRE — DeKalb County, GA Assessor Parcel Ingest (Atlanta metro)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "DeKalb").eq("state_code", "GA").single();
  if (!county) { console.error("DeKalb County, GA not in DB"); process.exit(1); }
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
      const pin = String(f.PARCELID || f.LOWPARCELID || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const siteAddr = String(f.SITEADDRESS || "").trim();
      if (!siteAddr) { skipped++; continue; }

      const { address, city } = parseAddressCity(siteAddr);
      const zip = String(f.PSTLZIP5 || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // DeKalb: CNTASSDVAL is assessed value (40% of appraised)
      const assessedValue = parseNum(f.CNTASSDVAL);
      const marketValue = assessedValue ? Math.round(assessedValue / 0.40) : null;

      const own1 = String(f.OWNERNME1 || "").trim();
      const own2 = String(f.OWNERNME2 || "").trim();
      const ownerName = own2 && own2 !== own1 ? `${own1}; ${own2}` : own1;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName || null,
        address,
        city: city || "DECATUR",
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        property_type: "residential",
        source: "dekalb_ga_gis",
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
