#!/usr/bin/env tsx
/**
 * MXRE — Davidson County, TN Assessor Parcel Ingest (Nashville)
 *
 * Source: Metro Nashville GIS — Cadastral Parcels MapServer
 *   https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0
 *   ~250K+ parcels, MaxRecordCount=10,000 (offset-based)
 *
 * Fields: ParID, Owner, PropAddr, PropStreet, PropZip, TotlAppr, LandAppr,
 *         ImprAppr, TotlAssd, SalePrice, LUCode, LUDesc
 *
 * TN assessed value = 25% of appraised for residential
 *
 * Usage:
 *   npx tsx scripts/ingest-davidson-tn.ts
 *   npx tsx scripts/ingest-davidson-tn.ts --skip=50000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0";
const PAGE_SIZE = 5000;
const BATCH_SIZE = 500;
const STATE_CODE = "TN";
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
  const c = String(code).toUpperCase();
  if (c.startsWith("A") || c.includes("FARM")) return "agricultural";
  if (c.startsWith("C") || c.includes("COMM") || c.includes("RETAIL")) return "commercial";
  if (c.startsWith("I") || c.includes("IND")) return "industrial";
  if (c.includes("EXE") || c.includes("EXEMPT")) return "exempt";
  if (c.startsWith("M") || c.includes("MULTI") || c.includes("APT")) return "multifamily";
  if (c.includes("CONDO")) return "condo";
  return "residential";
}

const FIELDS = [
  "OBJECTID",
  "ParID",
  "Owner",
  "OwnAddr1",
  "PropAddr",
  "PropStreet",
  "PropCity",
  "PropZip",
  "TotlAppr",
  "LandAppr",
  "ImprAppr",
  "TotlAssd",
  "SalePrice",
  "LUCode",
  "LUDesc",
  "Acres",
  "Council",
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

  console.log("MXRE — Davidson County, TN Assessor Parcel Ingest (Nashville)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Davidson").eq("state_code", "TN").single();
  if (!county) { console.error("Davidson County, TN not in DB"); process.exit(1); }
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
      const pin = String(f.ParID || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // Use PropAddr if available, else PropStreet
      const address = String(f.PropAddr || f.PropStreet || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.PropCity || "NASHVILLE").trim().toUpperCase();
      const zip = String(f.PropZip || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.TotlAppr);
      // TN residential assessed value = 25% of appraised
      const assessedValue = parseNum(f.TotlAssd) ?? (marketValue ? Math.round(marketValue * 0.25) : null);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.Owner || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.LandAppr),
        last_sale_price: parseNum(f.SalePrice),
        property_type: classifyLU(f.LUCode as string | null),
        source: "davidson_tn_metro_gis",
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
