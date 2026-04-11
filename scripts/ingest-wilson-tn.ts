#!/usr/bin/env tsx
/**
 * MXRE — Wilson County, TN Assessor Parcel Ingest (Lebanon/Mount Juliet)
 *
 * Source: Wilson County GIS — Tax Taxparcels MapServer
 *   https://gis.wilson-co.com/arcgis/rest/services/Tax/Taxparcels/MapServer/0
 *   ~90K parcels, ArcGIS Server v10.91, offset-based pagination
 *
 * Fields: PIN/GISPIN, Name1-3, PhysicalStreetAddress, TaxpayerCity, ZIPCode,
 *         TotalFMVCurrent (market value), TotalASVCurrent (assessed),
 *         LandFMVCur, ImproveFMVCur, SalesAmount, DateSold
 *
 * TN assessed = 25% of appraised for residential
 *
 * Usage:
 *   npx tsx scripts/ingest-wilson-tn.ts
 *   npx tsx scripts/ingest-wilson-tn.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis.wilson-co.com/arcgis/rest/services/Tax/Taxparcels/MapServer/0";
const PAGE_SIZE = 2000;
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

function parseDate(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "number" && v > 0) {
    const dt = new Date(v);
    if (dt.getFullYear() > 1900 && dt.getFullYear() < 2100) {
      return dt.toISOString().split("T")[0];
    }
  }
  if (typeof v === "string" && v.trim()) {
    const m = v.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  }
  return null;
}

const FIELDS = [
  "OBJECTID",
  "PIN",
  "GISPIN",
  "ParcelNumber",
  "Name1",
  "Name2",
  "Name3",
  "PhysicalStreetAddress",
  "TaxpayerCity",
  "State",
  "ZIPCode",
  "TotalFMVCurrent",
  "LandFMVCur",
  "ImproveFMVCur",
  "TotalASVCurrent",
  "LandASVCur",
  "ImproveASVCur",
  "SalesAmount",
  "DateSold",
  "MapAcres",
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

  console.log("MXRE — Wilson County, TN Assessor Parcel Ingest (Lebanon/Mt Juliet)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Wilson").eq("state_code", "TN").single();
  if (!county) { console.error("Wilson County, TN not in DB"); process.exit(1); }
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
      const pin = String(f.PIN || f.GISPIN || f.ParcelNumber || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.PhysicalStreetAddress || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.TaxpayerCity || "LEBANON").trim().toUpperCase();
      const zip = String(f.ZIPCode || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.TotalFMVCurrent);
      const assessedValue = parseNum(f.TotalASVCurrent) ?? (marketValue ? Math.round(marketValue * 0.25) : null);

      const owners = [f.Name1, f.Name2, f.Name3]
        .filter(Boolean).map((o) => String(o).trim()).filter((o) => o.length > 0);
      const ownerName = owners.join("; ") || null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.LandFMVCur),
        last_sale_price: parseNum(f.SalesAmount),
        last_sale_date: parseDate(f.DateSold),
        property_type: "residential",
        source: "wilson_tn_gis",
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
