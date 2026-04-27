#!/usr/bin/env tsx
/**
 * Check what CAMA fields are populated for Marion County properties.
 * Helps identify gaps and which sources already have data.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(q: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return res.json();
}

async function main() {
  const COUNTY_ID = 797583;

  const rows = await pg(`
    SELECT
      source,
      COUNT(*)::int AS total,
      COUNT(year_built)::int AS has_year_built,
      COUNT(total_sqft)::int AS has_total_sqft,
      COUNT(bedrooms)::int AS has_bedrooms,
      COUNT(bathrooms)::int AS has_bathrooms,
      COUNT(lot_sqft)::int AS has_lot_sqft,
      COUNT(owner_name)::int AS has_owner_name,
      COUNT(market_value)::int AS has_market_value,
      COUNT(city)::int AS has_city,
      COUNT(zip)::int AS has_zip
    FROM properties
    WHERE county_id = ${COUNTY_ID}
    GROUP BY source
    ORDER BY total DESC
  `);

  console.log("\nMarion County (797583) — field coverage by source");
  console.log("=".repeat(80));
  for (const r of rows) {
    const pct = (n: number, d: number) => d > 0 ? `${Math.round(n/d*100)}%` : "0%";
    console.log(`\nSource: ${r.source} (${r.total.toLocaleString()} rows)`);
    console.log(`  year_built : ${r.has_year_built.toLocaleString()} (${pct(r.has_year_built, r.total)})`);
    console.log(`  total_sqft : ${r.has_total_sqft.toLocaleString()} (${pct(r.has_total_sqft, r.total)})`);
    console.log(`  bedrooms   : ${r.has_bedrooms.toLocaleString()} (${pct(r.has_bedrooms, r.total)})`);
    console.log(`  bathrooms  : ${r.has_bathrooms.toLocaleString()} (${pct(r.has_bathrooms, r.total)})`);
    console.log(`  lot_sqft   : ${r.has_lot_sqft.toLocaleString()} (${pct(r.has_lot_sqft, r.total)})`);
    console.log(`  owner_name : ${r.has_owner_name.toLocaleString()} (${pct(r.has_owner_name, r.total)})`);
    console.log(`  market_val : ${r.has_market_value.toLocaleString()} (${pct(r.has_market_value, r.total)})`);
    console.log(`  city       : ${r.has_city.toLocaleString()} (${pct(r.has_city, r.total)})`);
    console.log(`  zip        : ${r.has_zip.toLocaleString()} (${pct(r.has_zip, r.total)})`);
  }

  // Sample a few rows from in-data-harvest-parcels to see raw data
  const sample = await pg(`
    SELECT id, parcel_id, address, owner_name, year_built, total_sqft, bedrooms, bathrooms, lot_sqft, city, zip, market_value
    FROM properties
    WHERE county_id = ${COUNTY_ID} AND source = 'in-data-harvest-parcels'
    ORDER BY id LIMIT 5
  `);
  console.log("\nSample in-data-harvest-parcels rows:");
  for (const r of sample) {
    console.log(JSON.stringify(r, null, 2));
  }
}

main().catch(e => { console.error(e); process.exit(1); });
