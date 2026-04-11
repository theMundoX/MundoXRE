#!/usr/bin/env tsx
/**
 * Delete and re-ingest Fairfield OH properties with correct field mapping.
 * Faster than updating 74K individual records.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Re-ingest Fairfield OH Properties (correct mapping)\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) { console.log("No county"); return; }
  console.log(`  County ID: ${county.id}`);

  // Delete old bad data
  console.log("  Deleting old records...");
  const { count: deleted } = await db.from("properties")
    .delete({ count: "exact" }).eq("county_id", county.id);
  console.log(`  Deleted: ${deleted}`);

  // Re-read shapefile with correct mapping
  const source = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");
  let inserted = 0;
  let batch: Array<Record<string, unknown>> = [];

  while (true) {
    const result = await source.read();
    if (result.done) break;
    const p = result.value.properties || {};

    batch.push({
      county_id: county.id,
      parcel_id: p.PIN || p.PARID || "",
      owner_name: p.OWN1 || null,
      address: p.PADDR1 || "",
      city: p.MCITYNAME || "",
      state_code: "OH",
      zip: p.MZIP1 || "",
      assessed_value: parseFloat(p.APPRVAL || "0") || null,
      property_type: p.LUC || "unknown",
      total_sqft: parseFloat(p.SFLA || "0") || null,
      year_built: parseInt(p.YRBLT || "0", 10) || null,
      total_units: 1,
    });

    if (batch.length >= 500) {
      const { error } = await db.from("properties").insert(batch);
      if (error) console.error(`  Error: ${error.message.slice(0, 60)}`);
      else inserted += batch.length;
      batch = [];
      if (inserted % 5000 === 0) process.stdout.write(`\r  Inserted: ${inserted.toLocaleString()}`);
    }
  }

  if (batch.length > 0) {
    const { error } = await db.from("properties").insert(batch);
    if (!error) inserted += batch.length;
  }

  console.log(`\n  Done: ${inserted.toLocaleString()} properties`);

  // Verify
  const { data: sample } = await db.from("properties")
    .select("parcel_id, owner_name, address, city, assessed_value")
    .eq("county_id", county.id).not("owner_name", "is", null).limit(3);
  console.log("\n  Samples:");
  for (const s of sample || []) {
    console.log(`    ${s.parcel_id}: ${s.owner_name} | ${s.address}, ${s.city} | $${s.assessed_value}`);
  }
}

main().catch(console.error);
