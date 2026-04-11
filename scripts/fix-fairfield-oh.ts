#!/usr/bin/env tsx
/**
 * Fix Fairfield OH property data — update owner_name, address, city, zip, etc.
 * from the shapefile fields (OWN1, PADDR1, MCITYNAME, etc.)
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Fix Fairfield OH Property Data\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) { console.log("No county"); return; }

  // Build a map of parcel_id -> correct data from shapefile
  const source = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");
  const parcelMap = new Map<string, Record<string, unknown>>();

  while (true) {
    const result = await source.read();
    if (result.done) break;
    const p = result.value.properties || {};
    const parcelId = p.PIN || p.PARID || "";
    if (!parcelId) continue;

    parcelMap.set(parcelId, {
      owner_name: p.OWN1 || null,
      address: p.PADDR1 || "",
      city: p.MCITYNAME || "",
      zip: p.MZIP1 || "",
      assessed_value: parseFloat(p.APPRVAL || "0") || null,
      total_sqft: parseFloat(p.SFLA || "0") || null,
      year_built: parseInt(p.YRBLT || "0", 10) || null,
      property_tax: parseFloat(p.APRLAND || "0") + parseFloat(p.APRBLDG || "0") || null,
    });
  }

  console.log(`  Loaded ${parcelMap.size} parcels from shapefile`);

  // Update properties in batches
  let updated = 0;
  const parcelIds = [...parcelMap.keys()];

  for (let i = 0; i < parcelIds.length; i += 100) {
    const batch = parcelIds.slice(i, i + 100);

    for (const pid of batch) {
      const data = parcelMap.get(pid)!;
      const { error } = await db.from("properties")
        .update(data)
        .eq("county_id", county.id)
        .eq("parcel_id", pid);
      if (!error) updated++;
    }

    if (updated % 5000 === 0 && updated > 0) {
      process.stdout.write(`\r  Updated: ${updated.toLocaleString()} / ${parcelMap.size.toLocaleString()}`);
    }
  }

  console.log(`\n  Done: ${updated.toLocaleString()} properties updated`);

  // Verify
  const { data: sample } = await db.from("properties")
    .select("parcel_id, owner_name, address, city, assessed_value")
    .eq("county_id", county.id)
    .not("owner_name", "is", null)
    .limit(3);
  console.log("\n  Verified samples:");
  for (const s of sample || []) {
    console.log(`    ${s.parcel_id}: ${s.owner_name} | ${s.address}, ${s.city} | $${s.assessed_value}`);
  }
}

main().catch(console.error);
