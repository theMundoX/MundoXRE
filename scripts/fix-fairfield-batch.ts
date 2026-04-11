#!/usr/bin/env tsx
/**
 * Fix Fairfield OH properties using batch RPC or upsert.
 * Instead of individual UPDATEs, build a batch of parcel_id -> owner_name mappings
 * and update using a WHERE IN clause.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Batch Fix Fairfield OH Properties\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) return;

  // Read shapefile into a map
  const source = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");
  const parcelMap = new Map<string, { owner: string; addr: string; city: string; zip: string; value: number | null; sqft: number | null; yrbuilt: number | null }>();

  while (true) {
    const result = await source.read();
    if (result.done) break;
    const p = result.value.properties || {};
    const pid = p.PIN || p.PARID;
    if (!pid || !p.OWN1) continue;
    parcelMap.set(pid, {
      owner: p.OWN1,
      addr: p.PADDR1 || "",
      city: p.MCITYNAME || "",
      zip: p.MZIP1 || "",
      value: parseFloat(p.APPRVAL || "0") || null,
      sqft: parseFloat(p.SFLA || "0") || null,
      yrbuilt: parseInt(p.YRBLT || "0", 10) || null,
    });
  }
  console.log(`  Loaded ${parcelMap.size} parcels with owner names`);

  // Get all property IDs for this county that need updating
  let updated = 0;
  let offset = 0;
  const BATCH = 200;

  while (true) {
    const { data: props } = await db.from("properties")
      .select("id, parcel_id")
      .eq("county_id", county.id)
      .is("owner_name", null)
      .range(offset, offset + BATCH - 1);

    if (!props || props.length === 0) break;

    // Update each one with data from shapefile
    for (const prop of props) {
      const data = parcelMap.get(prop.parcel_id);
      if (!data) continue;

      const { error } = await db.from("properties")
        .update({
          owner_name: data.owner,
          address: data.addr,
          city: data.city,
          zip: data.zip,
          assessed_value: data.value,
          total_sqft: data.sqft,
          year_built: data.yrbuilt,
        })
        .eq("id", prop.id);

      if (!error) updated++;
    }

    process.stdout.write(`\r  Updated: ${updated.toLocaleString()}`);
    // Don't increment offset — we're filtering by owner_name IS NULL, so already-updated ones won't appear again
  }

  console.log(`\n  Done: ${updated.toLocaleString()} properties updated`);
}

main().catch(console.error);
