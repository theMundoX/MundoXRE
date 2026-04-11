#!/usr/bin/env tsx
/**
 * Fix Fairfield OH by creating a temp table with correct data,
 * then doing a single UPDATE JOIN via Supabase SQL/RPC.
 * If RPC isn't available, fall back to small-batch updates with explicit IDs.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Fix Fairfield OH (Small Batch by ID)\n");

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) return;

  // Read shapefile
  const source = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");
  const parcelMap = new Map<string, { owner: string; addr: string; city: string; zip: string; value: number | null; sqft: number | null; yrbuilt: number | null }>();
  while (true) {
    const result = await source.read();
    if (result.done) break;
    const p = result.value.properties || {};
    const pid = p.PIN || p.PARID;
    if (!pid || !p.OWN1) continue;
    parcelMap.set(pid, {
      owner: p.OWN1, addr: p.PADDR1 || "", city: p.MCITYNAME || "",
      zip: p.MZIP1 || "", value: parseFloat(p.APPRVAL || "0") || null,
      sqft: parseFloat(p.SFLA || "0") || null, yrbuilt: parseInt(p.YRBLT || "0", 10) || null,
    });
  }
  console.log(`  Loaded ${parcelMap.size} parcels`);

  // Get properties by small batches using ID ranges
  let updated = 0;
  let lastId = 0;

  // First, find the ID range for this county
  const { data: minRow } = await db.from("properties").select("id").eq("county_id", county.id).order("id").limit(1);
  const { data: maxRow } = await db.from("properties").select("id").eq("county_id", county.id).order("id", { ascending: false }).limit(1);

  if (!minRow?.length || !maxRow?.length) { console.log("No properties"); return; }

  const minId = minRow[0].id;
  const maxId = maxRow[0].id;
  console.log(`  ID range: ${minId} to ${maxId}`);

  lastId = minId - 1;
  const CHUNK = 50; // Very small chunks to avoid timeout

  while (lastId < maxId) {
    const { data: props } = await db.from("properties")
      .select("id, parcel_id, owner_name")
      .eq("county_id", county.id)
      .gt("id", lastId)
      .order("id")
      .limit(CHUNK);

    if (!props || props.length === 0) break;
    lastId = props[props.length - 1].id;

    for (const prop of props) {
      if (prop.owner_name) continue; // Already has owner name
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

    if (updated % 500 === 0 && updated > 0) {
      process.stdout.write(`\r  Updated: ${updated.toLocaleString()} (id: ${lastId})`);
    }
  }

  console.log(`\n  Done: ${updated.toLocaleString()} properties updated`);
}

main().catch(console.error);
