#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  console.log("MXRE — Ingest Wyandot County OH (GeoJSON)\n");
  const { data: county } = await db.from("counties").select("id").eq("county_name", "Wyandot").eq("state_code", "OH").single();
  if (!county) { console.log("No county"); return; }
  console.log("  County ID:", county.id);

  const raw = readFileSync("data/wyandot-oh-parcels.geojson", "utf-8");
  const geo = JSON.parse(raw);
  console.log("  Features:", geo.features?.length);

  let inserted = 0;
  let batch: Array<Record<string, unknown>> = [];

  for (const f of geo.features || []) {
    const p = f.properties || {};
    const addr = [p.SITEADDR || p.ADDRESS || p.LOC_ADDR || p.LOCATION_A || ""].filter(Boolean).join(" ");
    batch.push({
      county_id: county.id,
      parcel_id: p.PARCEL_ID || p.PIN || p.PARCELID || p.PARCEL || p.PARCELNUMB || "",
      owner_name: p.OWNER || p.OWNER1 || p.OWN_NAME || p.OWNERNAME || p.OwnName || p.NAME || null,
      address: addr,
      city: p.CITY || p.SITECITY || p.LOC_CITY || "",
      state_code: "OH",
      zip: p.ZIP || p.SITEZIP || p.LOC_ZIP || "",
      assessed_value: parseFloat(p.APPRTOTAL || p.TOTAL_APPR || p.APPRVAL || p.ASSESSED || "0") || null,
      property_type: p.CLASS || p.LAND_USE || p.LUC || "unknown",
      total_units: 1,
    });
    if (batch.length >= 500) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
      if (error) console.error("  Error:", error.message.slice(0, 60));
      else inserted += batch.length;
      batch = [];
      if (inserted % 5000 === 0 && inserted > 0) process.stdout.write("\r  Inserted: " + inserted.toLocaleString());
    }
  }
  if (batch.length > 0) {
    const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
    if (!error) inserted += batch.length;
  }

  // Show sample
  const { data: sample } = await db.from("properties").select("parcel_id, owner_name, address").eq("county_id", county.id).not("owner_name", "is", null).limit(3);
  console.log("\n  Done:", inserted.toLocaleString(), "properties");
  for (const s of sample || []) console.log("    " + s.parcel_id + ": " + s.owner_name + " | " + s.address);
}
main().catch(console.error);
