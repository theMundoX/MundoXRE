#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  console.log("MXRE — Ingest Paulding County OH\n");
  let { data: county } = await db.from("counties").select("id").eq("county_name", "Paulding").eq("state_code", "OH").single();
  if (!county) {
    const { data: nc, error } = await db.from("counties").insert({ county_name: "Paulding", state_code: "OH", state_fips: "39", county_fips: "125", active: true }).select("id").single();
    if (error) {
      const { data: retry } = await db.from("counties").select("id").eq("county_name", "Paulding").eq("state_code", "OH").single();
      county = retry;
    } else county = nc;
  }
  if (!county) { console.log("No county"); return; }
  console.log("  County ID:", county.id);

  const source = await shapefile.open("data/paulding-oh-parcels/Parcels.shp");
  let inserted = 0, batch: Array<Record<string, unknown>> = [];
  while (true) {
    const r = await source.read();
    if (r.done) break;
    const p = r.value.properties || {};
    const addr = [p.mlocStrNo, p.mlocStrDir, p.mlocStrNam, p.mlocStrSuf].filter(Boolean).join(" ");
    batch.push({
      county_id: county.id, parcel_id: p.mpropertyN || p.PIN_no_das || "",
      owner_name: p.OwnName || p.DeededOwne || null, address: addr, city: p.mlocCity || "",
      state_code: "OH", zip: p.mlocZipCod || "", assessed_value: parseFloat(p.MKT_Total_ || "0") || null,
      property_type: p.mClassific || "unknown", total_units: 1,
    });
    if (batch.length >= 500) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
      if (error) console.error("  Error:", error.message.slice(0, 60));
      else inserted += batch.length;
      batch = [];
      if (inserted % 5000 === 0 && inserted > 0) process.stdout.write("\r  Inserted: " + inserted.toLocaleString());
    }
  }
  if (batch.length > 0) { const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" }); if (!error) inserted += batch.length; }
  console.log("\n  Done:", inserted.toLocaleString(), "properties");
}
main().catch(console.error);
