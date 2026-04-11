#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { data } = await db.from("rent_snapshots").select("*").gt("property_id", 100).limit(3);
  if (data && data.length > 0) {
    console.log("Columns:", Object.keys(data[0]).join(", "));
    for (const row of data) {
      console.log(JSON.stringify(row));
    }
  }

  // Also check a property from Dallas
  const { data: prop } = await db.from("properties").select("id, address, city, state_code, zip, owner_name, property_type, assessed_value, total_sqft, total_units").eq("state_code", "TX").eq("property_type", "apartment").not("assessed_value", "is", null).gt("total_units", 10).limit(1);
  if (prop && prop.length > 0) {
    console.log("\nSample apartment:", JSON.stringify(prop[0]));
    const pid = prop[0].id;
    const { data: rents } = await db.from("rent_snapshots").select("*").eq("property_id", pid).limit(1);
    console.log("Rent for this property:", rents && rents.length > 0 ? JSON.stringify(rents[0]) : "NONE");
  }
}

main().catch(console.error);
