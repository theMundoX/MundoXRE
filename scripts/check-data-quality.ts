#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  // Check rent_snapshots linking
  const { data: sampleRent } = await db.from("rent_snapshots").select("*").limit(3);
  console.log("Sample rent_snapshots:");
  for (const r of sampleRent || []) {
    console.log(`  property_id=${r.property_id}, rent=$${r.monthly_rent}, source=${r.source}, date=${r.snapshot_date}`);
  }

  // Check if any rent snapshots have valid property links
  const { count: linkedRents } = await db.from("rent_snapshots").select("*", { count: "exact", head: true }).not("property_id", "is", null).gt("property_id", 0);
  console.log(`\nRent snapshots with property_id > 0: ${linkedRents}`);

  // Check property ID range
  const { data: minMax } = await db.from("properties").select("id").order("id", { ascending: true }).limit(1);
  const { data: maxP } = await db.from("properties").select("id").order("id", { ascending: false }).limit(1);
  console.log(`Property ID range: ${minMax?.[0]?.id} to ${maxP?.[0]?.id}`);

  const { data: rentMinMax } = await db.from("rent_snapshots").select("property_id").order("property_id", { ascending: true }).limit(1);
  const { data: rentMax } = await db.from("rent_snapshots").select("property_id").order("property_id", { ascending: false }).limit(1);
  console.log(`Rent snapshot property_id range: ${rentMinMax?.[0]?.property_id} to ${rentMax?.[0]?.property_id}`);

  // Try to find a property with rent data by joining
  const { data: withRent } = await db.rpc("", {}).limit(1); // Can't do joins easily

  // Alternative: get a property_id from rent_snapshots and look it up
  const { data: rentSample } = await db.from("rent_snapshots").select("property_id, monthly_rent").not("property_id", "is", null).gt("monthly_rent", 500).limit(1);
  if (rentSample && rentSample.length > 0) {
    const pid = rentSample[0].property_id;
    console.log(`\nLooking up property ${pid}...`);
    const { data: prop } = await db.from("properties").select("*").eq("id", pid).single();
    if (prop) {
      console.log(`  Found: ${prop.address}, ${prop.city}, ${prop.state_code} ${prop.zip}`);
      console.log(`  Owner: ${prop.owner_name}`);
      console.log(`  Type: ${prop.property_type}`);
      console.log(`  Value: $${prop.assessed_value?.toLocaleString()}`);
      console.log(`  Rent: $${rentSample[0].monthly_rent}`);
    } else {
      console.log(`  Property ${pid} not found in properties table!`);
    }
  }
}

main().catch(console.error);
