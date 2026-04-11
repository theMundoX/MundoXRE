#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  // Get some properties WITHOUT owner names
  const { data } = await db.from("properties")
    .select("id, parcel_id")
    .eq("county_id", 31)
    .is("owner_name", null)
    .limit(5);
  console.log("Properties WITHOUT owner names:");
  for (const p of data || []) console.log(`  id=${p.id} parcel_id="${p.parcel_id}"`);

  // Get some properties WITH owner names
  const { data: data2 } = await db.from("properties")
    .select("id, parcel_id, owner_name")
    .eq("county_id", 31)
    .not("owner_name", "is", null)
    .limit(5);
  console.log("\nProperties WITH owner names:");
  for (const p of data2 || []) console.log(`  id=${p.id} parcel_id="${p.parcel_id}" owner="${p.owner_name?.slice(0,30)}"`);
}
main().catch(console.error);
