#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { count: total } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", 31);
  const { count: withOwner } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", 31).not("owner_name", "is", null);
  const { count: withAddr } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", 31).neq("address", "");
  const { count: withValue } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", 31).not("assessed_value", "is", null);
  console.log(`Fairfield OH: ${total} total | ${withOwner} with owner | ${withAddr} with address | ${withValue} with value`);

  // Sample
  const { data } = await db.from("properties").select("parcel_id, owner_name, address, city, assessed_value").eq("county_id", 31).not("owner_name", "is", null).limit(3);
  for (const p of data || []) console.log(`  ${p.parcel_id}: ${p.owner_name} | ${p.address}, ${p.city} | $${p.assessed_value}`);
}
main().catch(console.error);
