#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { data, error } = await db.from("counties").select("id, county_name, state_code").eq("active", true);
  if (error) { console.log("Error:", error.message); return; }
  console.log("All active counties:");
  for (const c of data || []) console.log(`  id=${c.id} ${c.county_name}, ${c.state_code}`);
}
main().catch(console.error);
