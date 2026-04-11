#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { data } = await db.from("counties").select("county_name, state_code").eq("active", true);
  const states = [...new Set(data?.map(c => c.state_code))].sort();
  console.log("States with properties:", states.join(", "));
  console.log("Total active counties:", data?.length);
  // Count by state
  const byState: Record<string, number> = {};
  for (const c of data || []) { byState[c.state_code] = (byState[c.state_code] || 0) + 1; }
  for (const [s, n] of Object.entries(byState).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${s}: ${n} counties`);
  }
}
main().catch(console.error);
