#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { data } = await db.from("mortgage_records")
    .select("id, property_id, borrower_name, source_url")
    .like("source_url", "%publicsearch%")
    .limit(5);
  console.log("PublicSearch records:");
  for (const r of data || []) {
    console.log(`  ${r.property_id ? "LINKED" : "UNLINKED"} | ${r.borrower_name?.slice(0,40)} | ${r.source_url}`);
  }

  // Also check how many Dallas records are linked
  const { data: dallas } = await db.from("mortgage_records")
    .select("id, property_id")
    .like("source_url", "%dallas%")
    .not("property_id", "is", null)
    .limit(1);
  console.log("\nDallas linked records exist:", (dallas?.length || 0) > 0);

  // Check total linked count
  const { count } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .not("property_id", "is", null);
  console.log("Total linked:", count);
}
main().catch(console.error);
