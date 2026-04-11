#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`Total mortgage_records: ${count}`);

  // Check recent ones
  const { data } = await db.from("mortgage_records")
    .select("document_type, recording_date, borrower_name, lender_name, document_number, source_url, created_at")
    .order("created_at", { ascending: false })
    .limit(5);

  console.log("\nMost recent records:");
  for (const r of data || []) {
    console.log(`  ${r.document_type} | ${r.recording_date} | ${r.borrower_name?.slice(0, 30)} | ${r.source_url?.slice(0, 30)} | created: ${r.created_at}`);
  }

  // Check if any are from publicsearch (Dallas)
  const { count: dallasCount } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .like("source_url", "%publicsearch%");
  console.log(`\nRecords from publicsearch.us: ${dallasCount}`);
}
main().catch(console.error);
