#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { data: mortgages } = await db.from("mortgage_records")
    .select("borrower_name, lender_name, document_type")
    .eq("source_url", "https://ava.fidlar.com/OHFairfield/AvaWeb/")
    .eq("document_type", "mortgage")
    .not("borrower_name", "is", null)
    .limit(5);
  console.log("Mortgage borrower names:");
  for (const m of mortgages || []) console.log("  ", JSON.stringify(m.borrower_name));

  const { data: props } = await db.from("properties")
    .select("owner_name, address")
    .eq("county_id", 31)
    .not("owner_name", "is", null)
    .limit(5);
  console.log("\nProperty owner names:");
  for (const p of props || []) console.log("  ", JSON.stringify(p.owner_name), "|", p.address);

  // Try exact match test
  if (mortgages?.[0]) {
    const name = mortgages[0].borrower_name.split(";")[0].trim().split(/\s+/)[0];
    console.log("\nSearching properties for:", name);
    const { data: matches } = await db.from("properties")
      .select("owner_name").eq("county_id", 31)
      .ilike("owner_name", `${name}%`).limit(3);
    console.log("Matches:", matches?.map(m => m.owner_name));
  }
}
main().catch(console.error);
