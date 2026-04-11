#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  // Check what fields actually have data in properties
  const { data } = await db.from("properties")
    .select("*")
    .eq("county_id", 31)
    .limit(3);
  for (const p of data || []) {
    console.log(JSON.stringify(p, null, 2).slice(0, 500));
    console.log("---");
  }

  // Check mortgage records with Party data (from direct API run)
  const { data: morts } = await db.from("mortgage_records")
    .select("borrower_name, lender_name, loan_amount, source_url")
    .eq("source_url", "https://ava.fidlar.com/OHFairfield/AvaWeb/")
    .not("borrower_name", "eq", "")
    .not("borrower_name", "is", null)
    .limit(5);
  console.log("\nMortgage records WITH names:");
  for (const m of morts || []) console.log("  ", m.borrower_name?.slice(0, 60), "|", m.lender_name?.slice(0, 40), "|", m.loan_amount);
  console.log("Count with names:", morts?.length);

  // Count records with empty borrower_name
  const { count: empty } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .eq("source_url", "https://ava.fidlar.com/OHFairfield/AvaWeb/")
    .or("borrower_name.is.null,borrower_name.eq.");
  const { count: total } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .eq("source_url", "https://ava.fidlar.com/OHFairfield/AvaWeb/");
  console.log(`\nEmpty borrower names: ${empty} / ${total}`);
}
main().catch(console.error);
