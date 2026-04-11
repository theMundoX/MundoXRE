#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { data } = await db.from("mortgage_records")
    .select("*")
    .eq("document_type", "mortgage")
    .limit(5);

  for (const r of data || []) {
    console.log("─────────────────────────────────");
    console.log(`Type: ${r.document_type}`);
    console.log(`Borrower: ${r.borrower_name}`);
    console.log(`Lender: ${r.lender_name}`);
    console.log(`Loan Amount: ${r.loan_amount}`);
    console.log(`Original Amount: ${r.original_amount}`);
    console.log(`Interest Rate: ${r.interest_rate}`);
    console.log(`Doc #: ${r.document_number}`);
    console.log(`Book/Page: ${r.book_page}`);
    console.log(`Source: ${r.source_url}`);
    console.log(`Raw cols:`, JSON.stringify(r.raw)?.slice(0, 300));
    console.log();
  }
}
main().catch(console.error);
