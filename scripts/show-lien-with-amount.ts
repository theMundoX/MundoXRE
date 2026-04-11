#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Find mortgage records with actual lien amounts
  const { data } = await db.from("mortgage_records")
    .select("*")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .order("loan_amount", { ascending: false })
    .limit(5);

  if (!data || data.length === 0) {
    console.log("No mortgage records with amounts yet.");
    return;
  }

  console.log("═══════════════════════════════════════════════════════════════════════════");
  console.log("  MXRE — RECORDED LIENS WITH ACTUAL AMOUNTS");
  console.log("═══════════════════════════════════════════════════════════════════════════\n");

  for (const m of data) {
    console.log(`  ─── ${m.document_type.toUpperCase()} ─────────────────────────────────────────`);
    console.log(`  Borrower:              ${m.borrower_name}`);
    console.log(`  Lender:                ${m.lender_name}`);
    console.log(`  Recording Date:        ${m.recording_date}`);
    console.log(`  Document #:            ${m.document_number}`);
    console.log(`  Book/Page:             ${m.book_page}`);
    console.log(`  Source:                ${m.source_url}`);
    console.log(`  ─── FINANCIAL DETAILS ─────────────────────────────────────────`);
    console.log(`  Lien Amount:           $${m.loan_amount?.toLocaleString()}     [source: actual]`);
    console.log(`  Interest Rate:         ${m.interest_rate}%                     [source: estimated - Freddie Mac]`);
    console.log(`  Term:                  ${m.term_months} months (${(m.term_months || 0) / 12} years)`);
    console.log(`  Est. Monthly Payment:  $${m.estimated_monthly_payment?.toLocaleString()}    [source: estimated]`);
    console.log(`  Est. Current Balance:  $${m.estimated_current_balance?.toLocaleString()}    [source: estimated]`);
    console.log(`  Balance As Of:         ${m.balance_as_of}`);
    console.log(`  Maturity Date:         ${m.maturity_date}`);
    console.log();
  }

  // Count totals
  const { count: withAmount } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("loan_amount", "is", null).gt("loan_amount", 0);
  const { count: total } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`  Total records: ${total} | With lien amounts: ${withAmount}`);
}

main().catch(console.error);
