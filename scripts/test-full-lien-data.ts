#!/usr/bin/env tsx
/**
 * Pull real lien data WITH amounts from Florida using updated adapter,
 * compute interest rate, monthly payment, balance, and maturity.
 */
import "dotenv/config";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

async function main() {
  // Use Levy County — confirmed working with amounts
  const config = LANDMARK_COUNTIES.find(c => c.county_name === "Levy")!;
  console.log(`County: ${config.county_name}, FL\n`);

  const adapter = new LandmarkWebAdapter();
  await adapter.init();

  const date = "2026-03-19";
  const allDocs: any[] = [];

  try {
    for await (const doc of adapter.fetchDocuments(config, date, "2026-03-20", undefined, (p) => {
      process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found}`);
    })) {
      allDocs.push(doc);
    }
  } finally {
    await adapter.close();
  }

  console.log(`\n\nTotal documents: ${allDocs.length}\n`);

  // Filter to liens with amounts
  const withAmount = allDocs.filter(d => d.consideration && d.consideration > 0);
  const mortgages = withAmount.filter(d =>
    d.document_type.includes("MORTGAGE") || d.document_type.includes("DEED OF TRUST")
  );

  console.log("═══════════════════════════════════════════════════════════════════════════");
  console.log("  REAL LIEN DATA WITH AMOUNTS — Levy County, FL");
  console.log("═══════════════════════════════════════════════════════════════════════════\n");

  console.log(`  Total documents: ${allDocs.length}`);
  console.log(`  Documents with amounts: ${withAmount.length}`);
  console.log(`  Mortgages with amounts: ${mortgages.length}\n`);

  for (const doc of mortgages) {
    // Compute full mortgage details
    const fields = computeMortgageFields({
      originalAmount: doc.consideration,
      recordingDate: doc.recording_date,
    });

    console.log(`  ─── ${doc.document_type} ───────────────────────────────────────────`);
    console.log(`  Borrower:            ${doc.grantor}`);
    console.log(`  Lender:              ${doc.grantee}`);
    console.log(`  Recording Date:      ${doc.recording_date}`);
    console.log(`  Document #:          ${doc.instrument_number}`);
    console.log(`  Book/Page:           ${doc.book_page}`);
    console.log(`  Legal:               ${(doc.legal_description || "").slice(0, 60)}`);
    console.log(`  ─── LIEN DETAILS ─────────────────────────────────────────────`);
    console.log(`  Lien Amount:         $${doc.consideration.toLocaleString()}`);
    console.log(`  Est. Interest Rate:  ${fields.interest_rate}% (Freddie Mac 30yr avg)`);
    console.log(`  Term:                ${fields.term_months} months (${fields.term_months / 12} years)`);
    console.log(`  Est. Monthly Payment:$${fields.estimated_monthly_payment.toLocaleString()}`);
    console.log(`  Est. Current Balance:$${fields.estimated_current_balance.toLocaleString()}`);
    console.log(`  Balance As Of:       ${fields.balance_as_of}`);
    console.log(`  Maturity Date:       ${fields.maturity_date}`);
    console.log();
  }

  // Also show deeds with consideration (sale prices)
  const deeds = withAmount.filter(d => d.document_type.includes("DEED") && !d.document_type.includes("TRUST"));
  if (deeds.length > 0) {
    console.log(`\n  ─── DEEDS WITH SALE PRICES ───────────────────────────────────\n`);
    for (const d of deeds.slice(0, 5)) {
      console.log(`  ${d.document_type} — $${d.consideration.toLocaleString()}`);
      console.log(`    Seller: ${d.grantor.slice(0, 40)}`);
      console.log(`    Buyer:  ${d.grantee.slice(0, 40)}`);
      console.log(`    Date:   ${d.recording_date}`);
      console.log(`    Doc #:  ${d.instrument_number}`);
      console.log();
    }
  }
}

main().catch(console.error);
