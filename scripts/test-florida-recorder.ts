#!/usr/bin/env tsx
/**
 * Pull real mortgage records from a Florida county using LandmarkWeb adapter.
 * No database writes — just fetch and display.
 */

import "dotenv/config";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";

async function main() {
  // Use Levy County — confirmed working, no captcha
  const config = LANDMARK_COUNTIES.find(c => c.county_name === "Levy")!;
  console.log(`Testing LandmarkWeb recorder adapter`);
  console.log(`County: ${config.county_name}, FL`);
  console.log(`Portal: ${config.base_url}${config.path_prefix}\n`);

  const adapter = new LandmarkWebAdapter();
  console.log("Launching browser...");
  await adapter.init();

  // Pull last 3 days
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 7);

  const start = startDate.toISOString().split("T")[0];
  const end = endDate.toISOString().split("T")[0];
  console.log(`Date range: ${start} to ${end}\n`);

  const allDocs: any[] = [];
  try {
    for await (const doc of adapter.fetchDocuments(config, start, end, undefined, (p) => {
      process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Errors: ${p.errors}`);
    })) {
      allDocs.push(doc);
    }
  } finally {
    await adapter.close();
  }

  console.log(`\n\nTotal documents: ${allDocs.length}\n`);

  // Group by type
  const typeCounts = new Map<string, number>();
  for (const d of allDocs) {
    typeCounts.set(d.document_type, (typeCounts.get(d.document_type) || 0) + 1);
  }

  console.log("═══════════════════════════════════════════════════════════════");
  console.log(`  REAL RECORDED DOCUMENTS — ${config.county_name} County, FL`);
  console.log(`  ${start} to ${end}`);
  console.log("═══════════════════════════════════════════════════════════════\n");

  console.log("  Document types:");
  for (const [type, count] of [...typeCounts.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${type.padEnd(35)} ${count}`);
  }

  // Show mortgage-related records
  const mortgages = allDocs.filter(d =>
    d.document_type.includes("MORTGAGE") ||
    d.document_type.includes("DEED OF TRUST") ||
    d.document_type.includes("SATISFACTION") ||
    d.document_type.includes("ASSIGNMENT")
  );

  if (mortgages.length > 0) {
    console.log(`\n  ─── Mortgage-Related Records (${mortgages.length}) ───\n`);
    for (const r of mortgages.slice(0, 15)) {
      console.log(`  Borrower:   ${r.grantor}`);
      console.log(`  Lender:     ${r.grantee}`);
      console.log(`  Type:       ${r.document_type}`);
      console.log(`  Recorded:   ${r.recording_date}`);
      console.log(`  Doc #:      ${r.instrument_number || "N/A"}`);
      console.log(`  Book/Page:  ${r.book_page || "N/A"}`);
      if (r.consideration) console.log(`  Amount:     $${r.consideration.toLocaleString()}`);
      console.log(`  Legal:      ${(r.legal_description || "N/A").slice(0, 80)}`);
      console.log();
    }
  }

  console.log("Done.");
}

main().catch(console.error);
