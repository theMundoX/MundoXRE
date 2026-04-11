#!/usr/bin/env tsx
/**
 * Pull real mortgage records from Martin County, FL using LandmarkWeb.
 * Martin is a bigger county — should have daily recordings.
 */

import "dotenv/config";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";

async function main() {
  const config = LANDMARK_COUNTIES.find(c => c.county_name === "Martin")!;
  console.log(`County: ${config.county_name}, FL`);
  console.log(`Portal: ${config.base_url}${config.path_prefix}\n`);

  const adapter = new LandmarkWebAdapter();
  console.log("Launching browser...");
  await adapter.init();

  // Just try ONE day to be fast
  const date = "2026-03-20";
  console.log(`Searching: ${date}\n`);

  const allDocs: any[] = [];
  try {
    for await (const doc of adapter.fetchDocuments(config, date, date, undefined, (p) => {
      process.stdout.write(`\r  Found: ${p.total_found} | Errors: ${p.errors}`);
    })) {
      allDocs.push(doc);
    }
  } catch (err: any) {
    console.error(`\nError: ${err.message}`);
  } finally {
    await adapter.close();
  }

  console.log(`\n\nTotal documents for ${date}: ${allDocs.length}\n`);

  if (allDocs.length === 0) {
    console.log("No records found. The portal may not have data for this date,");
    console.log("or the adapter needs adjustment for this county's HTML structure.");
    return;
  }

  // Show results
  const typeCounts = new Map<string, number>();
  for (const d of allDocs) {
    typeCounts.set(d.document_type, (typeCounts.get(d.document_type) || 0) + 1);
  }

  console.log("═══════════════════════════════════════════════════════════════");
  console.log(`  REAL RECORDED DOCUMENTS — Martin County, FL — ${date}`);
  console.log("═══════════════════════════════════════════════════════════════\n");

  console.log("  Document types:");
  for (const [type, count] of [...typeCounts.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${type.padEnd(35)} ${count}`);
  }

  const mortgages = allDocs.filter(d =>
    d.document_type.includes("MORTGAGE") ||
    d.document_type.includes("DEED OF TRUST") ||
    d.document_type.includes("SATISFACTION") ||
    d.document_type.includes("ASSIGNMENT")
  );

  if (mortgages.length > 0) {
    console.log(`\n  ─── Mortgage Records (${mortgages.length}) ───\n`);
    for (const r of mortgages.slice(0, 10)) {
      console.log(`  Borrower:   ${r.grantor}`);
      console.log(`  Lender:     ${r.grantee}`);
      console.log(`  Type:       ${r.document_type}`);
      console.log(`  Recorded:   ${r.recording_date}`);
      console.log(`  Doc #:      ${r.instrument_number || "N/A"}`);
      console.log(`  Book/Page:  ${r.book_page || "N/A"}`);
      if (r.consideration) console.log(`  Amount:     $${r.consideration.toLocaleString()}`);
      console.log();
    }
  }
}

main().catch(console.error);
