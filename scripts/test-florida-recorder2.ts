#!/usr/bin/env tsx
/**
 * Pull real mortgage records from Florida — try multiple counties and wider date range.
 */

import "dotenv/config";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";

async function main() {
  const adapter = new LandmarkWebAdapter();
  console.log("Launching browser...\n");
  await adapter.init();

  // Try wider date range — go back 60 days
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 60);
  const start = startDate.toISOString().split("T")[0];
  const end = endDate.toISOString().split("T")[0];

  // Try each confirmed-working county
  const workingCounties = LANDMARK_COUNTIES.filter(c =>
    !c.county_name.startsWith("//") && // skip commented-out ones
    ["Levy", "Martin", "Walton", "Citrus"].includes(c.county_name)
  );

  for (const config of workingCounties) {
    console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    console.log(`Testing: ${config.county_name} County, FL`);
    console.log(`Portal: ${config.base_url}${config.path_prefix}`);
    console.log(`Date range: ${start} to ${end}\n`);

    const allDocs: any[] = [];
    try {
      for await (const doc of adapter.fetchDocuments(config, start, end, undefined, (p) => {
        process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Errors: ${p.errors}`);
      })) {
        allDocs.push(doc);
        // Stop after 100 to keep it quick
        if (allDocs.length >= 100) break;
      }
    } catch (err: any) {
      console.log(`\n  Error: ${err.message.slice(0, 100)}`);
      continue;
    }

    console.log(`\n  Total: ${allDocs.length} documents`);

    if (allDocs.length > 0) {
      // Group by type
      const typeCounts = new Map<string, number>();
      for (const d of allDocs) {
        typeCounts.set(d.document_type, (typeCounts.get(d.document_type) || 0) + 1);
      }
      console.log("\n  Document types:");
      for (const [type, count] of [...typeCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10)) {
        console.log(`    ${type.padEnd(35)} ${count}`);
      }

      // Show mortgage records
      const mortgages = allDocs.filter(d =>
        d.document_type.includes("MORTGAGE") ||
        d.document_type.includes("DEED OF TRUST") ||
        d.document_type.includes("SATISFACTION") ||
        d.document_type.includes("ASSIGNMENT")
      );

      if (mortgages.length > 0) {
        console.log(`\n  ─── Mortgage Records (${mortgages.length}) ───\n`);
        for (const r of mortgages.slice(0, 5)) {
          console.log(`  Borrower:   ${r.grantor}`);
          console.log(`  Lender:     ${r.grantee}`);
          console.log(`  Type:       ${r.document_type}`);
          console.log(`  Recorded:   ${r.recording_date}`);
          console.log(`  Doc #:      ${r.instrument_number || "N/A"}`);
          console.log(`  Book/Page:  ${r.book_page || "N/A"}`);
          if (r.consideration) console.log(`  Amount:     $${r.consideration.toLocaleString()}`);
          console.log();
        }
        // Found real data — we're done
        console.log("  *** REAL MORTGAGE DATA CONFIRMED ***");
        break;
      }
    }
  }

  await adapter.close();
  console.log("\nDone.");
}

main().catch(console.error);
