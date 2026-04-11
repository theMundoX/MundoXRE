#!/usr/bin/env tsx
import "dotenv/config";
import { FidlarAvaAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava.js";

async function main() {
  console.log("=== Fidlar Search Test: TaxId vs Address ===\n");

  const config = FIDLAR_AVA_COUNTIES.find(c => c.county_name === "Fairfield" && c.state === "OH");
  if (!config) { console.log("Config not found"); return; }

  const adapter = new FidlarAvaAdapter();

  const tests = [
    { label: "TaxId raw", overrides: { TaxId: "0240211600" } },
    { label: "TaxId dashed", overrides: { TaxId: "024-0211600" } },
    { label: "Address: 121 HIGH", overrides: { AddressNumber: "121", AddressStreetName: "HIGH" } },
    { label: "LastName: RICHARDSON", overrides: { LastBusinessName: "RICHARDSON" } },
  ];

  for (const test of tests) {
    console.log(`\n--- ${test.label} ---`);
    let count = 0;
    try {
      for await (const doc of adapter.fetchDocuments(config, "01/01/2015", "03/28/2026", undefined, test.overrides)) {
        count++;
        console.log(`  ${count}. ${doc.document_type} | ${doc.recording_date} | $${doc.consideration || 0} | ${doc.grantor || "?"} -> ${doc.grantee || "?"}`);
        if (count >= 3) { console.log("  (capped)"); break; }
      }
    } catch (e: any) {
      console.log("  Error:", e.message?.substring(0, 150));
    }
    console.log(`  Results: ${count}`);
    if (count > 0) console.log("  *** WORKS ***");
  }

  await adapter.close();
}

main().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
