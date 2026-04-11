#!/usr/bin/env tsx
/**
 * Quick diagnostic: test each new LandmarkWeb county with a single date
 * to see which ones return data.
 */
import "dotenv/config";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";

const alreadyDone = ["Levy", "Martin", "Walton", "Citrus"];

async function main() {
  const adapter = new LandmarkWebAdapter();
  await adapter.init();

  // Test date: 2026-03-01 (a Monday, should have filings)
  const testDate = "2026-03-03";

  const newCounties = LANDMARK_COUNTIES.filter(c => !alreadyDone.includes(c.county_name));

  for (const config of newCounties) {
    console.log(`\nTesting ${config.county_name} County...`);
    console.log(`  URL: ${config.base_url}${config.path_prefix}`);

    let count = 0;
    let sampleDoc: any = null;

    try {
      const timeout = setTimeout(() => {
        console.log(`  TIMEOUT after 60s — skipping ${config.county_name}`);
      }, 60_000);

      for await (const doc of adapter.fetchDocuments(config, testDate, testDate)) {
        count++;
        if (!sampleDoc) sampleDoc = doc;
        if (count >= 3) break; // Just need to see if it returns anything
      }
      clearTimeout(timeout);

      if (count > 0) {
        console.log(`  SUCCESS: ${count}+ documents found`);
        console.log(`  Sample: ${sampleDoc.document_type} | ${sampleDoc.grantor} -> ${sampleDoc.grantee} | $${sampleDoc.consideration ?? 'N/A'}`);
      } else {
        console.log(`  EMPTY: 0 documents returned (portal may not work)`);
      }
    } catch (err: any) {
      console.log(`  ERROR: ${err.message.slice(0, 100)}`);
    }
  }

  await adapter.close();
  console.log("\nDone.");
}

main().catch(console.error);
