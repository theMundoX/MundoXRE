#!/usr/bin/env tsx
/**
 * Seed the MXRE database with Oklahoma county records.
 *
 * Usage:
 *   npm run seed
 *
 * This creates the three Oklahoma counties we're tracking.
 * Property data ingestion happens in Phase 2 (assessor scraping).
 */

import "dotenv/config";
import { insertCounty, getCounties } from "../src/db/queries.js";

const OKLAHOMA_COUNTIES = [
  {
    state_fips: "40",
    county_fips: "109",
    state_code: "OK",
    county_name: "Oklahoma",
    assessor_url: "https://assessor.oklahomacounty.org",
    recorder_url: "https://countyclerk.oklahomacounty.org",
  },
  {
    state_fips: "40",
    county_fips: "143",
    state_code: "OK",
    county_name: "Tulsa",
    assessor_url: "https://assessor.tulsacounty.org",
    recorder_url: "https://www.tulsacounty.org/Elected-Offices/County-Clerk",
  },
  {
    state_fips: "40",
    county_fips: "031",
    state_code: "OK",
    county_name: "Comanche",
    assessor_url: "https://www.comanchecounty.us",
    recorder_url: "https://www.comanchecounty.us",
  },
];

async function main() {
  console.log("Seeding Oklahoma counties...\n");

  for (const county of OKLAHOMA_COUNTIES) {
    try {
      const result = await insertCounty(county);
      console.log(`  + ${county.county_name} County, ${county.state_code} (id: ${result.id})`);
    } catch (err) {
      console.error(
        `  ! Failed to insert ${county.county_name}: ${err instanceof Error ? err.message : err}`,
      );
    }
  }

  console.log("\nVerifying...");
  const counties = await getCounties();
  console.log(`  ${counties.length} active counties in database.`);

  for (const c of counties) {
    console.log(`  - ${c.county_name} County, ${c.state_code}`);
  }

  console.log("\nDone.");
}

main().catch(() => {
  console.error("Seed failed. Check your .env configuration.");
  process.exit(1);
});
