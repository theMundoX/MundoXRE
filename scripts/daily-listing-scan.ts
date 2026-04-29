#!/usr/bin/env tsx
/**
 * Rent Tracker — Daily Scan
 * Ingest on-market listings for all cities in a state,
 * then match them to properties.
 *
 * Usage:
 *   npx tsx scripts/daily-listing-scan.ts --state TX
 *   npx tsx scripts/daily-listing-scan.ts --state TX --cities "Dallas,Fort Worth"
 */

import "dotenv/config";
import { ingestListings } from "../src/rent-tracker/ingest.js";
import { matchListingsToProperties } from "../src/rent-tracker/address-matcher.js";

// ─── Default City Lists ─────────────────────────────────────────────

const DEFAULT_CITIES: Record<string, string[]> = {
  TX: ["Dallas", "Fort Worth", "Houston", "San Antonio", "Austin"],
  OK: ["Oklahoma City", "Tulsa", "Lawton"],
  FL: ["Miami", "Orlando", "Tampa", "Jacksonville"],
  IL: ["Chicago"],
};

// ─── Arg Parsing ────────────────────────────────────────────────────

function parseArgs(): { state: string; cities: string[] } {
  const args = process.argv.slice(2);
  let state = "";
  let citiesRaw = "";

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--state" && args[i + 1]) {
      state = args[++i].toUpperCase();
    } else if (args[i] === "--cities" && args[i + 1]) {
      citiesRaw = args[++i];
    }
  }

  if (!state) {
    console.error("Usage: npx tsx scripts/daily-listing-scan.ts --state TX [--cities \"Dallas,Fort Worth\"]");
    process.exit(1);
  }

  let cities: string[];
  if (citiesRaw) {
    cities = citiesRaw.split(",").map((c) => c.trim()).filter(Boolean);
  } else {
    cities = DEFAULT_CITIES[state] ?? [];
    if (cities.length === 0) {
      console.error(`No default cities for state ${state}. Use --cities to specify.`);
      process.exit(1);
    }
  }

  return { state, cities };
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  const { state, cities } = parseArgs();
  const start = Date.now();

  console.log(`\n===== Rent Tracker — Daily Scan: ${state} =====`);
  console.log(`Cities: ${cities.join(", ")}\n`);

  // Phase 1: Ingest listings for each city
  let totalFound = 0;
  let totalUpserted = 0;
  let totalDelisted = 0;
  let totalErrors = 0;

  for (const city of cities) {
    console.log(`\n── Ingesting: ${city}, ${state} ──`);
    try {
      const results = await ingestListings({ state, city });
      for (const r of results) {
        totalFound += r.listings_found;
        totalUpserted += r.listings_upserted;
        totalDelisted += r.delisted;
        totalErrors += r.errors;
      }
    } catch (err) {
      console.error(`  Failed to ingest ${city}: ${err instanceof Error ? err.message : "Unknown"}`);
      totalErrors++;
    }
  }

  // Phase 2: Match listings to properties
  console.log(`\n── Matching listings to properties for ${state} ──`);
  let matchResult = { matched: 0, unmatched: 0 };
  try {
    for (const city of cities) {
      const cityMatch = await matchListingsToProperties(state, city);
      matchResult.matched += cityMatch.matched;
      matchResult.unmatched += cityMatch.unmatched;
    }
  } catch (err) {
    console.error(`  Matching failed: ${err instanceof Error ? err.message : "Unknown"}`);
    totalErrors++;
  }

  // Phase 3: Summary
  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  const total = matchResult.matched + matchResult.unmatched;
  const matchRate = total > 0 ? ((matchResult.matched / total) * 100).toFixed(1) : "N/A";

  console.log(`\n===== Rent Tracker — Scan Complete =====`);
  console.log(`  State:        ${state}`);
  console.log(`  Cities:       ${cities.length}`);
  console.log(`  Found:        ${totalFound} listings`);
  console.log(`  Upserted:     ${totalUpserted} listings`);
  console.log(`  Delisted:     ${totalDelisted}`);
  console.log(`  Matched:      ${matchResult.matched} to properties`);
  console.log(`  Unmatched:    ${matchResult.unmatched}`);
  console.log(`  Match rate:   ${matchRate}%`);
  console.log(`  Errors:       ${totalErrors}`);
  console.log(`  Duration:     ${elapsed}s\n`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
