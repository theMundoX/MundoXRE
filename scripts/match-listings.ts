#!/usr/bin/env tsx
/**
 * Rent Tracker — match unlinked listing_signals to properties by address.
 *
 * Usage:
 *   npx tsx scripts/match-listings.ts --state TX
 *   npx tsx scripts/match-listings.ts --state TX --city Dallas
 */

import "dotenv/config";
import { matchListingsToProperties } from "../src/rent-tracker/address-matcher.js";

function parseArgs(): { state: string; city?: string } {
  const args = process.argv.slice(2);
  let state = "";
  let city: string | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--state" && args[i + 1]) {
      state = args[++i].toUpperCase();
    } else if (args[i] === "--city" && args[i + 1]) {
      city = args[++i];
    }
  }

  if (!state) {
    console.error("Usage: npx tsx scripts/match-listings.ts --state TX [--city Dallas]");
    process.exit(1);
  }

  return { state, city };
}

async function main() {
  const { state, city } = parseArgs();
  const label = city ? `${city}, ${state}` : state;

  console.log(`\nMatching listing_signals to properties for ${label}...\n`);

  const start = Date.now();
  const result = await matchListingsToProperties(state, city);
  const elapsed = ((Date.now() - start) / 1000).toFixed(1);

  const total = result.matched + result.unmatched;
  const rate = total > 0 ? ((result.matched / total) * 100).toFixed(1) : "0.0";

  console.log(`\n── Results ──`);
  console.log(`  Matched:   ${result.matched}`);
  console.log(`  Unmatched: ${result.unmatched}`);
  console.log(`  Match rate: ${rate}%`);
  console.log(`  Duration:  ${elapsed}s\n`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
