#!/usr/bin/env tsx
/**
 * Rent Tracker — Ingest on-market listing data from Zillow, Redfin, and Realtor.com.
 *
 * Usage:
 *   npx tsx scripts/ingest-listings.ts --state TX --city Dallas
 *   npx tsx scripts/ingest-listings.ts --state TX --zip 75201
 *   npx tsx scripts/ingest-listings.ts --state TX --city Dallas --sources zillow,redfin
 *   npx tsx scripts/ingest-listings.ts --state TX --city Dallas --dry-run
 *   npx tsx scripts/ingest-listings.ts --state TX --city Dallas --skip-agents
 *   npx tsx scripts/ingest-listings.ts --state TX --city Dallas --max 100
 */

import "dotenv/config";
import { ingestListings, type ListingIngestOptions } from "../src/rent-tracker/ingest.js";

function parseArgs(): ListingIngestOptions {
  const args = process.argv.slice(2);
  const options: ListingIngestOptions = { state: "" };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];

    switch (arg) {
      case "--state":
        options.state = next?.toUpperCase() ?? "";
        i++;
        break;
      case "--city":
        options.city = next;
        i++;
        break;
      case "--zip":
        options.zip = next;
        i++;
        break;
      case "--sources":
        options.sources = next?.split(",").map((s) => s.trim().toLowerCase());
        i++;
        break;
      case "--dry-run":
        options.dryRun = true;
        break;
      case "--skip-agents":
        options.skipAgentLookup = true;
        break;
      case "--max":
        options.maxRecords = parseInt(next ?? "0", 10) || undefined;
        i++;
        break;
      default:
        if (!arg.startsWith("--")) {
          console.error(`Unknown argument: ${arg}`);
        }
    }
  }

  return options;
}

async function main() {
  const options = parseArgs();

  if (!options.state) {
    console.error("Error: --state is required");
    console.log("\nUsage:");
    console.log("  npx tsx scripts/ingest-listings.ts --state TX --city Dallas");
    console.log("  npx tsx scripts/ingest-listings.ts --state TX --zip 75201");
    console.log("\nOptions:");
    console.log("  --state <ST>        State code (required)");
    console.log("  --city <name>       City name");
    console.log("  --zip <code>        ZIP code");
    console.log("  --sources <list>    Comma-separated: zillow,redfin,realtor");
    console.log("  --dry-run           Don't write to database");
    console.log("  --skip-agents       Skip agent license lookups");
    console.log("  --max <n>           Max records to process");
    process.exit(1);
  }

  if (!options.city && !options.zip) {
    console.error("Error: --city or --zip is required");
    process.exit(1);
  }

  console.log("Rent Tracker — On-Market Ingestion");
  console.log("==================================");
  console.log(`State: ${options.state}`);
  console.log(`Area: ${options.city ?? options.zip}`);
  console.log(`Sources: ${options.sources?.join(", ") ?? "all"}`);
  console.log(`Dry run: ${options.dryRun ?? false}`);
  console.log(`Agent lookup: ${!options.skipAgentLookup}`);
  if (options.maxRecords) console.log(`Max records: ${options.maxRecords}`);

  const results = await ingestListings(options);

  if (results.length === 0) {
    console.log("\nNo results.");
    process.exit(1);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
