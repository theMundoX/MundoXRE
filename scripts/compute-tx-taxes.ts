#!/usr/bin/env tsx
/**
 * Compute annual property taxes for Texas counties using
 * county-average effective tax rates applied to appraised value.
 *
 * Texas has no state income tax but has high property taxes.
 * Formula: annual_tax = assessed_value × effective_rate
 *
 * County rates (average effective rates):
 *   Tarrant:  2.19%
 *   Denton:   2.07%
 *   Dallas:   2.18%
 *
 * Usage:
 *   npx tsx scripts/compute-tx-taxes.ts
 *   npx tsx scripts/compute-tx-taxes.ts --dry-run
 *   npx tsx scripts/compute-tx-taxes.ts --county=Tarrant
 *   npx tsx scripts/compute-tx-taxes.ts --county=Dallas --dry-run
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────────────────

const BATCH_SIZE = 500;
const TAX_YEAR = 2024;

/** County-average effective rates as fraction of appraised value */
const TX_COUNTY_RATES: Record<string, number> = {
  Tarrant: 0.0219,
  Denton: 0.0207,
  Dallas: 0.0218,
};

// ── CLI args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const countyArg = args.find((a) => a.startsWith("--county="));
const countyFilter = countyArg ? countyArg.split("=")[1] : null;

if (countyFilter && !TX_COUNTY_RATES[countyFilter]) {
  console.error(`Unknown county: ${countyFilter}`);
  console.error(`Available: ${Object.keys(TX_COUNTY_RATES).join(", ")}`);
  process.exit(1);
}

// ── DB client ───────────────────────────────────────────────────────────────

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY;
if (!url || !key) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}
const db = createClient(url, key, { auth: { persistSession: false } });

// ── Process one county ──────────────────────────────────────────────────────

async function processCounty(countyName: string, rate: number) {
  console.log(`\n  Processing: ${countyName} County, TX`);
  console.log(`  Effective rate: ${(rate * 100).toFixed(2)}%`);
  console.log("  " + "-".repeat(50));

  // Look up county ID
  const { data: county, error: countyErr } = await db
    .from("counties")
    .select("id")
    .eq("county_name", countyName)
    .eq("state_code", "TX")
    .single();

  if (countyErr || !county) {
    console.error(`  Could not find ${countyName} County TX: ${countyErr?.message}`);
    return { processed: 0, updated: 0, skipped: 0 };
  }

  let cursor = 0;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  const samples: Array<{ address: string; appraised: number; tax: number }> = [];

  while (true) {
    const { data: properties, error } = await db
      .from("properties")
      .select("id, address, assessed_value")
      .eq("county_id", county.id)
      .is("annual_tax", null)
      .not("assessed_value", "is", null)
      .gt("id", cursor)
      .order("id")
      .limit(BATCH_SIZE);

    if (error) {
      console.error("  Query error:", error.message);
      break;
    }

    if (!properties || properties.length === 0) break;

    const updates: Array<{ id: number; annual_tax: number; tax_year: number }> = [];

    for (const prop of properties) {
      const appraised = prop.assessed_value as number;

      if (!appraised || appraised <= 0) {
        totalSkipped++;
        continue;
      }

      const annualTax = Math.round(appraised * rate);

      updates.push({ id: prop.id, annual_tax: annualTax, tax_year: TAX_YEAR });

      if (samples.length < 10) {
        samples.push({
          address: prop.address || "(no address)",
          appraised,
          tax: annualTax,
        });
      }
    }

    if (!dryRun && updates.length > 0) {
      const ids = updates.map((u) => u.id);
      const taxes = updates.map((u) => u.annual_tax);

      const { error: rpcErr } = await db.rpc("batch_update_taxes", {
        p_ids: ids,
        p_taxes: taxes,
        p_tax_year: TAX_YEAR,
      });

      if (rpcErr) {
        console.error(`\n  RPC fallback for ${countyName}:`, rpcErr.message);
        for (const u of updates) {
          await db
            .from("properties")
            .update({ annual_tax: u.annual_tax, tax_year: u.tax_year, property_tax: u.annual_tax })
            .eq("id", u.id);
        }
      }
    }

    totalUpdated += updates.length;
    totalProcessed += properties.length;
    cursor = properties[properties.length - 1].id;

    process.stdout.write(
      `\r  Processed: ${totalProcessed.toLocaleString()} | Updated: ${totalUpdated.toLocaleString()} | Skipped: ${totalSkipped.toLocaleString()}`
    );
  }

  console.log(); // newline after progress

  // Print samples
  if (samples.length > 0) {
    console.log("\n  Sample results:");
    console.log("  " + "-".repeat(70));
    console.log(
      "  " +
        "Address".padEnd(35) +
        "Appraised".padStart(14) +
        "Annual Tax".padStart(14)
    );
    console.log("  " + "-".repeat(70));

    for (const s of samples) {
      const addr = s.address.length > 33 ? s.address.slice(0, 30) + "..." : s.address;
      console.log(
        "  " +
          addr.padEnd(35) +
          `$${s.appraised.toLocaleString()}`.padStart(14) +
          `$${s.tax.toLocaleString()}`.padStart(14)
      );
    }
    console.log("  " + "-".repeat(70));
  }

  console.log(`\n  ${countyName} County — processed: ${totalProcessed.toLocaleString()}, updated: ${totalUpdated.toLocaleString()}, skipped: ${totalSkipped.toLocaleString()}`);

  return { processed: totalProcessed, updated: totalUpdated, skipped: totalSkipped };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Compute TX Property Taxes");
  console.log("=".repeat(55));
  console.log(`  Tax year:    ${TAX_YEAR}`);
  console.log(`  Formula:     annual_tax = assessed_value x county_rate`);
  console.log(`  Batch size:  ${BATCH_SIZE}`);
  if (dryRun) console.log("  Mode:        DRY RUN (no writes)");
  if (countyFilter) console.log(`  County:      ${countyFilter}`);

  const counties = countyFilter
    ? { [countyFilter]: TX_COUNTY_RATES[countyFilter] }
    : TX_COUNTY_RATES;

  let grandProcessed = 0;
  let grandUpdated = 0;
  let grandSkipped = 0;

  for (const [name, rate] of Object.entries(counties)) {
    const result = await processCounty(name, rate);
    grandProcessed += result.processed;
    grandUpdated += result.updated;
    grandSkipped += result.skipped;
  }

  console.log("\n" + "=".repeat(55));
  console.log("  GRAND TOTAL");
  console.log(`  Processed: ${grandProcessed.toLocaleString()}`);
  console.log(`  Updated:   ${grandUpdated.toLocaleString()}`);
  console.log(`  Skipped:   ${grandSkipped.toLocaleString()}`);
  console.log(`  Tax year:  ${TAX_YEAR}`);
  if (dryRun) console.log("\n  DRY RUN — no changes written to database");
  console.log();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
