#!/usr/bin/env tsx
/**
 * MXRE — Compute annual property taxes for ALL counties across ALL states.
 *
 * Uses state-level average effective property tax rates (2024) applied to
 * assessed_value to compute: annual_tax = Math.round(assessed_value * rate)
 *
 * Processes state-by-state, updates in batches of 1000.
 *
 * Usage:
 *   npx tsx scripts/compute-all-taxes.ts
 *   npx tsx scripts/compute-all-taxes.ts --dry-run
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────────────────

const BATCH_SIZE = 1000;
const TAX_YEAR = 2024;

/** State average effective property tax rates (2024) as fractions */
const STATE_RATES: Record<string, number> = {
  OH: 0.0137,
  TX: 0.0218,
  FL: 0.0086,
  IL: 0.0207,
  NJ: 0.0223,
  PA: 0.0135,
  NC: 0.0073,
  AR: 0.0062,
  IA: 0.0150,
  MI: 0.0131,
  NH: 0.0186,
  WA: 0.0087,
  IN: 0.0081,
  WI: 0.0161,
  MN: 0.0102,
  CO: 0.0051,
  MD: 0.0099,
  NY: 0.0140,
  OR: 0.0087,
  ME: 0.0109,
};

// ── CLI args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");

// ── DB client ───────────────────────────────────────────────────────────────

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY;
if (!url || !key) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}
const db = createClient(url, key, { auth: { persistSession: false } });

// ── Process one state ───────────────────────────────────────────────────────

async function processCounty(
  stateCode: string,
  countyId: number,
  countyName: string,
  rate: number
): Promise<number> {
  let updated = 0;
  let cursor = 0;

  while (true) {
    const { data: properties, error } = await db
      .from("properties")
      .select("id, assessed_value")
      .eq("county_id", countyId)
      .is("annual_tax", null)
      .not("assessed_value", "is", null)
      .gt("assessed_value", 0)
      .gt("id", cursor)
      .order("id")
      .limit(BATCH_SIZE);

    if (error) {
      console.error(`  ${stateCode}/${countyName}: query error — ${error.message}`);
      break;
    }

    if (!properties || properties.length === 0) break;

    const updates = properties.map((p) => ({
      id: p.id,
      annual_tax: Math.round((p.assessed_value as number) * rate),
      tax_year: TAX_YEAR,
    }));

    if (!dryRun && updates.length > 0) {
      const ids = updates.map((u) => u.id);
      const taxes = updates.map((u) => u.annual_tax);

      const { error: rpcErr } = await db.rpc("batch_update_taxes", {
        p_ids: ids,
        p_taxes: taxes,
        p_tax_year: TAX_YEAR,
      });

      if (rpcErr) {
        // Fallback to individual updates
        for (const u of updates) {
          await db
            .from("properties")
            .update({
              annual_tax: u.annual_tax,
              tax_year: u.tax_year,
              property_tax: u.annual_tax,
            })
            .eq("id", u.id);
        }
      }
    }

    updated += updates.length;
    cursor = properties[properties.length - 1].id;
  }

  return updated;
}

async function processState(stateCode: string, rate: number): Promise<number> {
  // Get all county IDs for this state
  const { data: counties, error: cErr } = await db
    .from("counties")
    .select("id, county_name")
    .eq("state_code", stateCode);

  if (cErr || !counties || counties.length === 0) {
    console.log(`  ${stateCode}: no counties found, skipping`);
    return 0;
  }

  let totalUpdated = 0;

  for (const county of counties) {
    const countyUpdated = await processCounty(stateCode, county.id, county.county_name, rate);
    totalUpdated += countyUpdated;

    if (countyUpdated > 0) {
      process.stdout.write(
        `\r  ${stateCode}: ${totalUpdated.toLocaleString()} properties so far...`
      );
    }
  }

  return totalUpdated;
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Compute Property Taxes (All States)");
  console.log("=".repeat(55));
  console.log(`  Tax year:    ${TAX_YEAR}`);
  console.log(`  Formula:     annual_tax = assessed_value x state_rate`);
  console.log(`  Batch size:  ${BATCH_SIZE}`);
  console.log(`  States:      ${Object.keys(STATE_RATES).length}`);
  if (dryRun) console.log("  Mode:        DRY RUN (no writes)");
  console.log();

  const results: Array<{ state: string; updated: number; rate: number }> = [];
  let grandTotal = 0;

  for (const [state, rate] of Object.entries(STATE_RATES)) {
    const updated = await processState(state, rate);
    // Clear progress line and print final count
    process.stdout.write("\r" + " ".repeat(60) + "\r");
    if (updated > 0) {
      console.log(`  ${state}: ${updated.toLocaleString()} properties updated (rate: ${(rate * 100).toFixed(2)}%)`);
    } else {
      console.log(`  ${state}: 0 properties to update`);
    }
    results.push({ state, updated, rate });
    grandTotal += updated;
  }

  console.log();
  console.log("=".repeat(55));
  console.log("  SUMMARY");
  console.log("-".repeat(55));
  for (const r of results.filter((r) => r.updated > 0)) {
    console.log(`  ${r.state}: ${r.updated.toLocaleString()} updated @ ${(r.rate * 100).toFixed(2)}%`);
  }
  console.log("-".repeat(55));
  console.log(`  TOTAL: ${grandTotal.toLocaleString()} properties updated`);
  console.log(`  Tax year: ${TAX_YEAR}`);
  if (dryRun) console.log("\n  DRY RUN — no changes written to database");
  console.log();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
