#!/usr/bin/env tsx
/**
 * Compute actual annual property taxes for Fairfield County OH using
 * Ohio's official tax formula:
 *
 *   Assessed Value = Appraised Value × 35%
 *   Annual Tax = Assessed Value × (Effective Millage / 1000)
 *
 * Our DB stores appraised value (APRLAND + APRBLDG) in `assessed_value` column
 * (misnomer — it's actually the county's total appraised/market value in whole dollars).
 *
 * We use the Fairfield County average effective rate of ~1.37% of appraised value
 * which equals ~39.14 mills on assessed (35%) value.
 *
 * Usage:
 *   npx tsx scripts/compute-oh-taxes.ts
 *   npx tsx scripts/compute-oh-taxes.ts --dry-run
 *   npx tsx scripts/compute-oh-taxes.ts --limit=100
 *   npx tsx scripts/compute-oh-taxes.ts --dry-run --limit=10
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────────────────

const BATCH_SIZE = 500;
const TAX_YEAR = 2024;

/**
 * County-wide average effective rate as a fraction of appraised (market) value.
 * Source: Ohio DTE — Fairfield County average effective rate ≈ 1.37%
 * Equivalent to ~39.14 mills on the 35% assessed value.
 */
const COUNTY_AVG_EFFECTIVE_RATE = 0.0137;

// ── CLI args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const limitArg = args.find((a) => a.startsWith("--limit="));
const limit = limitArg ? parseInt(limitArg.split("=")[1], 10) : 0;

// ── DB client ───────────────────────────────────────────────────────────────

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY;
if (!url || !key) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}
const db = createClient(url, key, { auth: { persistSession: false } });

// ── Tax computation ─────────────────────────────────────────────────────────

function computeAnnualTax(appraisedValue: number): number {
  // Ohio formula: assessed = appraised × 35%, tax = assessed × (mills / 1000)
  // Simplified: tax = appraised × 0.35 × (39.14 / 1000) ≈ appraised × 0.0137
  return Math.round(appraisedValue * COUNTY_AVG_EFFECTIVE_RATE);
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Compute OH Property Taxes (Fairfield County)");
  console.log("─".repeat(55));
  console.log(`  Tax year:       ${TAX_YEAR}`);
  console.log(`  Effective rate: ${(COUNTY_AVG_EFFECTIVE_RATE * 100).toFixed(2)}% of appraised value`);
  console.log(`  Formula:        annual_tax = appraised_value × ${COUNTY_AVG_EFFECTIVE_RATE}`);
  console.log(`  Batch size:     ${BATCH_SIZE}`);
  if (dryRun) console.log("  Mode:           DRY RUN (no writes)");
  if (limit) console.log(`  Limit:          ${limit} properties`);
  console.log();

  // Get county ID
  const { data: county, error: countyErr } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Fairfield")
    .eq("state_code", "OH")
    .single();

  if (countyErr || !county) {
    console.error("Could not find Fairfield County OH:", countyErr?.message);
    process.exit(1);
  }

  let cursor = 0;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  const samples: Array<{ address: string; appraised: number; tax: number }> = [];

  while (true) {
    // Fetch batch of properties with appraised value, no computed tax yet
    let query = db
      .from("properties")
      .select("id, address, assessed_value")
      .eq("county_id", county.id)
      .not("assessed_value", "is", null)
      .gt("id", cursor)
      .order("id")
      .limit(BATCH_SIZE);

    const { data: properties, error } = await query;

    if (error) {
      console.error("  Query error:", error.message);
      break;
    }

    if (!properties || properties.length === 0) break;

    const updates: Array<{
      id: number;
      annual_tax: number;
      tax_year: number;
    }> = [];

    for (const prop of properties) {
      const appraised = prop.assessed_value as number;

      if (!appraised || appraised <= 0) {
        totalSkipped++;
        continue;
      }

      const annualTax = computeAnnualTax(appraised);

      updates.push({
        id: prop.id,
        annual_tax: annualTax,
        tax_year: TAX_YEAR,
      });

      // Collect first 10 samples for logging
      if (samples.length < 10) {
        samples.push({
          address: prop.address || "(no address)",
          appraised,
          tax: annualTax,
        });
      }
    }

    if (!dryRun && updates.length > 0) {
      // Batch update via Postgres RPC for speed
      const ids = updates.map((u) => u.id);
      const taxes = updates.map((u) => u.annual_tax);

      const { data: rowCount, error: rpcErr } = await db.rpc("batch_update_taxes", {
        p_ids: ids,
        p_taxes: taxes,
        p_tax_year: TAX_YEAR,
      });

      if (rpcErr) {
        console.error(`\n  Batch update error:`, rpcErr.message);
        // Fallback to individual updates
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

    if (limit && totalProcessed >= limit) break;
  }

  console.log("\n");

  // Print sample results
  if (samples.length > 0) {
    console.log("  Sample results:");
    console.log("  " + "─".repeat(70));
    console.log(
      "  " +
        "Address".padEnd(35) +
        "Appraised".padStart(14) +
        "Annual Tax".padStart(14)
    );
    console.log("  " + "─".repeat(70));

    for (const s of samples) {
      const addr = s.address.length > 33 ? s.address.slice(0, 30) + "..." : s.address;
      console.log(
        "  " +
          addr.padEnd(35) +
          `$${s.appraised.toLocaleString()}`.padStart(14) +
          `$${s.tax.toLocaleString()}`.padStart(14)
      );
    }
    console.log("  " + "─".repeat(70));
  }

  console.log();
  console.log(`  Total processed: ${totalProcessed.toLocaleString()}`);
  console.log(`  Total updated:   ${totalUpdated.toLocaleString()}`);
  console.log(`  Total skipped:   ${totalSkipped.toLocaleString()}`);
  console.log(`  Source:           ohio_dte_formula`);
  console.log(`  Tax year:         ${TAX_YEAR}`);
  if (dryRun) console.log("\n  ⚠ DRY RUN — no changes written to database");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
