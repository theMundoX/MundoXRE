#!/usr/bin/env tsx
/**
 * Estimate rents for NJ, CO, NC, NY, and IA properties that don't have rent snapshots yet.
 * Uses the hedonic pricing model in rent-estimator.ts.
 * For multifamily (total_units > 1), uses per-unit sqft = total_sqft / total_units.
 *
 * Processes one state at a time, sequentially, to avoid OOM.
 * Batch size: 500. Offset-based pagination.
 *
 * Skip strategy: for each batch of 500 properties, do a quick lookup in
 * rent_snapshots for those property_ids — no full table pre-scan needed.
 *
 * City filter is handled in code (not in SQL) to avoid statement timeouts
 * on large state tables (NJ has 3M rows). Properties with no city fall back
 * to a state-default city for MSA mapping.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 500;
const STATES = ["NJ", "CO", "NC", "NY", "IA"];

// Default city for each state (used when a property has no city value)
const STATE_DEFAULT_CITY: Record<string, string> = {
  "NJ": "NEWARK",
  "CO": "DENVER",
  "NC": "CHARLOTTE",
  "NY": "NEW YORK",
  "IA": "DES MOINES",
};

/**
 * Given a list of property IDs, return the subset that already have at least
 * one row in rent_snapshots (regardless of date).
 */
async function getPropertyIdsWithSnapshots(ids: number[]): Promise<Set<number>> {
  if (ids.length === 0) return new Set();

  const { data, error } = await db
    .from("rent_snapshots")
    .select("property_id")
    .in("property_id", ids);

  if (error) {
    // On error, return empty set (we'll attempt to insert; extra snapshots are acceptable)
    return new Set();
  }

  const result = new Set<number>();
  for (const row of data || []) {
    result.add(row.property_id);
  }
  return result;
}

async function processState(
  state: string,
): Promise<{ processed: number; inserted: number; skipped: number }> {
  let offset = 0;
  let totalProcessed = 0;
  let totalInserted = 0;
  let totalSkipped = 0;
  const observedAt = new Date().toISOString().split("T")[0];
  const defaultCity = STATE_DEFAULT_CITY[state] || "NEWARK";

  console.log(`\n  Processing ${state}...`);

  while (true) {
    // No city filters in SQL — handle null city in code to avoid timeouts on large tables
    const { data: properties, error } = await db
      .from("properties")
      .select(
        "id, city, state_code, assessed_value, year_built, total_sqft, total_units",
      )
      .eq("state_code", state)
      .gt("id", offset)
      .order("id")
      .limit(BATCH_SIZE);

    if (error) {
      console.error(`  [${state}] Query error:`, error.message);
      break;
    }
    if (!properties || properties.length === 0) break;

    // Check which of these property IDs already have a snapshot
    const propIds = properties.map((p) => p.id);
    const hasSnapshot = await getPropertyIdsWithSnapshots(propIds);
    totalSkipped += hasSnapshot.size;

    const toEstimate = properties.filter((p) => !hasSnapshot.has(p.id));

    const snapshots: Array<Record<string, unknown>> = [];

    for (const prop of toEstimate) {
      const city = ((prop.city || "").trim()) || defaultCity;

      const isMultifamily = prop.total_units && prop.total_units > 1;
      const sqft = prop.total_sqft
        ? isMultifamily
          ? Math.round(prop.total_sqft / prop.total_units)
          : prop.total_sqft
        : undefined;

      const assessedValue =
        prop.assessed_value && isMultifamily
          ? Math.round(prop.assessed_value / prop.total_units)
          : prop.assessed_value || undefined;

      const estimate = estimateRent({
        city,
        state: prop.state_code,
        sqft,
        yearBuilt: prop.year_built || undefined,
        assessedValue,
        totalUnits: prop.total_units || undefined,
      });

      if (estimate.estimated_rent > 0) {
        snapshots.push({
          property_id: prop.id,
          observed_at: observedAt,
          beds: estimate.beds,
          baths: Math.max(1, estimate.beds),
          sqft: sqft || null,
          asking_rent: estimate.estimated_rent,
          asking_psf:
            estimate.estimated_rent_psf > 0
              ? Math.round(estimate.estimated_rent_psf * 100)
              : null,
          raw: {
            method: estimate.estimation_source,
            confidence: estimate.confidence_level,
            confidence_score: estimate.confidence_score,
            fmr: estimate.fmr_rent,
            source: `estimated_v2_${state.toLowerCase()}`,
            ...(isMultifamily
              ? { sqft_per_unit: sqft, total_units: prop.total_units }
              : {}),
          },
        });
      }
    }

    // Insert batch
    if (snapshots.length > 0) {
      const { error: insertError } = await db.from("rent_snapshots").insert(snapshots);
      if (insertError) {
        console.error(`  [${state}] Insert error:`, insertError.message.slice(0, 120));
      } else {
        totalInserted += snapshots.length;
      }
    }

    totalProcessed += properties.length;
    offset = properties[properties.length - 1].id;

    if (totalProcessed % 10000 === 0) {
      console.log(
        `    [${state}] ${totalProcessed.toLocaleString()} checked | ${totalInserted.toLocaleString()} inserted | ${totalSkipped.toLocaleString()} skipped`,
      );
    }
  }

  return { processed: totalProcessed, inserted: totalInserted, skipped: totalSkipped };
}

async function main() {
  console.log("MXRE — Estimate Rents for NJ, CO, NC, NY, IA Properties\n");
  console.log(`  Started at: ${new Date().toISOString()}`);

  // Print property counts
  console.log("\n  Property counts:");
  for (const state of STATES) {
    const { count } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("state_code", state);
    console.log(`    ${state}: ${(count ?? 0).toLocaleString()} total properties`);
  }

  const results: Record<string, { processed: number; inserted: number; skipped: number }> = {};

  for (const state of STATES) {
    console.log(`\n========== ${state} ==========`);
    const result = await processState(state);
    results[state] = result;
    console.log(
      `  [${state}] DONE — checked: ${result.processed.toLocaleString()} | inserted: ${result.inserted.toLocaleString()} | skipped: ${result.skipped.toLocaleString()}`,
    );
  }

  console.log("\n========== FINAL SUMMARY ==========");
  console.log(`  Completed at: ${new Date().toISOString()}\n`);

  let grandProcessed = 0;
  let grandInserted = 0;
  let grandSkipped = 0;

  for (const state of STATES) {
    const r = results[state];
    console.log(
      `  ${state}: ${r.processed.toLocaleString()} checked | ${r.inserted.toLocaleString()} inserted | ${r.skipped.toLocaleString()} skipped`,
    );
    grandProcessed += r.processed;
    grandInserted += r.inserted;
    grandSkipped += r.skipped;
  }

  console.log(
    `\n  TOTAL: ${grandProcessed.toLocaleString()} checked | ${grandInserted.toLocaleString()} inserted | ${grandSkipped.toLocaleString()} skipped`,
  );
}

main().catch(console.error);
