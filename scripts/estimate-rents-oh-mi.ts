#!/usr/bin/env tsx
/**
 * Estimate rents for OH and MI properties that don't have rent snapshots yet.
 * Uses the hedonic pricing model in rent-estimator.ts.
 * For multifamily (total_units > 1), uses per-unit sqft = total_sqft / total_units.
 *
 * Approach: First collect all property IDs that already have snapshots (for OH/MI),
 * then iterate properties skipping those.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 500;
const STATES = ["OH", "MI"];

async function collectExistingSnapshotIds(): Promise<Set<number>> {
  console.log("  Collecting property IDs that already have rent snapshots...");
  const ids = new Set<number>();
  let offset = 0;

  // Get all property IDs in OH/MI that have snapshots via a join approach
  // We'll paginate through rent_snapshots joined with properties
  while (true) {
    const { data, error } = await db
      .from("rent_snapshots")
      .select("property_id, properties!inner(state_code)")
      .in("properties.state_code", STATES)
      .gt("property_id", offset)
      .order("property_id")
      .limit(1000);

    if (error) {
      // If the join doesn't work, fall back to just getting all snapshot property_ids
      console.log("  Join approach failed, using simpler approach...");
      return await collectExistingSimple();
    }
    if (!data || data.length === 0) break;

    for (const row of data) {
      ids.add(row.property_id);
    }
    offset = data[data.length - 1].property_id;
  }

  console.log(`  Found ${ids.size.toLocaleString()} properties with existing snapshots`);
  return ids;
}

async function collectExistingSimple(): Promise<Set<number>> {
  // Simpler: just get all property_ids from rent_snapshots (may include non-OH/MI but that's fine)
  const ids = new Set<number>();
  let offset = 0;

  while (true) {
    const { data, error } = await db
      .from("rent_snapshots")
      .select("property_id")
      .gt("property_id", offset)
      .order("property_id")
      .limit(5000);

    if (error) {
      console.error("  Error fetching existing snapshots:", error.message);
      break;
    }
    if (!data || data.length === 0) break;

    for (const row of data) {
      ids.add(row.property_id);
    }
    offset = data[data.length - 1].property_id;

    if (ids.size % 50000 === 0) {
      console.log(`    ... ${ids.size.toLocaleString()} snapshot IDs loaded`);
    }
  }

  console.log(`  Loaded ${ids.size.toLocaleString()} existing snapshot property IDs`);
  return ids;
}

async function main() {
  console.log("MXRE — Estimate Rents for OH & MI Properties\n");

  // Counts
  for (const state of STATES) {
    const { count } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("state_code", state);
    console.log(`  ${state}: ${(count ?? 0).toLocaleString()} total properties`);
  }

  // Collect existing snapshot IDs to skip
  const hasSnapshot = await collectExistingSimple();

  let offset = 0;
  let totalProcessed = 0;
  let totalEstimated = 0;
  let totalSkipped = 0;

  for (const state of STATES) {
    console.log(`\n  Processing ${state}...`);
    offset = 0;

    while (true) {
      const { data: properties, error } = await db
        .from("properties")
        .select("id, address, city, state_code, zip, assessed_value, year_built, total_sqft, total_units, property_type")
        .eq("state_code", state)
        .not("city", "is", null)
        .not("city", "eq", "")
        .gt("id", offset)
        .order("id")
        .limit(BATCH_SIZE);

      if (error) {
        console.error(`  Query error (${state}):`, error.message);
        break;
      }
      if (!properties || properties.length === 0) break;

      // Filter to only properties without snapshots
      const toEstimate = properties.filter((p) => !hasSnapshot.has(p.id));
      totalSkipped += properties.length - toEstimate.length;

      const snapshots: Array<Record<string, unknown>> = [];

      for (const prop of toEstimate) {
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
          city: prop.city,
          state: prop.state_code,
          sqft,
          yearBuilt: prop.year_built || undefined,
          assessedValue,
          totalUnits: prop.total_units || undefined,
        });

        if (estimate.estimated_rent > 0) {
          snapshots.push({
            property_id: prop.id,
            observed_at: new Date().toISOString().split("T")[0],
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
              source: "estimated_v2_oh_mi",
              ...(isMultifamily
                ? { sqft_per_unit: sqft, total_units: prop.total_units }
                : {}),
            },
          });
          totalEstimated++;
        }
      }

      // Insert batch
      if (snapshots.length > 0) {
        const { error: insertError } = await db.from("rent_snapshots").insert(snapshots);
        if (insertError) {
          console.error("  Insert error:", insertError.message.slice(0, 120));
        }
      }

      totalProcessed += properties.length;
      offset = properties[properties.length - 1].id;

      if (totalProcessed % 5000 === 0) {
        console.log(
          `    ${state}: ${totalProcessed.toLocaleString()} checked | ${totalEstimated.toLocaleString()} estimated | ${totalSkipped.toLocaleString()} skipped`,
        );
      }
    }
  }

  console.log(`\n  Done.`);
  console.log(`  Properties checked: ${totalProcessed.toLocaleString()}`);
  console.log(`  New estimates inserted: ${totalEstimated.toLocaleString()}`);
  console.log(`  Already had snapshots: ${totalSkipped.toLocaleString()}`);
}

main().catch(console.error);
