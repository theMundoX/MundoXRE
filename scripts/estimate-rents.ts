#!/usr/bin/env tsx
/**
 * Phase 3 (Bootstrap): Estimate rents for all properties using market data.
 *
 * Generates baseline rent estimates from:
 * - Property sqft + market $/sqft rates
 * - Assessed value / price-to-rent ratio
 * - HUD Fair Market Rents by location
 *
 * These estimates get replaced with actual advertised rents
 * once we scrape property websites.
 *
 * Usage:
 *   npx tsx scripts/estimate-rents.ts
 *   npx tsx scripts/estimate-rents.ts --state=TX
 *   npx tsx scripts/estimate-rents.ts --county=Dallas --state=TX
 */

import "dotenv/config";
import { getDb, getWriteDb } from "../src/db/client.js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const args = process.argv.slice(2);
const state = args.find((a) => a.startsWith("--state="))?.split("=")[1];
const county = args.find((a) => a.startsWith("--county="))?.split("=")[1];
const startOffset = parseInt(args.find((a) => a.startsWith("--from-id="))?.split("=")[1] || "0");
const BATCH_SIZE = 500;

async function main() {
  console.log("MXRE Phase 3: Rent Estimation (Bootstrap)");
  console.log("─".repeat(40));
  if (state) console.log(`State: ${state}`);
  if (county) console.log(`County: ${county}`);
  console.log();

  const db = getDb();
  const writeDb = getWriteDb();
  let offset = startOffset;
  let totalProcessed = 0;
  let totalEstimated = 0;

  while (true) {
    // Fetch properties that don't have rent estimates yet
    // Get properties that don't have rent estimates yet
    // by checking for IDs greater than the last processed
    let query = db
      .from("properties")
      .select("id, address, city, state_code, zip, assessed_value, year_built, total_sqft, total_units, property_type")
      .not("city", "eq", "")
      .gt("id", offset) // Use ID cursor instead of offset for resume support
      .order("id")
      .limit(BATCH_SIZE);

    if (state) query = query.eq("state_code", state);

    const { data: properties, error } = await query;
    if (error) {
      console.error("  Query error:", error.message);
      break;
    }

    if (!properties || properties.length === 0) break;

    const snapshots: Array<{
      property_id: number;
      observed_at: string;
      beds: number;
      baths: number;
      sqft: number | null;
      asking_rent: number;
      asking_psf: number | null;
      raw: Record<string, unknown>;
    }> = [];

    for (const prop of properties) {
      const isMultiUnit = prop.total_units && prop.total_units > 1;

      // For multifamily: divide building sqft by unit count to get per-unit sqft
      // For SFR/condo: total_sqft IS per-unit sqft
      let perUnitSqft: number | undefined;
      let perUnitValue: number | undefined;

      if (isMultiUnit && prop.total_units > 1) {
        perUnitSqft = prop.total_sqft ? Math.round(prop.total_sqft / prop.total_units) : undefined;
        perUnitValue = prop.assessed_value ? Math.round(prop.assessed_value / prop.total_units) : undefined;
      } else {
        perUnitSqft = prop.total_sqft || undefined;
        perUnitValue = prop.assessed_value || undefined;
      }

      const estimate = estimateRent({
        city: prop.city,
        sqft: perUnitSqft,
        yearBuilt: prop.year_built || undefined,
        assessedValue: perUnitValue,
        totalUnits: prop.total_units || undefined,
      });

      if (estimate.estimated_rent > 0) {
        snapshots.push({
          property_id: prop.id,
          observed_at: new Date().toISOString().split("T")[0],
          beds: estimate.beds,
          baths: Math.max(1, estimate.beds),
          sqft: perUnitSqft || null,
          asking_rent: estimate.estimated_rent,
          asking_psf: estimate.estimated_rent_psf > 0 ? Math.round(estimate.estimated_rent_psf * 100) : null,
          raw: {
            method: estimate.estimation_source,
            confidence: estimate.confidence_level,
            confidence_score: estimate.confidence_score,
            fmr: estimate.fmr_rent,
            source: "estimated_v2",
            sqft_per_unit: perUnitSqft,
            total_units: prop.total_units,
          },
        });
        totalEstimated++;
      }
    }

    // Batch insert rent estimates
    if (snapshots.length > 0) {
      const { error: insertError } = await writeDb
        .from("rent_snapshots")
        .insert(snapshots);

      if (insertError) {
        console.error("  Insert error:", insertError.message);
      }
    }

    totalProcessed += properties.length;
    // Use last ID as cursor for next batch
    offset = properties[properties.length - 1].id;

    if (totalProcessed % 5000 === 0) {
      console.log(`  Progress: ${totalProcessed.toLocaleString()} processed, ${totalEstimated.toLocaleString()} estimated`);
    }
  }

  console.log();
  console.log("── Summary ──");
  console.log(`Total processed: ${totalProcessed.toLocaleString()}`);
  console.log(`Rent estimates created: ${totalEstimated.toLocaleString()}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
