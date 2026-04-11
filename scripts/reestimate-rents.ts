#!/usr/bin/env tsx
/**
 * Re-estimate rents for TX multifamily properties with fixed per-unit logic.
 * Only updates properties where the old estimate used wrong sqft (total building instead of per-unit).
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 500;

async function main() {
  console.log("MXRE — Re-estimate Rents (Fixed Per-Unit Logic)\n");

  // Delete old bad estimates for multifamily properties
  console.log("Deleting old multifamily rent estimates...");

  let offset = 0;
  let totalProcessed = 0;
  let totalEstimated = 0;
  let totalDeleted = 0;

  while (true) {
    // Get multifamily properties with total_units > 1
    const { data: properties, error } = await db
      .from("properties")
      .select("id, address, city, state_code, zip, assessed_value, year_built, total_sqft, total_units, property_type")
      .not("city", "eq", "")
      .gt("total_units", 1)
      .not("total_sqft", "is", null)
      .gt("total_sqft", 0)
      .gt("id", offset)
      .order("id")
      .limit(BATCH_SIZE);

    if (error) { console.error("Query error:", error.message); break; }
    if (!properties || properties.length === 0) break;

    // Delete old estimates for these properties
    const ids = properties.map(p => p.id);
    const { count: deleted } = await db.from("rent_snapshots")
      .delete({ count: "exact" })
      .in("property_id", ids);
    totalDeleted += deleted || 0;

    // Create new estimates with per-unit sqft
    const snapshots: Array<Record<string, unknown>> = [];

    for (const prop of properties) {
      const perUnitSqft = Math.round(prop.total_sqft / prop.total_units);
      const perUnitValue = prop.assessed_value ? Math.round(prop.assessed_value / prop.total_units) : undefined;

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
          sqft: perUnitSqft,
          asking_rent: estimate.estimated_rent,
          asking_psf: estimate.estimated_rent_psf > 0 ? Math.round(estimate.estimated_rent_psf * 100) : null,
          raw: {
            method: estimate.estimation_source,
            confidence: estimate.confidence_level,
            confidence_score: estimate.confidence_score,
            fmr: estimate.fmr_rent,
            source: "estimated_v2_fixed",
            sqft_per_unit: perUnitSqft,
            total_units: prop.total_units,
          },
        });
        totalEstimated++;
      }
    }

    if (snapshots.length > 0) {
      const { error: insertError } = await db.from("rent_snapshots").insert(snapshots);
      if (insertError) console.error("Insert error:", insertError.message.slice(0, 80));
    }

    totalProcessed += properties.length;
    offset = properties[properties.length - 1].id;

    if (totalProcessed % 2000 === 0) {
      console.log(`  Progress: ${totalProcessed.toLocaleString()} properties | ${totalEstimated.toLocaleString()} re-estimated | ${totalDeleted.toLocaleString()} old deleted`);
    }
  }

  console.log(`\n  Done: ${totalProcessed.toLocaleString()} multifamily properties processed`);
  console.log(`  Re-estimated: ${totalEstimated.toLocaleString()}`);
  console.log(`  Old estimates deleted: ${totalDeleted.toLocaleString()}`);
}

main().catch(console.error);
