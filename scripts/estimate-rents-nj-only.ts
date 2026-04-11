#!/usr/bin/env tsx
/**
 * Estimate rents for NJ properties only.
 * Separate from the CO/NC/NY/IA run because NJ (3M rows) requires
 * simpler queries (no city filters in the DB query — handle null city in code).
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 500;

async function getPropertyIdsWithSnapshots(ids: number[]): Promise<Set<number>> {
  if (ids.length === 0) return new Set();
  const { data, error } = await db
    .from("rent_snapshots")
    .select("property_id")
    .in("property_id", ids);
  if (error) return new Set();
  const result = new Set<number>();
  for (const row of data || []) result.add(row.property_id);
  return result;
}

async function main() {
  console.log("MXRE — Estimate Rents for NJ Properties\n");
  console.log(`  Started at: ${new Date().toISOString()}`);

  const { count } = await db
    .from("properties")
    .select("id", { count: "exact", head: true })
    .eq("state_code", "NJ");
  console.log(`  NJ: ${(count ?? 0).toLocaleString()} total properties\n`);

  let offset = 0;
  let totalProcessed = 0;
  let totalInserted = 0;
  let totalSkipped = 0;
  const observedAt = new Date().toISOString().split("T")[0];

  while (true) {
    // No city filters — handle null city in code to avoid timeout on 3M-row table
    const { data: properties, error } = await db
      .from("properties")
      .select(
        "id, city, state_code, assessed_value, year_built, total_sqft, total_units",
      )
      .eq("state_code", "NJ")
      .gt("id", offset)
      .order("id")
      .limit(BATCH_SIZE);

    if (error) {
      console.error("  Query error:", error.message);
      break;
    }
    if (!properties || properties.length === 0) break;

    const propIds = properties.map((p) => p.id);
    const hasSnapshot = await getPropertyIdsWithSnapshots(propIds);
    totalSkipped += hasSnapshot.size;

    const toEstimate = properties.filter((p) => !hasSnapshot.has(p.id));
    const snapshots: Array<Record<string, unknown>> = [];

    for (const prop of toEstimate) {
      const city = (prop.city || "").trim();
      // Fall back to a NJ statewide average if city is unknown
      const cityForEstimate = city || "NEWARK"; // Newark is the NJ default MSA city

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
        city: cityForEstimate,
        state: "NJ",
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
            source: "estimated_v2_nj",
            city_used: cityForEstimate,
            ...(isMultifamily
              ? { sqft_per_unit: sqft, total_units: prop.total_units }
              : {}),
          },
        });
      }
    }

    if (snapshots.length > 0) {
      const { error: insertError } = await db.from("rent_snapshots").insert(snapshots);
      if (insertError) {
        console.error("  Insert error:", insertError.message.slice(0, 120));
      } else {
        totalInserted += snapshots.length;
      }
    }

    totalProcessed += properties.length;
    offset = properties[properties.length - 1].id;

    if (totalProcessed % 10000 === 0) {
      console.log(
        `    [NJ] ${totalProcessed.toLocaleString()} checked | ${totalInserted.toLocaleString()} inserted | ${totalSkipped.toLocaleString()} skipped`,
      );
    }
  }

  console.log(`\n  Completed at: ${new Date().toISOString()}`);
  console.log(`  NJ: ${totalProcessed.toLocaleString()} checked | ${totalInserted.toLocaleString()} inserted | ${totalSkipped.toLocaleString()} skipped`);
}

main().catch(console.error);
