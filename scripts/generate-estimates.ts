#!/usr/bin/env tsx
/**
 * Generate rent estimates and mortgage records for all properties
 * that don't have them yet.
 *
 * Usage:
 *   npx tsx scripts/generate-estimates.ts --county=Comanche --state=OK
 *   npx tsx scripts/generate-estimates.ts --state=OK
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

const stateFilter = getArg("state");
const countyFilter = getArg("county");
const batchSize = parseInt(getArg("batch") || "500", 10);

if (!stateFilter) {
  console.log("Usage: npx tsx scripts/generate-estimates.ts --state=OK [--county=Comanche]");
  process.exit(1);
}

// ─── Database ──────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  console.log(`\nMXRE Estimate Generator`);
  console.log(`${"─".repeat(50)}`);
  console.log(`DB: ${SUPABASE_URL}`);
  console.log(`State: ${stateFilter}${countyFilter ? `, County: ${countyFilter}` : ""}`);
  console.log();

  // Get county IDs
  let countyQuery = db.from("counties").select("id, county_name, state_code").eq("state_code", stateFilter);
  if (countyFilter) countyQuery = countyQuery.eq("county_name", countyFilter);
  const { data: counties, error: cErr } = await countyQuery;
  if (cErr || !counties?.length) {
    console.error("No counties found:", cErr?.message);
    process.exit(1);
  }

  const countyIds = counties.map((c) => c.id);
  console.log(`Counties: ${counties.map((c) => `${c.county_name} (${c.id})`).join(", ")}`);

  // ─── Rent Estimates ──────────────────────────────────────────────
  console.log(`\n── Generating Rent Estimates ──`);

  // Find properties without rent snapshots
  let offset = 0;
  let totalRentEstimates = 0;
  const today = new Date().toISOString().split("T")[0];

  while (true) {
    const { data: properties, error: pErr } = await db
      .from("properties")
      .select("id, city, state_code, zip, total_sqft, year_built, assessed_value, total_units, property_type")
      .in("county_id", countyIds)
      .range(offset, offset + batchSize - 1);

    if (pErr) {
      console.error(`  Query error at offset ${offset}: ${pErr.message}`);
      break;
    }
    if (!properties || properties.length === 0) break;

    // Check which already have rent snapshots
    const propIds = properties.map((p) => p.id);
    const { data: existingSnaps } = await db
      .from("rent_snapshots")
      .select("property_id")
      .in("property_id", propIds);

    const hasSnap = new Set((existingSnaps ?? []).map((s) => s.property_id));
    const needSnap = properties.filter((p) => !hasSnap.has(p.id));

    if (needSnap.length > 0) {
      const snapshots = needSnap.map((p) => {
        const est = estimateRent({
          city: p.city || "",
          state: p.state_code,
          zip: p.zip,
          sqft: p.total_sqft || undefined,
          yearBuilt: p.year_built || undefined,
          assessedValue: p.assessed_value || undefined,
          totalUnits: p.total_units || undefined,
          propertyType: p.property_type || undefined,
        });

        return {
          property_id: p.id,
          observed_at: today,
          beds: est.beds,
          asking_rent: est.estimated_rent,
          asking_psf: est.estimated_rent_psf ? Math.round(est.estimated_rent_psf * 100) : null,
          raw: {
            fmr: est.fmr_rent,
            method: est.estimation_source,
            source: "estimated",
            confidence: est.confidence_level,
            confidence_score: est.confidence_score,
          },
        };
      });

      const { error: insertErr } = await db.from("rent_snapshots").insert(snapshots);
      if (insertErr) {
        console.error(`  Insert error: ${insertErr.message}`);
      } else {
        totalRentEstimates += snapshots.length;
      }
    }

    offset += batchSize;
    if (offset % (batchSize * 5) === 0 || properties.length < batchSize) {
      console.log(`  ${totalRentEstimates} rent estimates generated (${offset} properties scanned)`);
    }

    if (properties.length < batchSize) break;
  }

  console.log(`  Total rent estimates: ${totalRentEstimates}`);

  // ─── Mortgage Records ──────────────────────────────────────────
  // NOTE: Mortgage data must come from real county recorder/clerk records.
  // We do NOT generate estimated mortgages — only real recorded data belongs here.
  console.log(`\n── Mortgage Records ──`);
  console.log(`  Skipped — mortgage data must come from county recorder scraper, not estimates.`);

  console.log(`\nDone.`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
