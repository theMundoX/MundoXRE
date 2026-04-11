#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const propId = 123668;

  // Delete bad rent snapshots
  await db.from("rent_snapshots").delete().eq("property_id", propId);

  // Get property
  const { data: prop } = await db.from("properties").select("*").eq("id", propId).single();
  if (!prop) return;

  // Generate correct per-unit estimate
  const perUnitSqft = prop.total_units > 1 ? Math.round(prop.total_sqft / prop.total_units) : prop.total_sqft;
  const perUnitValue = prop.total_units > 1 ? Math.round(prop.assessed_value / prop.total_units) : prop.assessed_value;

  const estimate = estimateRent({
    city: prop.city,
    sqft: perUnitSqft,
    yearBuilt: prop.year_built,
    assessedValue: perUnitValue,
    totalUnits: prop.total_units,
  });

  // Insert correct rent snapshot
  await db.from("rent_snapshots").insert({
    property_id: propId,
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
      source: "estimated_v2",
      sqft_per_unit: perUnitSqft,
      total_units: prop.total_units,
    },
  });

  // Get mortgage records
  const { data: mortgages } = await db.from("mortgage_records").select("*").eq("property_id", propId).limit(5);

  // Show complete profile
  const grossMonthly = estimate.estimated_rent * prop.total_units;
  const grossAnnual = grossMonthly * 12;

  console.log("═══════════════════════════════════════════════════════════════════════════");
  console.log("                       MXRE — COMPLETE PROPERTY PROFILE                   ");
  console.log("═══════════════════════════════════════════════════════════════════════════\n");

  console.log("  ─── PROPERTY (Dallas County Tax Assessor) ───────────────────────────\n");
  console.log(`  Address:            ${prop.address}`);
  console.log(`  City/State/Zip:     ${prop.city}, ${prop.state_code} ${prop.zip}`);
  console.log(`  Owner:              ${prop.owner_name}`);
  console.log(`  Parcel ID:          ${prop.parcel_id}`);
  console.log(`  Property Type:      ${prop.property_type}`);
  console.log(`  Year Built:         ${prop.year_built}`);
  console.log(`  Total Sq Ft:        ${prop.total_sqft?.toLocaleString()}`);
  console.log(`  Per-Unit Sq Ft:     ${perUnitSqft?.toLocaleString()}`);
  console.log(`  Total Units:        ${prop.total_units}`);
  console.log(`  Stories:            ${prop.stories}`);
  console.log(`  Assessed Value:     $${prop.assessed_value?.toLocaleString()}`);
  console.log(`  Per-Unit Value:     $${perUnitValue?.toLocaleString()}`);
  console.log(`  Last Sale Date:     ${prop.last_sale_date}`);
  console.log(`  Source:             ${prop.assessor_url}`);

  console.log("\n  ─── RENT ESTIMATE (Per Unit) ────────────────────────────────────────\n");
  console.log(`  Unit Type:          ${estimate.beds}BR/${Math.max(1, estimate.beds)}BA`);
  console.log(`  Est. Unit Sqft:     ${perUnitSqft} sqft`);
  console.log(`  Est. Monthly Rent:  $${estimate.estimated_rent.toLocaleString()}/mo`);
  console.log(`  Est. $/Sq Ft/Mo:    $${estimate.estimated_rent_psf.toFixed(2)}`);
  console.log(`  HUD FMR (1BR DFW):  $${estimate.fmr_rent}/mo`);
  console.log(`  Method:             ${estimate.estimation_source}`);
  console.log(`  Confidence:         ${estimate.confidence_level} (score: ${estimate.confidence_score}/100)`);

  console.log("\n  ─── BUILDING-LEVEL INCOME ───────────────────────────────────────────\n");
  console.log(`  Units:              ${prop.total_units}`);
  console.log(`  Gross Monthly:      $${grossMonthly.toLocaleString()}`);
  console.log(`  Gross Annual:       $${grossAnnual.toLocaleString()}`);
  console.log(`  GRM:                ${(prop.assessed_value / grossAnnual).toFixed(1)}x`);
  console.log(`  Cap Rate (gross):   ${((grossAnnual / prop.assessed_value) * 100).toFixed(1)}%`);

  console.log("\n  ─── MORTGAGE RECORDS (Dallas County Clerk) ──────────────────────────\n");
  if (mortgages && mortgages.length > 0) {
    for (const m of mortgages) {
      console.log(`  ${m.document_type.toUpperCase()}`);
      console.log(`    Recorded:     ${m.recording_date}`);
      console.log(`    Borrower:     ${m.borrower_name}`);
      console.log(`    Lender:       ${m.lender_name}`);
      console.log(`    Amount:       ${m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "N/A"}`);
      console.log(`    Doc Number:   ${m.document_number}`);
      console.log(`    Book/Page:    ${m.book_page || "N/A"}`);
      console.log();
    }
  } else {
    console.log("  No recorded mortgage data.");
    console.log("  (Dallas County recorder ingestion will populate this — adapters are built,");
    console.log("   need to run from VPS with proxy for date-filtered search to work)");
  }

  console.log("═══════════════════════════════════════════════════════════════════════════\n");
}

main().catch(console.error);
