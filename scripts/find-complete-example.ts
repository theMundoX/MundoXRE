#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Find mortgage records with property links that have document_type = 'mortgage'
  const { data: morts } = await db.from("mortgage_records")
    .select("property_id, document_type, recording_date, lender_name, borrower_name, document_number, book_page, source_url, loan_amount")
    .eq("document_type", "mortgage")
    .not("property_id", "is", null)
    .limit(50);

  if (!morts || morts.length === 0) {
    console.log("No mortgage records linked to properties. Trying deeds...");
    return;
  }

  // Get those properties
  const pids = [...new Set(morts.map(m => m.property_id))];

  for (const pid of pids) {
    const { data: prop } = await db.from("properties").select("*").eq("id", pid).single();
    if (!prop || !prop.assessed_value || !prop.total_sqft || prop.total_sqft === 0) continue;

    // Generate fresh rent estimate with the fixed logic
    const perUnitSqft = (prop.total_units && prop.total_units > 1)
      ? Math.round(prop.total_sqft / prop.total_units)
      : prop.total_sqft;
    const perUnitValue = (prop.total_units && prop.total_units > 1)
      ? Math.round(prop.assessed_value / prop.total_units)
      : prop.assessed_value;

    const estimate = estimateRent({
      city: prop.city || "UNKNOWN",
      sqft: perUnitSqft,
      yearBuilt: prop.year_built || undefined,
      assessedValue: perUnitValue,
      totalUnits: prop.total_units || undefined,
    });

    if (estimate.estimated_rent <= 0) continue;

    // Get all mortgage records for this property
    const propMorts = morts.filter(m => m.property_id === pid);

    // This property has everything — show it
    const units = prop.total_units || 1;
    const grossMonthly = estimate.estimated_rent * units;
    const grossAnnual = grossMonthly * 12;

    console.log("┌─────────────────────────────────────────────────────────────────────────┐");
    console.log("│                    MXRE — COMPLETE PROPERTY PROFILE                     │");
    console.log("├─────────────────────────────────────────────────────────────────────────┤");
    console.log("│  DATA SOURCE          │  FIELD                │  VALUE                  │");
    console.log("├───────────────────────┼───────────────────────┼─────────────────────────┤");

    // Assessor Data
    console.log(`│  County Assessor      │  Address              │  ${(prop.address || "").padEnd(23)} │`);
    console.log(`│                       │  City/State/Zip       │  ${(`${prop.city}, ${prop.state_code} ${prop.zip}`).padEnd(23)} │`);
    console.log(`│                       │  Owner                │  ${(prop.owner_name || "N/A").slice(0, 23).padEnd(23)} │`);
    console.log(`│                       │  Parcel ID            │  ${(prop.parcel_id || "N/A").slice(0, 23).padEnd(23)} │`);
    console.log(`│                       │  Property Type        │  ${(prop.property_type || "N/A").padEnd(23)} │`);
    console.log(`│                       │  Year Built           │  ${String(prop.year_built || "N/A").padEnd(23)} │`);
    console.log(`│                       │  Total Sq Ft          │  ${(prop.total_sqft?.toLocaleString() || "N/A").padEnd(23)} │`);
    console.log(`│                       │  Units                │  ${String(units).padEnd(23)} │`);
    console.log(`│                       │  Assessed Value       │  ${("$" + prop.assessed_value?.toLocaleString()).padEnd(23)} │`);
    console.log(`│                       │  Market Value         │  ${(prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A").padEnd(23)} │`);
    console.log(`│                       │  Last Sale            │  ${(prop.last_sale_date || "N/A").padEnd(23)} │`);

    console.log("├───────────────────────┼───────────────────────┼─────────────────────────┤");

    // Rent Estimate
    console.log(`│  Rent Estimate        │  Unit Type            │  ${(`${estimate.beds}BR/${Math.max(1, estimate.beds)}BA`).padEnd(23)} │`);
    console.log(`│  (HUD FMR Model)      │  Per-Unit Sqft        │  ${(perUnitSqft + " sqft").padEnd(23)} │`);
    console.log(`│                       │  Monthly Rent/Unit    │  ${("$" + estimate.estimated_rent.toLocaleString() + "/mo").padEnd(23)} │`);
    console.log(`│                       │  $/Sq Ft/Mo           │  ${("$" + estimate.estimated_rent_psf.toFixed(2)).padEnd(23)} │`);
    console.log(`│                       │  HUD FMR Compare      │  ${("$" + estimate.fmr_rent + "/mo").padEnd(23)} │`);
    console.log(`│                       │  Confidence           │  ${(`${estimate.confidence_level} (${estimate.confidence_score}/100)`).padEnd(23)} │`);
    console.log(`│                       │  Method               │  ${estimate.estimation_source.padEnd(23)} │`);

    if (units > 1) {
      console.log("├───────────────────────┼───────────────────────┼─────────────────────────┤");
      console.log(`│  Building Income      │  Gross Monthly        │  ${("$" + grossMonthly.toLocaleString()).padEnd(23)} │`);
      console.log(`│                       │  Gross Annual         │  ${("$" + grossAnnual.toLocaleString()).padEnd(23)} │`);
      console.log(`│                       │  GRM                  │  ${((prop.assessed_value / grossAnnual).toFixed(1) + "x").padEnd(23)} │`);
      console.log(`│                       │  Cap Rate (gross)     │  ${(((grossAnnual / prop.assessed_value) * 100).toFixed(1) + "%").padEnd(23)} │`);
    }

    console.log("├───────────────────────┼───────────────────────┼─────────────────────────┤");

    // Mortgage Records
    for (let i = 0; i < propMorts.length; i++) {
      const m = propMorts[i];
      const label = i === 0 ? "County Recorder" : "";
      const label2 = i === 0 ? "(Actual Recorded)" : "";
      console.log(`│  ${label.padEnd(21)} │  Type                 │  ${(m.document_type || "").toUpperCase().padEnd(23)} │`);
      console.log(`│  ${label2.padEnd(21)} │  Recording Date       │  ${(m.recording_date || "N/A").padEnd(23)} │`);
      console.log(`│                       │  Borrower             │  ${(m.borrower_name || "N/A").slice(0, 23).padEnd(23)} │`);
      console.log(`│                       │  Lender               │  ${(m.lender_name || "N/A").slice(0, 23).padEnd(23)} │`);
      console.log(`│                       │  Amount               │  ${(m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "N/A").padEnd(23)} │`);
      console.log(`│                       │  Document #           │  ${(m.document_number || "N/A").padEnd(23)} │`);
      console.log(`│                       │  Book/Page            │  ${(m.book_page || "N/A").padEnd(23)} │`);
      console.log(`│                       │  Source URL           │  ${(m.source_url || "N/A").slice(0, 23).padEnd(23)} │`);
      if (i < propMorts.length - 1) {
        console.log("│                       ├───────────────────────┼─────────────────────────┤");
      }
    }

    console.log("└───────────────────────┴───────────────────────┴─────────────────────────┘");
    console.log();

    // Only show first complete example
    return;
  }

  console.log("No property found with all three data types populated.");
}

main().catch(console.error);
