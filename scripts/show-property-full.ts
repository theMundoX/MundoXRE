#!/usr/bin/env tsx
/**
 * Show a complete property profile: assessor data + rent estimate + mortgage records.
 * Finds a property that has all three data types populated.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("Finding a property with complete data...\n");

  // Find properties that have mortgage records linked
  const { data: withMortgage } = await db
    .from("mortgage_records")
    .select("property_id")
    .not("property_id", "is", null)
    .limit(50);

  if (!withMortgage || withMortgage.length === 0) {
    console.log("No mortgage records linked to properties yet.");
    console.log("Showing a sample property with rent estimate instead...\n");

    // Just show a property with rent data
    const { data: props } = await db
      .from("properties")
      .select("*")
      .not("assessed_value", "is", null)
      .not("total_sqft", "is", null)
      .gt("total_sqft", 0)
      .limit(5);

    if (!props || props.length === 0) {
      console.log("No properties found.");
      return;
    }

    for (const prop of props.slice(0, 3)) {
      await showProperty(prop);
    }
    return;
  }

  // Get full property data for ones with mortgages
  const propertyIds = [...new Set(withMortgage.map(m => m.property_id))];
  const { data: props } = await db
    .from("properties")
    .select("*")
    .in("id", propertyIds.slice(0, 10))
    .not("assessed_value", "is", null);

  if (!props || props.length === 0) {
    console.log("Properties with mortgage links not found in properties table.");
    return;
  }

  for (const prop of props.slice(0, 3)) {
    await showProperty(prop);
  }
}

async function showProperty(prop: any) {
  console.log("═══════════════════════════════════════════════════════════════════");
  console.log("  COMPLETE PROPERTY PROFILE — MXRE");
  console.log("═══════════════════════════════════════════════════════════════════\n");

  // ─── Assessor Data ───
  console.log("  ─── ASSESSOR DATA (County Tax Records) ───\n");
  console.log(`  Address:          ${prop.address || "N/A"}`);
  console.log(`  City/State/Zip:   ${prop.city || ""}, ${prop.state_code || ""} ${prop.zip || ""}`);
  console.log(`  Owner:            ${prop.owner_name || "N/A"}`);
  console.log(`  Parcel ID:        ${prop.parcel_id || "N/A"}`);
  console.log(`  Property Type:    ${prop.property_type || "N/A"}`);
  console.log(`  Year Built:       ${prop.year_built || "N/A"}`);
  console.log(`  Total Sq Ft:      ${prop.total_sqft ? prop.total_sqft.toLocaleString() : "N/A"}`);
  console.log(`  Total Units:      ${prop.total_units || "N/A"}`);
  console.log(`  Assessed Value:   ${prop.assessed_value ? "$" + prop.assessed_value.toLocaleString() : "N/A"}`);
  console.log(`  Market Value:     ${prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A"}`);
  console.log(`  Land Value:       ${prop.land_value ? "$" + prop.land_value.toLocaleString() : "N/A"}`);
  console.log(`  Property Tax:     ${prop.property_tax ? "$" + prop.property_tax.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Price:  ${prop.last_sale_price ? "$" + prop.last_sale_price.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Date:   ${prop.last_sale_date || "N/A"}`);

  // ─── Rent Estimate ───
  const { data: rents } = await db
    .from("rent_snapshots")
    .select("*")
    .eq("property_id", prop.id)
    .order("snapshot_date", { ascending: false })
    .limit(1);

  console.log("\n  ─── RENT ESTIMATE ───\n");
  if (rents && rents.length > 0) {
    const rent = rents[0];
    console.log(`  Monthly Rent:     ${rent.monthly_rent ? "$" + rent.monthly_rent.toLocaleString() : "N/A"}`);
    console.log(`  Annual Rent:      ${rent.monthly_rent ? "$" + (rent.monthly_rent * 12).toLocaleString() : "N/A"}`);
    console.log(`  $/Sq Ft/Mo:       ${rent.rent_per_sqft ? "$" + rent.rent_per_sqft.toFixed(2) : "N/A"}`);
    console.log(`  Source:           ${rent.source || "statistical"}`);
    console.log(`  Snapshot Date:    ${rent.snapshot_date || "N/A"}`);
    console.log(`  Confidence:       ${rent.confidence || "N/A"}`);
  } else {
    console.log("  No rent estimate available.");
  }

  // ─── Mortgage Records ───
  const { data: mortgages } = await db
    .from("mortgage_records")
    .select("*")
    .eq("property_id", prop.id)
    .order("recording_date", { ascending: false })
    .limit(5);

  console.log("\n  ─── MORTGAGE RECORDS (County Recorder) ───\n");
  if (mortgages && mortgages.length > 0) {
    for (const m of mortgages) {
      console.log(`  Doc Type:         ${m.document_type || "N/A"}`);
      console.log(`  Recording Date:   ${m.recording_date || "N/A"}`);
      console.log(`  Borrower:         ${m.borrower_name || "N/A"}`);
      console.log(`  Lender:           ${m.lender_name || "N/A"}`);
      console.log(`  Loan Amount:      ${m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "N/A"}`);
      console.log(`  Doc Number:       ${m.document_number || "N/A"}`);
      console.log(`  Book/Page:        ${m.book_page || "N/A"}`);
      console.log(`  Source:           ${m.source_url || "N/A"}`);
      console.log();
    }
  } else {
    console.log("  No recorded mortgage data for this property.");
  }

  // ─── Investment Metrics ───
  if (rents && rents.length > 0 && prop.assessed_value) {
    const annualRent = (rents[0].monthly_rent || 0) * 12;
    const capRate = annualRent / prop.assessed_value;
    const grm = prop.assessed_value / annualRent;

    console.log("  ─── INVESTMENT METRICS ───\n");
    console.log(`  Cap Rate (est):   ${(capRate * 100).toFixed(2)}%`);
    console.log(`  GRM:              ${grm.toFixed(1)}`);
    if (prop.property_tax) {
      const noi = annualRent - prop.property_tax;
      const noiCapRate = noi / prop.assessed_value;
      console.log(`  NOI (rent-tax):   $${noi.toLocaleString()}`);
      console.log(`  Cap Rate (NOI):   ${(noiCapRate * 100).toFixed(2)}%`);
    }
  }

  console.log("\n");
}

main().catch(console.error);
