#!/usr/bin/env tsx
/**
 * Show a Dallas County property with assessor data + rent estimate.
 * Then pull its mortgage records live from PublicSearch.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  // Check what tables have data
  const tables = ["properties", "rent_snapshots", "mortgage_records", "counties"];
  for (const t of tables) {
    const { count } = await db.from(t).select("*", { count: "exact", head: true });
    console.log(`${t}: ${count} rows`);
  }

  // Find a Dallas multifamily property with rent data
  console.log("\nSearching for Dallas MF properties with rent estimates...\n");

  // Get Dallas county ID
  const { data: dallasCounty } = await db
    .from("counties")
    .select("id, county_name")
    .eq("county_name", "Dallas")
    .single();

  if (!dallasCounty) {
    console.log("Dallas county not found in DB. Trying by state...");
  }

  // Find a multifamily property in Dallas
  const { data: props } = await db
    .from("properties")
    .select("*")
    .eq("state_code", "TX")
    .in("property_type", ["apartment", "multifamily", "multi_family"])
    .not("total_sqft", "is", null)
    .gt("total_sqft", 1000)
    .not("assessed_value", "is", null)
    .gt("total_units", 4)
    .order("total_units", { ascending: false })
    .limit(5);

  if (!props || props.length === 0) {
    console.log("No multifamily properties found. Trying any property...");
    const { data: anyProp } = await db
      .from("properties")
      .select("*")
      .eq("state_code", "TX")
      .not("assessed_value", "is", null)
      .not("total_sqft", "is", null)
      .gt("total_sqft", 500)
      .limit(3);

    if (anyProp && anyProp.length > 0) {
      for (const p of anyProp) {
        await showFull(p);
      }
    }
    return;
  }

  // Show top results
  for (const p of props.slice(0, 2)) {
    await showFull(p);
  }
}

async function showFull(prop: any) {
  console.log("\n═══════════════════════════════════════════════════════════════════");
  console.log("  MXRE PROPERTY PROFILE");
  console.log("═══════════════════════════════════════════════════════════════════\n");

  console.log("  ─── ASSESSOR DATA ───\n");
  console.log(`  Address:          ${prop.address || "N/A"}`);
  console.log(`  City/State/Zip:   ${prop.city || ""}, ${prop.state_code || ""} ${prop.zip || ""}`);
  console.log(`  Owner:            ${prop.owner_name || "N/A"}`);
  console.log(`  Parcel ID:        ${prop.parcel_id || "N/A"}`);
  console.log(`  Property Type:    ${prop.property_type || "N/A"}`);
  console.log(`  Year Built:       ${prop.year_built || "N/A"}`);
  console.log(`  Total Sq Ft:      ${prop.total_sqft ? prop.total_sqft.toLocaleString() : "N/A"}`);
  console.log(`  Total Units:      ${prop.total_units || "N/A"}`);
  console.log(`  Total Buildings:  ${prop.total_buildings || "N/A"}`);
  console.log(`  Assessed Value:   ${prop.assessed_value ? "$" + prop.assessed_value.toLocaleString() : "N/A"}`);
  console.log(`  Market Value:     ${prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A"}`);
  console.log(`  Land Value:       ${prop.land_value ? "$" + prop.land_value.toLocaleString() : "N/A"}`);
  console.log(`  Property Tax:     ${prop.property_tax ? "$" + prop.property_tax.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Price:  ${prop.last_sale_price ? "$" + prop.last_sale_price.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Date:   ${prop.last_sale_date || "N/A"}`);
  console.log(`  Construction:     ${prop.construction_class || "N/A"}`);
  console.log(`  Quality:          ${prop.improvement_quality || "N/A"}`);
  console.log(`  Stories:          ${prop.stories || "N/A"}`);
  console.log(`  Land Sq Ft:       ${prop.land_sqft ? prop.land_sqft.toLocaleString() : "N/A"}`);

  // Rent data
  const { data: rents } = await db
    .from("rent_snapshots")
    .select("*")
    .eq("property_id", prop.id)
    .order("snapshot_date", { ascending: false })
    .limit(3);

  console.log("\n  ─── RENT DATA ───\n");
  if (rents && rents.length > 0) {
    for (const r of rents) {
      console.log(`  Snapshot:     ${r.snapshot_date}`);
      console.log(`  Monthly Rent: ${r.monthly_rent ? "$" + r.monthly_rent.toLocaleString() : "N/A"}`);
      console.log(`  $/Sq Ft/Mo:   ${r.rent_per_sqft ? "$" + r.rent_per_sqft.toFixed(2) : "N/A"}`);
      console.log(`  Source:       ${r.source || "N/A"}`);
      console.log();
    }

    const annualRent = (rents[0].monthly_rent || 0) * 12;
    if (annualRent > 0 && prop.assessed_value) {
      console.log(`  Annual Rent:  $${annualRent.toLocaleString()}`);
      console.log(`  Cap Rate:     ${((annualRent / prop.assessed_value) * 100).toFixed(2)}%`);
      console.log(`  GRM:          ${(prop.assessed_value / annualRent).toFixed(1)}`);
    }
  } else {
    console.log("  No rent data available.");
  }

  // Mortgage data
  const { data: mortgages } = await db
    .from("mortgage_records")
    .select("*")
    .eq("property_id", prop.id)
    .order("recording_date", { ascending: false })
    .limit(5);

  console.log("\n  ─── MORTGAGE RECORDS ───\n");
  if (mortgages && mortgages.length > 0) {
    for (const m of mortgages) {
      console.log(`  Type:         ${m.document_type}`);
      console.log(`  Recorded:     ${m.recording_date}`);
      console.log(`  Borrower:     ${m.borrower_name || "N/A"}`);
      console.log(`  Lender:       ${m.lender_name || "N/A"}`);
      console.log(`  Amount:       ${m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "N/A"}`);
      console.log(`  Doc #:        ${m.document_number || "N/A"}`);
      console.log(`  Book/Page:    ${m.book_page || "N/A"}`);
      console.log();
    }
  } else {
    console.log("  No recorded mortgage data. (Recorder ingestion needed for this county)");
  }
}

main().catch(console.error);
