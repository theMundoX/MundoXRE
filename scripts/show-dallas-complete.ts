#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { estimateRent } from "../src/utils/rent-estimator.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Find a Dallas mortgage record that matched a property AND is a deed of trust/mortgage
  const { data: morts } = await db.from("mortgage_records")
    .select("*")
    .like("source_url", "%publicsearch%")
    .eq("document_type", "mortgage")
    .not("property_id", "is", null)
    .limit(20);

  if (!morts || morts.length === 0) { console.log("No matched Dallas mortgages yet"); return; }

  // Find the best one — a property with sqft and assessed value
  for (const mort of morts) {
    const { data: prop } = await db.from("properties").select("*").eq("id", mort.property_id).single();
    if (!prop || !prop.assessed_value || !prop.total_sqft) continue;

    // Generate rent estimate
    const units = prop.total_units || 1;
    const perUnitSqft = units > 1 ? Math.round(prop.total_sqft / units) : prop.total_sqft;
    const perUnitValue = units > 1 ? Math.round(prop.assessed_value / units) : prop.assessed_value;

    const est = estimateRent({
      city: prop.city || "DALLAS",
      sqft: perUnitSqft,
      yearBuilt: prop.year_built || undefined,
      assessedValue: perUnitValue,
      totalUnits: units,
    });

    if (est.estimated_rent <= 0) continue;

    // Get all mortgage records for this property
    const { data: allMorts } = await db.from("mortgage_records")
      .select("*")
      .eq("property_id", prop.id)
      .order("recording_date", { ascending: false });

    console.log("═══════════════════════════════════════════════════════════════════════════");
    console.log("                  MXRE — COMPLETE PROPERTY PROFILE                        ");
    console.log("                  Dallas County, TX — All Data Layers                     ");
    console.log("═══════════════════════════════════════════════════════════════════════════\n");

    console.log("  ┌─── ASSESSOR DATA (Dallas Central Appraisal District) ──────────────┐\n");
    console.log(`    Address:            ${prop.address}`);
    console.log(`    City/State/Zip:     ${prop.city}, ${prop.state_code} ${prop.zip}`);
    console.log(`    Owner:              ${prop.owner_name}`);
    console.log(`    Parcel ID:          ${prop.parcel_id}`);
    console.log(`    Property Type:      ${prop.property_type}`);
    console.log(`    Year Built:         ${prop.year_built || "N/A"}`);
    console.log(`    Total Sq Ft:        ${prop.total_sqft?.toLocaleString()}`);
    console.log(`    Units:              ${units}`);
    console.log(`    Stories:            ${prop.stories || "N/A"}`);
    console.log(`    Assessed Value:     $${prop.assessed_value?.toLocaleString()}`);
    console.log(`    Market Value:       ${prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A"}`);
    console.log(`    Last Sale Date:     ${prop.last_sale_date || "N/A"}`);
    console.log(`    Assessor Link:      ${prop.assessor_url || "N/A"}`);

    console.log("\n  ┌─── RENT ESTIMATE (Per Unit) ───────────────────────────────────────┐\n");
    console.log(`    Unit Type:          ${est.beds}BR/${Math.max(1, est.beds)}BA`);
    console.log(`    Est. Per-Unit Sqft: ${perUnitSqft} sqft`);
    console.log(`    Monthly Rent:       $${est.estimated_rent.toLocaleString()}/mo`);
    console.log(`    $/Sq Ft/Mo:         $${est.estimated_rent_psf.toFixed(2)}`);
    console.log(`    HUD FMR Compare:    $${est.fmr_rent}/mo`);
    console.log(`    Confidence:         ${est.confidence_level} (${est.confidence_score}/100)`);
    console.log(`    Method:             ${est.estimation_source}`);

    if (units > 1) {
      const gm = est.estimated_rent * units;
      const ga = gm * 12;
      console.log(`\n    Gross Monthly:      $${gm.toLocaleString()}`);
      console.log(`    Gross Annual:       $${ga.toLocaleString()}`);
      console.log(`    GRM:                ${(prop.assessed_value / ga).toFixed(1)}x`);
      console.log(`    Cap Rate (gross):   ${((ga / prop.assessed_value) * 100).toFixed(1)}%`);
    }

    console.log("\n  ┌─── RECORDED DOCUMENTS (Dallas County Clerk) ───────────────────────┐\n");
    for (const m of allMorts || []) {
      console.log(`    ${m.document_type.toUpperCase()}`);
      console.log(`      Recorded:       ${m.recording_date}`);
      console.log(`      Borrower:       ${m.borrower_name || "N/A"}`);
      console.log(`      Lender:         ${m.lender_name || "N/A"}`);
      console.log(`      Loan Amount:    ${m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "Not in index"}`);
      console.log(`      Doc Number:     ${m.document_number || "N/A"}`);
      console.log(`      Book/Page:      ${m.book_page || "N/A"}`);
      console.log();
    }

    console.log("  ┌─── DATA QUALITY NOTES ─────────────────────────────────────────────┐\n");
    console.log("    - Assessor data: Real, from DCAD bulk download");
    console.log("    - Rent estimate: Statistical (HUD FMR + $/sqft model), not scraped");
    console.log("    - Mortgage data: Real recorded instrument from county clerk portal");
    console.log("    - Loan amount: Not available in PublicSearch index (in document PDF)");
    console.log("    - Interest rate: Never in recorder index (inside document image)");

    console.log("\n═══════════════════════════════════════════════════════════════════════════\n");
    return;
  }

  console.log("No Dallas property found with all three data layers populated.");
}

main().catch(console.error);
