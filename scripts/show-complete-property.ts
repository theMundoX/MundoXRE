#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Find a TX apartment with rent data
  const { data: rents } = await db.from("rent_snapshots")
    .select("property_id, asking_rent, beds, baths, sqft, observed_at, raw")
    .gt("asking_rent", 500)
    .not("property_id", "is", null)
    .order("property_id", { ascending: true })
    .limit(100);

  if (!rents || rents.length === 0) { console.log("No rent data"); return; }

  // Batch lookup properties
  const pids = [...new Set(rents.map(r => r.property_id))];
  const { data: props } = await db.from("properties")
    .select("*")
    .in("id", pids.slice(0, 20))
    .eq("state_code", "TX")
    .not("assessed_value", "is", null)
    .gt("total_units", 10);

  if (!props || props.length === 0) {
    // Try any state
    const { data: anyProps } = await db.from("properties")
      .select("*")
      .in("id", pids.slice(0, 50))
      .not("assessed_value", "is", null)
      .gt("assessed_value", 100000);

    if (!anyProps || anyProps.length === 0) {
      console.log("No properties found matching rent data IDs.");
      // Just show the rent data raw
      console.log("\nSample rent data:");
      for (const r of rents.slice(0, 3)) {
        console.log(`  property_id=${r.property_id}, rent=$${r.asking_rent}, beds=${r.beds}, source=${r.raw?.source}`);
      }
      // Look up that property
      const { data: p } = await db.from("properties").select("*").eq("id", rents[0].property_id).single();
      if (p) {
        console.log("\nProperty:", JSON.stringify(p, null, 2).slice(0, 500));
      }
      return;
    }

    for (const p of anyProps.slice(0, 1)) {
      await showProfile(p, rents);
    }
    return;
  }

  for (const p of props.slice(0, 1)) {
    await showProfile(p, rents);
  }
}

async function showProfile(prop: any, allRents: any[]) {
  // Get all rent snapshots for this property
  const { data: propRents } = await db.from("rent_snapshots")
    .select("*")
    .eq("property_id", prop.id)
    .order("asking_rent", { ascending: false })
    .limit(5);

  // Get mortgage records
  const { data: mortgages } = await db.from("mortgage_records")
    .select("*")
    .eq("property_id", prop.id)
    .order("recording_date", { ascending: false })
    .limit(5);

  console.log("═══════════════════════════════════════════════════════════════════════");
  console.log("                    MXRE — COMPLETE PROPERTY PROFILE                  ");
  console.log("═══════════════════════════════════════════════════════════════════════\n");

  console.log("  ─── PROPERTY (County Assessor) ─────────────────────────────────\n");
  console.log(`  Address:           ${prop.address}`);
  console.log(`  City/State/Zip:    ${prop.city}, ${prop.state_code} ${prop.zip}`);
  console.log(`  Owner:             ${prop.owner_name}`);
  console.log(`  Parcel ID:         ${prop.parcel_id}`);
  console.log(`  Property Type:     ${prop.property_type}`);
  console.log(`  Year Built:        ${prop.year_built || "N/A"}`);
  console.log(`  Total Sq Ft:       ${prop.total_sqft ? prop.total_sqft.toLocaleString() : "N/A"}`);
  console.log(`  Total Units:       ${prop.total_units || "N/A"}`);
  console.log(`  Stories:           ${prop.stories || "N/A"}`);
  console.log(`  Assessed Value:    ${prop.assessed_value ? "$" + prop.assessed_value.toLocaleString() : "N/A"}`);
  console.log(`  Market Value:      ${prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A"}`);
  console.log(`  Land Value:        ${prop.land_value ? "$" + prop.land_value.toLocaleString() : "N/A"}`);
  console.log(`  Property Tax:      ${prop.property_tax ? "$" + prop.property_tax.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Price:   ${prop.last_sale_price ? "$" + prop.last_sale_price.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Date:    ${prop.last_sale_date || "N/A"}`);

  console.log("\n  ─── RENT ESTIMATES ─────────────────────────────────────────────\n");
  if (propRents && propRents.length > 0) {
    for (const r of propRents) {
      const src = r.raw?.source || "estimated";
      const conf = r.raw?.confidence || "N/A";
      console.log(`  ${r.beds}BR/${r.baths}BA — $${r.asking_rent}/mo | $/sqft: ${r.asking_psf ? "$" + r.asking_psf.toFixed(2) : "N/A"} | Source: ${src} | Confidence: ${conf}`);
    }

    const totalMonthly = propRents.reduce((sum: number, r: any) => sum + (r.asking_rent || 0), 0);
    const avgMonthly = totalMonthly / propRents.length;
    if (prop.total_units && prop.total_units > 1) {
      const estGrossMonthly = avgMonthly * prop.total_units;
      const estGrossAnnual = estGrossMonthly * 12;
      console.log(`\n  Est. Avg Unit Rent: $${Math.round(avgMonthly)}/mo`);
      console.log(`  Est. Gross Monthly: $${Math.round(estGrossMonthly).toLocaleString()}`);
      console.log(`  Est. Gross Annual:  $${Math.round(estGrossAnnual).toLocaleString()}`);
      if (prop.assessed_value) {
        console.log(`  Est. GRM:           ${(prop.assessed_value / estGrossAnnual).toFixed(1)}`);
        console.log(`  Est. Cap Rate:      ${((estGrossAnnual / prop.assessed_value) * 100).toFixed(1)}%`);
      }
    }
  } else {
    console.log("  No rent estimates available for this property.");
  }

  console.log("\n  ─── MORTGAGE RECORDS (County Recorder) ─────────────────────────\n");
  if (mortgages && mortgages.length > 0) {
    for (const m of mortgages) {
      console.log(`  ${m.document_type.toUpperCase()}`);
      console.log(`    Recorded:    ${m.recording_date}`);
      console.log(`    Borrower:    ${m.borrower_name}`);
      console.log(`    Lender:      ${m.lender_name}`);
      console.log(`    Amount:      ${m.loan_amount ? "$" + m.loan_amount.toLocaleString() : "N/A"}`);
      console.log(`    Doc #:       ${m.document_number}`);
      console.log(`    Book/Page:   ${m.book_page || "N/A"}`);
      console.log();
    }
  } else {
    console.log("  No recorded mortgage data. (Need recorder ingestion for this county)");
  }

  console.log("═══════════════════════════════════════════════════════════════════════\n");
}

main().catch(console.error);
