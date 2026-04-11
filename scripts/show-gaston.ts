#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { data: prop } = await db.from("properties").select("*").eq("id", 123668).single();
  if (!prop) { console.log("Not found"); return; }

  const { data: rents } = await db.from("rent_snapshots").select("*").eq("property_id", prop.id).order("beds", { ascending: true });
  const { data: mortgages } = await db.from("mortgage_records").select("*").eq("property_id", prop.id).order("recording_date", { ascending: false }).limit(5);

  console.log("═══════════════════════════════════════════════════════════════════════════");
  console.log("                       MXRE — COMPLETE PROPERTY PROFILE                   ");
  console.log("═══════════════════════════════════════════════════════════════════════════\n");

  console.log("  ─── PROPERTY (Dallas County Tax Assessor) ───────────────────────────\n");
  console.log(`  Address:            ${prop.address}`);
  console.log(`  City/State/Zip:     ${prop.city}, ${prop.state_code} ${prop.zip}`);
  console.log(`  Owner:              ${prop.owner_name}`);
  console.log(`  Parcel ID:          ${prop.parcel_id}`);
  console.log(`  Property Type:      ${prop.property_type}`);
  console.log(`  Year Built:         ${prop.year_built || "N/A"}`);
  console.log(`  Total Sq Ft:        ${prop.total_sqft ? prop.total_sqft.toLocaleString() : "N/A"}`);
  console.log(`  Total Units:        ${prop.total_units || "N/A"}`);
  console.log(`  Stories:            ${prop.stories || "N/A"}`);
  console.log(`  Assessed Value:     ${prop.assessed_value ? "$" + prop.assessed_value.toLocaleString() : "N/A"}`);
  console.log(`  Market Value:       ${prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A"}`);
  console.log(`  Land Value:         ${prop.land_value ? "$" + prop.land_value.toLocaleString() : "N/A"}`);
  console.log(`  Property Tax:       ${prop.property_tax ? "$" + prop.property_tax.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Price:    ${prop.last_sale_price ? "$" + prop.last_sale_price.toLocaleString() : "N/A"}`);
  console.log(`  Last Sale Date:     ${prop.last_sale_date || "N/A"}`);
  console.log(`  Construction:       ${prop.construction_class || "N/A"}`);
  console.log(`  Legal Description:  ${(prop.legal_description || "N/A").slice(0, 80)}`);

  console.log("\n  ─── RENT ESTIMATES (HUD FMR Based) ─────────────────────────────────\n");
  if (rents && rents.length > 0) {
    for (const r of rents) {
      const src = r.raw?.source || "estimated";
      const conf = r.raw?.confidence || "N/A";
      const method = r.raw?.method || "N/A";
      console.log(`  ${r.beds}BR/${r.baths}BA — $${r.asking_rent?.toLocaleString()}/mo | Method: ${method} | Confidence: ${conf}`);
    }

    const totalMonthly = rents.reduce((s: number, r: any) => s + (r.asking_rent || 0), 0);
    if (prop.total_units && prop.total_units > 1) {
      const avgRent = totalMonthly / rents.length;
      const grossMonthly = avgRent * prop.total_units;
      const grossAnnual = grossMonthly * 12;
      console.log(`\n  Est. Avg Rent/Unit:  $${Math.round(avgRent).toLocaleString()}/mo`);
      console.log(`  Est. Gross Monthly:  $${Math.round(grossMonthly).toLocaleString()}`);
      console.log(`  Est. Gross Annual:   $${Math.round(grossAnnual).toLocaleString()}`);
      if (prop.assessed_value) {
        console.log(`  GRM (on assessed):   ${(prop.assessed_value / grossAnnual).toFixed(1)}`);
        console.log(`  Cap Rate (gross):    ${((grossAnnual / prop.assessed_value) * 100).toFixed(1)}%`);
      }
    }
  } else {
    console.log("  No rent data.");
  }

  console.log("\n  ─── MORTGAGE RECORDS (Dallas County Clerk) ─────────────────────────\n");
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
    console.log("  No recorded mortgage data yet.");
    console.log("  (Dallas County recorder ingestion will populate this)");
  }

  console.log("═══════════════════════════════════════════════════════════════════════════\n");
}

main().catch(console.error);
