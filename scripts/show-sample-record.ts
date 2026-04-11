#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Get fully linked mortgages with amounts
  const { data } = await db.from("mortgage_records")
    .select("*")
    .not("property_id", "is", null)
    .not("loan_amount", "is", null)
    .gt("loan_amount", 100000)
    .eq("document_type", "mortgage")
    .order("recording_date", { ascending: false })
    .limit(20);

  for (const rec of (data || [])) {
    const { data: prop } = await db.from("properties")
      .select("*")
      .eq("id", rec.property_id)
      .single();

    if (!prop || !prop.address) continue;

    const { data: rent } = await db.from("rent_snapshots")
      .select("*")
      .eq("property_id", rec.property_id)
      .limit(1);

    console.log("═══════════════════════════════════════════════════");
    console.log("PROPERTY");
    console.log("  Address:", [prop.address, prop.city, prop.state_code, prop.zip].filter(Boolean).join(", "));
    console.log("  Owner:", prop.owner_name || "N/A");
    console.log("  Type:", prop.property_type || "N/A");
    console.log("  Year Built:", prop.year_built || "N/A");
    console.log("  Sqft:", prop.total_sqft ? prop.total_sqft.toLocaleString() : "N/A");
    console.log("  Units:", prop.total_units || 1);
    console.log("  Assessed Value:", prop.assessed_value ? "$" + prop.assessed_value.toLocaleString() : "N/A");
    console.log("  Market Value:", prop.market_value ? "$" + prop.market_value.toLocaleString() : "N/A");
    console.log("  Last Sale:", [prop.last_sale_date, prop.last_sale_price ? "$" + prop.last_sale_price.toLocaleString() : ""].filter(Boolean).join(" — ") || "N/A");
    console.log();
    console.log("RECORDED LIEN");
    console.log("  Document Type:", rec.document_type);
    console.log("  Recording Date:", rec.recording_date);
    console.log("  Loan Amount:", rec.loan_amount ? "$" + rec.loan_amount.toLocaleString() : "N/A");
    console.log("  Interest Rate:", rec.interest_rate ? rec.interest_rate + "% (Freddie Mac avg at recording)" : "Not recorded");
    console.log("  Term:", rec.term_months ? rec.term_months + " months" : "N/A");
    console.log("  Est Monthly Payment:", rec.estimated_monthly_payment ? "$" + rec.estimated_monthly_payment.toLocaleString() : "N/A");
    console.log("  Est Current Balance:", rec.estimated_current_balance ? "$" + rec.estimated_current_balance.toLocaleString() : "N/A");
    console.log("  Maturity Date:", rec.maturity_date || "N/A");
    console.log("  Borrower:", rec.borrower_name || "N/A");
    console.log("  Lender:", rec.lender_name || "N/A");
    console.log("  Document #:", rec.document_number || "N/A");
    console.log("  Source:", rec.source_url || "N/A");
    console.log();
    if (rent?.length) {
      console.log("RENT ESTIMATE");
      console.log("  Asking Rent:", rent[0].asking_rent ? "$" + rent[0].asking_rent.toLocaleString() + "/mo" : "N/A");
      console.log("  Per Sqft:", rent[0].asking_psf ? "$" + (rent[0].asking_psf / 100).toFixed(2) + "/sqft" : "N/A");
      console.log("  Beds:", rent[0].beds || "N/A");
      console.log("  Method:", rent[0].raw?.method || "N/A");
    } else {
      console.log("RENT ESTIMATE: None for this property");
    }
    console.log();
  }
}
main().catch(console.error);
