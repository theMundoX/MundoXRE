#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { data: mortgages } = await db.from("mortgage_records")
    .select("id, property_id, document_type, loan_amount, interest_rate, estimated_monthly_payment, lender_name, borrower_name, recording_date, document_number, term_months, maturity_date, estimated_current_balance")
    .not("property_id", "is", null)
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .eq("document_type", "mortgage")
    .order("loan_amount", { ascending: false })
    .limit(10);

  if (!mortgages?.length) { console.log("No complete records."); return; }

  for (const mort of mortgages.slice(0, 3)) {
    const { data: prop } = await db.from("properties").select("*").eq("id", mort.property_id).single();
    if (!prop) continue;
    const { data: rent } = await db.from("rent_snapshots").select("*").eq("property_id", mort.property_id).order("observed_at", { ascending: false }).limit(1).single();

    console.log("\n====== COMPLETE PROPERTY PROFILE ======");
    console.log("\n--- PROPERTY ---");
    console.log("  Address:", prop.address);
    console.log("  City:", prop.city + ", " + prop.state_code + " " + prop.zip);
    console.log("  Owner:", prop.owner_name);
    console.log("  Parcel:", prop.parcel_id);
    console.log("  Type:", prop.property_type);
    console.log("  Year Built:", prop.year_built);
    console.log("  SqFt:", prop.total_sqft);
    console.log("  Assessed Value: $" + prop.assessed_value?.toLocaleString());

    console.log("\n--- MORTGAGE/LIEN ---");
    console.log("  Recorded Amount: $" + mort.loan_amount?.toLocaleString());
    console.log("  Interest Rate:", mort.interest_rate + "%");
    console.log("  Term:", mort.term_months, "months");
    console.log("  Monthly Payment: $" + mort.estimated_monthly_payment?.toLocaleString());
    console.log("  Current Balance: $" + mort.estimated_current_balance?.toLocaleString());
    console.log("  Maturity:", mort.maturity_date);
    console.log("  Recording Date:", mort.recording_date);
    console.log("  Doc #:", mort.document_number);
    console.log("  Lender:", mort.lender_name);
    console.log("  Borrower:", mort.borrower_name);

    if (rent) {
      console.log("\n--- RENT ESTIMATE ---");
      console.log("  Rent: $" + rent.asking_rent + "/mo");
      console.log("  Beds:", rent.beds, "| Baths:", rent.baths);
      console.log("  SqFt:", rent.sqft);
    } else {
      console.log("\n--- RENT: None ---");
    }
    console.log("\n=======================================");
  }
}
main().catch(console.error);
