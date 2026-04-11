#!/usr/bin/env tsx
/**
 * Re-ingest Florida recorder data with updated adapter that captures lien amounts.
 * Purges old records first, then re-ingests with Consideration search.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { LandmarkWebAdapter, LANDMARK_COUNTIES, type RecorderDocument } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType.toUpperCase();
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase" };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("WARRANTY DEED") || upper === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (upper.includes("QUIT CLAIM") || upper === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

async function main() {
  console.log("MXRE — Re-ingest Florida Recorder Data (with Lien Amounts)\n");

  // Purge old Levy County records
  const { count: oldCount } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .like("source_url", "%levyclerk%");
  console.log(`Purging ${oldCount} old Levy County records...`);
  await db.from("mortgage_records").delete().like("source_url", "%levyclerk%");

  // Init adapter
  const adapter = new LandmarkWebAdapter();
  await adapter.init();

  const counties = LANDMARK_COUNTIES.filter(c =>
    ["Levy", "Martin", "Walton", "Citrus"].includes(c.county_name)
  );

  for (const config of counties) {
    console.log(`\n━━━ ${config.county_name} County, FL ━━━`);

    // Look up county in DB
    const { data: dbCounty } = await db.from("counties")
      .select("id")
      .eq("county_name", config.county_name)
      .eq("state_code", "FL")
      .single();
    if (dbCounty) config.county_id = dbCounty.id;

    const start = "2026-03-17";
    const end = "2026-03-21";
    let inserted = 0, withAmount = 0, errors = 0;

    try {
      const batch: Array<Record<string, unknown>> = [];

      for await (const doc of adapter.fetchDocuments(config, start, end, undefined, (p) => {
        process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Errors: ${p.errors}`);
      })) {
        const classified = classifyDocType(doc.document_type);

        const record: Record<string, unknown> = {
          property_id: null,
          document_type: classified.document_type,
          recording_date: doc.recording_date,
          loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
          original_amount: doc.consideration ? Math.round(doc.consideration) : null,
          lender_name: doc.grantee,
          borrower_name: doc.grantor,
          document_number: doc.instrument_number,
          book_page: doc.book_page,
          source_url: doc.source_url,
          loan_type: classified.loan_type,
          deed_type: classified.deed_type,
        };

        // Compute mortgage math when we have an amount
        if (doc.consideration && doc.consideration > 0 && doc.recording_date) {
          withAmount++;
          const isMortgage = classified.document_type === "mortgage" || classified.document_type === "lien";
          if (isMortgage) {
            const fields = computeMortgageFields({
              originalAmount: doc.consideration,
              recordingDate: doc.recording_date,
            });
            record.interest_rate = fields.interest_rate;
            record.term_months = fields.term_months;
            record.estimated_monthly_payment = fields.estimated_monthly_payment;
            record.estimated_current_balance = fields.estimated_current_balance;
            record.balance_as_of = fields.balance_as_of;
            record.maturity_date = fields.maturity_date;
          }
        }

        batch.push(record);

        if (batch.length >= 50) {
          const { error } = await db.from("mortgage_records").insert(batch);
          if (error) { console.error(`\n  Insert error: ${error.message}`); errors++; }
          else inserted += batch.length;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (error) { console.error(`\n  Insert error: ${error.message}`); errors++; }
        else inserted += batch.length;
      }
    } catch (err: any) {
      console.error(`\n  Error: ${err.message.slice(0, 100)}`);
    }

    console.log(`\n  Inserted: ${inserted} | With amounts: ${withAmount} | Errors: ${errors}`);
  }

  await adapter.close();

  // Final count
  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\nTotal mortgage_records in DB: ${count}`);
}

main().catch(console.error);
