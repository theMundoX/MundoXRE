#!/usr/bin/env tsx
/**
 * Fast Fidlar AVA ingestion using direct API (no browser).
 * Runs all 28 counties sequentially with 60-day lookback.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaApiAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava-api.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType;
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase" };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("WARRANTY DEED") || upper === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (upper.includes("QUIT CLAIM") || upper === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

async function isDuplicate(docNumber: string, sourceUrl: string): Promise<boolean> {
  if (!docNumber) return false;
  const { data } = await db.from("mortgage_records").select("id")
    .eq("document_number", docNumber).eq("source_url", sourceUrl).limit(1);
  return (data?.length || 0) > 0;
}

async function main() {
  console.log("MXRE — Fast Fidlar AVA Ingestion (Direct API, No Browser)\n");

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 60);
  const start = startDate.toISOString().split("T")[0];
  const end = endDate.toISOString().split("T")[0];

  const adapter = new FidlarAvaApiAdapter();
  let grandTotal = 0, grandAmounts = 0;

  for (const config of FIDLAR_AVA_COUNTIES) {
    console.log(`\n━━━ ${config.county_name}, ${config.state} ━━━`);

    let inserted = 0, withAmount = 0, duplicates = 0, errors = 0;
    const batch: Array<Record<string, unknown>> = [];

    try {
      for await (const doc of adapter.fetchDocuments(config, start, end, (p) => {
        process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Processed: ${p.total_processed} | Errors: ${p.errors}`);
      })) {
        // Dedup
        if (doc.instrument_number && await isDuplicate(doc.instrument_number, doc.source_url)) {
          duplicates++;
          continue;
        }

        const classified = classifyDocType(doc.document_type);
        const record: Record<string, unknown> = {
          property_id: null,
          document_type: classified.document_type,
          recording_date: doc.recording_date,
          loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
          original_amount: doc.consideration ? Math.round(doc.consideration) : null,
          lender_name: doc.grantee?.slice(0, 500),
          borrower_name: doc.grantor?.slice(0, 500),
          document_number: doc.instrument_number,
          book_page: doc.book_page,
          source_url: doc.source_url,
          loan_type: classified.loan_type,
          deed_type: classified.deed_type,
        };

        if (doc.consideration && doc.consideration > 0 && doc.recording_date) {
          withAmount++;
          if (classified.document_type === "mortgage" || classified.document_type === "lien") {
            const fields = computeMortgageFields({
              originalAmount: Math.round(doc.consideration),
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
        if (batch.length >= 100) {
          const { error } = await db.from("mortgage_records").insert(batch);
          if (error) { errors++; }
          else inserted += batch.length;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (error) { errors++; }
        else inserted += batch.length;
        batch.length = 0;
      }
    } catch (err: any) {
      console.error(`\n  Error: ${err.message.slice(0, 80)}`);
    }

    console.log(`\n  Inserted: ${inserted} | Amounts: ${withAmount} | Dupes: ${duplicates} | Errors: ${errors}`);
    grandTotal += inserted;
    grandAmounts += withAmount;
  }

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n═══════════════════════════════════════`);
  console.log(`  Grand total: ${grandTotal} | With amounts: ${grandAmounts}`);
  console.log(`  DB total: ${count}`);
}

main().catch(console.error);
