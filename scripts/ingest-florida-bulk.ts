#!/usr/bin/env tsx
/**
 * Bulk ingest Florida recorder data with lien amounts.
 * Runs across all 11 working LandmarkWeb counties for 60 days.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";
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

async function isDuplicate(doc: any): Promise<boolean> {
  if (doc.instrument_number) {
    const { data } = await db.from("mortgage_records").select("id")
      .eq("document_number", doc.instrument_number).eq("source_url", doc.source_url).limit(1);
    if (data?.length) return true;
  }
  return false;
}

async function main() {
  const daysBack = parseInt(process.argv.find(a => a.startsWith("--days="))?.split("=")[1] || "60", 10);
  const fromArg = process.argv.find(a => a.startsWith("--from="))?.split("=")[1];
  const countyArg = process.argv.find(a => a.startsWith("--county="))?.split("=")[1];

  const endDate = new Date();
  const startDate = fromArg ? new Date(fromArg) : new Date();
  if (!fromArg) startDate.setDate(startDate.getDate() - daysBack);
  const start = startDate.toISOString().split("T")[0];
  const end = endDate.toISOString().split("T")[0];

  console.log(`MXRE — Bulk Florida Recorder Ingestion (${daysBack} days | ${start} → ${end})\n`);

  const adapter = new LandmarkWebAdapter();
  await adapter.init();

  // Run all counties — skip the 4 already ingested unless --force
  const alreadyDone: string[] = [];
  const force = process.argv.includes("--force");
  let counties = force ? LANDMARK_COUNTIES : LANDMARK_COUNTIES.filter(c => !alreadyDone.includes(c.county_name));
  if (countyArg) counties = counties.filter(c => c.county_name.toLowerCase() === countyArg.toLowerCase());

  let grandTotal = 0;
  let grandAmounts = 0;

  for (const config of counties) {
    console.log(`\n━━━ ${config.county_name} County, FL ━━━`);
    console.log(`  Date range: ${start} to ${end}`);

    const { data: dbCounty } = await db.from("counties")
      .select("id").eq("county_name", config.county_name).eq("state_code", "FL").single();
    if (dbCounty) config.county_id = dbCounty.id;

    let inserted = 0, withAmount = 0, duplicates = 0, errors = 0;

    try {
      const batch: Array<Record<string, unknown>> = [];

      for await (const doc of adapter.fetchDocuments(config, start, end, (p) => {
        process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Inserted: ${inserted} | Dupes: ${duplicates} | Errors: ${p.errors}`);
      })) {
        if (await isDuplicate(doc)) { duplicates++; continue; }

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

        if (doc.consideration && doc.consideration > 0) {
          withAmount++;
          if (classified.document_type === "mortgage" || classified.document_type === "lien") {
            const fields = computeMortgageFields({
              originalAmount: Math.round(doc.consideration),
              recordingDate: doc.recording_date,
            });
            record.interest_rate = fields.interest_rate;
            record.interest_rate_type = "estimated";
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
          if (error) { console.error(`\n  Insert error: ${error.message.slice(0, 80)}`); errors++; }
          else inserted += batch.length;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (error) { console.error(`\n  Insert error: ${error.message.slice(0, 80)}`); errors++; }
        else inserted += batch.length;
      }
    } catch (err: any) {
      console.error(`\n  Fatal: ${err.message.slice(0, 100)}`);
    }

    console.log(`\n  Result: ${inserted} inserted | ${withAmount} with amounts | ${duplicates} dupes | ${errors} errors`);
    grandTotal += inserted;
    grandAmounts += withAmount;
  }

  await adapter.close();

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n═══════════════════════════════════════════════════`);
  console.log(`  Grand total inserted: ${grandTotal} | With amounts: ${grandAmounts}`);
  console.log(`  Total mortgage_records in DB: ${count}`);
  console.log(`═══════════════════════════════════════════════════\n`);
}

main().catch(console.error);
