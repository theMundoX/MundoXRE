#!/usr/bin/env tsx
/**
 * Ingest recorder data from Fidlar AVA counties.
 * Captures all document types with ConsiderationAmount (lien amounts).
 *
 * Usage:
 *   npx tsx scripts/ingest-fidlar-ava.ts --county=Fairfield --state=OH --days=30
 *   npx tsx scripts/ingest-fidlar-ava.ts --all --days=7
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const args = process.argv.slice(2);
const countyFilter = args.find(a => a.startsWith("--county="))?.split("=")[1];
const stateFilter = args.find(a => a.startsWith("--state="))?.split("=")[1];
const daysBack = parseInt(args.find(a => a.startsWith("--days="))?.split("=")[1] || "7", 10);
const runAll = args.includes("--all");

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
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

async function main() {
  console.log("MXRE — Fidlar AVA Recorder Ingestion\n");

  let counties = FIDLAR_AVA_COUNTIES;
  if (countyFilter) counties = counties.filter(c => c.county_name.toLowerCase() === countyFilter.toLowerCase());
  if (stateFilter) counties = counties.filter(c => c.state === stateFilter.toUpperCase());
  if (!runAll && !countyFilter && !stateFilter) {
    console.log("Usage: --county=Name --state=ST --days=N | --all --days=N");
    console.log("\nAvailable counties:");
    for (const c of FIDLAR_AVA_COUNTIES) console.log(`  ${c.county_name}, ${c.state}`);
    return;
  }

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - daysBack);

  // Build daily chunks to stay under Fidlar's ~200 result page cap.
  // Weekly windows on high-volume counties (Linn IA: 700+/week) hit the cap and lose data.
  // Daily chunks guarantee <200 results per query for all counties.
  function buildDailyChunks(from: Date, to: Date): Array<{ start: string; end: string }> {
    const chunks: Array<{ start: string; end: string }> = [];
    const cursor = new Date(from);
    while (cursor <= to) {
      const day = cursor.toISOString().split("T")[0];
      chunks.push({ start: day, end: day });
      cursor.setDate(cursor.getDate() + 1);
    }
    return chunks;
  }

  const chunks = buildDailyChunks(startDate, endDate);
  console.log(`  Date range: ${chunks[0].start} to ${chunks[chunks.length - 1].end} (${chunks.length} daily chunks)`);

  const adapter = new FidlarAvaAdapter();
  await adapter.init();

  let grandTotal = 0, grandAmounts = 0;

  for (const config of counties) {
    console.log(`\n━━━ ${config.county_name} County, ${config.state} ━━━`);

    let inserted = 0, withAmount = 0, errors = 0;

    for (const chunk of chunks) {
      console.log(`\n  Chunk: ${chunk.start} to ${chunk.end}`);

      try {
        const batch: Array<Record<string, unknown>> = [];

        for await (const doc of adapter.fetchDocuments(config, chunk.start, chunk.end, (p) => {
          process.stdout.write(`\r  Found: ${p.total_found} | Processed: ${p.total_processed} | Errors: ${p.errors}`);
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

          if (doc.consideration && doc.consideration > 0) {
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
          if (batch.length >= 50) {
            const { error } = await db.from("mortgage_records").insert(batch);
            if (error) { console.error(`\n  Insert: ${error.message.slice(0, 80)}`); errors++; }
            else inserted += batch.length;
            batch.length = 0;
          }
        }

        if (batch.length > 0) {
          const { error } = await db.from("mortgage_records").insert(batch);
          if (error) { console.error(`\n  Insert: ${error.message.slice(0, 80)}`); errors++; }
          else inserted += batch.length;
        }
      } catch (err: any) {
        console.error(`\n  Error in chunk ${chunk.start}-${chunk.end}: ${err.message.slice(0, 100)}`);
      }
    }

    console.log(`\n  Inserted: ${inserted} | With amounts: ${withAmount} | Errors: ${errors}`);
    grandTotal += inserted;
    grandAmounts += withAmount;
  }

  await adapter.close();

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n  Grand total: ${grandTotal} inserted | ${grandAmounts} with amounts`);
  console.log(`  Total mortgage_records in DB: ${count}`);
}

main().catch(console.error);
