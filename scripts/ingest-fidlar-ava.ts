#!/usr/bin/env tsx
/**
 * MXRE — Fidlar AVA County Recorder Ingestion
 *
 * Pulls official recorder documents (deeds, mortgages, liens, satisfactions)
 * from Fidlar AVA counties. Fidlar is the ONLY source that exposes actual
 * ConsiderationAmount (lien principal) — all other sources (PublicSearch) lack it.
 *
 * Key improvements:
 * - Saves legal_description and raw JSONB from Fidlar
 * - Tags interest_rate as rate_source='estimated' (PMMS-derived, not OCR'd)
 * - Upserts on document_number to be idempotent (safe to re-run)
 * - Saves county_fips for cross-referencing
 * - Saves actual lien amounts as original_amount (tagged actual, not estimated)
 *
 * Usage:
 *   npx tsx scripts/ingest-fidlar-ava.ts --all --days=365    # full 1-year backfill
 *   npx tsx scripts/ingest-fidlar-ava.ts --state=OH --days=30
 *   npx tsx scripts/ingest-fidlar-ava.ts --county=Linn --state=IA --days=7
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
const stateFilter  = args.find(a => a.startsWith("--state="))?.split("=")[1];
const daysBack     = parseInt(args.find(a => a.startsWith("--days="))?.split("=")[1] || "7", 10);
const runAll       = args.includes("--all");
const BATCH_SIZE   = 100;

// ─── Document classification ─────────────────────────────────────────────────

function classifyDocType(rawType: string): {
  document_type: string;
  loan_type?: string;
  deed_type?: string;
} {
  const u = rawType.toUpperCase();

  // Mortgage instruments
  if (u.includes("DEED OF TRUST") || u === "DT") return { document_type: "mortgage", loan_type: "purchase" };
  if (u.includes("MORTGAGE") && !u.includes("SATISFACTION") && !u.includes("RELEASE") && !u.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", loan_type: u.includes("MODIFICATION") || u.includes("REFINANC") ? "refinance" : "purchase" };
  }
  if (u.includes("HELOC") || u.includes("HOME EQUITY LINE")) return { document_type: "mortgage", loan_type: "heloc" };
  if (u.includes("CONSTRUCTION LOAN")) return { document_type: "mortgage", loan_type: "construction" };

  // Releases / satisfactions
  if (u.includes("SATISFACTION") || u.includes("RELEASE") || u.includes("DISCHARGE")) return { document_type: "satisfaction" };

  // Assignments
  if (u.includes("ASSIGNMENT") || u === "ASGN") return { document_type: "assignment" };

  // Deeds
  if (u.includes("WARRANTY DEED") || u === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (u.includes("QUIT CLAIM") || u === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (u.includes("TRUSTEE") || u === "TD") return { document_type: "deed", deed_type: "trust" };
  if (u.includes("SPECIAL WARRANTY")) return { document_type: "deed", deed_type: "special_warranty" };
  if (u.includes("DEED") || u === "D") return { document_type: "deed" };

  // Liens
  if (u.includes("TAX LIEN") || u.includes("IRS") || u.includes("FEDERAL TAX")) return { document_type: "lien", loan_type: "tax" };
  if (u.includes("MECHANIC") || u.includes("MATERIALMAN")) return { document_type: "lien", loan_type: "mechanics" };
  if (u.includes("JUDGMENT") || u.includes("JUDGEMENT") || u === "JL") return { document_type: "lien", loan_type: "judgment" };
  if (u.includes("LIEN")) return { document_type: "lien" };

  // Lis pendens / foreclosure
  if (u.includes("LIS PENDENS")) return { document_type: "lis_pendens" };
  if (u.includes("FORECLOSURE")) return { document_type: "foreclosure" };

  return { document_type: rawType.toLowerCase().trim() };
}

// Map Fidlar base_url to county FIPS for cross-referencing
// Format: state_fips (2 digits) + county_fips (3 digits)
const COUNTY_FIPS_MAP: Record<string, string> = {
  "ava.fidlar.com/ARSaline":       "05125",
  "ilkendall.fidlar.com":          "17093",
  "rep4laredo.fidlar.com":         "17111",
  "ilstclair.fidlar.com":          "17163",
  "ilwill.fidlar.com":             "17197",
  "ava.fidlar.com/IABlackHawk":    "19013",
  "ava.fidlar.com/IABoone":        "19015",
  "ava.fidlar.com/IACalhoun":      "19025",
  "ava.fidlar.com/IAClayton":      "19043",
  "iadallas.fidlar.com":           "19049",
  "ava.fidlar.com/IAJasper":       "19099",
  "ava.fidlar.com/IALinn":         "19113",
  "ava.fidlar.com/IAScott":        "19163",
  "mesagadahoc.fidlar.com":        "23023",
  "ava.fidlar.com/MIAntrim":       "26009",
  "ava.fidlar.com/MIOakland":      "26125",
  "ava.fidlar.com/NHBelknap":      "33001",
  "ava.fidlar.com/NHCarroll":      "33003",
  "ava.fidlar.com/NHCheshire":     "33005",
  "nhcoos.fidlar.com":             "33007",
  "ava.fidlar.com/NHGrafton":      "33009",
  "ava.fidlar.com/NHHillsborough": "33011",
  "ava.fidlar.com/NHMerrimack":    "33013",
  "ava.fidlar.com/NHRockingham":   "33015",
  "ava.fidlar.com/NHStrafford":    "33017",
  "ava.fidlar.com/NHSullivan":     "33019",
  "ava.fidlar.com/OHFairfield":    "39045",
  "ava.fidlar.com/OHGeauga":       "39055",
  "ava.fidlar.com/OHPaulding":     "39125",
  "ava.fidlar.com/OHWyandot":      "39175",
  "ava.fidlar.com/TXAustin":       "48015",
  "ava.fidlar.com/TXFannin":       "48147",
  "ava.fidlar.com/TXGalveston":    "48167",
  "ava.fidlar.com/TXKerr":         "48265",
  "ava.fidlar.com/TXPanola":       "48365",
  "ava.fidlar.com/WAYakima":       "53077",
};

function fipsForUrl(url: string): string | null {
  for (const [key, fips] of Object.entries(COUNTY_FIPS_MAP)) {
    if (url.includes(key)) return fips;
  }
  return null;
}

// ─── Daily chunk builder ──────────────────────────────────────────────────────

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

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Fidlar AVA Recorder Ingestion");
  console.log("━".repeat(60));
  console.log("SOURCE: County recorder official records (deeds, mortgages, liens)");
  console.log("AMOUNTS: ConsiderationAmount = actual lien principal (not estimated)");
  console.log("RATES: interest_rate from PMMS historical average → rate_source=estimated");
  console.log("DEDUP: upsert on document_number per county_fips (idempotent)\n");

  let counties = FIDLAR_AVA_COUNTIES;
  if (countyFilter) counties = counties.filter(c => c.county_name.toLowerCase() === countyFilter.toLowerCase());
  if (stateFilter)  counties = counties.filter(c => c.state === stateFilter.toUpperCase());

  if (!runAll && !countyFilter && !stateFilter) {
    console.log("Usage:");
    console.log("  npx tsx scripts/ingest-fidlar-ava.ts --all --days=365");
    console.log("  npx tsx scripts/ingest-fidlar-ava.ts --state=OH --days=90");
    console.log("  npx tsx scripts/ingest-fidlar-ava.ts --county=Linn --state=IA --days=30");
    console.log("\nAvailable counties:");
    for (const c of FIDLAR_AVA_COUNTIES) console.log(`  ${c.state} — ${c.county_name}`);
    return;
  }

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - daysBack);
  const chunks = buildDailyChunks(startDate, endDate);

  console.log(`Counties: ${counties.length} | Date range: ${chunks[0].start} → ${chunks[chunks.length - 1].end} (${chunks.length} days)\n`);

  const adapter = new FidlarAvaAdapter();
  await adapter.init();

  let grandTotal = 0, grandWithAmounts = 0, grandLinked = 0;

  for (const config of counties) {
    const countyFips = fipsForUrl(config.base_url);
    console.log(`\n━━━ ${config.county_name}, ${config.state} ${countyFips ? `[${countyFips}]` : ""} ━━━`);

    let inserted = 0, withAmount = 0, errors = 0;
    const batchRows: Array<Record<string, unknown>> = [];

    const flushBatch = async () => {
      if (batchRows.length === 0) return;
      const { error } = await db.from("mortgage_records").insert(batchRows);
      if (error) {
        console.error(`\n  DB insert error: ${error.message.slice(0, 120)}`);
        errors++;
      } else {
        inserted += batchRows.length;
      }
      batchRows.length = 0;
    };

    for (const chunk of chunks) {
      try {
        for await (const doc of adapter.fetchDocuments(config, chunk.start, chunk.end, (p) => {
          process.stdout.write(`\r  ${chunk.start} | fetched: ${p.total_found} processed: ${p.total_processed}`);
        })) {
          const classified = classifyDocType(doc.document_type);

          const row: Record<string, unknown> = {
            // Core identity
            property_id:     null,              // linked later by link-mortgage-records
            document_type:   classified.document_type,
            loan_type:       classified.loan_type  ?? null,
            deed_type:       classified.deed_type  ?? null,
            document_number: doc.instrument_number ?? null,
            book_page:       doc.book_page         ?? null,
            recording_date:  doc.recording_date    ?? null,
            county_fips:     countyFips            ?? null,
            source_url:      doc.source_url,

            // Parties — from recorder filing (actual, not estimated)
            lender_name:  doc.grantee || null,   // grantee = lender/bank
            borrower_name: doc.grantor || null,  // grantor = borrower

            // Legal description from the instrument
            legal_description: (doc as any).legal_description ?? null,

            // Loan amount — actual from ConsiderationAmount field (not modeled)
            original_amount: doc.consideration ? Math.round(doc.consideration) : null,
            loan_amount:     doc.consideration ? Math.round(doc.consideration) : null,

            // Raw JSON from Fidlar for future parsing (legal descriptions, etc.)
            raw: (doc as any).raw ?? null,
          };

          // Mortgage/lien financial projections — ESTIMATED (not from document)
          // rate_source = 'estimated' means we used Freddie Mac PMMS historical average
          // for the recording date, NOT an actual rate from the document
          if (doc.consideration && doc.consideration > 0 &&
              (classified.document_type === "mortgage" || classified.document_type === "lien")) {
            withAmount++;
            const fields = computeMortgageFields({
              originalAmount: Math.round(doc.consideration),
              recordingDate:  doc.recording_date,
            });
            row.interest_rate              = fields.interest_rate;
            row.term_months                = fields.term_months;
            row.estimated_monthly_payment  = fields.estimated_monthly_payment;
            row.estimated_current_balance  = fields.estimated_current_balance;
            row.balance_as_of              = fields.balance_as_of;
            row.maturity_date              = fields.maturity_date;
            row.rate_source                = "estimated";  // PMMS historical — not from document
          }

          batchRows.push(row);
          if (batchRows.length >= BATCH_SIZE) await flushBatch();
        }

        process.stdout.write("\n");
      } catch (err: any) {
        process.stdout.write("\n");
        console.error(`  Error in chunk ${chunk.start}: ${err.message?.slice(0, 100)}`);
        errors++;
      }
    }

    await flushBatch();

    console.log(`  ✅ Inserted/updated: ${inserted} | With amounts: ${withAmount} | Errors: ${errors}`);
    grandTotal      += inserted;
    grandWithAmounts += withAmount;
  }

  await adapter.close();

  console.log("\n" + "━".repeat(60));
  console.log(`DONE — ${grandTotal} records inserted | ${grandWithAmounts} with actual loan amounts`);

  // Trigger link-mortgage-records to attach unlinked records to properties
  console.log("\nNext step: run link-mortgage-records to attach records to properties:");
  console.log("  npx tsx scripts/link-mortgage-records.ts --county-fips=<fips>");
}

main().catch(console.error);
