#!/usr/bin/env tsx
/**
 * MXRE Daily Recorder Pull
 *
 * Pulls the last 2 days of recordings from all active recorder platforms:
 * - Fidlar AVA: 28 counties (AR, IA, MI, NH, OH, TX, WA) — direct API
 * - LandmarkWeb: 11 FL counties — browser + Consideration search
 *
 * Designed to run as a daily cron: `npx tsx scripts/daily-recorder-pull.ts`
 * Run on the VPS: `cd /opt/mxre && npx tsx scripts/daily-recorder-pull.ts`
 *
 * Usage:
 *   npx tsx scripts/daily-recorder-pull.ts            # Pull last 2 days
 *   npx tsx scripts/daily-recorder-pull.ts --days=1   # Pull yesterday only
 *   npx tsx scripts/daily-recorder-pull.ts --days=7   # Weekly catch-up
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaApiAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava-api.js";
import { LandmarkWebAdapter, LANDMARK_COUNTIES } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const daysBack = parseInt(process.argv.find(a => a.startsWith("--days="))?.split("=")[1] || "2", 10);

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

async function insertBatch(batch: Array<Record<string, unknown>>): Promise<{ inserted: number; errors: number }> {
  if (!batch.length) return { inserted: 0, errors: 0 };
  const { error } = await db.from("mortgage_records").insert(batch);
  if (error) {
    // Retry individually to handle partial dupes
    let inserted = 0, errors = 0;
    for (const record of batch) {
      const { error: e2 } = await db.from("mortgage_records").insert(record);
      if (e2) errors++;
      else inserted++;
    }
    return { inserted, errors };
  }
  return { inserted: batch.length, errors: 0 };
}

function enrichWithMortgageFields(record: Record<string, unknown>, amount: number, recordingDate: string): void {
  const fields = computeMortgageFields({ originalAmount: Math.round(amount), recordingDate });
  record.interest_rate = fields.interest_rate;
  record.term_months = fields.term_months;
  record.estimated_monthly_payment = fields.estimated_monthly_payment;
  record.estimated_current_balance = fields.estimated_current_balance;
  record.balance_as_of = fields.balance_as_of;
  record.maturity_date = fields.maturity_date;
}

async function pullFidlarAVA(startDate: string, endDate: string): Promise<{ inserted: number; withAmounts: number; errors: number }> {
  const adapter = new FidlarAvaApiAdapter();
  let inserted = 0, withAmounts = 0, errors = 0;

  for (const config of FIDLAR_AVA_COUNTIES) {
    const batch: Array<Record<string, unknown>> = [];
    let countyInserted = 0;

    try {
      for await (const doc of adapter.fetchDocuments(config, startDate, endDate)) {
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
          withAmounts++;
          if (classified.document_type === "mortgage" || classified.document_type === "lien") {
            enrichWithMortgageFields(record, doc.consideration, doc.recording_date);
          }
        }

        batch.push(record);
        if (batch.length >= 100) {
          const result = await insertBatch([...batch]);
          countyInserted += result.inserted;
          errors += result.errors;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const result = await insertBatch(batch);
        countyInserted += result.inserted;
        errors += result.errors;
      }
    } catch (err: any) {
      errors++;
    }

    if (countyInserted > 0) {
      console.log(`  Fidlar: ${config.county_name}, ${config.state} — ${countyInserted} new records`);
    }
    inserted += countyInserted;
  }

  return { inserted, withAmounts, errors };
}

async function pullLandmarkWeb(startDate: string, endDate: string): Promise<{ inserted: number; withAmounts: number; errors: number }> {
  const adapter = new LandmarkWebAdapter();
  await adapter.init();
  let inserted = 0, withAmounts = 0, errors = 0;

  for (const config of LANDMARK_COUNTIES) {
    const batch: Array<Record<string, unknown>> = [];
    let countyInserted = 0;

    try {
      for await (const doc of adapter.fetchDocuments(config, startDate, endDate)) {
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
          withAmounts++;
          if (classified.document_type === "mortgage" || classified.document_type === "lien") {
            enrichWithMortgageFields(record, doc.consideration, doc.recording_date);
          }
        }

        batch.push(record);
        if (batch.length >= 100) {
          const result = await insertBatch([...batch]);
          countyInserted += result.inserted;
          errors += result.errors;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const result = await insertBatch(batch);
        countyInserted += result.inserted;
        errors += result.errors;
      }
    } catch (err: any) {
      errors++;
    }

    if (countyInserted > 0) {
      console.log(`  LandmarkWeb: ${config.county_name}, FL — ${countyInserted} new records`);
    }
    inserted += countyInserted;
  }

  await adapter.close();
  return { inserted, withAmounts, errors };
}

async function main() {
  const endDate = new Date().toISOString().split("T")[0];
  const startDate = new Date(Date.now() - daysBack * 86400000).toISOString().split("T")[0];

  console.log(`MXRE Daily Recorder Pull — ${startDate} to ${endDate}`);
  console.log(`Platforms: Fidlar AVA (28 counties) + LandmarkWeb FL (11 counties)\n`);

  const startTime = Date.now();

  // Pull Fidlar AVA (no browser — fast)
  console.log("Pulling Fidlar AVA...");
  const fidlar = await pullFidlarAVA(startDate, endDate);
  console.log(`  Total: ${fidlar.inserted} inserted, ${fidlar.withAmounts} with amounts\n`);

  // Pull LandmarkWeb FL (browser — slower)
  console.log("Pulling LandmarkWeb FL...");
  const lw = await pullLandmarkWeb(startDate, endDate);
  console.log(`  Total: ${lw.inserted} inserted, ${lw.withAmounts} with amounts\n`);

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const totalInserted = fidlar.inserted + lw.inserted;
  const totalAmounts = fidlar.withAmounts + lw.withAmounts;

  // Get DB totals
  const { count: totalLiens } = await db.from("mortgage_records").select("*", { count: "exact", head: true });

  console.log(`═══════════════════════════════════════════════════`);
  console.log(`  Daily pull complete in ${elapsed}s`);
  console.log(`  New records: ${totalInserted} | With amounts: ${totalAmounts}`);
  console.log(`  Total lien records in DB: ${totalLiens?.toLocaleString()}`);
  console.log(`═══════════════════════════════════════════════════`);
}

main().catch(console.error);
