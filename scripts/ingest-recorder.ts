#!/usr/bin/env tsx
/**
 * MXRE Recorder Ingest — Florida Clerk of Court Official Records
 *
 * Pulls real mortgage/deed records from Florida county Landmark Web portals
 * and inserts them into the mortgage_records table, linked to existing properties.
 *
 * Usage:
 *   npx tsx scripts/ingest-recorder.ts --county="Palm Beach"
 *   npx tsx scripts/ingest-recorder.ts --county="Palm Beach" --from=2024-01-01 --to=2024-12-31
 *   npx tsx scripts/ingest-recorder.ts --county="Palm Beach" --days=30
 *
 * Records are linked to properties by matching grantor/grantee names to owner_name.
 * Unmatched records are still stored with property_id=0 for later linking.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { LandmarkWebAdapter, LANDMARK_COUNTIES, type RecorderDocument, type LandmarkCountyConfig } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

const countyFilter = getArg("county");
const fromDate = getArg("from");
const toDate = getArg("to");
const daysBack = parseInt(getArg("days") || "30", 10);

if (!countyFilter) {
  console.log("Usage: npx tsx scripts/ingest-recorder.ts --county=\"Palm Beach\" [--from=2024-01-01] [--to=2024-12-31] [--days=30]");
  console.log("\nAvailable Landmark Web counties:");
  for (const c of LANDMARK_COUNTIES) {
    console.log(`  ${c.county_name} — ${c.base_url}`);
  }
  process.exit(1);
}

// ─── Database ──────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── Date Range ────────────────────────────────────────────────────

const endDate = toDate || new Date().toISOString().split("T")[0];
let startDate: string;
if (fromDate) {
  startDate = fromDate;
} else {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  startDate = d.toISOString().split("T")[0];
}

// ─── Property Matching ─────────────────────────────────────────────

/**
 * Try to find a matching property by owner name.
 * Searches across ALL FL properties since they're currently all under county_id=12.
 * Returns property_id or null if no match found.
 */
async function findPropertyByOwner(ownerName: string, _countyId: number): Promise<number | null> {
  if (!ownerName || ownerName.length < 3) return null;

  // Clean the name — Landmark often concatenates multiple names without separator
  const cleanName = ownerName.replace(/\s+/g, " ").trim();

  // Try exact match across all FL properties
  const { data } = await db
    .from("properties")
    .select("id")
    .eq("state_code", "FL")
    .ilike("owner_name", cleanName)
    .limit(1);

  if (data?.length) return data[0].id;

  // Try partial match on last name (first word)
  const lastName = cleanName.split(/[,\s]+/)[0];
  if (lastName.length >= 3) {
    const { data: partial } = await db
      .from("properties")
      .select("id")
      .eq("state_code", "FL")
      .ilike("owner_name", `${lastName}%`)
      .limit(1);
    if (partial?.length) return partial[0].id;
  }

  return null;
}

/**
 * Check if a mortgage record already exists (dedup by book_page or document_number).
 */
async function isDuplicate(doc: RecorderDocument): Promise<boolean> {
  if (doc.book_page) {
    const { data } = await db
      .from("mortgage_records")
      .select("id")
      .eq("book_page", doc.book_page)
      .eq("source_url", doc.source_url)
      .limit(1);
    if (data?.length) return true;
  }
  if (doc.instrument_number) {
    const { data } = await db
      .from("mortgage_records")
      .select("id")
      .eq("document_number", doc.instrument_number)
      .eq("source_url", doc.source_url)
      .limit(1);
    if (data?.length) return true;
  }
  return false;
}

/**
 * Classify document type into our schema categories.
 */
function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType.toUpperCase();

  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return {
      document_type: "mortgage",
      loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase",
    };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) {
    return { document_type: "satisfaction" };
  }
  if (upper.includes("ASSIGNMENT")) {
    return { document_type: "assignment" };
  }
  if (upper.includes("WARRANTY DEED") || upper === "WD") {
    return { document_type: "deed", deed_type: "warranty" };
  }
  if (upper.includes("QUIT CLAIM") || upper === "QCD") {
    return { document_type: "deed", deed_type: "quitclaim" };
  }
  if (upper.includes("SPECIAL WARRANTY")) {
    return { document_type: "deed", deed_type: "special_warranty" };
  }
  if (upper.includes("TRUST")) {
    return { document_type: "deed", deed_type: "trust" };
  }
  if (upper.includes("TAX DEED")) {
    return { document_type: "deed", deed_type: "tax" };
  }
  if (upper.includes("DEED")) {
    return { document_type: "deed" };
  }

  return { document_type: rawType.toLowerCase() };
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  console.log(`\nMXRE Recorder Ingest — Florida Landmark Web`);
  console.log(`${"─".repeat(55)}`);
  console.log(`DB: ${SUPABASE_URL}`);
  console.log(`County: ${countyFilter}`);
  console.log(`Date range: ${startDate} to ${endDate}`);
  console.log();

  // Find county config
  const countyConfig = LANDMARK_COUNTIES.find(
    (c) => c.county_name.toLowerCase() === countyFilter.toLowerCase(),
  );
  if (!countyConfig) {
    console.error(`County "${countyFilter}" not found in Landmark Web registry.`);
    console.log("Available counties:", LANDMARK_COUNTIES.map((c) => c.county_name).join(", "));
    process.exit(1);
  }

  // Look up or create county in DB
  const { data: dbCounty } = await db
    .from("counties")
    .select("id")
    .eq("county_name", countyConfig.county_name)
    .eq("state_code", "FL")
    .single();

  if (dbCounty) {
    countyConfig.county_id = dbCounty.id;
  } else {
    console.log(`  Creating county record for ${countyConfig.county_name}, FL...`);
    const { data: newCounty } = await db
      .from("counties")
      .insert({
        county_name: countyConfig.county_name,
        state_code: "FL",
        state_fips: "12",
        county_fips: "000", // Will need proper FIPS
        recorder_url: countyConfig.base_url,
        active: true,
      })
      .select()
      .single();
    if (newCounty) countyConfig.county_id = newCounty.id;
  }

  console.log(`  County ID: ${countyConfig.county_id}`);
  console.log(`  Portal: ${countyConfig.base_url}`);
  console.log();

  // Initialize adapter
  const adapter = new LandmarkWebAdapter();
  console.log("Launching browser...");
  await adapter.init();

  let inserted = 0;
  let skipped = 0;
  let duplicates = 0;
  let unmatched = 0;

  try {
    const docStream = adapter.fetchDocuments(
      countyConfig,
      startDate,
      endDate,
      undefined, // Search all doc types, filter in post-processing
      (progress) => {
        process.stdout.write(
          `\r  ${progress.current_date} | Found: ${progress.total_found} | Inserted: ${inserted} | Skipped: ${skipped} | Dupes: ${duplicates} | Errors: ${progress.errors}   `,
        );
      },
    );

    const batch: Array<Record<string, unknown>> = [];
    const BATCH_SIZE = 50;

    for await (const doc of docStream) {
      // Dedup
      if (await isDuplicate(doc)) {
        duplicates++;
        continue;
      }

      // Classify
      const classified = classifyDocType(doc.document_type);

      // Try to match to a property
      const propertyId = await findPropertyByOwner(doc.grantor, countyConfig.county_id)
        || await findPropertyByOwner(doc.grantee, countyConfig.county_id);

      if (!propertyId) {
        unmatched++;
      }

      // Build mortgage record
      const record: Record<string, unknown> = {
        property_id: propertyId || null,
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

      // Compute mortgage math if we have an amount and it's a mortgage
      if (classified.document_type === "mortgage" && doc.consideration && doc.recording_date) {
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

      batch.push(record);

      // Flush batch
      if (batch.length >= BATCH_SIZE) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (error) {
          console.error(`\n  Batch insert error: ${error.message}`);
        } else {
          inserted += batch.length;
        }
        batch.length = 0;
      }
    }

    // Flush remaining
    if (batch.length > 0) {
      const { error } = await db.from("mortgage_records").insert(batch);
      if (error) {
        console.error(`\n  Final batch error: ${error.message}`);
      } else {
        inserted += batch.length;
      }
    }
  } finally {
    await adapter.close();
  }

  console.log(`\n\n${"─".repeat(55)}`);
  console.log(`Results:`);
  console.log(`  Inserted: ${inserted}`);
  console.log(`  Duplicates: ${duplicates}`);
  console.log(`  Unmatched (skipped): ${unmatched}`);
  console.log(`  Other skipped: ${skipped - unmatched}`);
  console.log(`Done.\n`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
