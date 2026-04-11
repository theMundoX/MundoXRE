#!/usr/bin/env tsx
/**
 * MXRE — Indiana Recorder Ingest (Fidlar DirectSearch)
 *
 * Pulls mortgage, deed, and lien records from Indiana counties that expose
 * free anonymous access via the Fidlar DirectSearch platform.
 *
 * Free counties: Marion (Indianapolis), Allen (Fort Wayne), St. Joseph
 *                (South Bend), Porter (Valparaiso), Floyd (New Albany)
 *
 * Anonymous cap: 200 results per search. The adapter automatically splits
 * date windows until each fits under the cap.
 *
 * Usage:
 *   npx tsx scripts/ingest-indiana-recorder.ts
 *   npx tsx scripts/ingest-indiana-recorder.ts --county=Marion
 *   npx tsx scripts/ingest-indiana-recorder.ts --from=2020-01-01 --to=2025-12-31
 *   npx tsx scripts/ingest-indiana-recorder.ts --county=Marion --from=2024-01-01 --days=90
 *   npx tsx scripts/ingest-indiana-recorder.ts --dry-run
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import {
  FidlarDirectSearchAdapter,
  DIRECT_SEARCH_COUNTIES,
  type DirectSearchCountyDef,
  type RecorderDocument,
} from "../src/discovery/adapters/fidlar-direct-search.js";

// ─── CLI Args ──────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  return args.find(a => a.startsWith(`--${name}=`))?.split("=")[1];
}
const hasFlag = (f: string) => args.includes(`--${f}`);

const countyFilter = getArg("county")?.toLowerCase();
const fromArg = getArg("from");
const toArg = getArg("to");
const daysBack = parseInt(getArg("days") ?? "30", 10);
const dryRun = hasFlag("dry-run");

const endDate = toArg ?? new Date().toISOString().split("T")[0];
const startDate = fromArg ?? (() => {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  return d.toISOString().split("T")[0];
})();

// ─── Database ──────────────────────────────────────────────────────────────

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
  { auth: { persistSession: false } },
);

// ─── Doc type classifier ───────────────────────────────────────────────────

function classifyDocType(raw: string): string {
  const u = (raw ?? "").toUpperCase();
  if ((u.includes("MORTGAGE") || u === "MTG") &&
      !u.includes("SATISFACTION") && !u.includes("RELEASE") && !u.includes("ASSIGNMENT"))
    return "mortgage";
  if (u.includes("SATISFACTION") || u.includes("RELEASE") || u.includes("DISCHARGE"))
    return "satisfaction";
  if (u.includes("ASSIGNMENT"))
    return "assignment";
  if (u.includes("WARRANTY DEED") || u === "WD")
    return "deed";
  if (u.includes("QUIT CLAIM") || u === "QCD")
    return "quit_claim_deed";
  if (u.includes("DEED"))
    return "deed";
  if (u.includes("MECHANIC") || u.includes("MATERIALMAN"))
    return "mechanics_lien";
  if (u.includes("FEDERAL TAX") || u.includes("IRS LIEN"))
    return "federal_tax_lien";
  if (u.includes("TAX LIEN") || u.includes("STATE TAX"))
    return "state_tax_lien";
  if (u.includes("JUDGMENT") || u.includes("JUDG"))
    return "judgment";
  if (u.includes("LIEN"))
    return "lien";
  return raw.toLowerCase().trim() || "other";
}

// ─── Dedup ─────────────────────────────────────────────────────────────────

async function getExistingDocNumbers(
  instruments: string[],
  sourceUrl: string,
): Promise<Set<string>> {
  const existing = new Set<string>();
  for (let i = 0; i < instruments.length; i += 200) {
    const chunk = instruments.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .eq("source_url", sourceUrl)
      .in("document_number", chunk);
    if (data) for (const r of data) if (r.document_number) existing.add(r.document_number);
  }
  return existing;
}

// ─── County lookup / create ────────────────────────────────────────────────

async function resolveCountyId(county: DirectSearchCountyDef): Promise<number | null> {
  // First: try by FIPS
  if (county.state_fips && county.county_fips) {
    const { data } = await db.from("counties")
      .select("id")
      .eq("state_fips", county.state_fips)
      .eq("county_fips", county.county_fips)
      .single();
    if (data) return data.id as number;
  }

  // Second: try by name + state
  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", county.county_name)
    .eq("state_code", county.state)
    .single();
  if (data) return data.id as number;

  // Create it
  console.log(`  Creating county record: ${county.county_name}, ${county.state}...`);
  const { data: created, error } = await db.from("counties")
    .insert({
      county_name: county.county_name,
      state_code: county.state,
      state_fips: county.state_fips,
      county_fips: county.county_fips,
      active: true,
    })
    .select("id")
    .single();
  if (error) {
    console.error(`  Failed to create county: ${error.message}`);
    return null;
  }
  return created?.id as number;
}

// ─── Process one county ────────────────────────────────────────────────────

const BATCH_SIZE = 200;
const DOC_TYPES = ["MORTGAGE", "DEED", "LIEN", "JUDGMENT", "MECHANICS LIEN"];

async function ingestCounty(
  adapter: FidlarDirectSearchAdapter,
  county: DirectSearchCountyDef,
): Promise<{ inserted: number; dupes: number; errors: number }> {
  const stats = { inserted: 0, dupes: 0, errors: 0 };

  const countyId = await resolveCountyId(county);
  if (!countyId) {
    console.error(`  Cannot resolve county ID for ${county.county_name}`);
    stats.errors++;
    return stats;
  }

  console.log(`  County ID in DB: ${countyId}`);
  console.log(`  Portal: ${county.base_url}`);

  const batch: Array<Record<string, unknown>> = [];

  async function flushBatch() {
    if (batch.length === 0) return;
    if (dryRun) {
      console.log(`  [dry-run] Would insert ${batch.length} records`);
      stats.inserted += batch.length;
      batch.length = 0;
      return;
    }
    // Retry with exponential backoff for connection pool exhaustion
    for (let attempt = 0; attempt < 4; attempt++) {
      const { error } = await db.from("mortgage_records").insert(batch);
      if (!error) {
        stats.inserted += batch.length;
        batch.length = 0;
        return;
      }
      const isRetryable = error.message.includes("pool") || error.message.includes("timeout") || error.message.includes("connection");
      if (!isRetryable || attempt === 3) {
        // Try one-by-one on final failure
        for (const rec of batch) {
          const { error: e2 } = await db.from("mortgage_records").insert(rec);
          if (e2) {
            if (stats.errors < 5) console.error(`  Insert error: ${e2.message.slice(0, 100)}`);
            stats.errors++;
          } else {
            stats.inserted++;
          }
        }
        batch.length = 0;
        return;
      }
      const delay = 2000 * Math.pow(2, attempt);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  const docStream = adapter.fetchDocuments(
    county,
    startDate,
    endDate,
    DOC_TYPES,
    (progress) => {
      process.stdout.write(
        `\r  ${progress.current_date} | found=${progress.total_found} ins=${stats.inserted} dupes=${stats.dupes} err=${progress.errors}   `,
      );
    },
  );

  // Collect a window of docs for batch dedup
  let pendingDocs: RecorderDocument[] = [];

  async function processPending() {
    if (pendingDocs.length === 0) return;

    const instruments = pendingDocs
      .map(d => d.instrument_number)
      .filter((n): n is string => Boolean(n));

    const existing = instruments.length > 0
      ? await getExistingDocNumbers(instruments, county.base_url)
      : new Set<string>();

    for (const doc of pendingDocs) {
      // Dedup by instrument number
      if (doc.instrument_number && existing.has(doc.instrument_number)) {
        stats.dupes++;
        continue;
      }

      const docType = classifyDocType(doc.document_type);

      batch.push({
        property_id: null,
        document_type: docType,
        recording_date: doc.recording_date,
        borrower_name: doc.grantor?.slice(0, 500) || null,
        lender_name: doc.grantee?.slice(0, 500) || null,
        document_number: doc.instrument_number ?? null,
        book_page: doc.book_page ?? null,
        loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
        original_amount: doc.consideration ? Math.round(doc.consideration) : null,
        source_url: county.base_url,
      });

      if (batch.length >= BATCH_SIZE) {
        await flushBatch();
      }
    }

    pendingDocs = [];
  }

  for await (const doc of docStream) {
    pendingDocs.push(doc);
    // Process in chunks of BATCH_SIZE to amortize dedup DB calls
    if (pendingDocs.length >= BATCH_SIZE) {
      await processPending();
    }
  }

  await processPending();
  await flushBatch();

  return stats;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — Indiana Recorder Ingest (Fidlar DirectSearch)");
  console.log("═".repeat(60));
  console.log(`Date range : ${startDate} to ${endDate}`);
  console.log(`Dry run    : ${dryRun}`);
  if (countyFilter) console.log(`County     : ${countyFilter}`);
  console.log();

  const counties = countyFilter
    ? DIRECT_SEARCH_COUNTIES.filter(c => c.county_name.toLowerCase() === countyFilter)
    : DIRECT_SEARCH_COUNTIES;

  if (counties.length === 0) {
    console.error(`No matching county found. Available: ${DIRECT_SEARCH_COUNTIES.map(c => c.county_name).join(", ")}`);
    process.exit(1);
  }

  const adapter = new FidlarDirectSearchAdapter();

  let totalInserted = 0, totalDupes = 0, totalErrors = 0;

  for (const county of counties) {
    console.log(`\n── ${county.county_name} County, ${county.state} ──`);
    try {
      const stats = await ingestCounty(adapter, county);
      console.log(`\n  Done: ${stats.inserted} inserted, ${stats.dupes} dupes, ${stats.errors} errors`);
      totalInserted += stats.inserted;
      totalDupes += stats.dupes;
      totalErrors += stats.errors;
    } catch (err: any) {
      console.error(`\n  Fatal error for ${county.county_name}: ${err.message}`);
      totalErrors++;
    }
  }

  console.log(`\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${totalInserted.toLocaleString()} inserted, ${totalDupes.toLocaleString()} dupes, ${totalErrors} errors`);
  console.log("Done.\n");
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
