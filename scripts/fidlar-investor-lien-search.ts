#!/usr/bin/env tsx
/**
 * MXRE — Fidlar investor lien back-fill (Marion County, IN)
 *
 * The anonymous Fidlar DirectSearch API caps date-range searches at 200 results/day,
 * giving ~50% coverage. BUT the `LastBusinessName` param has no such cap — it returns
 * ALL documents for a named entity across all time.
 *
 * This script pulls every unique corporate owner name from Marion County properties
 * and searches Fidlar for their complete recorded document history (mortgages, deeds,
 * liens, satisfactions). Investor-owned properties are the core MXRE target market,
 * so this fills the most valuable coverage gap for free.
 *
 * Usage:
 *   npx tsx scripts/fidlar-investor-lien-search.ts
 *   npx tsx scripts/fidlar-investor-lien-search.ts --limit=500      # first 500 entities
 *   npx tsx scripts/fidlar-investor-lien-search.ts --from-year=2018 # only since 2018
 *   npx tsx scripts/fidlar-investor-lien-search.ts --dry-run
 *   npx tsx scripts/fidlar-investor-lien-search.ts --name="BEDSON PEAK"  # single entity
 *   npx tsx scripts/fidlar-investor-lien-search.ts --name-source=on-market
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import {
  FidlarDirectSearchAdapter,
  DIRECT_SEARCH_COUNTIES,
} from "../src/discovery/adapters/fidlar-direct-search.js";

// ─── CLI args ──────────────────────────────────────────────────────────────

const argv  = process.argv.slice(2);
const getArg  = (n: string) => argv.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => argv.includes(`--${n}`);

const DRY_RUN   = hasFlag("dry-run");
const LIMIT     = getArg("limit") ? parseInt(getArg("limit")!) : Infinity;
const FROM_YEAR = parseInt(getArg("from-year") ?? "2000");
const TO_YEAR   = parseInt(getArg("to-year") ?? String(new Date().getFullYear()));
const NAME_ARG  = getArg("name");   // single entity override
const NAME_SOURCE = getArg("name-source") ?? "corporate";
const MAX_RUN_MS = getArg("max-run-ms") ? parseInt(getArg("max-run-ms")!, 10) : 0;
const PER_ENTITY_TIMEOUT_MS = getArg("per-entity-timeout-ms") ? parseInt(getArg("per-entity-timeout-ms")!, 10) : 90_000;

// ─── DB ────────────────────────────────────────────────────────────────────

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
  { auth: { persistSession: false } },
);

const MARION_COUNTY_ID = 797583;
const SOURCE_URL = "https://inmarion.fidlar.com/INMarion/DirectSearch/";

// ─── Helpers ───────────────────────────────────────────────────────────────

function classifyDocType(raw: string): string {
  const u = (raw ?? "").toUpperCase();
  if (u.includes("MORTGAGE") && !u.includes("SATISF") && !u.includes("RELEASE") && !u.includes("ASSIGN"))
    return "mortgage";
  if (u.includes("SATISF") || u.includes("RELEASE") || u.includes("DISCHARGE"))
    return "satisfaction";
  if (u.includes("ASSIGN")) return "assignment";
  if (u.includes("WARRANTY DEED") || u === "WD")  return "deed";
  if (u.includes("QUIT CLAIM") || u === "QCD")    return "quit_claim_deed";
  if (u.includes("DEED"))          return "deed";
  if (u.includes("MECHANIC"))      return "mechanics_lien";
  if (u.includes("FEDERAL TAX") || u.includes("IRS LIEN")) return "federal_tax_lien";
  if (u.includes("TAX LIEN"))      return "state_tax_lien";
  if (u.includes("JUDGMENT"))      return "judgment";
  if (u.includes("LIEN"))          return "lien";
  return raw.toLowerCase().trim() || "other";
}

/** Normalise entity name: strip legal suffixes for broader matching */
function normaliseForSearch(name: string): string {
  return name
    .replace(/\s+(LLC|INC\.?|CORP\.?|LTD\.?|LP|LLP|PLLC|PC|CO\.?|DBA.*)$/i, "")
    .trim();
}

/**
 * Load distinct corporate owner names from Marion County.
 * Prioritises names we haven't searched yet (no existing mortgage_records for that
 * entity) so re-runs are incremental.
 */
async function loadEntityNames(): Promise<string[]> {
  if (NAME_ARG) return [NAME_ARG.toUpperCase()];

  if (NAME_SOURCE === "on-market") {
    const PAGE = 1000;
    const names = new Set<string>();
    let offset = 0;

    while (true) {
      const { data, error } = await db
        .from("listing_signals")
        .select("property_id, properties!inner(owner_name, county_id)")
        .eq("is_on_market", true)
        .eq("state_code", "IN")
        .eq("city", "INDIANAPOLIS")
        .not("property_id", "is", null)
        .range(offset, offset + PAGE - 1);

      if (error) throw new Error(`DB: ${error.message}`);
      if (!data || data.length === 0) break;

      for (const row of data as Array<{ properties?: { owner_name?: string | null; county_id?: number | null } }>) {
        const property = row.properties;
        if (property?.county_id === MARION_COUNTY_ID && property.owner_name) {
          names.add(property.owner_name.trim().toUpperCase());
        }
      }

      offset += data.length;
      if (data.length < PAGE) break;
    }

    return [...names];
  }

  if (NAME_SOURCE !== "corporate") {
    throw new Error(`Invalid --name-source=${NAME_SOURCE}. Use corporate or on-market.`);
  }

  // Pull all unique owner names where corporate_owned = true
  const PAGE = 1000;
  const names = new Set<string>();
  let lastId = 0;

  while (true) {
    const { data, error } = await db
      .from("properties")
      .select("id, owner_name")
      .eq("county_id", MARION_COUNTY_ID)
      .eq("corporate_owned", true)
      .not("owner_name", "is", null)
      .gt("id", lastId)
      .order("id", { ascending: true })
      .limit(PAGE);

    if (error) throw new Error(`DB: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const r of data) {
      if (r.owner_name) names.add((r.owner_name as string).trim().toUpperCase());
    }
    lastId = data[data.length - 1].id;
    if (data.length < PAGE) break;
  }

  return [...names];
}

/** Check which document numbers already exist so we can skip them */
async function existingDocNums(nums: string[]): Promise<Set<string>> {
  const existing = new Set<string>();
  for (let i = 0; i < nums.length; i += 200) {
    const chunk = nums.slice(i, i + 200);
    const { data } = await db
      .from("mortgage_records")
      .select("document_number")
      .eq("source_url", SOURCE_URL)
      .in("document_number", chunk);
    if (data) for (const r of data) existing.add(r.document_number!);
  }
  return existing;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — Fidlar investor lien back-fill (Marion County)");
  console.log("═".repeat(60));
  console.log(`Years    : ${FROM_YEAR}–${TO_YEAR}`);
  console.log(`Dry run  : ${DRY_RUN}`);
  console.log(`Limit    : ${LIMIT === Infinity ? "all" : LIMIT} entities`);
  console.log(`Names    : ${NAME_SOURCE}`);
  if (MAX_RUN_MS > 0) console.log(`Max run  : ${MAX_RUN_MS}ms`);
  console.log(`Timeout  : ${PER_ENTITY_TIMEOUT_MS}ms/entity`);
  console.log();

  const marionConfig = DIRECT_SEARCH_COUNTIES.find(c => c.county_name === "Marion")!;
  const adapter = new FidlarDirectSearchAdapter();

  console.log("Loading entity names from DB...");
  const allNames = await loadEntityNames();
  const names = LIMIT < Infinity ? allNames.slice(0, LIMIT) : allNames;
  console.log(`  ${allNames.length.toLocaleString()} unique entities → processing ${names.length.toLocaleString()}`);
  console.log();

  let totalEntities = 0, totalDocs = 0, totalInserted = 0, totalDupes = 0;
  const startedAt = Date.now();
  const batch: Record<string, unknown>[] = [];

  async function flushBatch() {
    if (batch.length === 0) return;
    if (DRY_RUN) { totalInserted += batch.length; batch.length = 0; return; }
    const { error } = await db.from("mortgage_records").insert(batch);
    if (error) {
      if (error.message.includes("duplicate") || error.message.includes("unique")) {
        // Try one-by-one to salvage non-dupes
        for (const rec of batch) {
          const { error: e2 } = await db.from("mortgage_records").insert(rec);
          if (!e2) totalInserted++;
          else totalDupes++;
        }
      } else {
        console.error(`  Insert error: ${error.message.slice(0, 100)}`);
      }
    } else {
      totalInserted += batch.length;
    }
    batch.length = 0;
  }

  for (const entityName of names) {
    if (MAX_RUN_MS > 0 && Date.now() - startedAt > MAX_RUN_MS) {
      console.log(`\nReached max runtime ${MAX_RUN_MS}ms; stopping cleanly.`);
      break;
    }

    totalEntities++;
    const searchName = normaliseForSearch(entityName);
    if (!searchName) continue;

    process.stdout.write(`\r  [${totalEntities}/${names.length}] ${searchName.slice(0, 40).padEnd(40)} docs=${totalDocs} ins=${totalInserted}   `);

    const docsThisEntity: { instrument: string; doc: Record<string, unknown> }[] = [];

    try {
      for await (const doc of withAsyncTimeout(
        adapter.fetchByBusinessName(marionConfig, searchName, FROM_YEAR, TO_YEAR),
        PER_ENTITY_TIMEOUT_MS,
        `Timed out searching ${searchName}`,
      )) {
        totalDocs++;
        docsThisEntity.push({
          instrument: doc.instrument_number ?? "",
          doc: {
            property_id:     null,
            document_type:   classifyDocType(doc.document_type),
            recording_date:  doc.recording_date,
            borrower_name:   doc.grantor?.slice(0, 500) || null,
            lender_name:     doc.grantee?.slice(0, 500) || null,
            document_number: doc.instrument_number ?? null,
            book_page:       doc.book_page ?? null,
            loan_amount:     doc.consideration ? Math.round(doc.consideration) : null,
            original_amount: doc.consideration ? Math.round(doc.consideration) : null,
            source_url:      SOURCE_URL,
          },
        });
      }
    } catch (err: any) {
      console.error(`\n  Error for "${searchName}": ${err.message?.slice(0, 80)}`);
      continue;
    }

    if (docsThisEntity.length === 0) continue;

    // Dedup against existing records
    const instrNums = docsThisEntity.map(d => d.instrument).filter(Boolean);
    const existing = instrNums.length > 0 ? await existingDocNums(instrNums) : new Set<string>();

    for (const { instrument, doc } of docsThisEntity) {
      if (instrument && existing.has(instrument)) { totalDupes++; continue; }
      batch.push(doc);
      if (batch.length >= 200) await flushBatch();
    }
  }

  await flushBatch();

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`Entities searched : ${totalEntities.toLocaleString()}`);
  console.log(`Documents found   : ${totalDocs.toLocaleString()}`);
  console.log(`Inserted          : ${totalInserted.toLocaleString()}`);
  console.log(`Dupes skipped     : ${totalDupes.toLocaleString()}`);
  console.log("Done.\n");
}

async function* withAsyncTimeout<T>(iterable: AsyncIterable<T>, ms: number, message: string): AsyncGenerator<T> {
  const iterator = iterable[Symbol.asyncIterator]();
  while (true) {
    const timeout = new Promise<IteratorResult<T>>((_, reject) => {
      setTimeout(() => reject(new Error(message)), ms);
    });
    const next = await Promise.race([iterator.next(), timeout]);
    if (next.done) return;
    yield next.value;
  }
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
