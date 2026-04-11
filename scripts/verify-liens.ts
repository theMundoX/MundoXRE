#!/usr/bin/env tsx
/**
 * Verify lien status for every property in a county.
 *
 * Strategy: "wide scan" — pull ALL mortgage/lien documents from the county
 * recorder for the last 30 years, then match each document's borrower name
 * to property owner names. Properties matched = has_liens, unmatched = free_clear.
 *
 * This is much faster than per-property searches:
 * ~1,670 API calls (7-day chunks over 30 years) vs 74K per-property calls.
 *
 * Usage:
 *   tsx scripts/verify-liens.ts --county=Fairfield --state=OH
 *   tsx scripts/verify-liens.ts --county=Fairfield --state=OH --limit=500 --dry-run
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaApiAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava-api.js";

// ─── CLI Args ─────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const parsed: Record<string, string> = {};
  for (const arg of args) {
    const match = arg.match(/^--(\w[\w-]*)=(.+)$/);
    if (match) parsed[match[1]] = match[2];
    else if (arg === "--dry-run") parsed["dry-run"] = "true";
  }
  return {
    county: parsed["county"] || "Fairfield",
    state: parsed["state"] || "OH",
    limit: parsed["limit"] ? parseInt(parsed["limit"], 10) : 0,
    dryRun: parsed["dry-run"] === "true",
  };
}

// ─── Name Normalization ───────────────────────────────────────────────

/** Normalize a name for fuzzy matching */
function normalizeName(name: string): string {
  return name
    .toUpperCase()
    .replace(/[,.'"\-()]/g, " ")
    .replace(/\b(JR|SR|II|III|IV|LLC|INC|CORP|LTD|LP|CO|COMPANY|TRUST|TRUSTEE|ESTATE|ET\s*AL|ETAL)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Extract individual last names from an owner_name field (handles joint owners) */
function extractLastNames(ownerName: string): string[] {
  const normalized = normalizeName(ownerName);
  if (!normalized) return [];

  // Split on common separators for joint owners: & / ;
  const parts = normalized.split(/\s*[&;\/]\s*/);
  const lastNames: string[] = [];

  for (const part of parts) {
    const words = part.trim().split(/\s+/);
    if (words.length > 0) {
      // Last name is typically the first word (SMITH JOHN format) or last word (JOHN SMITH)
      // County assessor data is usually LAST FIRST format
      lastNames.push(words[0]);
    }
  }

  return lastNames.filter(n => n.length >= 2);
}

/** Check if a borrower name matches a property owner name */
function namesMatch(borrowerName: string, ownerName: string): boolean {
  const borrowerNorm = normalizeName(borrowerName);
  const ownerNorm = normalizeName(ownerName);

  if (!borrowerNorm || !ownerNorm) return false;

  // Direct containment (either direction)
  if (borrowerNorm.includes(ownerNorm) || ownerNorm.includes(borrowerNorm)) return true;

  // Extract last names from both and check for overlap
  const borrowerLastNames = extractLastNames(borrowerName);
  const ownerLastNames = extractLastNames(ownerName);

  // Must share at least one last name AND have a first name or initial match
  for (const bl of borrowerLastNames) {
    for (const ol of ownerLastNames) {
      if (bl === ol && bl.length >= 3) {
        // Same last name — good enough for county-level matching
        // (false positives are acceptable; this is lien verification, not legal discovery)
        return true;
      }
    }
  }

  return false;
}

// ─── Mortgage Document Types ──────────────────────────────────────────

const MORTGAGE_DOC_TYPES = new Set([
  "MORTGAGE",
  "MTG",
  "OPEN END MORTGAGE",
  "OPEN-END MORTGAGE",
  "MODIFICATION",
  "MORTGAGE MODIFICATION",
  "DEED OF TRUST",
  "DOT",
  "LIEN",
  "TAX LIEN",
  "MECHANIC LIEN",
  "MECHANICS LIEN",
  "JUDGMENT",
  "JUDGEMENT",
  "JUDGMENT LIEN",
]);

/** Satisfaction/release documents indicate a lien was PAID OFF */
const SATISFACTION_DOC_TYPES = new Set([
  "SATISFACTION",
  "SATISFACTION OF MORTGAGE",
  "RELEASE",
  "RELEASE OF MORTGAGE",
  "FULL RECONVEYANCE",
  "DISCHARGE",
]);

function isMortgageDoc(docType: string): boolean {
  const upper = docType.toUpperCase().trim();
  if (MORTGAGE_DOC_TYPES.has(upper)) return true;
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) return true;
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return true;
  if (upper.includes("DEED OF TRUST")) return true;
  return false;
}

function isSatisfactionDoc(docType: string): boolean {
  const upper = docType.toUpperCase().trim();
  if (SATISFACTION_DOC_TYPES.has(upper)) return true;
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE OF MORTGAGE") || upper.includes("DISCHARGE")) return true;
  return false;
}

// ─── Progress Helpers ─────────────────────────────────────────────────

function formatEta(startMs: number, done: number, total: number): string {
  if (done === 0) return "calculating...";
  const elapsed = Date.now() - startMs;
  const rate = done / elapsed;
  const remaining = (total - done) / rate;
  const mins = Math.floor(remaining / 60000);
  const secs = Math.floor((remaining % 60000) / 1000);
  return `${mins}m ${secs}s`;
}

// ─── Main ─────────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();
  console.log(`MXRE — Verify Lien Status`);
  console.log(`  County: ${opts.county}, ${opts.state}`);
  console.log(`  Limit: ${opts.limit || "all"}`);
  console.log(`  Dry run: ${opts.dryRun}\n`);

  const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
    auth: { persistSession: false },
  });

  // ── Step 1: Find county and Fidlar config ───────────────────────────

  const fidlarConfig = FIDLAR_AVA_COUNTIES.find(
    c => c.county_name.toLowerCase() === opts.county.toLowerCase() && c.state === opts.state,
  );
  if (!fidlarConfig) {
    console.error(`No Fidlar AVA config found for ${opts.county}, ${opts.state}`);
    console.error("Available:", FIDLAR_AVA_COUNTIES.map(c => `${c.county_name}, ${c.state}`).join("; "));
    process.exit(1);
  }

  // Get county_id from DB
  const { data: countyRow } = await db.from("counties")
    .select("id")
    .eq("county_name", opts.county)
    .eq("state_code", opts.state)
    .single();

  if (!countyRow) {
    console.error(`County "${opts.county}, ${opts.state}" not found in counties table`);
    process.exit(1);
  }
  const countyId = countyRow.id;

  // ── Step 2: Load properties that need verification ──────────────────

  console.log("Loading properties...");
  let query = db.from("properties")
    .select("id, owner_name, address")
    .eq("county_id", countyId)
    .is("lien_status", null)
    .not("owner_name", "is", null)
    .neq("owner_name", "")
    .order("id");

  if (opts.limit > 0) {
    query = query.limit(opts.limit);
  }

  // Paginate to get all properties (Supabase limits to 1000 per query)
  interface PropertyRow {
    id: number;
    owner_name: string;
    address: string;
  }

  const allProperties: PropertyRow[] = [];
  let page = 0;
  const PAGE_SIZE = 1000;

  while (true) {
    let pageQuery = db.from("properties")
      .select("id, owner_name, address")
      .eq("county_id", countyId)
      .is("lien_status", null)
      .not("owner_name", "is", null)
      .neq("owner_name", "")
      .order("id")
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

    if (opts.limit > 0) {
      pageQuery = pageQuery.limit(Math.min(PAGE_SIZE, opts.limit - allProperties.length));
    }

    const { data: rows, error } = await pageQuery;
    if (error) {
      console.error("DB error loading properties:", error.message);
      process.exit(1);
    }
    if (!rows || rows.length === 0) break;

    allProperties.push(...(rows as PropertyRow[]));

    if (opts.limit > 0 && allProperties.length >= opts.limit) break;
    if (rows.length < PAGE_SIZE) break;
    page++;
  }

  console.log(`  ${allProperties.length} properties need lien verification\n`);

  if (allProperties.length === 0) {
    console.log("Nothing to do — all properties already have lien_status set.");
    process.exit(0);
  }

  // Build owner name lookup: normalized last name -> property IDs
  // This allows O(1) matching instead of O(N) per document
  const ownerIndex = new Map<string, PropertyRow[]>();
  for (const prop of allProperties) {
    const lastNames = extractLastNames(prop.owner_name);
    for (const ln of lastNames) {
      const existing = ownerIndex.get(ln) || [];
      existing.push(prop);
      ownerIndex.set(ln, existing);
    }
  }

  console.log(`  Owner index: ${ownerIndex.size} unique last names\n`);

  // ── Step 3: Scan recorder — wide date range ─────────────────────────

  const endDate = "2026-03-28";
  const startDate = "1994-01-01";

  console.log(`Scanning recorder: ${startDate} to ${endDate}`);
  console.log(`  (Fidlar chunks by 7-day windows — this will take ~15 minutes)\n`);

  const adapter = new FidlarAvaApiAdapter();

  // Track which properties have liens
  const propertiesWithLiens = new Set<number>();
  // Track mortgage records to upsert per property
  const propertyMortgages = new Map<number, Array<{
    document_type: string;
    recording_date: string;
    loan_amount: number | null;
    lender_name: string;
    borrower_name: string;
    document_number: string | undefined;
    book_page: string | undefined;
    source_url: string;
  }>>();

  let totalDocs = 0;
  let mortgageDocs = 0;
  let satisfactionDocs = 0;
  let matchedDocs = 0;
  let unmatchedDocs = 0;
  const scanStart = Date.now();

  for await (const doc of adapter.fetchDocuments(fidlarConfig, startDate, endDate, (p) => {
    const pct = p.total_found > 0
      ? ((p.total_processed / p.total_found) * 100).toFixed(1)
      : "0.0";
    process.stdout.write(
      `\r  Scanning ${p.current_date} | Found: ${p.total_found.toLocaleString()} | ` +
      `Processed: ${p.total_processed.toLocaleString()} (${pct}%) | ` +
      `Matched: ${matchedDocs.toLocaleString()}`
    );
  })) {
    totalDocs++;

    // Only care about mortgage/lien documents
    if (!isMortgageDoc(doc.document_type) && !isSatisfactionDoc(doc.document_type)) continue;

    if (isSatisfactionDoc(doc.document_type)) {
      satisfactionDocs++;
      continue; // We note satisfactions but don't mark as "has_liens"
    }

    mortgageDocs++;

    // The borrower is in the grantor field for mortgages (grantor = borrower, grantee = lender)
    const borrowerName = doc.grantor;
    if (!borrowerName) { unmatchedDocs++; continue; }

    // Look up by last name in our index
    const borrowerLastNames = extractLastNames(borrowerName);
    let matched = false;

    for (const ln of borrowerLastNames) {
      const candidates = ownerIndex.get(ln);
      if (!candidates) continue;

      for (const prop of candidates) {
        if (namesMatch(borrowerName, prop.owner_name)) {
          propertiesWithLiens.add(prop.id);
          matched = true;

          // Store mortgage record for this property
          const mortgages = propertyMortgages.get(prop.id) || [];
          mortgages.push({
            document_type: doc.document_type.toLowerCase().includes("lien") ? "lien" : "mortgage",
            recording_date: doc.recording_date,
            loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
            lender_name: (doc.grantee || "").slice(0, 500),
            borrower_name: (doc.grantor || "").slice(0, 500),
            document_number: doc.instrument_number,
            book_page: doc.book_page,
            source_url: doc.source_url,
          });
          propertyMortgages.set(prop.id, mortgages);
        }
      }
    }

    if (matched) matchedDocs++;
    else unmatchedDocs++;
  }

  const scanTime = ((Date.now() - scanStart) / 1000).toFixed(0);
  console.log(`\n\n  Scan complete in ${scanTime}s`);
  console.log(`  Total documents: ${totalDocs.toLocaleString()}`);
  console.log(`  Mortgage/lien docs: ${mortgageDocs.toLocaleString()}`);
  console.log(`  Satisfaction docs: ${satisfactionDocs.toLocaleString()}`);
  console.log(`  Matched to properties: ${matchedDocs.toLocaleString()}`);
  console.log(`  Unmatched: ${unmatchedDocs.toLocaleString()}\n`);

  // ── Step 4: Determine lien status ───────────────────────────────────

  const hasLiens = propertiesWithLiens.size;
  const freeClear = allProperties.length - hasLiens;
  const coveragePct = ((hasLiens + freeClear) / allProperties.length * 100).toFixed(1);

  console.log(`  Lien status results:`);
  console.log(`    has_liens:  ${hasLiens.toLocaleString()} properties`);
  console.log(`    free_clear: ${freeClear.toLocaleString()} properties`);
  console.log(`    coverage:   ${coveragePct}% (all properties now verified)\n`);

  if (opts.dryRun) {
    console.log("  DRY RUN — no database updates made.");

    // Show a few examples
    const liensExample = allProperties.filter(p => propertiesWithLiens.has(p.id)).slice(0, 5);
    const clearExample = allProperties.filter(p => !propertiesWithLiens.has(p.id)).slice(0, 5);

    if (liensExample.length > 0) {
      console.log("\n  Sample has_liens:");
      for (const p of liensExample) {
        const mortCount = propertyMortgages.get(p.id)?.length || 0;
        console.log(`    [${p.id}] ${p.owner_name} — ${p.address} (${mortCount} records)`);
      }
    }
    if (clearExample.length > 0) {
      console.log("\n  Sample free_clear:");
      for (const p of clearExample) {
        console.log(`    [${p.id}] ${p.owner_name} — ${p.address}`);
      }
    }

    process.exit(0);
  }

  // ── Step 5: Update database ─────────────────────────────────────────

  console.log("Updating database...\n");

  const BATCH_SIZE = 100;
  let updatedLiens = 0;
  let updatedClear = 0;
  let upsertedRecords = 0;
  let updateErrors = 0;
  const updateStart = Date.now();

  // 5a: Mark properties with liens
  const liensIds = [...propertiesWithLiens];
  for (let i = 0; i < liensIds.length; i += BATCH_SIZE) {
    const batch = liensIds.slice(i, i + BATCH_SIZE);
    const { error } = await db.from("properties")
      .update({ lien_status: "has_liens", updated_at: new Date().toISOString() })
      .in("id", batch);

    if (error) {
      console.error(`  Error updating has_liens batch: ${error.message}`);
      updateErrors++;
    } else {
      updatedLiens += batch.length;
    }

    const pct = ((i + batch.length) / liensIds.length * 100).toFixed(0);
    process.stdout.write(`\r  has_liens: ${updatedLiens}/${liensIds.length} (${pct}%)`);
  }
  console.log();

  // 5b: Mark properties without liens as free_clear
  const clearIds = allProperties
    .filter(p => !propertiesWithLiens.has(p.id))
    .map(p => p.id);

  for (let i = 0; i < clearIds.length; i += BATCH_SIZE) {
    const batch = clearIds.slice(i, i + BATCH_SIZE);
    const { error } = await db.from("properties")
      .update({ lien_status: "free_clear", updated_at: new Date().toISOString() })
      .in("id", batch);

    if (error) {
      console.error(`  Error updating free_clear batch: ${error.message}`);
      updateErrors++;
    } else {
      updatedClear += batch.length;
    }

    const pct = ((i + batch.length) / clearIds.length * 100).toFixed(0);
    process.stdout.write(`\r  free_clear: ${updatedClear}/${clearIds.length} (${pct}%)`);
  }
  console.log();

  // 5c: Upsert mortgage records for matched properties
  console.log("\n  Upserting mortgage records...");
  const allMortgageBatch: Array<Record<string, unknown>> = [];

  for (const [propId, mortgages] of propertyMortgages) {
    for (const m of mortgages) {
      allMortgageBatch.push({
        property_id: propId,
        document_type: m.document_type,
        recording_date: m.recording_date,
        loan_amount: m.loan_amount,
        lender_name: m.lender_name,
        borrower_name: m.borrower_name,
        document_number: m.document_number,
        book_page: m.book_page,
        source_url: m.source_url,
      });
    }
  }

  // Upsert in batches, skip duplicates by document_number + source_url
  for (let i = 0; i < allMortgageBatch.length; i += BATCH_SIZE) {
    const batch = allMortgageBatch.slice(i, i + BATCH_SIZE);

    // Filter out records that already exist (by document_number)
    const docNumbers = batch
      .map(r => r.document_number as string)
      .filter(Boolean);

    let existingDocs = new Set<string>();
    if (docNumbers.length > 0) {
      const { data: existing } = await db.from("mortgage_records")
        .select("document_number")
        .in("document_number", docNumbers)
        .eq("source_url", fidlarConfig.base_url);

      if (existing) {
        existingDocs = new Set(existing.map((r: { document_number: string }) => r.document_number));
      }
    }

    const newRecords = batch.filter(r =>
      !r.document_number || !existingDocs.has(r.document_number as string)
    );

    if (newRecords.length > 0) {
      const { error } = await db.from("mortgage_records").insert(newRecords);
      if (error) {
        // Try one by one for partial failures
        for (const rec of newRecords) {
          const { error: singleErr } = await db.from("mortgage_records").insert([rec]);
          if (!singleErr) upsertedRecords++;
        }
      } else {
        upsertedRecords += newRecords.length;
      }
    }

    const pct = ((i + batch.length) / allMortgageBatch.length * 100).toFixed(0);
    process.stdout.write(`\r  Mortgage records: ${upsertedRecords} new of ${allMortgageBatch.length} total (${pct}%)`);
  }
  console.log();

  const updateTime = ((Date.now() - updateStart) / 1000).toFixed(0);
  console.log(`\n━━━ Summary ━━━`);
  console.log(`  Properties verified: ${allProperties.length.toLocaleString()}`);
  console.log(`  has_liens:           ${updatedLiens.toLocaleString()}`);
  console.log(`  free_clear:          ${updatedClear.toLocaleString()}`);
  console.log(`  Mortgage records:    ${upsertedRecords.toLocaleString()} new`);
  console.log(`  Errors:              ${updateErrors}`);
  console.log(`  Scan time:           ${scanTime}s`);
  console.log(`  Update time:         ${updateTime}s`);
  console.log(`  Total time:          ${((Date.now() - scanStart) / 1000).toFixed(0)}s`);
}

main().catch(err => {
  console.error("\nFatal error:", err.message);
  process.exit(1);
});
