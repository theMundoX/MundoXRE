#!/usr/bin/env tsx
/**
 * MXRE — Link unlinked mortgage_records to properties.
 *
 * Strategies (in priority order):
 *   1. book-page  — Match by book/page against already-linked records in same county
 *   2. owner      — Match borrower_name to properties.owner_name within same county
 *   3. legal      — Parse lot/block/subdivision from raw.legals and match parcel characteristics
 *
 * Usage:
 *   npx tsx scripts/link-mortgage-records.ts
 *   npx tsx scripts/link-mortgage-records.ts --state=OH
 *   npx tsx scripts/link-mortgage-records.ts --state=OH --county=Fairfield
 *   npx tsx scripts/link-mortgage-records.ts --dry-run --limit=500
 *   npx tsx scripts/link-mortgage-records.ts --strategy=owner --state=FL
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const stateFilter = getArg("state")?.toUpperCase();
const countyFilter = getArg("county");
const dryRun = hasFlag("dry-run");
const limitArg = getArg("limit") ? parseInt(getArg("limit")!, 10) : undefined;
const strategyFilter = getArg("strategy") as "owner" | "legal" | "bookpage" | undefined;

if (strategyFilter && !["owner", "legal", "bookpage"].includes(strategyFilter)) {
  console.error(`Invalid strategy: ${strategyFilter}. Use: owner, legal, bookpage`);
  process.exit(1);
}

// ─── Database ──────────────────────────────────────────────────────

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── Source URL -> County mapping ──────────────────────────────────

interface CountyInfo {
  county_name: string;
  state_code: string;
}

const SOURCE_PATTERNS: Array<{ pattern: string; county_name: string; state_code: string }> = [
  // Ohio
  { pattern: "ava.fidlar.com/OHFairfield", county_name: "Fairfield", state_code: "OH" },
  { pattern: "ava.fidlar.com/OHGeauga", county_name: "Geauga", state_code: "OH" },
  { pattern: "ava.fidlar.com/OHPaulding", county_name: "Paulding", state_code: "OH" },
  { pattern: "ava.fidlar.com/OHWyandot", county_name: "Wyandot", state_code: "OH" },
  // Michigan
  { pattern: "ava.fidlar.com/MIOakland", county_name: "Oakland", state_code: "MI" },
  { pattern: "ava.fidlar.com/MIAntrim", county_name: "Antrim", state_code: "MI" },
  // Texas
  { pattern: "ava.fidlar.com/TXAustin", county_name: "Austin", state_code: "TX" },
  { pattern: "ava.fidlar.com/TXFannin", county_name: "Fannin", state_code: "TX" },
  { pattern: "ava.fidlar.com/TXGalveston", county_name: "Galveston", state_code: "TX" },
  { pattern: "ava.fidlar.com/TXKerr", county_name: "Kerr", state_code: "TX" },
  { pattern: "ava.fidlar.com/TXPanola", county_name: "Panola", state_code: "TX" },
  { pattern: "dallas.tx.publicsearch.us", county_name: "Dallas", state_code: "TX" },
  { pattern: "denton.tx.publicsearch.us", county_name: "Denton", state_code: "TX" },
  { pattern: "tarrant.tx.publicsearch.us", county_name: "Tarrant", state_code: "TX" },
  // Florida
  { pattern: "levyclerk.com", county_name: "Levy", state_code: "FL" },
  { pattern: "martinclerk.com", county_name: "Martin", state_code: "FL" },
  { pattern: "clerkofcourts.co.walton.fl", county_name: "Walton", state_code: "FL" },
  { pattern: "citrusclerk.org", county_name: "Citrus", state_code: "FL" },
  // Iowa
  { pattern: "ava.fidlar.com/IABlackHawk", county_name: "Black Hawk", state_code: "IA" },
  { pattern: "ava.fidlar.com/IABoone", county_name: "Boone", state_code: "IA" },
  { pattern: "ava.fidlar.com/IACalhoun", county_name: "Calhoun", state_code: "IA" },
  { pattern: "ava.fidlar.com/IAClayton", county_name: "Clayton", state_code: "IA" },
  { pattern: "ava.fidlar.com/IAJasper", county_name: "Jasper", state_code: "IA" },
  { pattern: "ava.fidlar.com/IALinn", county_name: "Linn", state_code: "IA" },
  { pattern: "ava.fidlar.com/IAScott", county_name: "Scott", state_code: "IA" },
  // Arkansas
  { pattern: "ava.fidlar.com/ARSaline", county_name: "Saline", state_code: "AR" },
  // New Hampshire
  { pattern: "ava.fidlar.com/NHBelknap", county_name: "Belknap", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHCarroll", county_name: "Carroll", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHCheshire", county_name: "Cheshire", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHGrafton", county_name: "Grafton", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHHillsborough", county_name: "Hillsborough", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHRockingham", county_name: "Rockingham", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHStrafford", county_name: "Strafford", state_code: "NH" },
  { pattern: "ava.fidlar.com/NHSullivan", county_name: "Sullivan", state_code: "NH" },
  // Washington
  { pattern: "ava.fidlar.com/WAYakima", county_name: "Yakima", state_code: "WA" },
];

function findCountyFromUrl(url: string): CountyInfo | null {
  for (const src of SOURCE_PATTERNS) {
    if (url.includes(src.pattern)) {
      return { county_name: src.county_name, state_code: src.state_code };
    }
  }
  return null;
}

// ─── Name normalization ────────────────────────────────────────────

function normalizeName(name: string): string {
  return name
    .toUpperCase()
    .replace(/[,.'"\-]/g, " ")
    .replace(/\s+(JR|SR|II|III|IV|LLC|INC|CORP|LTD|LP|CO|TRUSTEE|TRUST|ET\s*AL|ETAL)\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extract meaningful name parts (>= 2 chars), handling
 * both "LASTNAME FIRSTNAME" and "FIRSTNAME LASTNAME" formats.
 */
function getNameParts(name: string): string[] {
  const normalized = normalizeName(name.split(";")[0]);
  return normalized.split(/\s+/).filter((p) => p.length >= 2);
}

/**
 * Score how well a borrower name matches a property owner name.
 * Returns 0-100. Higher = better match.
 */
function scoreName(borrowerName: string, ownerName: string): number {
  const borrowerParts = getNameParts(borrowerName);
  const ownerParts = getNameParts(ownerName);
  if (borrowerParts.length === 0 || ownerParts.length === 0) return 0;

  const ownerUpper = normalizeName(ownerName);
  const borrowerUpper = normalizeName(borrowerName.split(";")[0]);

  // Exact match after normalization
  if (ownerUpper === borrowerUpper) return 100;

  // Count matching parts
  let matchCount = 0;
  for (const part of borrowerParts) {
    if (ownerUpper.includes(part)) matchCount++;
  }

  // Need at least 2 parts matching (or all parts if only 1)
  const minRequired = Math.min(2, borrowerParts.length);
  if (matchCount < minRequired) return 0;

  // Score based on proportion of matching parts
  return Math.round((matchCount / borrowerParts.length) * 80);
}

// ─── Stats tracking ────────────────────────────────────────────────

interface Stats {
  processed: number;
  linked_bookpage: number;
  linked_owner: number;
  linked_legal: number;
  skipped_ambiguous: number;
  skipped_no_match: number;
  skipped_no_county: number;
  errors: number;
}

const stats: Stats = {
  processed: 0,
  linked_bookpage: 0,
  linked_owner: 0,
  linked_legal: 0,
  skipped_ambiguous: 0,
  skipped_no_match: 0,
  skipped_no_county: 0,
  errors: 0,
};

// ─── County ID cache ───────────────────────────────────────────────

const countyIdCache = new Map<string, number | null>();

async function getCountyId(countyName: string, stateCode: string): Promise<number | null> {
  const key = `${stateCode}:${countyName}`;
  if (countyIdCache.has(key)) return countyIdCache.get(key)!;

  const { data } = await db
    .from("counties")
    .select("id")
    .eq("county_name", countyName)
    .eq("state_code", stateCode)
    .single();

  const id = data?.id ?? null;
  countyIdCache.set(key, id);
  return id;
}

// ─── Property cache per county (owner names) ──────────────────────

interface CachedProperty {
  id: number;
  owner_name: string;
}

const propertyCache = new Map<number, CachedProperty[]>();

/**
 * Load properties for a county into cache. Fetches in pages of 1000.
 */
async function loadCountyProperties(countyId: number): Promise<CachedProperty[]> {
  if (propertyCache.has(countyId)) return propertyCache.get(countyId)!;

  const allProps: CachedProperty[] = [];
  let offset = 0;
  const PAGE = 1000;

  while (true) {
    const { data, error } = await db
      .from("properties")
      .select("id, owner_name")
      .eq("county_id", countyId)
      .not("owner_name", "is", null)
      .range(offset, offset + PAGE - 1);

    if (error) {
      console.error(`  Error loading properties for county ${countyId}: ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;

    for (const p of data) {
      if (p.owner_name && p.owner_name.trim().length > 0) {
        allProps.push({ id: p.id, owner_name: p.owner_name });
      }
    }
    offset += data.length;
    if (data.length < PAGE) break;
  }

  propertyCache.set(countyId, allProps);
  return allProps;
}

// ─── Strategy 1: Book/Page match ───────────────────────────────────

/**
 * If this record has a book_page, look for already-linked records with the
 * same book_page in the same county and use their property_id.
 */
async function matchByBookPage(
  record: Record<string, any>,
  countyId: number,
): Promise<number | null> {
  if (!record.book_page) return null;

  // Find linked records with this book/page that belong to properties in this county
  const { data } = await db
    .from("mortgage_records")
    .select("property_id, properties!inner(county_id)")
    .eq("book_page", record.book_page)
    .not("property_id", "is", null)
    .limit(5);

  if (!data || data.length === 0) return null;

  // Filter to same county
  const sameCounty = data.filter((r: any) => r.properties?.county_id === countyId);
  if (sameCounty.length === 0) return null;

  // Check for unique property
  const uniqueIds = new Set(sameCounty.map((r: any) => r.property_id));
  if (uniqueIds.size === 1) return sameCounty[0].property_id;

  // Ambiguous — multiple properties share this book/page
  return null;
}

// ─── Strategy 2: Borrower = Owner match ────────────────────────────

function matchByOwner(
  record: Record<string, any>,
  properties: CachedProperty[],
): number | null {
  const borrowerName = record.borrower_name;
  if (!borrowerName || borrowerName.trim().length < 3) return null;

  // Try each semicolon-separated borrower name independently
  const borrowerNames = borrowerName.split(";").map((n: string) => n.trim()).filter((n: string) => n.length >= 3);
  if (borrowerNames.length === 0) return null;

  let overallBestScore = 0;
  let overallBestId: number | null = null;
  let overallAmbiguous = false;

  for (const name of borrowerNames) {
    const parts = getNameParts(name);
    if (parts.length === 0) continue;

    // Single-word names (just a last name) need special handling
    // They match too many properties, so require a higher threshold
    const isSingleWord = parts.length === 1;

    // Fast filter: properties whose owner_name contains the first part (last name)
    const lastName = parts[0];
    const candidates = properties.filter((p) =>
      p.owner_name.toUpperCase().includes(lastName),
    );

    if (candidates.length === 0) continue;

    // For single-word names, if there are multiple matches we can't disambiguate
    if (isSingleWord && candidates.length > 1) continue;

    for (const prop of candidates) {
      const score = scoreName(name, prop.owner_name);
      if (score > overallBestScore) {
        overallBestScore = score;
        overallBestId = prop.id;
        overallAmbiguous = false;
      } else if (score === overallBestScore && score > 0 && prop.id !== overallBestId) {
        overallAmbiguous = true;
      }
    }
  }

  // Skip ambiguous matches (same score, different properties)
  if (overallAmbiguous) {
    stats.skipped_ambiguous++;
    return null;
  }

  // Require minimum score of 50
  if (overallBestScore >= 50 && overallBestId) return overallBestId;
  return null;
}

// ─── Strategy 3: Legal description match ───────────────────────────

interface LegalParts {
  lot?: string;
  block?: string;
  subdivision?: string;
}

function parseLegalDescription(raw: Record<string, unknown> | null): LegalParts | null {
  if (!raw) return null;

  // Fidlar AVA stores legals in raw.legals or in the legal_description text
  // The raw may have Legals array from AVA: [{ Description: "...", LegalType: "..." }]
  const legals = (raw as any).legals as Array<{ Description?: string; LegalType?: string }> | undefined;

  let text = "";
  if (legals && Array.isArray(legals)) {
    text = legals.map((l) => l.Description || "").join(" ");
  }

  // Also check for standalone legal_description string in raw
  if (!text && typeof (raw as any).legal_description === "string") {
    text = (raw as any).legal_description;
  }

  if (!text) return null;

  const upper = text.toUpperCase();
  const parts: LegalParts = {};

  // Parse lot number
  const lotMatch = upper.match(/\bLOT\s+(\d+[A-Z]?)/);
  if (lotMatch) parts.lot = lotMatch[1];

  // Parse block
  const blockMatch = upper.match(/\bBLOCK\s+(\d+[A-Z]?)/);
  if (blockMatch) parts.block = blockMatch[1];

  // Parse subdivision name
  const subMatch = upper.match(/\bSUBDIVISION\s*[:\-]?\s*([A-Z][A-Z\s]+?)(?:\s+LOT|\s+BLOCK|\s+PH|\s*$)/);
  if (subMatch) parts.subdivision = subMatch[1].trim();
  if (!parts.subdivision) {
    const addnMatch = upper.match(/\b(?:ADD(?:ITI)?N?|ADDN|ADDITION)\s*[:\-]?\s*([A-Z][A-Z\s]+?)(?:\s+LOT|\s+BLOCK|\s*$)/);
    if (addnMatch) parts.subdivision = addnMatch[1].trim();
  }

  // Only return if we have at least lot + one other field
  if (parts.lot && (parts.block || parts.subdivision)) return parts;
  return null;
}

async function matchByLegal(
  record: Record<string, any>,
  countyId: number,
): Promise<number | null> {
  const legal = parseLegalDescription(record.raw);
  if (!legal) return null;

  // Build a search query on properties in the same county
  // Properties with parcel_id may encode lot/block info
  // This is a best-effort match using text search on address fields
  let query = db
    .from("properties")
    .select("id, parcel_id, address")
    .eq("county_id", countyId);

  // Search for lot/block in the parcel_id or address
  if (legal.subdivision) {
    query = query.or(
      `address.ilike.%${legal.subdivision.slice(0, 20)}%,parcel_id.ilike.%${legal.subdivision.slice(0, 20)}%`,
    );
  }

  const { data } = await query.limit(20);
  if (!data || data.length === 0) return null;

  // If subdivision search returns exactly 1 property, that's our match
  if (data.length === 1) return data[0].id;

  // With lot + block, try to narrow down
  if (legal.lot && data.length <= 10) {
    const lotMatches = data.filter(
      (p) =>
        (p.parcel_id || "").includes(legal.lot!) ||
        (p.address || "").toUpperCase().includes(`LOT ${legal.lot}`),
    );
    if (lotMatches.length === 1) return lotMatches[0].id;
  }

  return null;
}

// ─── Main processing ───────────────────────────────────────────────

async function processRecord(
  record: Record<string, any>,
  countyId: number,
  properties: CachedProperty[],
): Promise<{ propertyId: number; strategy: string } | null> {
  // Strategy 1: Book/page
  if (!strategyFilter || strategyFilter === "bookpage") {
    const bpMatch = await matchByBookPage(record, countyId);
    if (bpMatch) return { propertyId: bpMatch, strategy: "bookpage" };
  }

  // Strategy 2: Owner name
  if (!strategyFilter || strategyFilter === "owner") {
    const ownerMatch = matchByOwner(record, properties);
    if (ownerMatch) return { propertyId: ownerMatch, strategy: "owner" };
  }

  // Strategy 3: Legal description (requires `raw` jsonb column on mortgage_records)
  // Currently disabled — the raw column is not yet on the table.
  // When added, uncomment this block.
  // if (!strategyFilter || strategyFilter === "legal") {
  //   const legalMatch = await matchByLegal(record, countyId);
  //   if (legalMatch) return { propertyId: legalMatch, strategy: "legal" };
  // }

  return null;
}

function printProgress() {
  const total = stats.linked_bookpage + stats.linked_owner + stats.linked_legal;
  process.stdout.write(
    `\r  Processed: ${stats.processed.toLocaleString()} | ` +
    `Linked: ${total.toLocaleString()} (bp:${stats.linked_bookpage} own:${stats.linked_owner} leg:${stats.linked_legal}) | ` +
    `Skip: ${stats.skipped_no_match.toLocaleString()} | ` +
    `Ambig: ${stats.skipped_ambiguous.toLocaleString()}   `,
  );
}

async function main() {
  console.log("MXRE — Link Mortgage Records to Properties");
  console.log("═".repeat(55));
  if (dryRun) console.log("  ** DRY RUN — no updates will be made **");
  if (stateFilter) console.log(`  State filter: ${stateFilter}`);
  if (countyFilter) console.log(`  County filter: ${countyFilter}`);
  if (strategyFilter) console.log(`  Strategy: ${strategyFilter}`);
  if (limitArg) console.log(`  Limit: ${limitArg.toLocaleString()}`);
  console.log();

  // Discover which counties have unlinked records by pulling distinct source_urls
  const { data: sampleRecords } = await db
    .from("mortgage_records")
    .select("source_url")
    .is("property_id", null)
    .not("source_url", "is", null)
    .limit(1000);

  if (!sampleRecords || sampleRecords.length === 0) {
    console.log("No unlinked mortgage records found.");
    return;
  }

  // Deduplicate source_urls and map to counties
  const sourceUrls = [...new Set(sampleRecords.map((r) => r.source_url as string))];
  const countiesToProcess = new Map<string, { countyId: number; countyName: string; stateCode: string; sourceUrls: string[] }>();

  for (const url of sourceUrls) {
    const info = findCountyFromUrl(url);
    if (!info) continue;

    // Apply state/county filters
    if (stateFilter && info.state_code !== stateFilter) continue;
    if (countyFilter && info.county_name.toLowerCase() !== countyFilter.toLowerCase()) continue;

    const countyId = await getCountyId(info.county_name, info.state_code);
    if (!countyId) continue;

    const key = `${info.state_code}:${info.county_name}`;
    if (!countiesToProcess.has(key)) {
      countiesToProcess.set(key, {
        countyId,
        countyName: info.county_name,
        stateCode: info.state_code,
        sourceUrls: [],
      });
    }
    countiesToProcess.get(key)!.sourceUrls.push(url);
  }

  if (countiesToProcess.size === 0) {
    console.log("No matching counties found with unlinked records.");
    return;
  }

  console.log(`Found ${countiesToProcess.size} counties with unlinked records:\n`);
  for (const [, info] of countiesToProcess) {
    console.log(`  ${info.stateCode} / ${info.countyName} (county_id=${info.countyId})`);
  }
  console.log();

  let globalLimit = limitArg ?? Infinity;
  const BATCH = 100;
  const updateBatch: Array<{ id: number; property_id: number }> = [];

  for (const [, county] of countiesToProcess) {
    if (globalLimit <= 0) break;

    console.log(`\n── ${county.stateCode} / ${county.countyName} ──`);

    // Preload properties for this county
    const properties = await loadCountyProperties(county.countyId);
    console.log(`  ${properties.length.toLocaleString()} properties loaded`);

    if (properties.length === 0 && (!strategyFilter || strategyFilter === "owner")) {
      console.log("  No properties — skipping owner matching");
      if (strategyFilter === "owner") continue;
    }

    // Process unlinked records in batches
    let offset = 0;

    while (globalLimit > 0) {
      const fetchSize = Math.min(BATCH, globalLimit);

      // Build query: unlinked records from this county's source URLs
      // Only fetch records with matchable data (borrower_name or book_page)
      let query = db
        .from("mortgage_records")
        .select("id, borrower_name, lender_name, book_page, source_url")
        .is("property_id", null)
        .or("borrower_name.neq.,book_page.not.is.null")
        .order("id")
        .range(offset, offset + fetchSize - 1);

      // Filter by source URLs for this county
      if (county.sourceUrls.length === 1) {
        query = query.eq("source_url", county.sourceUrls[0]);
      } else {
        query = query.in("source_url", county.sourceUrls);
      }

      const { data: records, error } = await query;

      if (error) {
        console.error(`  Query error: ${error.message}`);
        stats.errors++;
        break;
      }
      if (!records || records.length === 0) break;

      for (const rec of records) {
        stats.processed++;
        globalLimit--;

        const result = await processRecord(rec, county.countyId, properties);

        if (result) {
          if (!dryRun) {
            updateBatch.push({ id: rec.id, property_id: result.propertyId });

            // Flush in batches
            if (updateBatch.length >= 50) {
              await flushUpdates(updateBatch);
              updateBatch.length = 0;
            }
          }

          if (result.strategy === "bookpage") stats.linked_bookpage++;
          else if (result.strategy === "owner") stats.linked_owner++;
          else if (result.strategy === "legal") stats.linked_legal++;
        } else {
          stats.skipped_no_match++;
        }

        if (stats.processed % 25 === 0) printProgress();
      }

      offset += records.length;
      if (records.length < fetchSize) break;
    }
  }

  // Flush remaining updates
  if (updateBatch.length > 0 && !dryRun) {
    await flushUpdates(updateBatch);
    updateBatch.length = 0;
  }

  printProgress();
  console.log("\n");

  // Final summary
  const totalLinked = stats.linked_bookpage + stats.linked_owner + stats.linked_legal;

  console.log("═".repeat(55));
  console.log("  Results");
  console.log("═".repeat(55));
  console.log(`  Processed:        ${stats.processed.toLocaleString()}`);
  console.log(`  Linked (total):   ${totalLinked.toLocaleString()}`);
  console.log(`    By book/page:   ${stats.linked_bookpage.toLocaleString()}`);
  console.log(`    By owner name:  ${stats.linked_owner.toLocaleString()}`);
  console.log(`    By legal desc:  ${stats.linked_legal.toLocaleString()}`);
  console.log(`  No match:         ${(stats.skipped_no_match - stats.skipped_ambiguous).toLocaleString()}`);
  console.log(`  Ambiguous:        ${stats.skipped_ambiguous.toLocaleString()}`);
  console.log(`  No county:        ${stats.skipped_no_county.toLocaleString()}`);
  console.log(`  Errors:           ${stats.errors.toLocaleString()}`);

  if (stats.processed > 0) {
    const rate = ((totalLinked / stats.processed) * 100).toFixed(1);
    console.log(`  Match rate:       ${rate}%`);
  }

  if (dryRun) {
    console.log("\n  ** DRY RUN — no records were updated **");
  }

  // Show total linked in DB
  const { count: dbLinked } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .not("property_id", "is", null);
  const { count: dbUnlinked } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("property_id", null);
  console.log(`\n  DB totals: ${dbLinked?.toLocaleString()} linked | ${dbUnlinked?.toLocaleString()} unlinked`);
}

async function flushUpdates(batch: Array<{ id: number; property_id: number }>) {
  // Supabase doesn't support batch update with different values per row,
  // so we update one at a time (but we batch the awaits)
  const promises = batch.map((item) =>
    db
      .from("mortgage_records")
      .update({ property_id: item.property_id })
      .eq("id", item.id),
  );

  const results = await Promise.all(promises);
  for (const { error } of results) {
    if (error) {
      stats.errors++;
    }
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
