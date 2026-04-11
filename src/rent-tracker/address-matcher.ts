/**
 * Rent Tracker — Address Matcher
 * Links listing_signals records to properties by normalized address comparison.
 *
 * Strategy:
 *   1. Exact match: normalized address + city + state_code
 *   2. Fuzzy match: strip unit/apt numbers, expand abbreviations
 */

import { getDb, getWriteDb } from "../db/client.js";

// ─── Abbreviation Map ───────────────────────────────────────────────

const ABBREVIATIONS: Record<string, string> = {
  ST: "STREET",
  AVE: "AVENUE",
  AV: "AVENUE",
  BLVD: "BOULEVARD",
  DR: "DRIVE",
  LN: "LANE",
  CT: "COURT",
  RD: "ROAD",
  PL: "PLACE",
  CIR: "CIRCLE",
  HWY: "HIGHWAY",
  PKWY: "PARKWAY",
  TRL: "TRAIL",
  WAY: "WAY",
  N: "NORTH",
  S: "SOUTH",
  E: "EAST",
  W: "WEST",
  NE: "NORTHEAST",
  NW: "NORTHWEST",
  SE: "SOUTHEAST",
  SW: "SOUTHWEST",
};

// Build reverse map so we can also normalize full words that are already expanded
const REVERSE_ABBREVIATIONS: Record<string, string> = {};
for (const [abbr, full] of Object.entries(ABBREVIATIONS)) {
  REVERSE_ABBREVIATIONS[full] = full; // identity — keeps expanded form
  REVERSE_ABBREVIATIONS[abbr] = full;
}

// ─── Normalization ──────────────────────────────────────────────────

/**
 * Aggressive address normalization for matching.
 * - Uppercases
 * - Removes unit/apt/suite numbers
 * - Expands abbreviations (ST->STREET, etc.)
 * - Removes punctuation
 * - Collapses whitespace
 */
export function normalizeForMatching(address: string): string {
  let norm = address.toUpperCase();

  // Remove punctuation (periods, commas, hashes)
  norm = norm.replace(/[.,#]/g, "");

  // Remove unit/apt/suite numbers (e.g. "APT 4B", "UNIT 201", "STE 100", "#5")
  norm = norm.replace(/\b(APT|APARTMENT|UNIT|STE|SUITE|BLDG|BUILDING|FL|FLOOR|RM|ROOM)\s*\.?\s*\S+/gi, "");

  // Remove trailing hash-style unit numbers (e.g. "123 MAIN ST #4B")
  norm = norm.replace(/#\s*\S+/, "");

  // Collapse whitespace first so word splitting is clean
  norm = norm.replace(/\s+/g, " ").trim();

  // Expand abbreviations — process each word
  const words = norm.split(" ");
  const expanded = words.map((word) => {
    return REVERSE_ABBREVIATIONS[word] ?? word;
  });

  norm = expanded.join(" ");

  // Final whitespace collapse
  norm = norm.replace(/\s+/g, " ").trim();

  return norm;
}

// ─── Matching Logic ─────────────────────────────────────────────────

interface UnmatchedListing {
  id: number;
  address: string;
  city: string;
  state_code: string;
}

interface PropertyCandidate {
  id: number;
  address: string;
  city: string;
  state_code: string;
}

const BATCH_SIZE = 100;

/**
 * Match listing_signals records (where property_id IS NULL) to properties
 * by normalized address. Updates listing_signals.property_id on match.
 */
export async function matchListingsToProperties(
  stateCode: string,
  city?: string,
): Promise<{ matched: number; unmatched: number }> {
  const db = getDb();
  const writeDb = getWriteDb();
  const state = stateCode.toUpperCase();

  let matched = 0;
  let unmatched = 0;
  let offset = 0;

  // Paginate through unmatched listings
  while (true) {
    let query = db
      .from("listing_signals")
      .select("id, address, city, state_code")
      .eq("state_code", state)
      .is("property_id", null)
      .range(offset, offset + BATCH_SIZE - 1);

    if (city) {
      query = query.ilike("city", city.toUpperCase());
    }

    const { data: listings, error } = await query;
    if (error) throw new Error(`Failed to fetch unmatched listings: ${error.message}`);
    if (!listings || listings.length === 0) break;

    const unmatchedListings = listings as UnmatchedListing[];

    for (const listing of unmatchedListings) {
      const propertyId = await findMatchingProperty(db, listing);

      if (propertyId) {
        // Update listing_signals with the matched property_id
        const { error: updateError } = await writeDb
          .from("listing_signals")
          .update({ property_id: propertyId, updated_at: new Date().toISOString() })
          .eq("id", listing.id);

        if (updateError) {
          console.error(`  Failed to update listing ${listing.id}: ${updateError.message}`);
          unmatched++;
        } else {
          matched++;
        }
      } else {
        unmatched++;
      }
    }

    // If we got fewer than BATCH_SIZE, we've reached the end
    if (unmatchedListings.length < BATCH_SIZE) break;
    offset += BATCH_SIZE;
  }

  return { matched, unmatched };
}

/**
 * Try to find a matching property for a listing signal.
 * Strategy 1: Exact normalized address + city + state
 * Strategy 2: Fuzzy match with expanded abbreviations and stripped units
 */
async function findMatchingProperty(
  db: ReturnType<typeof getDb>,
  listing: UnmatchedListing,
): Promise<number | null> {
  const city = listing.city.toUpperCase();
  const state = listing.state_code.toUpperCase();

  // Strategy 1: Exact match on normalized address
  const { data: exactMatches, error: exactError } = await db
    .from("properties")
    .select("id, address, city, state_code")
    .eq("state_code", state)
    .ilike("city", city)
    .ilike("address", listing.address)
    .limit(1);

  if (!exactError && exactMatches && exactMatches.length > 0) {
    return (exactMatches[0] as PropertyCandidate).id;
  }

  // Strategy 2: Fuzzy match — normalize both sides aggressively
  const listingNorm = normalizeForMatching(listing.address);

  // Query properties in the same city/state, then compare normalized addresses.
  // We fetch a broader set and filter in-memory to handle abbreviation differences.
  const { data: candidates, error: fuzzyError } = await db
    .from("properties")
    .select("id, address, city, state_code")
    .eq("state_code", state)
    .ilike("city", city)
    .limit(500);

  if (fuzzyError || !candidates) return null;

  for (const candidate of candidates as PropertyCandidate[]) {
    const candidateNorm = normalizeForMatching(candidate.address);
    if (candidateNorm === listingNorm) {
      return candidate.id;
    }
  }

  return null;
}
