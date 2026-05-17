#!/usr/bin/env tsx
/**
 * Link active listing_signals rows to parcel properties for a specific market.
 *
 * This is intentionally conservative:
 * - load one target market at a time
 * - match by normalized address + ZIP
 * - unit/base matching only succeeds when it resolves to exactly one property
 * - URL-derived address is only a fallback and still requires a unique property
 * - no coordinate-only matching and no paid API calls
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const args = process.argv.slice(2);
const flag = (name: string) => args.includes(`--${name}`);
const arg = (name: string) => args.find((value) => value.startsWith(`--${name}=`))?.split("=")[1];

const STATE = (arg("state") ?? "").toUpperCase();
const CITY = (arg("city") ?? "").toUpperCase();
const COUNTY_ID = Number(arg("county_id") ?? arg("county-id"));
const LIMIT = Math.max(1, Number(arg("limit") ?? "5000"));
const DRY_RUN = flag("dry-run");
const RELINK_EXISTING_SHELLS = flag("relink-existing-shells");

if (!STATE || !CITY || !Number.isFinite(COUNTY_ID)) {
  console.error("Usage: npx tsx scripts/link-market-listings-to-parcels.ts --state=OH --city=COLUMBUS --county_id=1698985 [--dry-run] [--limit=5000]");
  process.exit(1);
}

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

type ListingRow = {
  id: number;
  address: string | null;
  zip: string | null;
  property_id?: number | null;
  listing_url?: string | null;
  last_seen_at?: string | null;
};

type PropertyRow = {
  id: number;
  address: string | null;
  zip: string | null;
  city?: string | null;
  parcel_id?: string | null;
  apn_formatted?: string | null;
  source?: string | null;
};

type Match = {
  listingId: number;
  propertyId: number;
  currentPropertyId?: number | null;
  strategy: string;
};

function normZip(value: string | null | undefined): string {
  return String(value ?? "").match(/\d{5}/)?.[0] ?? "";
}

function normalizeAddress(value: string | null | undefined): string {
  let out = String(value ?? "").toUpperCase();
  out = out.replace(/&/g, " AND ");
  out = out.replace(/[.,]/g, " ");
  out = out.replace(/#/g, " UNIT ");
  out = out.replace(/\b(APARTMENT|APT|UNIT|UN|STE|SUITE|BLDG|BUILDING|CONDO|LOT|SPACE)\b/g, " UNIT ");
  out = out.replace(/\b0+(\d+(?:ST|ND|RD|TH))\b/g, "$1");
  out = out.replace(/\bAVENUE\b/g, "AVE");
  out = out.replace(/\bSTREET\b/g, "ST");
  out = out.replace(/\bROAD\b/g, "RD");
  out = out.replace(/\bDRIVE\b/g, "DR");
  out = out.replace(/\bBOULEVARD\b/g, "BLVD");
  out = out.replace(/\bCOURT\b/g, "CT");
  out = out.replace(/\bLANE\b/g, "LN");
  out = out.replace(/\bCIRCLE\b/g, "CIR");
  out = out.replace(/\bTERRACE\b/g, "TER");
  out = out.replace(/\bPLACE\b/g, "PL");
  out = out.replace(/\bPLAZA\b/g, "PLZ");
  out = out.replace(/\bTRAIL\b/g, "TRL");
  out = out.replace(/\bPARKWAY\b/g, "PKWY");
  out = out.replace(/\bNORTH\b/g, "N");
  out = out.replace(/\bSOUTH\b/g, "S");
  out = out.replace(/\bEAST\b/g, "E");
  out = out.replace(/\bWEST\b/g, "W");
  out = out.replace(/\bNORTHEAST\b/g, "NE");
  out = out.replace(/\bNORTHWEST\b/g, "NW");
  out = out.replace(/\bSOUTHEAST\b/g, "SE");
  out = out.replace(/\bSOUTHWEST\b/g, "SW");
  return out.replace(/[^A-Z0-9\s-]/g, " ").replace(/\s+/g, " ").trim();
}

function streetNumber(value: string | null | undefined): string {
  return normalizeAddress(value).match(/^(\d+[A-Z]?)/)?.[1] ?? "";
}

function stripUnit(value: string): string {
  return value
    .replace(/\s+UNIT\s+[A-Z0-9-]+(?:\s.*)?$/, "")
    .replace(/\s+(?:APT|STE|SUITE|BLDG)\s+[A-Z0-9-]+(?:\s.*)?$/, "")
    .trim();
}

function addressKeys(value: string | null | undefined): Array<{ key: string; strategy: string }> {
  const full = normalizeAddress(value);
  if (!full) return [];
  const keys = new Map<string, string>();
  const add = (key: string, strategy: string) => {
    const cleaned = key.replace(/\s+/g, " ").trim();
    if (cleaned.length >= 5) keys.set(cleaned, strategy);
  };

  add(full, "exact_normalized");
  add(full.replace(/^(\d+)\s+(N|S|E|W|NE|NW|SE|SW)\s+/, "$1 "), "directionless");
  add(full.replace(/\s+UNIT\s+0+([A-Z0-9-]+)$/, " UNIT $1"), "unit_unpadded");

  const base = stripUnit(full);
  if (base && base !== full) {
    add(base, "unique_base_without_unit");
    const unit = full.match(/\s+UNIT\s+([A-Z0-9-]+)\b/)?.[1];
    if (unit) {
      add(`${base} ${unit}`, "unit_no_marker");
      if (/^\d+$/.test(unit)) add(`${base} UNIT ${unit.padStart(4, "0")}`, "unit_padded");
    }
  }

  const range = full.match(/^(\d+)-(\d+)\s+(.+)$/);
  if (range) {
    add(`${range[1]} ${range[3]}`, "range_low");
    add(`${range[2]} ${range[3]}`, "range_high");
  }

  return [...keys.entries()].map(([key, strategy]) => ({ key, strategy }));
}

function urlAddressCandidates(url: string | null | undefined, city: string, state: string): string[] {
  if (!url) return [];
  try {
    const parsed = new URL(url);
    const parts = parsed.pathname.split("/").filter(Boolean);
    const zipIndex = parts.findIndex((part) => /^\d{5}$/.test(part));
    const candidates = new Set<string>();
    for (let i = 0; i < parts.length; i++) {
      const part = decodeURIComponent(parts[i]);
      if (!/^\d+[A-Za-z-]*-/.test(part)) continue;
      const words = part.replace(/-/g, " ");
      if (normalizeAddress(words).startsWith("0 ")) continue;
      candidates.add(words);
    }
    if (zipIndex >= 2 && parts[zipIndex - 2]?.toUpperCase() === city && parts[zipIndex - 3]?.toUpperCase() === state) {
      candidates.add(parts[zipIndex - 1].replace(/-/g, " "));
    }
    return [...candidates];
  } catch {
    return [];
  }
}

function uniqueCandidate(index: Map<string, Set<number>>, zip: string, key: string): number | null {
  const candidates = index.get(`${zip}|${key}`);
  return candidates?.size === 1 ? [...candidates][0] : null;
}

async function loadListings(): Promise<ListingRow[]> {
  const rows: ListingRow[] = [];
  let offset = 0;
  while (rows.length < LIMIT) {
    let query = db.from("listing_signals")
      .select("id,address,zip,property_id,listing_url,last_seen_at")
      .eq("state_code", STATE)
      .ilike("city", CITY)
      .eq("is_on_market", true)
      .not("address", "is", null)
      .order("last_seen_at", { ascending: false, nullsFirst: false });
    query = RELINK_EXISTING_SHELLS ? query.not("property_id", "is", null) : query.is("property_id", null);
    const { data, error } = await query.range(offset, Math.min(offset + 999, LIMIT - 1));
    if (error) throw new Error(`Failed to load listings: ${error.message}`);
    if (!data || data.length === 0) break;
    rows.push(...data as ListingRow[]);
    offset += data.length;
    if (data.length < 1000) break;
  }
  if (!RELINK_EXISTING_SHELLS) return rows;

  const propertyIds = [...new Set(rows.map((row) => row.property_id).filter((id): id is number => Number.isFinite(id)))];
  const shellIds = new Set<number>();
  for (const chunk of chunks(propertyIds, 500)) {
    const { data, error } = await db.from("properties")
      .select("id")
      .in("id", chunk)
      .like("source", "listing_signal_shell:%");
    if (error) throw new Error(`Failed to load current shell properties: ${error.message}`);
    for (const row of data ?? []) shellIds.add(Number(row.id));
  }

  return rows.filter((row) => row.property_id != null && shellIds.has(row.property_id));
}

async function loadProperties(zips: string[], numbers: string[]): Promise<PropertyRow[]> {
  const rows: PropertyRow[] = [];
  for (const zipChunk of chunks(zips, 50)) {
    let offset = 0;
    while (true) {
      const { data, error } = await db.from("properties")
        .select("id,address,zip,city,parcel_id,apn_formatted,source")
        .eq("county_id", COUNTY_ID)
        .eq("state_code", STATE)
        .in("zip", zipChunk)
        .not("address", "is", null)
        .range(offset, offset + 999);
      if (error) throw new Error(`Failed to load properties: ${error.message}`);
      if (!data || data.length === 0) break;
      rows.push(...(data as PropertyRow[]).filter((row) =>
        !String(row.source ?? "").startsWith("listing_signal_shell:")
        && numbers.includes(streetNumber(row.address))
      ));
      offset += data.length;
      if (data.length < 1000) break;
    }
  }
  return rows;
}

function chunks<T>(values: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < values.length; i += size) out.push(values.slice(i, i + size));
  return out;
}

async function updateMatches(matches: Match[]): Promise<void> {
  if (DRY_RUN || matches.length === 0) return;
  for (const chunk of chunks(matches, 100)) {
    for (const match of chunk) {
      let query = db.from("listing_signals")
        .update({ property_id: match.propertyId, updated_at: new Date().toISOString() })
        .eq("id", match.listingId);
      query = RELINK_EXISTING_SHELLS
        ? query.eq("property_id", match.currentPropertyId)
        : query.is("property_id", null);
      const { error } = await query;
      if (error) throw new Error(`Failed to update listing ${match.listingId}: ${error.message}`);
    }
  }
}

async function main(): Promise<void> {
  console.log("MXRE market listing to parcel linker");
  console.log(JSON.stringify({ state: STATE, city: CITY, county_id: COUNTY_ID, limit: LIMIT, dry_run: DRY_RUN, relink_existing_shells: RELINK_EXISTING_SHELLS }, null, 2));

  const listings = await loadListings();
  const zips = [...new Set(listings.map((row) => normZip(row.zip)).filter(Boolean))].sort();
  const listingNumbers = listings.flatMap((row) => [
    streetNumber(row.address),
    ...urlAddressCandidates(row.listing_url, CITY, STATE).map(streetNumber),
  ]).filter(Boolean);
  const numbers = [...new Set(listingNumbers)].sort();
  const properties = await loadProperties(zips, numbers);

  const index = new Map<string, Set<number>>();
  for (const property of properties) {
    const zip = normZip(property.zip);
    if (!zip) continue;
    for (const { key } of addressKeys(property.address)) {
      const mapKey = `${zip}|${key}`;
      const ids = index.get(mapKey) ?? new Set<number>();
      ids.add(property.id);
      index.set(mapKey, ids);
    }
  }

  const matches: Match[] = [];
  const strategyCounts = new Map<string, number>();
  const samples: Array<Record<string, unknown>> = [];
  let ambiguous = 0;

  for (const listing of listings) {
    const zip = normZip(listing.zip);
    let matched: Match | null = null;
    for (const { key, strategy } of addressKeys(listing.address)) {
      const propertyId = uniqueCandidate(index, zip, key);
      if (propertyId) {
        matched = { listingId: listing.id, propertyId, currentPropertyId: listing.property_id, strategy };
        break;
      }
      const candidates = index.get(`${zip}|${key}`);
      if (candidates && candidates.size > 1) ambiguous++;
    }

    if (!matched) {
      for (const candidate of urlAddressCandidates(listing.listing_url, CITY, STATE)) {
        if (streetNumber(candidate) !== streetNumber(listing.address)) continue;
        for (const { key } of addressKeys(candidate)) {
          const propertyId = uniqueCandidate(index, zip, key);
          if (propertyId) {
            matched = { listingId: listing.id, propertyId, currentPropertyId: listing.property_id, strategy: "url_address_fallback" };
            break;
          }
        }
        if (matched) break;
      }
    }

    if (matched) {
      matches.push(matched);
      strategyCounts.set(matched.strategy, (strategyCounts.get(matched.strategy) ?? 0) + 1);
    } else if (samples.length < 15) {
      samples.push({
        listing_id: listing.id,
        address: listing.address,
        zip,
        listing_url: listing.listing_url,
        last_seen_at: listing.last_seen_at,
      });
    }
  }

  await updateMatches(matches);

  console.log(JSON.stringify({
    scanned_listings: listings.length,
    zips_loaded: zips.length,
    street_numbers_loaded: numbers.length,
    candidate_properties_loaded: properties.length,
    matched: matches.length,
    ambiguous_key_hits: ambiguous,
    dry_run: DRY_RUN,
    strategy_counts: Object.fromEntries([...strategyCounts.entries()].sort()),
    unmatched_samples: samples,
  }, null, 2));
}

main().catch((error) => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
