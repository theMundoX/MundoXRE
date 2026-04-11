/**
 * Geocoding utility for resolving addresses to zip codes.
 *
 * Cascading strategy:
 *   1. US Census Bureau Geocoder (free, no key, ~10 req/s)
 *   2. Nominatim / OpenStreetMap (free, 1 req/s)
 *
 * Also supports Census batch endpoint for bulk geocoding.
 * Results are cached to the local filesystem to avoid repeat lookups.
 */

import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";

// ─── Types ──────────────────────────────────────────────────────────

export interface GeoResult {
  zip: string;
  lat: number;
  lng: number;
}

export interface AddressInput {
  address: string;
  city: string;
  state: string;
}

export interface BatchResult {
  input: AddressInput;
  result: GeoResult | null;
}

// ─── Cache ──────────────────────────────────────────────────────────

const CACHE_DIR = join(process.cwd(), ".cache", "geocode");
const CACHE_TTL_MS = 90 * 24 * 60 * 60 * 1000; // 90 days — zip codes don't change

function ensureCacheDir() {
  if (!existsSync(CACHE_DIR)) {
    mkdirSync(CACHE_DIR, { recursive: true });
  }
}

function cacheKey(address: string, city: string, state: string): string {
  const raw = `${address}|${city}|${state}`.toLowerCase().trim();
  return createHash("sha256").update(raw).digest("hex").slice(0, 20);
}

function getCached(address: string, city: string, state: string): GeoResult | null | undefined {
  const file = join(CACHE_DIR, cacheKey(address, city, state));
  if (!existsSync(file)) return undefined; // not cached
  try {
    const stat = require("node:fs").statSync(file);
    if (Date.now() - stat.mtimeMs > CACHE_TTL_MS) return undefined;
    const data = JSON.parse(readFileSync(file, "utf-8"));
    // null means "previously looked up but no result"
    return data as GeoResult | null;
  } catch {
    return undefined;
  }
}

function setCache(address: string, city: string, state: string, result: GeoResult | null) {
  ensureCacheDir();
  const file = join(CACHE_DIR, cacheKey(address, city, state));
  writeFileSync(file, JSON.stringify(result), "utf-8");
}

// ─── Rate Limiting ──────────────────────────────────────────────────

class SimpleRateLimiter {
  private lastRequest = 0;
  constructor(private intervalMs: number) {}

  async wait(): Promise<void> {
    const now = Date.now();
    const elapsed = now - this.lastRequest;
    const wait = Math.max(0, this.intervalMs - elapsed);
    if (wait > 0) {
      await new Promise((r) => setTimeout(r, wait));
    }
    this.lastRequest = Date.now();
  }
}

const censusLimiter = new SimpleRateLimiter(120);   // ~8 req/s (conservative)
const nominatimLimiter = new SimpleRateLimiter(1100); // just over 1 req/s

// ─── Census Bureau Geocoder ─────────────────────────────────────────

const CENSUS_SINGLE_URL =
  "https://geocoding.geo.census.gov/geocoder/geographies/address";

async function geocodeCensus(
  address: string,
  city: string,
  state: string,
): Promise<GeoResult | null> {
  await censusLimiter.wait();

  const params = new URLSearchParams({
    street: address,
    city,
    state,
    benchmark: "Public_AR_Current",
    vintage: "Current_Current",
    format: "json",
  });

  try {
    const resp = await fetch(`${CENSUS_SINGLE_URL}?${params}`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) return null;

    const data = await resp.json();
    const matches = data?.result?.addressMatches;
    if (!matches || matches.length === 0) return null;

    const match = matches[0];
    const coords = match.coordinates;
    // Extract zip from the matched address or geographies
    const matchedZip =
      match.addressComponents?.zip ??
      extractZipFromAddress(match.matchedAddress ?? "");

    if (!matchedZip) return null;

    return {
      zip: matchedZip,
      lat: coords.y,
      lng: coords.x,
    };
  } catch {
    return null;
  }
}

function extractZipFromAddress(addr: string): string | null {
  // Census matched addresses end with ", STATE, ZIP"
  const match = addr.match(/\b(\d{5})(?:-\d{4})?\s*$/);
  return match ? match[1] : null;
}

// ─── Census Batch Geocoder ──────────────────────────────────────────

const CENSUS_BATCH_URL =
  "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch";
const MAX_BATCH_SIZE = 1000; // Census limit

/**
 * Batch geocode using the Census Bureau batch endpoint.
 * Accepts up to 10,000 addresses (splits into chunks of 1000).
 * Returns results in the same order as input.
 */
export async function geocodeBatch(
  addresses: AddressInput[],
): Promise<BatchResult[]> {
  const results: BatchResult[] = addresses.map((input) => ({
    input,
    result: null,
  }));

  // Check cache first
  const uncached: { index: number; input: AddressInput }[] = [];
  for (let i = 0; i < addresses.length; i++) {
    const { address, city, state } = addresses[i];
    const cached = getCached(address, city, state);
    if (cached !== undefined) {
      results[i].result = cached;
    } else {
      uncached.push({ index: i, input: addresses[i] });
    }
  }

  if (uncached.length === 0) return results;

  // Process in chunks of MAX_BATCH_SIZE
  for (let start = 0; start < uncached.length; start += MAX_BATCH_SIZE) {
    const chunk = uncached.slice(start, start + MAX_BATCH_SIZE);
    await censusLimiter.wait();

    // Build CSV payload: id, street, city, state, zip
    const csv = chunk
      .map(
        ({ index, input }) =>
          `${index},"${input.address}","${input.city}","${input.state}",`,
      )
      .join("\n");

    try {
      const formData = new FormData();
      formData.append(
        "addressFile",
        new Blob([csv], { type: "text/csv" }),
        "addresses.csv",
      );
      formData.append("benchmark", "Public_AR_Current");
      formData.append("vintage", "Current_Current");

      const resp = await fetch(CENSUS_BATCH_URL, {
        method: "POST",
        body: formData,
        signal: AbortSignal.timeout(120_000), // 2 min for large batches
      });

      if (!resp.ok) {
        console.error(`  Census batch returned ${resp.status}`);
        continue;
      }

      const text = await resp.text();
      parseBatchResponse(text, chunk, results);
    } catch (err) {
      console.error(`  Census batch error: ${(err as Error).message}`);
    }
  }

  return results;
}

function parseBatchResponse(
  csv: string,
  chunk: { index: number; input: AddressInput }[],
  results: BatchResult[],
) {
  const indexMap = new Map(chunk.map((c) => [String(c.index), c]));

  for (const line of csv.split("\n")) {
    if (!line.trim()) continue;

    // CSV fields: id, input_address, match_status, match_type, matched_address,
    //             lon/lat, tiger_line_id, side, state_fips, county_fips, tract, block
    const fields = parseCSVLine(line);
    const id = fields[0]?.replace(/"/g, "").trim();
    const entry = indexMap.get(id);
    if (!entry) continue;

    const matchStatus = fields[2]?.replace(/"/g, "").trim().toLowerCase();
    if (matchStatus !== "match") {
      setCache(entry.input.address, entry.input.city, entry.input.state, null);
      continue;
    }

    const matchedAddr = fields[4]?.replace(/"/g, "") ?? "";
    const coordStr = fields[5]?.replace(/"/g, "") ?? "";
    const [lngStr, latStr] = coordStr.split(",").map((s) => s.trim());
    const lat = parseFloat(latStr);
    const lng = parseFloat(lngStr);

    const zip = extractZipFromAddress(matchedAddr);
    if (!zip || isNaN(lat) || isNaN(lng)) {
      setCache(entry.input.address, entry.input.city, entry.input.state, null);
      continue;
    }

    const geo: GeoResult = { zip, lat, lng };
    results[entry.index].result = geo;
    setCache(entry.input.address, entry.input.city, entry.input.state, geo);
  }
}

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"') {
      inQuotes = !inQuotes;
      current += ch;
    } else if (ch === "," && !inQuotes) {
      fields.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

// ─── Nominatim Fallback ─────────────────────────────────────────────

const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";

async function geocodeNominatim(
  address: string,
  city: string,
  state: string,
): Promise<GeoResult | null> {
  await nominatimLimiter.wait();

  const q = `${address}, ${city}, ${state}, USA`;
  const params = new URLSearchParams({
    q,
    format: "jsonv2",
    addressdetails: "1",
    limit: "1",
    countrycodes: "us",
  });

  try {
    const resp = await fetch(`${NOMINATIM_URL}?${params}`, {
      headers: {
        "User-Agent": "MXRE-DataEnrichment/1.0 (property-data-project)",
      },
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) return null;

    const data = (await resp.json()) as Array<{
      lat: string;
      lon: string;
      address?: { postcode?: string };
    }>;
    if (!data || data.length === 0) return null;

    const hit = data[0];
    const zip = hit.address?.postcode?.split("-")[0]; // strip +4
    if (!zip || !/^\d{5}$/.test(zip)) return null;

    return {
      zip,
      lat: parseFloat(hit.lat),
      lng: parseFloat(hit.lon),
    };
  } catch {
    return null;
  }
}

// ─── Main Entry Point ───────────────────────────────────────────────

/**
 * Geocode a single address. Tries Census first, falls back to Nominatim.
 * Results are cached to disk.
 */
export async function geocodeAddress(
  address: string,
  city: string,
  state: string,
): Promise<GeoResult | null> {
  // Check cache
  const cached = getCached(address, city, state);
  if (cached !== undefined) return cached;

  // Try Census Bureau first
  let result = await geocodeCensus(address, city, state);

  // Fallback to Nominatim
  if (!result) {
    result = await geocodeNominatim(address, city, state);
  }

  // Cache either way (null = "we tried, no result")
  setCache(address, city, state, result);
  return result;
}
