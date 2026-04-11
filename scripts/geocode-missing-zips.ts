#!/usr/bin/env tsx
/**
 * Geocode properties with missing zip codes.
 *
 * Usage:
 *   npx tsx scripts/geocode-missing-zips.ts --state TX --county Tarrant
 *   npx tsx scripts/geocode-missing-zips.ts --state TX
 *   npx tsx scripts/geocode-missing-zips.ts --county-id 42
 *   npx tsx scripts/geocode-missing-zips.ts --dry-run --state TX --county Tarrant
 *
 * Options:
 *   --state       Filter by state code (e.g. TX)
 *   --county      Filter by county name (partial match)
 *   --county-id   Filter by exact county_id
 *   --batch-size  Number of addresses per Census batch (default: 500)
 *   --limit       Max properties to process (default: all)
 *   --dry-run     Count missing zips without geocoding
 *   --single      Use single-address mode instead of batch (slower but better fallback)
 */

import "dotenv/config";
import { getDb, getWriteDb } from "../src/db/client.js";
import { geocodeAddress, geocodeBatch } from "../src/utils/geocoder.js";
import type { AddressInput } from "../src/utils/geocoder.js";

// ─── CLI Arg Parsing ────────────────────────────────────────────────

function getArg(name: string): string | undefined {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1 || idx + 1 >= process.argv.length) return undefined;
  return process.argv[idx + 1];
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

const STATE = getArg("state")?.toUpperCase();
const COUNTY_NAME = getArg("county");
const COUNTY_ID = getArg("county-id") ? Number(getArg("county-id")) : undefined;
const BATCH_SIZE = Number(getArg("batch-size") ?? "500");
const LIMIT = getArg("limit") ? Number(getArg("limit")) : undefined;
const DRY_RUN = hasFlag("dry-run");
const SINGLE_MODE = hasFlag("single");

// ─── Resolve County ID ──────────────────────────────────────────────

async function resolveCountyId(): Promise<number | undefined> {
  if (COUNTY_ID) return COUNTY_ID;
  if (!COUNTY_NAME) return undefined;

  const db = getDb();
  let query = db
    .from("counties")
    .select("id, county_name, state_code")
    .ilike("county_name", `%${COUNTY_NAME}%`);

  if (STATE) query = query.eq("state_code", STATE);

  const { data, error } = await query;
  if (error) throw new Error(`County lookup failed: ${error.message}`);
  if (!data || data.length === 0) {
    console.error(`No county found matching "${COUNTY_NAME}"${STATE ? ` in ${STATE}` : ""}`);
    process.exit(1);
  }
  if (data.length > 1) {
    console.error(`Multiple counties match "${COUNTY_NAME}":`);
    for (const c of data) {
      console.error(`  id=${c.id}  ${c.county_name}, ${c.state_code}`);
    }
    console.error("Use --county-id to specify one.");
    process.exit(1);
  }

  console.log(`Resolved county: ${data[0].county_name}, ${data[0].state_code} (id=${data[0].id})`);
  return data[0].id;
}

// ─── Query Missing Zips ─────────────────────────────────────────────

interface PropertyRow {
  id: number;
  address: string;
  city: string;
  state_code: string;
}

async function countMissingZips(countyId?: number): Promise<number> {
  const db = getDb();
  let query = db
    .from("properties")
    .select("id", { count: "exact", head: true })
    .or("zip.is.null,zip.eq.");

  if (countyId) query = query.eq("county_id", countyId);
  if (STATE && !countyId) query = query.eq("state_code", STATE);

  const { count, error } = await query;
  if (error) throw new Error(`Count query failed: ${error.message}`);
  return count ?? 0;
}

async function fetchMissingZipBatch(
  countyId: number | undefined,
  offset: number,
  limit: number,
): Promise<PropertyRow[]> {
  const db = getDb();
  let query = db
    .from("properties")
    .select("id, address, city, state_code")
    .or("zip.is.null,zip.eq.");

  if (countyId) query = query.eq("county_id", countyId);
  if (STATE && !countyId) query = query.eq("state_code", STATE);

  const { data, error } = await query
    .order("id")
    .range(offset, offset + limit - 1);

  if (error) throw new Error(`Fetch query failed: ${error.message}`);
  return (data ?? []) as PropertyRow[];
}

// ─── Update Properties ──────────────────────────────────────────────

async function updatePropertyZip(
  id: number,
  zip: string,
  lat: number,
  lng: number,
) {
  const db = getWriteDb();
  const { error } = await db
    .from("properties")
    .update({ zip, lat, lng, updated_at: new Date().toISOString() })
    .eq("id", id);
  if (error) throw new Error(`Update failed for id=${id}: ${error.message}`);
}

async function updatePropertyZipsBatch(
  updates: { id: number; zip: string; lat: number; lng: number }[],
) {
  const db = getWriteDb();
  // Supabase doesn't support bulk UPDATE with different values per row,
  // so we do individual updates but in parallel (small batches).
  const PARALLEL = 10;
  for (let i = 0; i < updates.length; i += PARALLEL) {
    const chunk = updates.slice(i, i + PARALLEL);
    await Promise.all(
      chunk.map((u) => updatePropertyZip(u.id, u.zip, u.lat, u.lng)),
    );
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("=== Geocode Missing Zip Codes ===\n");

  const countyId = await resolveCountyId();

  const totalMissing = await countMissingZips(countyId);
  console.log(`Properties with missing zip: ${totalMissing.toLocaleString()}`);

  if (totalMissing === 0) {
    console.log("Nothing to do.");
    return;
  }

  if (DRY_RUN) {
    console.log("(dry-run mode, exiting)");
    return;
  }

  const toProcess = LIMIT ? Math.min(LIMIT, totalMissing) : totalMissing;
  console.log(`Will process: ${toProcess.toLocaleString()}`);
  console.log(`Mode: ${SINGLE_MODE ? "single-address" : "batch"}`);
  console.log(`Batch size: ${BATCH_SIZE}\n`);

  let processed = 0;
  let geocoded = 0;
  let failed = 0;
  const startTime = Date.now();

  while (processed < toProcess) {
    const fetchSize = Math.min(BATCH_SIZE, toProcess - processed);
    const rows = await fetchMissingZipBatch(countyId, 0, fetchSize);

    if (rows.length === 0) {
      console.log("No more rows to process (all remaining may have been updated).");
      break;
    }

    if (SINGLE_MODE) {
      // Single-address mode with Census + Nominatim fallback
      for (const row of rows) {
        const result = await geocodeAddress(row.address, row.city, row.state_code);
        if (result) {
          await updatePropertyZip(row.id, result.zip, result.lat, result.lng);
          geocoded++;
        } else {
          failed++;
        }
        processed++;
        if (processed % 50 === 0) {
          logProgress(processed, toProcess, geocoded, failed, startTime);
        }
      }
    } else {
      // Batch mode via Census Bureau
      const inputs: AddressInput[] = rows.map((r) => ({
        address: r.address,
        city: r.city,
        state: r.state_code,
      }));

      const batchResults = await geocodeBatch(inputs);

      const updates: { id: number; zip: string; lat: number; lng: number }[] = [];
      const needFallback: { row: PropertyRow; input: AddressInput }[] = [];

      for (let i = 0; i < batchResults.length; i++) {
        const br = batchResults[i];
        if (br.result) {
          updates.push({
            id: rows[i].id,
            zip: br.result.zip,
            lat: br.result.lat,
            lng: br.result.lng,
          });
        } else {
          needFallback.push({ row: rows[i], input: br.input });
        }
      }

      // Write batch successes
      if (updates.length > 0) {
        await updatePropertyZipsBatch(updates);
        geocoded += updates.length;
      }

      // Nominatim fallback for Census misses
      if (needFallback.length > 0) {
        console.log(`  Trying Nominatim for ${needFallback.length} Census misses...`);
        for (const { row, input } of needFallback) {
          // geocodeAddress checks cache first, which has the Census null.
          // We need to try Nominatim directly since Census already failed.
          // But geocodeAddress caches null results from Census too.
          // So we call it — it will use the cached null from Census but
          // the function itself tries Nominatim as fallback on first call.
          // Since batch cached nulls, let's call Nominatim directly.
          const result = await geocodeNominatimDirect(input.address, input.city, input.state);
          if (result) {
            await updatePropertyZip(row.id, result.zip, result.lat, result.lng);
            geocoded++;
          } else {
            failed++;
          }
        }
      }

      processed += rows.length;
      logProgress(processed, toProcess, geocoded, failed, startTime);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("\n=== Done ===");
  console.log(`Processed: ${processed.toLocaleString()}`);
  console.log(`Geocoded:  ${geocoded.toLocaleString()}`);
  console.log(`Failed:    ${failed.toLocaleString()}`);
  console.log(`Time:      ${elapsed}s`);
  console.log(
    `Rate:      ${(geocoded / (parseFloat(elapsed) || 1)).toFixed(1)} geocodes/sec`,
  );
}

function logProgress(
  processed: number,
  total: number,
  geocoded: number,
  failed: number,
  startTime: number,
) {
  const pct = ((processed / total) * 100).toFixed(1);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
  const rate = (processed / (parseFloat(elapsed) || 1)).toFixed(1);
  console.log(
    `  [${pct}%] ${processed.toLocaleString()}/${total.toLocaleString()} | ` +
      `geocoded=${geocoded} failed=${failed} | ${rate}/sec | ${elapsed}s`,
  );
}

/**
 * Direct Nominatim call (bypasses cache check since batch already cached null).
 * This is an inline version to avoid importing private functions.
 */
async function geocodeNominatimDirect(
  address: string,
  city: string,
  state: string,
): Promise<{ zip: string; lat: number; lng: number } | null> {
  // 1 req/sec for Nominatim
  await new Promise((r) => setTimeout(r, 1100));

  const q = `${address}, ${city}, ${state}, USA`;
  const params = new URLSearchParams({
    q,
    format: "jsonv2",
    addressdetails: "1",
    limit: "1",
    countrycodes: "us",
  });

  try {
    const resp = await fetch(
      `https://nominatim.openstreetmap.org/search?${params}`,
      {
        headers: {
          "User-Agent": "MXRE-DataEnrichment/1.0 (property-data-project)",
        },
        signal: AbortSignal.timeout(15_000),
      },
    );
    if (!resp.ok) return null;

    const data = (await resp.json()) as Array<{
      lat: string;
      lon: string;
      address?: { postcode?: string };
    }>;
    if (!data || data.length === 0) return null;

    const hit = data[0];
    const zip = hit.address?.postcode?.split("-")[0];
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

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
