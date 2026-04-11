/**
 * Ingestion Pipeline — orchestrates county data import.
 * Loads county config → picks adapter → scrapes → normalizes → upserts in batches.
 */

import { getCountyConfigs, getAdapterForCounty } from "./registry.js";
import { normalizeProperty } from "./normalizer.js";
import { insertCounty, upsertProperties, getCounties } from "../db/queries.js";
import { getOrCreateRouter } from "../llm/provider.js";
import type { CountyConfig, RawPropertyRecord } from "./adapters/base.js";
import type { Property } from "../db/queries.js";

const router = getOrCreateRouter();

const BATCH_SIZE = 50;

// Enrichment removed from ingest pipeline — handle via separate background job
// This keeps ingest throughput at maximum (millions/day)

/**
 * Deduplicate properties within a batch by (county_id, parcel_id).
 * Supabase upsert will fail if the same conflict key appears twice in one batch.
 * Keep the last occurrence (most recent data).
 */
function dedup(batch: Property[]): Property[] {
  const seen = new Map<string, Property>();
  for (const p of batch) {
    if (!p.parcel_id) {
      // No parcel_id — can't dedup, keep all
      seen.set(`no-id-${seen.size}`, p);
      continue;
    }
    const key = `${p.county_id}:${p.parcel_id}`;
    seen.set(key, p); // later entries overwrite earlier ones
  }
  return Array.from(seen.values());
}

export interface IngestOptions {
  state?: string;
  county?: string;
  platform?: string;
  dryRun?: boolean;
  maxRecords?: number;
}

export interface IngestResult {
  county: string;
  state: string;
  properties_found: number;
  properties_upserted: number;
  errors: number;
  duration_ms: number;
}

/**
 * Ensure the county exists in the database, create if needed.
 */
async function ensureCountyInDb(config: CountyConfig): Promise<number> {
  const existing = await getCounties();
  const match = existing.find(
    (c: { state_fips?: string; county_fips?: string }) =>
      c.state_fips === config.state_fips && c.county_fips === config.county_fips,
  );
  if (match) return match.id as number;

  const created = await insertCounty({
    state_fips: config.state_fips,
    county_fips: config.county_fips,
    state_code: config.state,
    county_name: config.name,
    assessor_url: config.base_url,
  });
  return created.id as number;
}

/**
 * Ingest properties for a single county.
 */
async function ingestCounty(
  config: CountyConfig,
  options: IngestOptions,
): Promise<IngestResult> {
  const start = Date.now();
  const result: IngestResult = {
    county: config.name,
    state: config.state,
    properties_found: 0,
    properties_upserted: 0,
    errors: 0,
    duration_ms: 0,
  };

  const adapter = getAdapterForCounty(config);
  if (!adapter) {
    console.error(`  No adapter for ${config.name} (platform: ${config.platform})`);
    result.errors = 1;
    result.duration_ms = Date.now() - start;
    return result;
  }

  console.log(`  Using adapter: ${adapter.platform}`);

  // Ensure county exists in DB
  const countyId = await ensureCountyInDb(config);
  console.log(`  County ID: ${countyId}`);

  // Estimate count if available
  const estimate = await adapter.estimateCount(config);
  if (estimate) {
    console.log(`  Estimated properties: ${estimate.toLocaleString()}`);
  }

  // Fetch and batch-upsert
  let batch: Property[] = [];
  const maxRecords = options.maxRecords ?? Infinity;

  try {
    for await (const raw of adapter.fetchProperties(config, (p) => {
      if (p.total_processed % 100 === 0 && p.total_processed > 0) {
        console.log(`  Progress: ${p.total_processed} processed, ${p.errors} errors`);
      }
    })) {
      result.properties_found++;

      if (result.properties_found > maxRecords) break;

      try {
        const normalized = normalizeProperty(raw, countyId);
        if (!normalized.address || !normalized.city) continue;

        // Skip enrichment here — ingest at max speed, enrich via background job
        batch.push(normalized);

        if (batch.length >= BATCH_SIZE) {
          const dedupedBatch = dedup(batch);
          if (!options.dryRun) {
            const upserted = await upsertProperties(dedupedBatch);
            result.properties_upserted += upserted.length;
          } else {
            result.properties_upserted += dedupedBatch.length;
          }
          batch = [];
        }
      } catch {
        result.errors++;
      }
    }

    // Flush remaining batch
    if (batch.length > 0) {
      const dedupedBatch = dedup(batch);
      if (!options.dryRun) {
        const upserted = await upsertProperties(dedupedBatch);
        result.properties_upserted += upserted.length;
      } else {
        result.properties_upserted += dedupedBatch.length;
      }
    }
  } catch (err) {
    console.error(`  Fatal error during ingestion:`, err instanceof Error ? err.message : "Unknown error");
    result.errors++;
  }

  result.duration_ms = Date.now() - start;
  return result;
}

/**
 * Main ingestion entry point. Processes all matching counties.
 */
export async function ingest(options: IngestOptions): Promise<IngestResult[]> {
  const configs = getCountyConfigs({
    state: options.state,
    county: options.county,
    platform: options.platform,
  });

  if (configs.length === 0) {
    console.log("No matching counties found in registry.");
    return [];
  }

  console.log(`Found ${configs.length} county config(s) to process.\n`);

  const results: IngestResult[] = [];

  for (const config of configs) {
    console.log(`\n── ${config.name} County, ${config.state} ──`);
    const result = await ingestCounty(config, options);
    results.push(result);

    console.log(
      `  Done: ${result.properties_upserted}/${result.properties_found} upserted, ` +
        `${result.errors} errors, ${(result.duration_ms / 1000).toFixed(1)}s`,
    );
  }

  // Summary
  console.log("\n── Summary ──");
  const totals = results.reduce(
    (acc, r) => ({
      found: acc.found + r.properties_found,
      upserted: acc.upserted + r.properties_upserted,
      errors: acc.errors + r.errors,
      ms: acc.ms + r.duration_ms,
    }),
    { found: 0, upserted: 0, errors: 0, ms: 0 },
  );
  console.log(`Total: ${totals.upserted}/${totals.found} properties, ${totals.errors} errors, ${(totals.ms / 1000).toFixed(1)}s`);

  return results;
}
