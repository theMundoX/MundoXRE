#!/usr/bin/env node
/**
 * PARALLEL INGEST — Process all counties concurrently for maximum throughput
 * Target: 6-7M properties/day (140M in 21 days)
 *
 * Usage: npx tsx run-parallel-ingest.ts [--concurrency 10] [--state TX] [--max-records 1000000]
 */

import "dotenv/config";
import { getCountyConfigs } from "./src/discovery/registry.js";
import { getAdapterForCounty } from "./src/discovery/registry.js";
import { normalizeProperty } from "./src/discovery/normalizer.js";
import { insertCounty, upsertProperties, getCounties } from "./src/db/queries.js";
import type { CountyConfig, RawPropertyRecord } from "./src/discovery/adapters/base.js";
import type { Property } from "./src/db/queries.js";

const BATCH_SIZE = 500;
const DEFAULT_CONCURRENCY = 10;

/**
 * Ensure county exists in DB
 */
async function ensureCountyInDb(config: CountyConfig): Promise<number> {
  const existing = await getCounties();
  const match = existing.find(
    (c: any) =>
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
 * Dedup batch
 */
function dedup(batch: Property[]): Property[] {
  const seen = new Map<string, Property>();
  for (const p of batch) {
    if (!p.parcel_id) {
      seen.set(`no-id-${seen.size}`, p);
      continue;
    }
    const key = `${p.county_id}:${p.parcel_id}`;
    seen.set(key, p);
  }
  return Array.from(seen.values());
}

/**
 * Ingest single county
 */
async function ingestCounty(
  config: CountyConfig,
  options: any
): Promise<{ county: string; state: string; count: number; errors: number; ms: number }> {
  const start = Date.now();

  const adapter = getAdapterForCounty(config);
  if (!adapter) {
    return {
      county: config.name,
      state: config.state,
      count: 0,
      errors: 1,
      ms: Date.now() - start,
    };
  }

  const countyId = await ensureCountyInDb(config);
  let propertyCount = 0;
  let errorCount = 0;
  let batch: Property[] = [];

  try {
    for await (const raw of adapter.fetchProperties(config)) {
      try {
        const normalized = normalizeProperty(raw, countyId);
        if (!normalized.address || !normalized.city) continue;

        batch.push(normalized);

        if (batch.length >= BATCH_SIZE) {
          const dedupedBatch = dedup(batch);
          if (!options.dryRun) {
            const result = await upsertProperties(dedupedBatch);
            propertyCount += result.length;
          } else {
            propertyCount += dedupedBatch.length;
          }
          batch = [];
        }
      } catch {
        errorCount++;
      }

      if (propertyCount + batch.length >= (options.maxRecords || Infinity)) {
        break;
      }
    }

    // Flush
    if (batch.length > 0) {
      const dedupedBatch = dedup(batch);
      if (!options.dryRun) {
        const result = await upsertProperties(dedupedBatch);
        propertyCount += result.length;
      } else {
        propertyCount += dedupedBatch.length;
      }
    }
  } catch (err) {
    errorCount++;
  }

  return {
    county: config.name,
    state: config.state,
    count: propertyCount,
    errors: errorCount,
    ms: Date.now() - start,
  };
}

/**
 * Run multiple counties concurrently
 */
async function processConcurrently(
  counties: CountyConfig[],
  concurrency: number,
  options: any
) {
  const results: any[] = [];
  const running: Promise<any>[] = [];

  for (const county of counties) {
    const promise = ingestCounty(county, options).then((result) => {
      results.push(result);
      return result;
    });

    running.push(promise);

    if (running.length >= concurrency) {
      await Promise.race(running);
      running.splice(
        running.findIndex((p) => Promise.resolve(p) === p),
        1
      );
    }
  }

  // Wait for all remaining
  await Promise.all(running);
  return results;
}

async function main() {
  const args = process.argv.slice(2);
  let concurrency = DEFAULT_CONCURRENCY;
  let stateFilter = "";
  let maxRecords = Infinity;
  let dryRun = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--concurrency") concurrency = parseInt(args[i + 1]);
    if (args[i] === "--state") stateFilter = args[i + 1];
    if (args[i] === "--max-records") maxRecords = parseInt(args[i + 1]);
    if (args[i] === "--dry-run") dryRun = true;
  }

  const configs = getCountyConfigs({ state: stateFilter || undefined });

  console.log("========================================");
  console.log("PARALLEL INGEST PIPELINE");
  console.log(`Processing ${configs.length} counties with concurrency ${concurrency}`);
  console.log("========================================\n");

  const startTime = Date.now();
  const results = await processConcurrently(
    configs,
    concurrency,
    { maxRecords, dryRun }
  );

  // Summary
  const totalCount = results.reduce((a: any, r: any) => a + r.count, 0);
  const totalErrors = results.reduce((a: any, r: any) => a + r.errors, 0);
  const totalMs = Date.now() - startTime;
  const propsPerSec = (totalCount / (totalMs / 1000)).toFixed(0);

  console.log("\n── SUMMARY ──");
  console.log(`Counties processed: ${results.length}`);
  console.log(`Total properties upserted: ${totalCount.toLocaleString()}`);
  console.log(`Total errors: ${totalErrors}`);
  console.log(`Time: ${(totalMs / 1000).toFixed(1)}s`);
  console.log(`Throughput: ${propsPerSec} properties/second`);
  console.log(`Estimated rate: ${Math.floor(totalCount / (totalMs / 1000 / 3600)).toLocaleString()} properties/hour`);

  // Per-state summary
  const byState = new Map<string, { count: number; time: number }>();
  for (const r of results) {
    if (!byState.has(r.state)) {
      byState.set(r.state, { count: 0, time: 0 });
    }
    const s = byState.get(r.state)!;
    s.count += r.count;
    s.time += r.ms;
  }

  console.log("\n── BY STATE ──");
  for (const [state, { count, time }] of byState) {
    console.log(
      `${state}: ${count.toLocaleString()} properties in ${(time / 1000).toFixed(1)}s`
    );
  }
}

main().catch(console.error);
