#!/usr/bin/env tsx
/**
 * STABLE INGEST v2 - Ultra-conservative, no spawning, no batching complexity
 * Simplest possible implementation for Windows stability
 */
import "dotenv/config";
import { getCountyConfigs } from "./src/discovery/registry.js";
import { getAdapterForCounty } from "./src/discovery/registry.js";
import { normalizeProperty } from "./src/discovery/normalizer.js";
import { insertCounty, upsertProperties, getCounties } from "./src/db/queries.js";

const BATCH_SIZE = 100;  // Smaller batches
const CONCURRENCY = 3;    // Ultra-conservative concurrency
let totalProcessed = 0;
let startTime = Date.now();

console.log("🚀 STABLE INGEST V2 (Ultra-Conservative)\n");

async function ensureCountyInDb(config: any): Promise<number> {
  const existing = await getCounties();
  const match = existing.find(
    (c: any) => c.state_fips === config.state_fips && c.county_fips === config.county_fips
  );
  if (match) return match.id;
  const created = await insertCounty({
    state_fips: config.state_fips,
    county_fips: config.county_fips,
    state_code: config.state,
    county_name: config.name,
    assessor_url: config.base_url,
  });
  return created.id;
}

async function ingestCounty(config: any): Promise<number> {
  const adapter = getAdapterForCounty(config);
  if (!adapter) return 0;

  const countyId = await ensureCountyInDb(config);
  let batch: any[] = [];
  let count = 0;

  try {
    for await (const raw of adapter.fetchProperties(config)) {
      const normalized = normalizeProperty(raw, countyId);
      if (!normalized.address || !normalized.city) continue;

      batch.push(normalized);
      if (batch.length >= BATCH_SIZE) {
        const deduped = Array.from(new Map(batch.map((p) => [`${p.county_id}:${p.parcel_id}`, p])).values());
        await upsertProperties(deduped);
        count += deduped.length;
        totalProcessed += deduped.length;
        batch = [];
        
        // Small delay to prevent overwhelming
        await new Promise(r => setTimeout(r, 50));
      }
    }
    if (batch.length > 0) {
      const deduped = Array.from(new Map(batch.map((p) => [`${p.county_id}:${p.parcel_id}`, p])).values());
      await upsertProperties(deduped);
      count += deduped.length;
      totalProcessed += deduped.length;
    }
  } catch (err: any) {
    console.error(`  ${config.name}: ${err.message}`);
  }

  return count;
}

async function main() {
  try {
    const counties = getCountyConfigs();
    console.log(`Processing ${counties.length} counties...\n`);

    let running = 0;
    let index = 0;

    while (index < counties.length || running > 0) {
      // Start new jobs up to concurrency limit
      while (running < CONCURRENCY && index < counties.length) {
        const config = counties[index++];
        running++;

        ingestCounty(config)
          .then((count) => {
            if (count > 0) {
              const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
              const rate = (totalProcessed / (Date.now() - startTime) * 1000).toFixed(0);
              console.log(`✓ ${config.name}: ${count.toLocaleString()} | Total: ${totalProcessed.toLocaleString()} (${rate} rec/s) [${elapsed}m]`);
            }
          })
          .catch((err: any) => console.error(`✗ ${config.name}: ${err.message}`))
          .finally(() => {
            running--;
          });
      }

      // Wait before checking again
      await new Promise((r) => setTimeout(r, 200));
    }

    console.log("\n✅ INGEST COMPLETE");
    process.exit(0);
  } catch (err: any) {
    console.error("FATAL:", err.message);
    process.exit(1);
  }
}

main();
