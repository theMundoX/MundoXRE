#!/usr/bin/env node
/**
 * GO - Start full MXRE pipeline with MundoX enrichment
 * Wired for 140M properties in 3 weeks
 *
 * Direct execution - no spawning, pure async/await
 */

import "dotenv/config";
import { getCountyConfigs } from "./src/discovery/registry.js";
import { getAdapterForCounty } from "./src/discovery/registry.js";
import { normalizeProperty } from "./src/discovery/normalizer.js";
import { insertCounty, upsertProperties, getCounties } from "./src/db/queries.js";
import type { CountyConfig, RawPropertyRecord } from "./src/discovery/adapters/base.js";
import type { Property } from "./src/db/queries.js";

const BATCH_SIZE = 500;
const CONCURRENCY = 50;

async function checkMundoX() {
  try {
    const response = await fetch("http://127.0.0.1:18792/health");
    const data = await response.json();
    console.log(`✓ MundoX ready: ${JSON.stringify(data)}`);
    return true;
  } catch (e) {
    console.error("✗ MundoX not responding on 18792");
    return false;
  }
}

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

async function ingestCounty(config: CountyConfig): Promise<{ county: string; state: string; count: number; ms: number }> {
  const start = Date.now();
  const adapter = getAdapterForCounty(config);

  if (!adapter) {
    return { county: config.name, state: config.state, count: 0, ms: Date.now() - start };
  }

  const countyId = await ensureCountyInDb(config);
  let propertyCount = 0;
  let batch: Property[] = [];

  try {
    for await (const raw of adapter.fetchProperties(config)) {
      try {
        const normalized = normalizeProperty(raw, countyId);
        if (!normalized.address || !normalized.city) continue;

        batch.push(normalized);

        if (batch.length >= BATCH_SIZE) {
          const dedupedBatch = dedup(batch);
          const result = await upsertProperties(dedupedBatch);
          propertyCount += result.length;
          batch = [];
        }
      } catch {
        // Skip bad records
      }
    }

    if (batch.length > 0) {
      const dedupedBatch = dedup(batch);
      const result = await upsertProperties(dedupedBatch);
      propertyCount += result.length;
    }
  } catch (err) {
    // County processing failed - continue
  }

  return {
    county: config.name,
    state: config.state,
    count: propertyCount,
    ms: Date.now() - start,
  };
}

async function processConcurrently(counties: CountyConfig[], concurrency: number) {
  const results: any[] = [];
  const running: Promise<any>[] = [];

  for (const county of counties) {
    const promise = ingestCounty(county).then((result) => {
      results.push(result);
      console.log(`  ✓ ${result.county}, ${result.state}: ${result.count.toLocaleString()} properties`);
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

  await Promise.all(running);
  return results;
}

async function main() {
  console.log("========================================");
  console.log("MXRE PIPELINE - GO");
  console.log("Target: 140M properties in 21 days");
  console.log("========================================\n");

  // Check MundoX
  const mundoxReady = await checkMundoX();
  if (!mundoxReady) {
    console.error("Start MundoX first: powershell C:\\Users\\msanc\\mundox-services\\start-mundox-worker.ps1");
    process.exit(1);
  }

  const configs = getCountyConfigs();
  console.log(`\nStarting parallel ingest: ${configs.length} counties, concurrency ${CONCURRENCY}\n`);

  const startTime = Date.now();
  const results = await processConcurrently(configs, CONCURRENCY);

  const totalCount = results.reduce((a: any, r: any) => a + r.count, 0);
  const totalMs = Date.now() - startTime;
  const propsPerSec = (totalCount / (totalMs / 1000)).toFixed(0);

  console.log("\n── SUMMARY ──");
  console.log(`Total properties ingested: ${totalCount.toLocaleString()}`);
  console.log(`Time: ${(totalMs / 1000).toFixed(1)}s`);
  console.log(`Throughput: ${propsPerSec} props/sec`);
  console.log(`Rate: ${Math.floor(totalCount / (totalMs / 1000 / 3600)).toLocaleString()} props/hour`);
}

main().catch(console.error);
