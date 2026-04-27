#!/usr/bin/env tsx
/**
 * Curated County Ingest - High Success Rate
 *
 * Only ingests from counties with known-working ArcGIS endpoints.
 * Skips broken/429-rate-limited endpoints entirely.
 * Much faster than retrying broken endpoints 5x each.
 *
 * Strategy: Ingest working counties at CONCURRENCY=8 to maximize throughput
 */

import "dotenv/config";
import { getCountyConfigs } from "./src/discovery/registry.js";
import { getAdapterForCounty } from "./src/discovery/registry.js";
import { normalizeProperty } from "./src/discovery/normalizer.js";
import { insertCounty, upsertProperties, getCounties } from "./src/db/queries.js";

const BATCH_SIZE = 500;
const CONCURRENCY = 10;  // Higher concurrency - no slow NY/Socrata blocking the queue
let totalProcessed = 0;
let startTime = Date.now();
let successfulCounties = 0;
let failedCounties = 0;
let skippedCounties = 0;

// Counties with confirmed-working or high-confidence endpoints
// Removed: Hamilton OH (ArcGIS 404), Cook/DuPage IL (Socrata returns HTML), NY counties (statewide API timeouts)
const WORKING_COUNTIES = [
  // Ohio - proven ArcGIS
  "Franklin", "Cuyahoga", "Montgomery",
  // North Carolina statewide - proven
  "Mecklenburg", "Wake", "Guilford", "Durham", "Forsyth", "Cumberland", "Gaston",
  // California statewide - proven (large)
  "Los Angeles", "Alameda", "Sacramento", "San Diego", "Santa Clara", "Orange", "Riverside",
  // Minnesota statewide - proven
  "Hennepin", "Ramsey", "Dakota", "Anoka", "Washington",
  // Arizona ArcGIS - proven
  "Maricopa", "Pima",
  // Texas assessors - proven
  "Harris", "Dallas", "Tarrant", "Bexar", "Travis",
  // Tennessee ArcGIS
  "Shelby", "Davidson",
  // Georgia ArcGIS
  "Fulton", "DeKalb",
  // Florida ArcGIS - new
  "Hillsborough", "Palm Beach", "Broward",
  // Nevada ArcGIS - new
  "Clark",
  // Washington ArcGIS - new
  "King",
  // Pennsylvania ArcGIS - new
  "Allegheny",
  // Colorado statewide ArcGIS - new (all share one endpoint)
  "Denver", "Arapahoe", "Jefferson", "Adams", "El Paso", "Douglas", "Larimer", "Boulder",
  // Utah ArcGIS - new
  "Salt Lake",
];

console.log("🚀 CURATED COUNTY INGEST v2 (Expanded + Fixed)\n");
console.log(`✅ Processing ${WORKING_COUNTIES.length} counties (removed broken Hamilton OH, Cook/DuPage IL, NY statewide)`);
console.log(`✅ Added: FL, NV, WA, PA, CO, UT, GA, TN counties`);
console.log(`✅ Concurrency: ${CONCURRENCY} (no slow endpoints blocking queue)\n`);

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

async function ingestCounty(config: any): Promise<{ count: number; success: boolean; error?: string }> {
  const adapter = getAdapterForCounty(config);
  if (!adapter) {
    return { count: 0, success: false, error: "No adapter" };
  }

  const countyId = await ensureCountyInDb(config);
  let batch: any[] = [];
  let count = 0;

  try {
    for await (const raw of adapter.fetchProperties(config)) {
      const normalized = normalizeProperty(raw, countyId);
      if (!normalized.address || !normalized.city) continue;

      batch.push(normalized);
      if (batch.length >= BATCH_SIZE) {
        const deduped = Array.from(
          new Map(batch.map((p) => [`${p.county_id}:${p.parcel_id}`, p])).values()
        );
        await upsertProperties(deduped);
        count += deduped.length;
        totalProcessed += deduped.length;
        batch = [];
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    if (batch.length > 0) {
      const deduped = Array.from(
        new Map(batch.map((p) => [`${p.county_id}:${p.parcel_id}`, p])).values()
      );
      await upsertProperties(deduped);
      count += deduped.length;
      totalProcessed += deduped.length;
    }

    if (count > 0) {
      successfulCounties++;
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const rate = (totalProcessed / (Date.now() - startTime) * 1000).toFixed(0);
      console.log(`✓ ${config.name}: ${count.toLocaleString()} | Total: ${totalProcessed.toLocaleString()} (${rate} rec/s) [${elapsed}m]`);
    }

    return { count, success: count > 0 };
  } catch (err: any) {
    failedCounties++;
    console.error(`✗ ${config.name}: ${err.message}`);
    return { count: 0, success: false, error: err.message };
  }
}

async function main() {
  try {
    const allCounties = getCountyConfigs();
    const workingCounties = allCounties.filter((c: any) =>
      WORKING_COUNTIES.includes(c.name)
    );

    const skipped = allCounties.length - workingCounties.length;
    skippedCounties = skipped;

    console.log(`Processing ${workingCounties.length} working counties`);
    console.log(`Skipping ${skipped} counties with broken endpoints\n`);

    let running = 0;
    let index = 0;

    while (index < workingCounties.length || running > 0) {
      while (running < CONCURRENCY && index < workingCounties.length) {
        const config = workingCounties[index++];
        running++;

        ingestCounty(config)
          .catch((err: any) => {
            console.error(`FATAL: ${config.name}: ${err.message}`);
            return { count: 0, success: false, error: err.message };
          })
          .finally(() => {
            running--;
          });
      }

      await new Promise((r) => setTimeout(r, 200));
    }

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const rate = (totalProcessed / (Date.now() - startTime) * 1000).toFixed(0);

    console.log("\n" + "=".repeat(60));
    console.log("✅ CURATED INGEST COMPLETE");
    console.log("=".repeat(60));
    console.log(`Total Records: ${totalProcessed.toLocaleString()}`);
    console.log(`Successful Counties: ${successfulCounties}`);
    console.log(`Failed Counties: ${failedCounties}`);
    console.log(`Skipped (broken): ${skippedCounties}`);
    console.log(`Elapsed: ${elapsed} minutes`);
    console.log(`Rate: ${rate} records/second`);
    console.log("=".repeat(60) + "\n");

    process.exit(0);
  } catch (err: any) {
    console.error("FATAL:", err.message);
    process.exit(1);
  }
}

main();
