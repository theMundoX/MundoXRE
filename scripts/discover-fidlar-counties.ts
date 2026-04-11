#!/usr/bin/env tsx
/**
 * Brute-force discover Fidlar AVA recorder portals across all US counties.
 * Tests the URL pattern: https://ava.fidlar.com/{State}{CountyNoSpaces}/AvaWeb/
 *
 * Uses the token endpoint for validation — it's lighter than loading the full page.
 * A 200 on the token endpoint means the county portal exists.
 */

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

interface CensusCounty {
  fips: string;
  name: string;
  state: string;
  state_name: string;
  [key: string]: unknown;
}

interface DiscoveredCounty {
  fips: string;
  name: string;
  state: string;
  state_name: string;
  slug: string;
  ava_url: string;
  api_base: string;
  method: string; // how we discovered it (token, ava-page, etc.)
}

const CONCURRENCY = 40;
const TIMEOUT_MS = 10000;
const MAX_RETRIES = 2;

async function tryToken(slug: string): Promise<number | "error"> {
  const tokenUrl = `https://ava.fidlar.com/${slug}/ScrapRelay.WebService.Ava/token`;
  try {
    const resp = await fetch(tokenUrl, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: "grant_type=password&username=anonymous&password=anonymous",
      signal: AbortSignal.timeout(TIMEOUT_MS),
      redirect: "manual",
    });
    return resp.status;
  } catch {
    return "error";
  }
}

async function checkCounty(
  county: CensusCounty,
): Promise<DiscoveredCounty | null> {
  // Remove spaces, apostrophes, hyphens, periods from county name
  const slug = `${county.state}${county.name.replace(/[\s'.\-]/g, "")}`;
  const avaUrl = `https://ava.fidlar.com/${slug}/AvaWeb/`;

  // Try token endpoint with retries
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const status = await tryToken(slug);

    if (status === 200) {
      return {
        fips: county.fips,
        name: county.name,
        state: county.state,
        state_name: county.state_name,
        slug,
        ava_url: avaUrl,
        api_base: `https://ava.fidlar.com/${slug}/ScrapRelay.WebService.Ava/`,
        method: "token-auth",
      };
    }

    if (status === "error" && attempt < MAX_RETRIES) {
      // Retry on timeout/network error
      await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
      continue;
    }

    // Got a definitive non-200 status — try HEAD on AvaWeb as fallback
    if (typeof status === "number" && status !== 200) {
      try {
        const headResp = await fetch(avaUrl, {
          method: "HEAD",
          signal: AbortSignal.timeout(TIMEOUT_MS),
          redirect: "manual",
        });
        if (headResp.status === 200) {
          return {
            fips: county.fips,
            name: county.name,
            state: county.state,
            state_name: county.state_name,
            slug,
            ava_url: avaUrl,
            api_base: `https://ava.fidlar.com/${slug}/ScrapRelay.WebService.Ava/`,
            method: "ava-head",
          };
        }
      } catch {
        // skip
      }
      break;
    }
  }

  return null;
}

async function runBatch<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
  onProgress?: (done: number, total: number) => void,
): Promise<R[]> {
  const results: R[] = [];
  let idx = 0;
  let done = 0;

  async function worker() {
    while (idx < items.length) {
      const i = idx++;
      const result = await fn(items[i]);
      results.push(result);
      done++;
      if (onProgress && done % 50 === 0) onProgress(done, items.length);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function main() {
  const dataDir = resolve(import.meta.dirname!, "..", "data");
  const censusPath = resolve(dataDir, "census-clean.json");
  const outputPath = resolve(dataDir, "fidlar-discovered.json");

  console.log("Loading county list...");
  const counties: CensusCounty[] = JSON.parse(readFileSync(censusPath, "utf-8"));
  console.log(`Total counties: ${counties.length}`);

  console.log(`\nProbing Fidlar AVA for all counties (concurrency: ${CONCURRENCY})...\n`);

  const startTime = Date.now();

  const results = await runBatch(
    counties,
    CONCURRENCY,
    checkCounty,
    (done, total) => {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (done / parseFloat(elapsed)).toFixed(0);
      console.log(`  Progress: ${done}/${total} (${elapsed}s, ~${rate}/sec)`);
    },
  );

  const discovered = results.filter((r): r is DiscoveredCounty => r !== null);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nDone in ${elapsed}s`);
  console.log(`\nDiscovered ${discovered.length} Fidlar AVA counties:\n`);

  // Sort by state then county
  discovered.sort((a, b) => a.state.localeCompare(b.state) || a.name.localeCompare(b.name));

  for (const c of discovered) {
    console.log(`  ${c.state} ${c.name.padEnd(20)} ${c.ava_url}  [${c.method}]`);
  }

  // Group by state for summary
  const byState = new Map<string, DiscoveredCounty[]>();
  for (const c of discovered) {
    if (!byState.has(c.state)) byState.set(c.state, []);
    byState.get(c.state)!.push(c);
  }
  console.log(`\nBy state:`);
  for (const [state, counties] of [...byState.entries()].sort()) {
    console.log(`  ${state}: ${counties.length} counties`);
  }

  writeFileSync(outputPath, JSON.stringify(discovered, null, 2));
  console.log(`\nResults saved to ${outputPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
