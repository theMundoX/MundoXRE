#!/usr/bin/env tsx
/**
 * Fast County Ingest — search-only mode.
 *
 * Instead of fetching ~57K individual detail pages (~100+ hours),
 * this script only does the 56 township/range search requests (~10 minutes)
 * and ingests basic property data from search results.
 *
 * Data captured: parcel_id, owner_name, address, subdivision/city, legal description, acres
 * Data NOT captured (needs detail backfill): assessed_value, year_built, sqft, stories, sale history
 *
 * Usage:
 *   npx tsx scripts/fast-ingest.ts --county=Comanche --state=OK
 *   npx tsx scripts/fast-ingest.ts --county=Comanche --state=OK --dry-run
 */

import "dotenv/config";
import { chromium, type Page, type BrowserContext } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";
import { getResidentialProxy } from "../src/utils/proxy.js";
import { initProxies } from "../src/utils/proxy.js";
import { getCached, setCache } from "../src/utils/cache.js";
import { normalizeProperty } from "../src/discovery/normalizer.js";
import type { RawPropertyRecord } from "../src/discovery/adapters/base.js";

initProxies();

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}
function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const stateFilter = getArg("state");
const countyFilter = getArg("county");
const dryRun = hasFlag("dry-run");

if (!stateFilter || !countyFilter) {
  console.log("Usage: npx tsx scripts/fast-ingest.ts --county=Comanche --state=OK");
  process.exit(1);
}

// ─── Database Setup (supports self-hosted via env override) ────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── County Configs ────────────────────────────────────────────────

interface CountySTRGrid {
  county_fips: string;
  state_fips: string;
  name: string;
  state: string;
  base_url: string;
  townships: number[];
  ranges: number[];
  estimated_properties: number;
}

const COUNTY_GRIDS: Record<string, CountySTRGrid> = {
  "Comanche_OK": {
    county_fips: "031",
    state_fips: "40",
    name: "Comanche",
    state: "OK",
    base_url: "https://www.actdatascout.com/RealProperty/Search/40031",
    townships: [1, 2, 3, 4, -1, -2, -3],
    ranges: [9, 10, 11, 12, 13, 14, 15, 16],
    estimated_properties: 57_081,
  },
  "Oklahoma_OK": {
    county_fips: "109",
    state_fips: "40",
    name: "Oklahoma",
    state: "OK",
    base_url: "https://www.actdatascout.com/RealProperty/Search/40109",
    townships: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, -1, -2, -3, -4, -5],
    ranges: [1, 2, 3, 4, 5, 6, 7, 8],
    estimated_properties: 350_000,
  },
  "Tulsa_OK": {
    county_fips: "143",
    state_fips: "40",
    name: "Tulsa",
    state: "OK",
    base_url: "https://www.actdatascout.com/RealProperty/Search/40143",
    townships: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -1, -2, -3],
    ranges: [10, 11, 12, 13, 14, 15],
    estimated_properties: 280_000,
  },
};

// ─── Browser Setup ─────────────────────────────────────────────────

async function createBrowser(): Promise<{ context: BrowserContext; close: () => Promise<void> }> {
  const stealth = getStealthConfig();
  const proxyUrl = getResidentialProxy();

  const launchOpts: Parameters<typeof chromium.launch>[0] = { headless: true };

  if (proxyUrl) {
    try {
      const parsed = new URL(proxyUrl);
      launchOpts.proxy = {
        server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
        username: parsed.username || undefined,
        password: parsed.password || undefined,
      };
    } catch { /* skip */ }
  }

  const browser = await chromium.launch(launchOpts);
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });

  return {
    context,
    close: async () => { await context.close(); await browser.close(); },
  };
}

// ─── Search Functions ──────────────────────────────────────────────

interface SearchResult {
  crpid: string;
  rpid: string;
  parcel: string;
  ownerName: string;
  businessName: string;
  address: string;
  str: string;
  subdivision: string;
  legal: string;
  acres: string;
}

async function searchBySTR(
  page: Page,
  baseUrl: string,
  countyId: string,
  township: number,
  range: number,
): Promise<SearchResult[]> {
  const cacheKey = `actds:str:${countyId}:0-${Math.abs(township)}-${range}`;
  const cached = getCached(cacheKey);
  if (cached) {
    const results = JSON.parse(cached) as SearchResult[];
    return results;
  }

  // Rate limit — 2s between requests
  await new Promise(r => setTimeout(r, 2000));

  try {
    await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const fields = { Township: String(Math.abs(township)), Range: String(range) };
    const fieldJson = JSON.stringify(fields);

    await page.evaluate(`
      (() => {
        const form = document.querySelector("#RealFormSTRSearch");
        if (!form) return;
        form.style.display = "block";
        const fields = ${fieldJson};
        for (const [name, value] of Object.entries(fields)) {
          const input = form.querySelector('input[name="' + name + '"]');
          if (input) {
            input.value = value;
            input.dispatchEvent(new Event("input", { bubbles: true }));
          }
        }
      })()
    `);

    await page.waitForTimeout(500);

    // Submit form via evaluate (more reliable than locator approach)
    await page.evaluate(`
      (() => {
        const btn = document.querySelector('#RealFormSTRSearch button[type="submit"]');
        if (btn) btn.click();
      })()
    `);
    await page.waitForLoadState("networkidle", { timeout: 30_000 }).catch(() => {});
    await page.waitForTimeout(2000);

    const results = await page.evaluate(`
      (() => {
        const rows = document.querySelectorAll("#RealPropertyResultsTable tbody tr");
        const results = [];
        for (const row of rows) {
          const cells = row.querySelectorAll("td");
          if (cells.length < 9) continue;
          results.push({
            crpid: cells[1]?.textContent?.trim() || "",
            rpid: cells[2]?.textContent?.trim() || "",
            parcel: cells[3]?.textContent?.trim() || "",
            ownerName: cells[4]?.textContent?.trim() || "",
            businessName: cells[5]?.textContent?.trim() || "",
            address: cells[6]?.textContent?.trim() || "",
            str: cells[7]?.textContent?.trim() || "",
            subdivision: cells[8]?.textContent?.trim() || "",
            legal: cells[9]?.textContent?.trim() || "",
            acres: cells[10]?.textContent?.trim() || "",
          });
        }
        return results;
      })()
    `) as SearchResult[];

    if (results.length > 0) {
      setCache(cacheKey, JSON.stringify(results));
    }

    return results;
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown";
    console.error(`  Search error T${township}R${range}: ${msg}`);
    return [];
  }
}

// ─── Convert Search Result → RawPropertyRecord ────────────────────

function searchResultToRaw(result: SearchResult, countyName: string, state: string): RawPropertyRecord {
  return {
    parcel_id: result.parcel,
    address: result.address,
    city: result.subdivision || countyName,
    state: state,
    zip: "",
    owner_name: result.ownerName || result.businessName || undefined,
    legal_description: result.legal || undefined,
    assessor_url: "https://www.actdatascout.com",
    raw: {
      crpid: result.crpid,
      rpid: result.rpid,
      subdivision: result.subdivision,
      str: result.str,
      acres: result.acres,
      source: "fast_search",
    },
  };
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const key = `${countyFilter}_${stateFilter}`;
  const grid = COUNTY_GRIDS[key];

  if (!grid) {
    console.error(`No grid config for ${key}. Available: ${Object.keys(COUNTY_GRIDS).join(", ")}`);
    process.exit(1);
  }

  console.log(`\nMXRE Fast Ingest — Search Only`);
  console.log(`${"─".repeat(50)}`);
  console.log(`County: ${grid.name}, ${grid.state}`);
  console.log(`DB: ${SUPABASE_URL}`);
  console.log(`Grid: ${grid.townships.length} townships × ${grid.ranges.length} ranges = ${grid.townships.length * grid.ranges.length} combos`);
  console.log(`Estimated properties: ${grid.estimated_properties.toLocaleString()}`);
  if (dryRun) console.log(`Mode: DRY RUN`);
  console.log();

  // Ensure county exists in DB
  const countyId = await ensureCounty(grid);
  console.log(`County ID: ${countyId}`);

  const { context, close } = await createBrowser();
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);
  page.setDefaultTimeout(30_000);

  const seenParcels = new Set<string>();
  const allRecords: RawPropertyRecord[] = [];
  const totalCombos = grid.townships.length * grid.ranges.length;
  let completedCombos = 0;
  let fromCache = 0;

  const startTime = Date.now();

  try {
    for (const township of grid.townships) {
      for (const range of grid.ranges) {
        completedCombos++;
        const countyId_str = `${grid.state_fips}${grid.county_fips}`;
        const tLabel = `T${township >= 0 ? township + "N" : Math.abs(township) + "S"} R${range}W`;

        let results: SearchResult[] = [];
        for (let attempt = 1; attempt <= 3; attempt++) {
          results = await searchBySTR(page, grid.base_url, countyId_str, township, range);
          if (results.length > 0 || attempt === 3) break;
          console.log(`  Retry ${attempt}/3 for ${tLabel}...`);
          await new Promise(r => setTimeout(r, 5000));
        }

        // Check if this was from cache
        const cacheKey = `actds:str:${countyId_str}:0-${Math.abs(township)}-${range}`;
        if (getCached(cacheKey)) fromCache++;

        let newCount = 0;
        for (const result of results) {
          const pid = result.parcel || result.crpid;
          if (!pid || seenParcels.has(pid)) continue;
          seenParcels.add(pid);
          allRecords.push(searchResultToRaw(result, grid.name, grid.state));
          newCount++;
        }

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        console.log(`  ${tLabel}: ${results.length} results, ${newCount} new (${completedCombos}/${totalCombos} combos, ${seenParcels.size} total, ${elapsed}s)`);
      }
    }
  } finally {
    await close();
  }

  console.log(`\nSearch complete: ${seenParcels.size} unique parcels from ${totalCombos} searches (${fromCache} cached)`);
  console.log(`Elapsed: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);

  // ─── Batch upsert to database ─────────────────────────────────────
  if (dryRun) {
    console.log(`DRY RUN — would upsert ${allRecords.length} records`);
    return;
  }

  console.log(`\nUpserting ${allRecords.length} properties to database...`);
  const BATCH_SIZE = 200;
  let upserted = 0;
  let errors = 0;

  for (let i = 0; i < allRecords.length; i += BATCH_SIZE) {
    const batch = allRecords.slice(i, i + BATCH_SIZE);
    const normalized = batch
      .map((r) => normalizeProperty(r, countyId))
      .filter((p) => p.address && p.city);

    if (normalized.length === 0) continue;

    const rows = normalized.map((p) => ({
      ...p,
      updated_at: new Date().toISOString(),
    }));

    const { data, error } = await db
      .from("properties")
      .upsert(rows, { onConflict: "county_id,parcel_id" })
      .select("id");

    if (error) {
      console.error(`  Batch ${Math.floor(i / BATCH_SIZE) + 1} error: ${error.message}`);
      errors++;
    } else {
      upserted += data?.length ?? 0;
    }

    if ((i / BATCH_SIZE) % 10 === 0) {
      console.log(`  ${upserted} upserted, ${errors} batch errors (${Math.round((i / allRecords.length) * 100)}%)`);
    }
  }

  console.log(`\nDone: ${upserted} properties upserted, ${errors} batch errors`);
  console.log(`Total time: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
}

async function ensureCounty(grid: CountySTRGrid): Promise<number> {
  const { data: existing } = await db
    .from("counties")
    .select("id")
    .eq("state_fips", grid.state_fips)
    .eq("county_fips", grid.county_fips)
    .single();

  if (existing) return existing.id;

  const { data: created, error } = await db
    .from("counties")
    .upsert({
      state_fips: grid.state_fips,
      county_fips: grid.county_fips,
      state_code: grid.state,
      county_name: grid.name,
      assessor_url: grid.base_url,
    }, { onConflict: "state_fips,county_fips" })
    .select("id")
    .single();

  if (error) throw new Error(`Failed to create county: ${error.message}`);
  return created!.id;
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
