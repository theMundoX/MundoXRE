/**
 * Rent Tracker — Ingestion Pipeline
 * Orchestrates on-market data collection.
 *
 * Flow: search areas → adapters → normalize → cross-reference → match → upsert
 * Runs all available adapters for each search area, then cross-references.
 */

import type { ListingSearchArea, OnMarketRecord } from "./adapters/base.js";
import { getAdaptersForArea } from "./registry.js";
import { normalizeListing, crossReferenceListings } from "./normalizer.js";
import { MultiStateLicenseAdapter } from "./adapters/state-license.js";
import {
  upsertListingSignals,
  upsertAgentLicense,
  getActiveListingsByArea,
  markDelisted,
  type ListingSignal,
} from "../db/queries.js";
import { initProxies } from "../utils/proxy.js";

const BATCH_SIZE = 50;

export interface ListingIngestOptions {
  state: string;
  city?: string;
  zip?: string;
  sources?: string[]; // ["zillow", "redfin", "realtor"] — defaults to all
  dryRun?: boolean;
  skipAgentLookup?: boolean;
  maxRecords?: number;
}

export interface ListingIngestResult {
  area: string;
  sources_used: string[];
  listings_found: number;
  listings_upserted: number;
  agents_enriched: number;
  delisted: number;
  errors: number;
  duration_ms: number;
}

/**
 * Ingest on-market listings for a single search area.
 */
async function ingestArea(
  area: ListingSearchArea,
  options: ListingIngestOptions,
): Promise<ListingIngestResult> {
  const start = Date.now();
  const areaLabel = area.zip ?? `${area.city}, ${area.state}`;

  const result: ListingIngestResult = {
    area: areaLabel,
    sources_used: [],
    listings_found: 0,
    listings_upserted: 0,
    agents_enriched: 0,
    delisted: 0,
    errors: 0,
    duration_ms: 0,
  };

  const adapters = getAdaptersForArea(area);
  const enabledSources = options.sources
    ? adapters.filter((a) => options.sources!.includes(a.source))
    : adapters;

  if (enabledSources.length === 0) {
    console.log(`  No adapters available for ${areaLabel}`);
    result.duration_ms = Date.now() - start;
    return result;
  }

  // Collect listings from all sources
  const allRecords: OnMarketRecord[] = [];
  const maxRecords = options.maxRecords ?? Infinity;

  for (const adapter of enabledSources) {
    console.log(`  Running ${adapter.source} adapter for ${areaLabel}...`);
    result.sources_used.push(adapter.source);

    try {
      for await (const record of adapter.fetchListings(area, (p) => {
        if (p.total_processed % 100 === 0 && p.total_processed > 0) {
          console.log(`  [${adapter.source}] ${p.total_processed} processed, ${p.errors} errors`);
        }
      })) {
        allRecords.push(record);
        result.listings_found++;

        if (result.listings_found >= maxRecords) break;
      }
    } catch (err) {
      console.error(`  ${adapter.source} failed:`, err instanceof Error ? err.message : "Unknown");
      result.errors++;
    }
  }

  if (allRecords.length === 0) {
    console.log(`  No listings found for ${areaLabel}`);
    result.duration_ms = Date.now() - start;
    return result;
  }

  // Normalize all records
  const signals: ListingSignal[] = [];
  for (const record of allRecords) {
    const normalized = normalizeListing(record);
    if (normalized) signals.push(normalized);
  }

  // Cross-reference: mark confidence as "high" when multiple sources agree
  const crossReferenced = crossReferenceListings(signals);
  const highConfidence = crossReferenced.filter((s) => s.confidence === "high").length;
  if (highConfidence > 0) {
    console.log(`  Cross-reference: ${highConfidence} listings confirmed by multiple sources`);
  }

  // Batch upsert
  if (!options.dryRun) {
    for (let i = 0; i < crossReferenced.length; i += BATCH_SIZE) {
      const batch = crossReferenced.slice(i, i + BATCH_SIZE);
      try {
        const upserted = await upsertListingSignals(batch);
        result.listings_upserted += upserted.length;
      } catch (err) {
        console.error("  Upsert error:", err instanceof Error ? err.message : "Unknown");
        result.errors++;
      }
    }
  } else {
    result.listings_upserted = crossReferenced.length;
  }

  // Delisting detection: properties previously on-market but not in this scan
  if (!options.dryRun && area.city) {
    try {
      const previouslyActive = await getActiveListingsByArea(area.state, area.city);
      const currentAddresses = new Set(
        crossReferenced.map((s) => `${s.address}|${s.city}|${s.state_code}|${s.listing_source}`),
      );

      const delistedIds = previouslyActive
        .filter((p) => {
          const key = `${p.address}|${p.city}|${p.state_code}|${p.listing_source}`;
          return !currentAddresses.has(key);
        })
        .map((p) => p.id as number)
        .filter(Boolean);

      if (delistedIds.length > 0) {
        await markDelisted(delistedIds);
        result.delisted = delistedIds.length;
        console.log(`  Marked ${delistedIds.length} listings as delisted`);
      }
    } catch (err) {
      console.log("  Delisting detection skipped:", err instanceof Error ? err.message : "Unknown");
    }
  }

  // Agent contact enrichment from state license databases
  if (!options.dryRun && !options.skipAgentLookup) {
    const licenseAdapter = new MultiStateLicenseAdapter();
    if (licenseAdapter.canHandle(area.state)) {
      const uniqueAgents = new Set<string>();
      for (const signal of crossReferenced) {
        if (signal.listing_agent_name && !uniqueAgents.has(signal.listing_agent_name)) {
          uniqueAgents.add(signal.listing_agent_name);
        }
      }

      console.log(`  Looking up ${uniqueAgents.size} agents in ${area.state} license database...`);

      for (const agentName of uniqueAgents) {
        try {
          const license = await licenseAdapter.lookupAgentInState(agentName, area.state);
          if (license) {
            await upsertAgentLicense(license);
            result.agents_enriched++;
          }
        } catch {
          // Non-fatal — agent lookup is best-effort
        }
      }
    }
  }

  result.duration_ms = Date.now() - start;
  return result;
}

/**
 * Main listing ingestion entry point.
 */
export async function ingestListings(options: ListingIngestOptions): Promise<ListingIngestResult[]> {
  initProxies();

  const areas: ListingSearchArea[] = [];

  if (options.zip) {
    areas.push({ state: options.state, zip: options.zip });
  } else if (options.city) {
    areas.push({ state: options.state, city: options.city });
  } else {
    console.error("Must specify --city or --zip");
    return [];
  }

  console.log(`\nIngesting on-market listings for ${areas.length} area(s)...\n`);

  const results: ListingIngestResult[] = [];

  for (const area of areas) {
    const areaLabel = area.zip ?? `${area.city}, ${area.state}`;
    console.log(`\n── ${areaLabel} ──`);

    const result = await ingestArea(area, options);
    results.push(result);

    console.log(
      `  Done: ${result.listings_upserted}/${result.listings_found} upserted, ` +
        `${result.agents_enriched} agents enriched, ${result.delisted} delisted, ` +
        `${result.errors} errors, ${(result.duration_ms / 1000).toFixed(1)}s`,
    );
  }

  // Summary
  console.log("\n── Summary ──");
  const totals = results.reduce(
    (acc, r) => ({
      found: acc.found + r.listings_found,
      upserted: acc.upserted + r.listings_upserted,
      agents: acc.agents + r.agents_enriched,
      delisted: acc.delisted + r.delisted,
      errors: acc.errors + r.errors,
      ms: acc.ms + r.duration_ms,
    }),
    { found: 0, upserted: 0, agents: 0, delisted: 0, errors: 0, ms: 0 },
  );
  console.log(
    `Total: ${totals.upserted}/${totals.found} listings, ${totals.agents} agents, ` +
      `${totals.delisted} delisted, ${totals.errors} errors, ${(totals.ms / 1000).toFixed(1)}s`,
  );

  return results;
}
