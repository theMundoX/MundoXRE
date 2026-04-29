#!/usr/bin/env tsx
/**
 * MXRE - Movoto Indianapolis on-market ingest.
 *
 * Lawful collection path:
 * - Uses Movoto's public sitemaps and public listing pages.
 * - Respects robots crawl-delay with a conservative per-page delay.
 * - Stores factual fields only; no photos, descriptions, or marketing copy.
 * - Does not use login, CAPTCHA bypass, or disallowed paging URLs.
 */

import "dotenv/config";
import { gunzipSync } from "node:zlib";
import { ProxyAgent } from "undici";
import { normalizeListing } from "../src/rent-tracker/normalizer.js";
import { upsertListingSignals, type ListingSignal } from "../src/db/queries.js";
import type { OnMarketRecord } from "../src/rent-tracker/adapters/base.js";
import { initProxies, getResidentialProxy, reportProxyFailure, reportProxySuccess } from "../src/utils/proxy.js";

const MOVOTO_BASE = "https://www.movoto.com";
const NEW_ACTIVE_SITEMAP = `${MOVOTO_BASE}/ssl/new-activehouses/new-sitemap.xml`;
const CITY_URLS = [
  `${MOVOTO_BASE}/indianapolis-in/`,
  `${MOVOTO_BASE}/indianapolis-in/single-family/`,
  `${MOVOTO_BASE}/indianapolis-in/multi-family/`,
  `${MOVOTO_BASE}/indianapolis-in/condos/`,
];

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);
const getArg = (name: string) => args.find((arg) => arg.startsWith(`--${name}=`))?.split("=")[1];

const DRY_RUN = hasFlag("dry-run");
const SKIP_PAGES = hasFlag("skip-pages");
const START_PAGE = Math.max(1, parseInt(getArg("start-page") ?? "1", 10) || 1);
const MAX_PAGES = parseInt(getArg("max-pages") ?? "0", 10) || Infinity;
const DELAY_MS = parseInt(getArg("delay-ms") ?? "1400", 10);
const REQUEST_TIMEOUT_MS = parseInt(getArg("timeout-ms") ?? "30000", 10);
const MAX_CONSECUTIVE_ERRORS = parseInt(getArg("max-consecutive-errors") ?? "20", 10);
const BATCH_SIZE = 50;

type MovotoListing = Record<string, any>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanNumber(value: unknown): number | undefined {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

function cleanText(value: unknown): string | undefined {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text || undefined;
}

function absoluteUrl(pathOrUrl: unknown): string | undefined {
  const value = cleanText(pathOrUrl);
  if (!value) return undefined;
  try {
    return new URL(value, MOVOTO_BASE).toString();
  } catch {
    return undefined;
  }
}

function publicRaw(listing: MovotoListing): Record<string, unknown> {
  const geo = listing.geo ?? {};
  return {
    source_id: listing.id,
    property_id: listing.propertyId,
    mls_number: listing.mlsNumber,
    mls_db_number: listing.mlsDbNumber,
    status: listing.status,
    house_real_status: listing.houseRealStatus,
    list_date: listing.listDate,
    price_changed_date: listing.priceChangedDate,
    price_change: listing.priceChange,
    close_price: listing.closePrice,
    sold_date: listing.soldDate,
    lat: geo.lat,
    lng: geo.lng,
    county: geo.county,
    neighborhood: geo.neighborhoodName,
  };
}

function toRecord(listing: MovotoListing, observedAt: string): OnMarketRecord | null {
  const geo = listing.geo ?? {};
  const address = cleanText(geo.address);
  const city = cleanText(geo.city);
  const state = cleanText(geo.state);
  const zip = cleanText(geo.zipcode);
  if (!address || !city || state !== "IN" || !zip) return null;
  if (city.toUpperCase() !== "INDIANAPOLIS") return null;

  const status = cleanText(listing.houseRealStatus ?? listing.status)?.toUpperCase() ?? "";
  const isActive = status === "ACTIVE" || status === "FOR_SALE" || status === "COMING_SOON";
  if (!isActive) return null;

  return {
    address,
    city,
    state,
    zip,
    is_on_market: true,
    mls_list_price: cleanNumber(listing.listPrice ?? listing.priceRaw),
    listing_agent_name: cleanText(listing.listingAgent),
    listing_brokerage: cleanText(listing.officeListName),
    listing_source: "movoto",
    listing_url: absoluteUrl(listing.path),
    days_on_market: cleanNumber(listing.daysOnMovoto),
    property_type: cleanText(listing.propertyTypeDisplayName ?? listing.propertyType),
    beds: cleanNumber(listing.bed),
    baths: cleanNumber(listing.bath),
    sqft: cleanNumber(listing.sqftTotal),
    lot_sqft: cleanNumber(listing.lotSize),
    year_built: cleanNumber(listing.yearBuilt),
    observed_at: observedAt,
    raw: publicRaw(listing),
  };
}

function extractInitialState(html: string): any | null {
  const match = html.match(/<script id="__INITIAL_STATE__" type="application\/json">([\s\S]*?)<\/script>/);
  if (!match) return null;
  return JSON.parse(match[1]);
}

function extractListingsFromState(state: any): MovotoListing[] {
  const pageData = state?.pageData;
  if (!pageData) return [];
  const out: MovotoListing[] = [];
  if (Array.isArray(pageData.listings)) out.push(...pageData.listings);
  if (pageData.geo?.address && pageData.listPrice) out.push(pageData);
  return out;
}

async function movotoFetch(url: string): Promise<string> {
  const proxyUrl = getResidentialProxy();
  const init: RequestInit & { dispatcher?: ProxyAgent } = {
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  };
  if (proxyUrl) init.dispatcher = new ProxyAgent(proxyUrl);

  try {
    const resp = await fetch(url, init);
    if (proxyUrl) {
      if (resp.status === 403 || resp.status === 429 || resp.status >= 500) reportProxyFailure(proxyUrl);
      else reportProxySuccess(proxyUrl);
    }
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.text();
  } catch (err) {
    if (proxyUrl) reportProxyFailure(proxyUrl);
    throw err;
  }
}

async function fetchSitemapUrls(): Promise<string[]> {
  const indexXml = await fetch(NEW_ACTIVE_SITEMAP).then((r) => {
    if (!r.ok) throw new Error(`Sitemap index HTTP ${r.status}`);
    return r.text();
  });
  const sitemapUrls = [...indexXml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]);
  const listingUrls = new Set<string>();

  for (const sitemapUrl of sitemapUrls) {
    const resp = await fetch(sitemapUrl);
    if (!resp.ok) throw new Error(`Sitemap ${sitemapUrl} HTTP ${resp.status}`);
    const xml = gunzipSync(Buffer.from(await resp.arrayBuffer())).toString("utf8");
    for (const match of xml.matchAll(/<loc>([^<]+)<\/loc>/g)) {
      const url = match[1];
      if (/^https:\/\/www\.movoto\.com\/indianapolis-in\//i.test(url)) listingUrls.add(url);
    }
  }

  return [...listingUrls].sort();
}

async function collectLandingSignals(observedAt: string): Promise<ListingSignal[]> {
  const signals: ListingSignal[] = [];
  const seen = new Set<string>();

  for (const url of CITY_URLS) {
    try {
      const html = await movotoFetch(url);
      const state = extractInitialState(html);
      for (const listing of extractListingsFromState(state)) {
        const record = toRecord(listing, observedAt);
        const signal = record ? normalizeListing(record) : null;
        if (!signal) continue;
        const key = `${signal.address}|${signal.city}|${signal.state_code}|${signal.listing_source}`;
        if (seen.has(key)) continue;
        seen.add(key);
        signals.push(signal);
      }
      console.log(`  Landing ${new URL(url).pathname}: ${signals.length.toLocaleString()} unique so far`);
    } catch (err) {
      console.log(`  Landing fetch skipped ${url}: ${err instanceof Error ? err.message : "unknown error"}`);
    }
    await sleep(DELAY_MS);
  }

  return signals;
}

async function collectPageSignals(urls: string[], observedAt: string): Promise<ListingSignal[]> {
  const signals: ListingSignal[] = [];
  const seen = new Set<string>();
  const start = Math.min(START_PAGE - 1, urls.length);
  const limit = Math.min(urls.length, start + MAX_PAGES);
  let consecutiveErrors = 0;

  for (let i = start; i < limit; i++) {
    const url = urls[i];
    try {
      const html = await movotoFetch(url);
      const state = extractInitialState(html);
      const listing = extractListingsFromState(state)[0];
      const record = listing ? toRecord(listing, observedAt) : null;
      const signal = record ? normalizeListing(record) : null;
      if (signal) {
        signal.listing_url = url;
        const key = `${signal.address}|${signal.city}|${signal.state_code}|${signal.listing_source}`;
        if (!seen.has(key)) {
          seen.add(key);
          signals.push(signal);
        }
      }
      consecutiveErrors = 0;
    } catch (err) {
      consecutiveErrors++;
      console.log(`  Page ${i + 1}/${limit} skipped: ${err instanceof Error ? err.message : "unknown error"}`);
      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        console.log(`  Stopping page crawl after ${consecutiveErrors} consecutive page errors; keeping collected signals and moving on.`);
        break;
      }
    }

    if ((i + 1) % 25 === 0 || i + 1 === limit) {
      console.log(`  Pages ${i + 1}/${urls.length}: ${signals.length.toLocaleString()} valid Indianapolis active listings in this slice`);
    }
    await sleep(DELAY_MS);
  }

  return signals;
}

async function upsertSignals(signals: ListingSignal[]): Promise<number> {
  if (DRY_RUN) return signals.length;
  let upserted = 0;
  for (let i = 0; i < signals.length; i += BATCH_SIZE) {
    const batch = signals.slice(i, i + BATCH_SIZE);
    const data = await upsertListingSignals(batch);
    upserted += data.length;
  }
  return upserted;
}

async function main() {
  initProxies();
  const observedAt = new Date().toISOString();

  console.log("MXRE - Movoto Indianapolis ingest");
  console.log("=".repeat(45));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Delay: ${DELAY_MS}ms`);
  console.log(`Max consecutive page errors: ${MAX_CONSECUTIVE_ERRORS}`);
  console.log(`Page slice: ${START_PAGE} to ${Number.isFinite(MAX_PAGES) ? START_PAGE + MAX_PAGES - 1 : "end"}`);

  const sitemapUrls = await fetchSitemapUrls();
  console.log(`Sitemap Indianapolis active URLs: ${sitemapUrls.length.toLocaleString()}`);

  const landingSignals = await collectLandingSignals(observedAt);
  const pageSignals = SKIP_PAGES ? [] : await collectPageSignals(sitemapUrls, observedAt);

  const merged = new Map<string, ListingSignal>();
  for (const signal of [...landingSignals, ...pageSignals]) {
    const key = `${signal.address}|${signal.city}|${signal.state_code}|${signal.listing_source}`;
    merged.set(key, signal);
  }
  const signals = [...merged.values()];

  const upserted = await upsertSignals(signals);
  console.log("=".repeat(45));
  console.log(`Movoto signals found: ${signals.length.toLocaleString()}`);
  console.log(`Movoto signals ${DRY_RUN ? "validated" : "upserted"}: ${upserted.toLocaleString()}`);
}

main().catch((err) => {
  console.error("Fatal:", err instanceof Error ? err.message : err);
  process.exit(1);
});
