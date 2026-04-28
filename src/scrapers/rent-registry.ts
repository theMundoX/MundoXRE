/**
 * Rent Scraper Registry
 *
 * Maps website domains to the appropriate scraper function.
 * Used by the bulk scraping script to route each property URL
 * to the correct platform-specific scraper.
 */

import type { Browser } from "playwright";
import type { ScrapedPropertyData } from "./rentcafe.js";
import type { RentSnapshot } from "../db/queries.js";
import { scrapeRentCafe, toRentSnapshots as rentcafeToSnapshots } from "./rentcafe.js";
import { scrapeEntrata, toRentSnapshots as entrataToSnapshots } from "./entrata.js";
import { scrapeAppFolio, toRentSnapshots as appfolioToSnapshots } from "./appfolio.js";
import { scrapeDirectPropertySite, toRentSnapshots as directToSnapshots } from "./direct.js";
import { isDomainBlocked } from "../utils/allowlist.js";

// ─── Types ───────────────────────────────────────────────────────────

export type PlatformId = "rentcafe" | "entrata" | "appfolio" | "direct" | "unknown";

export interface ScraperEntry {
  platform: PlatformId;
  /** Human-readable label */
  label: string;
  /** Regex patterns that match the platform's hostnames */
  patterns: RegExp[];
  /** Scraper function */
  scrape: (url: string, browser?: Browser) => Promise<ScrapedPropertyData | null>;
  /** Convert scraped data to RentSnapshot records */
  toSnapshots: (propertyId: number, websiteId: number | undefined, data: ScrapedPropertyData) => RentSnapshot[];
}

// ─── Registry ────────────────────────────────────────────────────────

const SCRAPERS: ScraperEntry[] = [
  {
    platform: "rentcafe",
    label: "RentCafe (Yardi)",
    patterns: [/\.rentcafe\.com$/i],
    scrape: scrapeRentCafe,
    toSnapshots: rentcafeToSnapshots,
  },
  {
    platform: "entrata",
    label: "Entrata",
    patterns: [/\.entrata\.com$/i],
    scrape: scrapeEntrata,
    toSnapshots: entrataToSnapshots,
  },
  {
    platform: "appfolio",
    label: "AppFolio",
    patterns: [/\.appfolio\.com$/i],
    scrape: scrapeAppFolio,
    toSnapshots: appfolioToSnapshots,
  },
  {
    platform: "direct",
    label: "Direct property website",
    patterns: [],
    scrape: scrapeDirectPropertySite,
    toSnapshots: directToSnapshots,
  },
];

// ─── Lookup Functions ───────────────────────────────────────────────

function extractHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return url.toLowerCase();
  }
}

/**
 * Detect which platform a URL belongs to.
 */
export function detectPlatform(url: string): PlatformId {
  const hostname = extractHostname(url);
  for (const entry of SCRAPERS) {
    for (const pattern of entry.patterns) {
      if (pattern.test(hostname)) return entry.platform;
    }
  }
  return isDomainBlocked(url) ? "unknown" : "direct";
}

/**
 * Get the scraper entry for a URL, or null if no matching scraper exists.
 */
export function getScraperForUrl(url: string): ScraperEntry | null {
  const hostname = extractHostname(url);
  for (const entry of SCRAPERS) {
    for (const pattern of entry.patterns) {
      if (pattern.test(hostname)) return entry;
    }
  }
  if (hostname && !isDomainBlocked(url)) {
    return SCRAPERS.find((entry) => entry.platform === "direct") ?? null;
  }
  return null;
}

/**
 * Get the scraper entry for a platform ID.
 */
export function getScraperByPlatform(platform: PlatformId): ScraperEntry | null {
  return SCRAPERS.find((s) => s.platform === platform) ?? null;
}

/**
 * Get all supported platform IDs.
 */
export function getSupportedPlatforms(): PlatformId[] {
  return SCRAPERS.map((s) => s.platform);
}

/**
 * Check if a URL matches a supported rent scraping platform.
 */
export function isSupportedRentPlatform(url: string): boolean {
  return detectPlatform(url) !== "unknown";
}

/**
 * Detect platform from a domain string (without full URL).
 * Useful when you have a hostname but not a full URL.
 */
export function detectPlatformFromDomain(domain: string): PlatformId {
  const hostname = domain.toLowerCase();
  for (const entry of SCRAPERS) {
    for (const pattern of entry.patterns) {
      if (pattern.test(hostname)) return entry.platform;
    }
  }
  return isDomainBlocked(`https://${hostname}`) ? "unknown" : "direct";
}

/**
 * Get all registered scrapers.
 */
export function getAllScrapers(): readonly ScraperEntry[] {
  return SCRAPERS;
}
