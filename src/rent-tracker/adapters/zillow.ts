/**
 * Rent Tracker — Zillow adapter. Public search page scraper.
 *
 * Strategy: Navigate to Zillow's public search pages and extract listing data
 * from the __NEXT_DATA__ JSON or searchPageState embedded in the page.
 * Uses map-splitting when results exceed 500 per bounding box.
 *
 * Legal safeguards:
 *   - Public pages only (no login)
 *   - No CAPTCHA bypass
 *   - robots.txt compliance
 *   - Only factual data (no photos, descriptions, marketing copy)
 *   - Rate limited with human-like browsing patterns
 */

import { chromium, type Page, type BrowserContext } from "playwright";
import { ListingAdapter, type ListingSearchArea, type OnMarketRecord, type ListingProgress, type GeoBounds } from "./base.js";
import { waitForListingSlot } from "../../utils/rate-limiter.js";
import { backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT, humanScroll, humanMouseMove, humanPause } from "../../utils/stealth.js";
import { validateUrlForListings } from "../../utils/allowlist.js";
import { getResidentialProxy, reportProxyFailure, reportProxySuccess } from "../../utils/proxy.js";
import { isPathAllowed } from "../robots-checker.js";
import { recursiveSplit, getBoundsForArea, CITY_BOUNDS } from "../geo-split.js";

const ZILLOW_BASE = "https://www.zillow.com";
const MAX_RESULTS_PER_PAGE = 500; // Zillow caps map results
const LISTING_CACHE_TTL = 24 * 60 * 60 * 1000; // 24h — listings change daily
const MAX_RETRIES = 3;

// ─── Browser Management ─────────────────────────────────────────────

async function createListingBrowser(): Promise<{ context: BrowserContext; close: () => Promise<void> }> {
  const stealth = getStealthConfig();
  const proxyUrl = getResidentialProxy();

  const launchOpts: Parameters<typeof chromium.launch>[0] = {
    headless: true,
  };

  if (proxyUrl) {
    try {
      const parsed = new URL(proxyUrl);
      launchOpts.proxy = {
        server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
        username: parsed.username || undefined,
        password: parsed.password || undefined,
      };
    } catch {
      // Invalid proxy URL — launch without proxy
    }
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
    close: async () => {
      await context.close();
      await browser.close();
    },
  };
}

// ─── URL Builders ───────────────────────────────────────────────────

function buildSearchUrl(area: ListingSearchArea, bounds?: GeoBounds): string {
  // Zillow search URL format: /homes/{city}-{state}/
  // With map bounds as query parameters
  if (area.zip) {
    const base = `${ZILLOW_BASE}/homes/${area.zip}_rb/`;
    if (bounds) {
      return `${base}?searchQueryState=${encodeSearchState(bounds)}`;
    }
    return base;
  }

  if (area.city && area.state) {
    const citySlug = area.city.toLowerCase().replace(/\s+/g, "-");
    const stateSlug = area.state.toLowerCase();
    const base = `${ZILLOW_BASE}/homes/${citySlug}-${stateSlug}/`;
    if (bounds) {
      return `${base}?searchQueryState=${encodeSearchState(bounds)}`;
    }
    return base;
  }

  return `${ZILLOW_BASE}/homes/`;
}

function encodeSearchState(bounds: GeoBounds): string {
  const state = {
    mapBounds: {
      north: bounds.north,
      south: bounds.south,
      east: bounds.east,
      west: bounds.west,
    },
    isMapVisible: true,
    filterState: {
      sortSelection: { value: "globalrelevanceex" },
    },
    isListVisible: true,
  };
  return encodeURIComponent(JSON.stringify(state));
}

// ─── Data Extraction ────────────────────────────────────────────────

interface ZillowSearchResult {
  zpid: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  price: number;
  beds?: number;
  baths?: number;
  sqft?: number;
  lotSize?: number;
  yearBuilt?: number;
  homeType?: string;
  daysOnZillow?: number;
  agentName?: string;
  brokerName?: string;
  listingUrl?: string;
  statusType?: string;
  raw: Record<string, unknown>;
}

/**
 * Extract listing data from Zillow's page-embedded JSON.
 * Zillow stores search results in __NEXT_DATA__ or window.__NEXT_DATA__.
 */
async function extractListings(page: Page): Promise<ZillowSearchResult[]> {
  return await page.evaluate(() => {
    const results: ZillowSearchResult[] = [];

    // Strategy 1: __NEXT_DATA__ script tag
    const nextDataScript = document.querySelector('script#__NEXT_DATA__');
    if (nextDataScript?.textContent) {
      try {
        const data = JSON.parse(nextDataScript.textContent);
        const searchResults =
          data?.props?.pageProps?.searchPageState?.cat1?.searchResults?.listResults ??
          data?.props?.pageProps?.searchPageState?.cat1?.searchResults?.mapResults ??
          [];

        for (const item of searchResults) {
          if (!item.zpid) continue;

          const addr = item.address || item.hdpData?.homeInfo?.streetAddress || "";
          const city = item.addressCity || item.hdpData?.homeInfo?.city || "";
          const state = item.addressState || item.hdpData?.homeInfo?.state || "";
          const zip = item.addressZipcode || item.hdpData?.homeInfo?.zipcode || "";

          results.push({
            zpid: String(item.zpid),
            address: addr,
            city,
            state,
            zip,
            price: item.unformattedPrice ?? item.price ?? item.hdpData?.homeInfo?.price ?? 0,
            beds: item.beds ?? item.hdpData?.homeInfo?.bedrooms,
            baths: item.baths ?? item.hdpData?.homeInfo?.bathrooms,
            sqft: item.area ?? item.hdpData?.homeInfo?.livingArea,
            lotSize: item.hdpData?.homeInfo?.lotSize ?? item.lotAreaValue,
            yearBuilt: item.hdpData?.homeInfo?.yearBuilt,
            homeType: item.hdpData?.homeInfo?.homeType ?? item.homeType,
            daysOnZillow: item.hdpData?.homeInfo?.daysOnZillow,
            agentName: item.hdpData?.homeInfo?.listing_agent?.name ??
              item.brokerName ?? undefined,
            brokerName: item.hdpData?.homeInfo?.brokerageName ??
              item.hdpData?.homeInfo?.listing_agent?.company ?? undefined,
            listingUrl: item.detailUrl ? `https://www.zillow.com${item.detailUrl}` : undefined,
            statusType: item.statusType ?? item.hdpData?.homeInfo?.homeStatus,
            raw: {
              zpid: item.zpid,
              statusType: item.statusType,
              homeStatus: item.hdpData?.homeInfo?.homeStatus,
              isZillowOwned: item.hdpData?.homeInfo?.isZillowOwned,
              isFeatured: item.isFeatured,
              isRentalWithBasePrice: item.isRentalWithBasePrice,
            },
          });
        }
      } catch {
        // JSON parse error — fall through to strategy 2
      }
    }

    // Strategy 2: Look for preloaded API data in script tags
    if (results.length === 0) {
      const scripts = document.querySelectorAll("script");
      for (const script of scripts) {
        const text = script.textContent || "";
        if (text.includes('"listResults"') || text.includes('"searchResults"')) {
          try {
            // Try to find JSON object with listing data
            const match = text.match(/"listResults"\s*:\s*(\[[\s\S]*?\])\s*[,}]/);
            if (match?.[1]) {
              const items = JSON.parse(match[1]);
              for (const item of items) {
                if (!item.zpid) continue;
                results.push({
                  zpid: String(item.zpid),
                  address: item.address || "",
                  city: item.addressCity || "",
                  state: item.addressState || "",
                  zip: item.addressZipcode || "",
                  price: item.unformattedPrice ?? item.price ?? 0,
                  beds: item.beds,
                  baths: item.baths,
                  sqft: item.area,
                  homeType: item.homeType,
                  daysOnZillow: item.daysOnZillow,
                  listingUrl: item.detailUrl ? `https://www.zillow.com${item.detailUrl}` : undefined,
                  statusType: item.statusType,
                  raw: { zpid: item.zpid, statusType: item.statusType },
                });
              }
            }
          } catch {
            continue;
          }
        }
      }
    }

    return results;
  }) as ZillowSearchResult[];
}

/**
 * Get total result count from the page to determine if splitting is needed.
 */
async function getResultCount(page: Page): Promise<{ count: number; capped: boolean }> {
  return await page.evaluate(() => {
    // Look for total count in __NEXT_DATA__
    const nextDataScript = document.querySelector('script#__NEXT_DATA__');
    if (nextDataScript?.textContent) {
      try {
        const data = JSON.parse(nextDataScript.textContent);
        const totalCount =
          data?.props?.pageProps?.searchPageState?.cat1?.searchResults?.totalResultCount ??
          data?.props?.pageProps?.searchPageState?.cat1?.searchResults?.resultCount ?? 0;
        return { count: totalCount, capped: totalCount >= 500 };
      } catch {
        // fall through
      }
    }

    // Fallback: count visible result cards
    const cards = document.querySelectorAll('[data-test="property-card"], article.list-card');
    return { count: cards.length, capped: cards.length >= 40 }; // 40 = 1 page, likely more
  }) as { count: number; capped: boolean };
}

// ─── Retry Logic ────────────────────────────────────────────────────

async function withRetry<T>(fn: () => Promise<T>, label: string): Promise<T> {
  let lastErr: Error | undefined;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err instanceof Error ? err : new Error(String(err));
      if (attempt < MAX_RETRIES) {
        const delay = 3000 * Math.pow(2, attempt - 1) + Math.random() * 2000;
        console.log(`  Retry ${attempt}/${MAX_RETRIES} for ${label}: ${lastErr.message}`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ─── Adapter ────────────────────────────────────────────────────────

export class ZillowListingAdapter extends ListingAdapter {
  readonly source = "zillow" as const;

  canHandle(area: ListingSearchArea): boolean {
    // Zillow covers all US states
    return area.state.length === 2;
  }

  async *fetchListings(
    area: ListingSearchArea,
    onProgress?: (progress: ListingProgress) => void,
  ): AsyncGenerator<OnMarketRecord> {
    const areaLabel = area.zip ?? `${area.city}, ${area.state}`;
    const progress: ListingProgress = {
      source: "zillow",
      area: areaLabel,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // robots.txt check
    const robotsOk = await isPathAllowed(`${ZILLOW_BASE}/homes/`);
    if (!robotsOk) {
      console.log("  Zillow robots.txt disallows /homes/ — skipping");
      return;
    }

    // Allowlist check
    const validation = validateUrlForListings(ZILLOW_BASE);
    if (!validation.allowed) {
      console.error(`  Blocked: ${validation.reason}`);
      return;
    }

    const { context, close } = await createListingBrowser();
    const proxyUrl = getResidentialProxy();

    try {
      const page = await context.newPage();
      await page.addInitScript(STEALTH_INIT_SCRIPT);
      page.setDefaultTimeout(30_000);

      // Get bounding box for this area
      const bounds = area.bounds ?? getBoundsForArea(area.city ?? "", area.state);

      if (bounds) {
        // Use recursive split for geographic coverage
        const searchFn = async (b: GeoBounds) => {
          const url = buildSearchUrl(area, b);
          await waitForListingSlot(ZILLOW_BASE);

          try {
            await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
            await humanPause(page);
            const result = await getResultCount(page);
            if (proxyUrl) reportProxySuccess(proxyUrl);
            resetDomainRate(ZILLOW_BASE);
            return result;
          } catch {
            if (proxyUrl) reportProxyFailure(proxyUrl);
            backoffDomain(ZILLOW_BASE);
            return { count: 0, capped: false };
          }
        };

        for await (const quadrant of recursiveSplit(bounds, searchFn, MAX_RESULTS_PER_PAGE)) {
          // Scrape this quadrant
          yield* this.scrapeSearchPage(page, area, quadrant, progress, proxyUrl);
          onProgress?.(progress);
        }
      } else {
        // No bounds — use direct URL search (city/zip)
        yield* this.scrapeSearchPage(page, area, undefined, progress, proxyUrl);
        onProgress?.(progress);
      }
    } finally {
      await close();
    }

    console.log(
      `  Zillow ${areaLabel}: ${progress.total_found} found, ${progress.total_processed} processed, ${progress.errors} errors`,
    );
  }

  private async *scrapeSearchPage(
    page: Page,
    area: ListingSearchArea,
    bounds: GeoBounds | undefined,
    progress: ListingProgress,
    proxyUrl: string | null,
  ): AsyncGenerator<OnMarketRecord> {
    const url = buildSearchUrl(area, bounds);

    // Check cache
    const cacheKey = `zillow:search:${url}`;
    const cached = getCached(cacheKey, LISTING_CACHE_TTL);
    if (cached) {
      const records = JSON.parse(cached) as OnMarketRecord[];
      for (const record of records) {
        progress.total_found++;
        progress.total_processed++;
        yield record;
      }
      return;
    }

    await waitForListingSlot(ZILLOW_BASE);

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
      await humanPause(page);
      await humanScroll(page);
      await humanMouseMove(page);

      // Check for CAPTCHA or block page
      const blocked = await page.evaluate(() => {
        const text = document.body.innerText?.toLowerCase() ?? "";
        return text.includes("captcha") ||
          text.includes("please verify") ||
          text.includes("access denied") ||
          text.includes("blocked");
      });

      if (blocked) {
        console.log("  Detected block/CAPTCHA — backing off (NOT solving)");
        backoffDomain(ZILLOW_BASE);
        if (proxyUrl) reportProxyFailure(proxyUrl);
        progress.errors++;
        return;
      }

      if (proxyUrl) reportProxySuccess(proxyUrl);
      resetDomainRate(ZILLOW_BASE);

      const listings = await withRetry(
        () => extractListings(page),
        `extract ${area.city ?? area.zip}`,
      );

      const records: OnMarketRecord[] = [];

      for (const listing of listings) {
        if (!listing.address || !listing.city) continue;

        const record: OnMarketRecord = {
          address: listing.address,
          city: listing.city,
          state: listing.state || area.state,
          zip: listing.zip,
          is_on_market: true,
          mls_list_price: listing.price > 0 ? listing.price : undefined,
          listing_agent_name: listing.agentName,
          listing_brokerage: listing.brokerName,
          listing_source: "zillow",
          listing_url: listing.listingUrl,
          days_on_market: listing.daysOnZillow,
          property_type: listing.homeType,
          beds: listing.beds,
          baths: listing.baths,
          sqft: listing.sqft,
          lot_sqft: listing.lotSize,
          year_built: listing.yearBuilt,
          observed_at: new Date().toISOString(),
          raw: listing.raw,
        };

        records.push(record);
        progress.total_found++;
        progress.total_processed++;
        yield record;
      }

      // Cache the results
      if (records.length > 0) {
        setCache(cacheKey, JSON.stringify(records));
      }

      // Paginate if there are more pages
      yield* this.paginateResults(page, area, progress, proxyUrl);

    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown";
      console.log(`  Zillow search error: ${msg}`);
      progress.errors++;
      if (proxyUrl) reportProxyFailure(proxyUrl);
      backoffDomain(ZILLOW_BASE);
    }
  }

  private async *paginateResults(
    page: Page,
    area: ListingSearchArea,
    progress: ListingProgress,
    proxyUrl: string | null,
  ): AsyncGenerator<OnMarketRecord> {
    // Check if there are more pages
    let pageNum = 2;
    const maxPages = 20; // Safety cap

    while (pageNum <= maxPages) {
      const hasNext = await page.evaluate(() => {
        const nextBtn = document.querySelector('a[rel="next"], a[title="Next page"]');
        return !!nextBtn;
      });

      if (!hasNext) break;

      await waitForListingSlot(ZILLOW_BASE);

      try {
        // Click next page
        await page.click('a[rel="next"], a[title="Next page"]');
        await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
        await humanPause(page);
        await humanScroll(page);

        const listings = await extractListings(page);

        if (listings.length === 0) break;

        for (const listing of listings) {
          if (!listing.address || !listing.city) continue;

          const record: OnMarketRecord = {
            address: listing.address,
            city: listing.city,
            state: listing.state || area.state,
            zip: listing.zip,
            is_on_market: true,
            mls_list_price: listing.price > 0 ? listing.price : undefined,
            listing_agent_name: listing.agentName,
            listing_brokerage: listing.brokerName,
            listing_source: "zillow",
            listing_url: listing.listingUrl,
            days_on_market: listing.daysOnZillow,
            property_type: listing.homeType,
            beds: listing.beds,
            baths: listing.baths,
            sqft: listing.sqft,
            lot_sqft: listing.lotSize,
            year_built: listing.yearBuilt,
            observed_at: new Date().toISOString(),
            raw: listing.raw,
          };

          progress.total_found++;
          progress.total_processed++;
          yield record;
        }

        if (proxyUrl) reportProxySuccess(proxyUrl);
        pageNum++;
      } catch {
        progress.errors++;
        break;
      }
    }
  }
}
