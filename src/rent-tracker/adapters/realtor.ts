/**
 * Rent Tracker — Realtor.com adapter. Public search page scraper.
 *
 * Realtor.com embeds structured listing data in __NEXT_DATA__ similar to Zillow.
 * This adapter serves as a third cross-reference source for on-market signals.
 *
 * Legal safeguards: public pages only, no login, no CAPTCHA bypass.
 */

import { chromium, type Page, type BrowserContext } from "playwright";
import { ListingAdapter, type ListingSearchArea, type OnMarketRecord, type ListingProgress } from "./base.js";
import { waitForListingSlot, backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT, humanPause, humanScroll } from "../../utils/stealth.js";
import { validateUrlForListings } from "../../utils/allowlist.js";
import { getResidentialProxy, reportProxyFailure, reportProxySuccess } from "../../utils/proxy.js";
import { isPathAllowed } from "../robots-checker.js";

const REALTOR_BASE = "https://www.realtor.com";
const LISTING_CACHE_TTL = 24 * 60 * 60 * 1000;

// ─── Browser ────────────────────────────────────────────────────────

async function createRealtorBrowser(): Promise<{ context: BrowserContext; close: () => Promise<void> }> {
  const stealth = getStealthConfig();
  const proxyUrl = process.env.REALTOR_USE_PROXY === "true" ? getResidentialProxy() : null;

  const launchOpts: Parameters<typeof chromium.launch>[0] = { headless: true };

  if (proxyUrl) {
    try {
      const parsed = new URL(proxyUrl);
      launchOpts.proxy = {
        server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
        username: parsed.username || undefined,
        password: parsed.password || undefined,
      };
    } catch { /* launch without proxy */ }
  }

  const browser = await chromium.launch(launchOpts);
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });

  return { context, close: async () => { await context.close(); await browser.close(); } };
}

// ─── URL Builders ───────────────────────────────────────────────────

function buildRealtorSearchUrl(area: ListingSearchArea): string {
  if (area.zip) {
    return `${REALTOR_BASE}/realestateandhomes-search/${area.zip}`;
  }
  if (area.city && area.state) {
    const citySlug = area.city.replace(/\s+/g, "-");
    const stateSlug = area.state;
    return `${REALTOR_BASE}/realestateandhomes-search/${citySlug}_${stateSlug}`;
  }
  return REALTOR_BASE;
}

// ─── Data Extraction ────────────────────────────────────────────────

interface RealtorListing {
  propertyId: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  price: number;
  beds?: number;
  baths?: number;
  sqft?: number;
  lotSqft?: number;
  yearBuilt?: number;
  propertyType?: string;
  daysOnMarket?: number;
  agentName?: string;
  brokerName?: string;
  listingUrl?: string;
}

async function extractRealtorListings(page: Page): Promise<RealtorListing[]> {
  return await page.evaluate(() => {
    const results: RealtorListing[] = [];

    // Strategy 1: __NEXT_DATA__
    const nextData = document.querySelector('script#__NEXT_DATA__');
    if (nextData?.textContent) {
      try {
        const data = JSON.parse(nextData.textContent);
        const searchResults =
          data?.props?.pageProps?.properties ??
          data?.props?.pageProps?.searchResults?.home_search?.results ??
          data?.props?.pageProps?.pageData?.home_search?.results ??
          [];

        for (const item of searchResults) {
          const loc = item.location ?? {};
          const addr = loc.address ?? item.address ?? {};
          const desc = item.description ?? {};

          results.push({
            propertyId: item.property_id ?? item.listing_id ?? "",
            address: addr.line ?? addr.street ?? "",
            city: addr.city ?? loc.city ?? "",
            state: addr.state_code ?? addr.state ?? loc.state ?? "",
            zip: addr.postal_code ?? addr.zip ?? loc.zip ?? "",
            price: item.list_price ?? desc.list_price ?? 0,
            beds: desc.beds ?? item.beds,
            baths: desc.baths ?? item.baths,
            sqft: desc.sqft ?? item.sqft,
            lotSqft: desc.lot_sqft ?? item.lot_sqft,
            yearBuilt: desc.year_built ?? item.year_built,
            propertyType: desc.type ?? item.prop_type,
            daysOnMarket: item.list_date ? Math.floor((Date.now() - new Date(item.list_date).getTime()) / 86400000) : undefined,
            agentName: item.advertisers?.[0]?.name ?? item.listing_agent?.name,
            brokerName: item.advertisers?.[0]?.broker?.name ?? item.listing_broker?.name,
            listingUrl: item.permalink ? `https://www.realtor.com/realestateandhomes-detail/${item.permalink}` :
                        item.href ? `https://www.realtor.com${item.href}` : undefined,
          });
        }
      } catch { /* fall through */ }
    }

    // Strategy 2: Search result cards in DOM
    if (results.length === 0) {
      const cards = document.querySelectorAll('[data-testid="property-card"], .BasePropertyCard_propertyCardWrap__J0xUj');
      for (const card of cards) {
        const priceEl = card.querySelector('[data-testid="card-price"]');
        const addrEl = card.querySelector('[data-testid="card-address"]');
        const link = card.querySelector('a[href*="/realestateandhomes-detail/"]') as HTMLAnchorElement | null;

        if (!addrEl?.textContent) continue;

        const fullAddr = addrEl.textContent.trim();
        const parts = fullAddr.split(",").map((s: string) => s.trim());

        results.push({
          propertyId: "",
          address: parts[0] || fullAddr,
          city: parts[1] || "",
          state: parts[2]?.split(" ")[0] || "",
          zip: parts[2]?.split(" ")[1] || "",
          price: priceEl?.textContent ? parseInt(priceEl.textContent.replace(/[^0-9]/g, "")) || 0 : 0,
          listingUrl: link?.href || undefined,
        });
      }
    }

    return results;
  }) as RealtorListing[];
}

// ─── Adapter ────────────────────────────────────────────────────────

export class RealtorListingAdapter extends ListingAdapter {
  readonly source = "realtor" as const;

  canHandle(area: ListingSearchArea): boolean {
    return area.state.length === 2;
  }

  async *fetchListings(
    area: ListingSearchArea,
    onProgress?: (progress: ListingProgress) => void,
  ): AsyncGenerator<OnMarketRecord> {
    const areaLabel = area.zip ?? `${area.city}, ${area.state}`;
    const progress: ListingProgress = {
      source: "realtor",
      area: areaLabel,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    const robotsOk = await isPathAllowed(`${REALTOR_BASE}/realestateandhomes-search/`);
    if (!robotsOk) {
      console.log("  Realtor.com robots.txt disallows path — skipping");
      return;
    }

    const validation = validateUrlForListings(REALTOR_BASE);
    if (!validation.allowed) {
      console.error(`  Blocked: ${validation.reason}`);
      return;
    }

    const cacheKey = `realtor:search:${areaLabel}`;
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

    const { context, close } = await createRealtorBrowser();
    const proxyUrl = getResidentialProxy();

    try {
      const page = await context.newPage();
      await page.addInitScript(STEALTH_INIT_SCRIPT);
      page.setDefaultTimeout(30_000);

      const searchUrl = buildRealtorSearchUrl(area);
      await waitForListingSlot(REALTOR_BASE);

      try {
        await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
        await humanPause(page);
        await humanScroll(page);

        const blocked = await page.evaluate(() => {
          const text = document.body.innerText?.toLowerCase() ?? "";
          return text.includes("captcha") || text.includes("access denied") || text.includes("blocked");
        });

        if (blocked) {
          console.log("  Realtor.com block detected — backing off");
          backoffDomain(REALTOR_BASE);
          if (proxyUrl) reportProxyFailure(proxyUrl);
          progress.errors++;
          return;
        }

        if (proxyUrl) reportProxySuccess(proxyUrl);
        resetDomainRate(REALTOR_BASE);

        const listings = await extractRealtorListings(page);
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
            listing_source: "realtor",
            listing_url: listing.listingUrl,
            days_on_market: listing.daysOnMarket,
            property_type: listing.propertyType,
            beds: listing.beds,
            baths: listing.baths,
            sqft: listing.sqft,
            lot_sqft: listing.lotSqft,
            year_built: listing.yearBuilt,
            observed_at: new Date().toISOString(),
            raw: { propertyId: listing.propertyId },
          };

          records.push(record);
          progress.total_found++;
          progress.total_processed++;
          yield record;
        }

        if (records.length > 0) {
          setCache(cacheKey, JSON.stringify(records));
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Unknown";
        console.log(`  Realtor.com search error: ${msg}`);
        progress.errors++;
        if (proxyUrl) reportProxyFailure(proxyUrl);
        backoffDomain(REALTOR_BASE);
      }
    } finally {
      await close();
    }

    onProgress?.(progress);
    console.log(
      `  Realtor.com ${areaLabel}: ${progress.total_found} found, ${progress.total_processed} processed, ${progress.errors} errors`,
    );
  }
}
