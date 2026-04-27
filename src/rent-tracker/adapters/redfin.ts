/**
 * Rent Tracker -- Redfin adapter. Uses Redfin's public stingray JSON search API.
 *
 * Strategy: Hit /stingray/api/gis which returns active listings as JSON.
 * This is the same endpoint the Redfin map view uses — plain HTTP fetch, no browser.
 * The JSON API is not soft-blocked like gis-csv; it works with normal headers.
 *
 * Legal safeguards:
 *   - Public API endpoint (no login, no CAPTCHA bypass)
 *   - Rate limited
 *   - Only factual listing data (no photos, descriptions, marketing copy)
 */

import { ListingAdapter, type ListingSearchArea, type OnMarketRecord, type ListingProgress } from "./base.js";
import { waitForListingSlot, backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig } from "../../utils/stealth.js";
import {
  getResidentialProxy,
  redactProxyUrl,
  reportProxyFailure,
  reportProxySuccess,
} from "../../utils/proxy.js";
import { ProxyAgent } from "undici";

const REDFIN_BASE = "https://www.redfin.com";
const LISTING_CACHE_TTL = 24 * 60 * 60 * 1000;
const REGION_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
const CACHE_VERSION = "v2";
const MAX_RETRIES = 3;
const PAGE_SIZE = 350; // Redfin max per page

// ─── HTTP Helpers ─────────────────────────────────────────────────

function getHeaders(): Record<string, string> {
  const stealth = getStealthConfig();
  return {
    "User-Agent": stealth.userAgent,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": `${REDFIN_BASE}/`,
    "X-Requested-With": "XMLHttpRequest",
  };
}

async function redfinFetch(url: string, init: RequestInit = {}): Promise<Response> {
  const proxyUrl = getResidentialProxy();
  const requestInit: RequestInit & { dispatcher?: ProxyAgent } = {
    ...init,
    headers: {
      ...getHeaders(),
      ...(init.headers ?? {}),
    },
  };

  if (proxyUrl) {
    requestInit.dispatcher = new ProxyAgent(proxyUrl);
  }

  try {
    const resp = await fetch(url, requestInit);
    if (proxyUrl) {
      if (resp.status === 403 || resp.status === 429 || resp.status >= 500) {
        reportProxyFailure(proxyUrl);
      } else {
        reportProxySuccess(proxyUrl);
      }
    }
    return resp;
  } catch (err) {
    if (proxyUrl) {
      reportProxyFailure(proxyUrl);
      console.log(`  Redfin proxy fetch failed via ${redactProxyUrl(proxyUrl)}: ${err instanceof Error ? err.message : "Unknown"}`);
    }
    throw err;
  }
}

// ─── Region ID Resolution ─────────────────────────────────────────

interface RegionResult {
  id: string;
  type: number;
  name: string;
}

function extractRegionFromHtml(html: string, label: string, expectedZip?: string): RegionResult | null {
  if (expectedZip) {
    const escapedZip = expectedZip.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const regionObject = html.match(
      new RegExp(`"region"\\s*:\\s*\\{[^}]*"id"\\s*:\\s*(\\d+)[^}]*"type"\\s*:\\s*2[^}]*"name"\\s*:\\s*"${escapedZip}"`),
    );
    if (regionObject) return { id: regionObject[1], type: 2, name: label };

    const dataLayer = html.match(/'region_id'\s*:\s*'(\d+)'\s*,\s*'region_type_id'\s*:\s*'2'/);
    if (dataLayer) return { id: dataLayer[1], type: 2, name: label };

    const encodedRegion = html.match(
      new RegExp(`%22id%22%3A(\\d+)%2C%22type%22%3A2%2C%22name%22%3A%22${escapedZip}%22`),
    );
    if (encodedRegion) return { id: encodedRegion[1], type: 2, name: label };
  }

  const regionPairs = html.match(/region_id[=:](\d+).*?region_type[=:](\d+)/g);
  if (regionPairs) {
    const seen = new Map<string, { id: string; type: number }>();
    for (const pair of regionPairs) {
      const idMatch = pair.match(/region_id[=:](\d+)/);
      const typeMatch = pair.match(/region_type[=:](\d+)/);
      if (idMatch && typeMatch) {
        const id = idMatch[1];
        const type = parseInt(typeMatch[1], 10);
        seen.set(`${id}:${type}`, { id, type });
      }
    }
    for (const [, entry] of seen) {
      if (entry.type === 6) return { id: entry.id, type: entry.type, name: label };
    }
    const first = seen.values().next().value;
    if (first) return { id: first.id, type: first.type, name: label };
  }

  const regionIdMatch = html.match(/regionId[\"':\s=]+(\d{3,})/);
  if (regionIdMatch && !expectedZip) return { id: regionIdMatch[1], type: 6, name: label };

  return null;
}

// Direct region_id lookup for major cities — bypasses HTML scraping.
// region_type 6 = city in Redfin's gis API.
const CITY_REGION_DIRECT: Record<string, { id: string; type: number }> = {
  "indianapolis,IN": { id: "9170",  type: 6 },
  "chicago,IL":      { id: "29470", type: 6 },
  "dallas,TX":       { id: "30794", type: 6 },
  "houston,TX":      { id: "25473", type: 6 },
  "austin,TX":       { id: "30818", type: 6 },
  "phoenix,AZ":      { id: "14804", type: 6 },
  "seattle,WA":      { id: "16163", type: 6 },
  "miami,FL":        { id: "10182", type: 6 },
  "tampa,FL":        { id: "10623", type: 6 },
  "orlando,FL":      { id: "10288", type: 6 },
  "columbus,OH":     { id: "12181", type: 6 },
  "cincinnati,OH":   { id: "11948", type: 6 },
  "cleveland,OH":    { id: "11958", type: 6 },
  "fort worth,TX":   { id: "30836", type: 6 },
  "san antonio,TX":  { id: "30856", type: 6 },
};

// Internal Redfin region_id per ZIP for cities that exceed the 350-per-region cap.
// Keys are "city,STATE"; values map ZIP → Redfin internal region_id (region_type=2).
// Resolved once via /zipcode/{zip} HTML scrape; hardcoded here to avoid repeated fetches.
const CITY_ZIP_REGIONS: Record<string, Record<string, string>> = {
  "indianapolis,IN": {
    "46107":"19442","46201":"19500","46202":"19501","46203":"19502","46204":"19503",
    "46205":"19504","46208":"19507","46214":"19510","46216":"19511","46217":"19512",
    "46218":"19513","46219":"19514","46220":"19515","46221":"19516","46222":"19517",
    "46224":"19519","46225":"19520","46226":"19521","46227":"19522","46228":"19523",
    "46229":"19524","46231":"19526","46234":"19527","46235":"19528","46236":"19529",
    "46237":"19530","46239":"19531","46240":"19532","46241":"19533","46250":"19538",
    "46254":"19541","46256":"19543","46259":"19544","46260":"19545","46268":"19547",
    "46278":"19551","46280":"19552",
  },
};

const CITY_ZIP_FALLBACK: Record<string, string> = {
  "indianapolis,IN": "46222",
  "chicago,IL":      "60601",
  "dallas,TX":       "75201",
  "fort worth,TX":   "76102",
  "houston,TX":      "77002",
  "san antonio,TX":  "78201",
  "austin,TX":       "78701",
  "oklahoma city,OK":"73102",
  "tulsa,OK":        "74103",
  "miami,FL":        "33101",
  "orlando,FL":      "32801",
  "tampa,FL":        "33601",
  "jacksonville,FL": "32202",
  "phoenix,AZ":      "85001",
  "columbus,OH":     "43215",
  "cincinnati,OH":   "45202",
  "cleveland,OH":    "44113",
  "seattle,WA":      "98101",
  "los angeles,CA":  "90001",
  "new york,NY":     "10001",
};

async function resolveRegionId(area: ListingSearchArea): Promise<RegionResult | null> {
  const cacheLabel = area.zip
    ? `zip:${area.zip}`
    : area.city && area.state
      ? `city:${area.city.toLowerCase()},${area.state.toUpperCase()}`
      : null;

  if (cacheLabel) {
    const cached = getCached(`redfin:${CACHE_VERSION}:region:${cacheLabel}`, REGION_CACHE_TTL);
    if (cached) {
      try {
        return JSON.parse(cached) as RegionResult;
      } catch {
        // Ignore corrupt cache entries and resolve live.
      }
    }
  }

  if (area.city && area.state && !area.zip) {
    const key = `${area.city.toLowerCase()},${area.state.toUpperCase()}`;
    const direct = CITY_REGION_DIRECT[key];
    if (direct) {
      const label = `${area.city}, ${area.state}`;
      console.log(`  Redfin: direct region lookup "${label}" -> region_id=${direct.id}`);
      const result = { id: direct.id, type: direct.type, name: label };
      if (cacheLabel) setCache(`redfin:${CACHE_VERSION}:region:${cacheLabel}`, JSON.stringify(result));
      return result;
    }
  }

  let pageUrl: string;
  let label: string;
  if (area.zip) {
    pageUrl = `${REDFIN_BASE}/zipcode/${area.zip}`;
    label = area.zip;
  } else if (area.city && area.state) {
    const citySlug = area.city.replace(/\s+/g, "-");
    pageUrl = `${REDFIN_BASE}/city/${citySlug}/${area.state}`;
    label = `${area.city}, ${area.state}`;
  } else {
    return null;
  }

  try {
    const resp = await redfinFetch(pageUrl, {
      signal: AbortSignal.timeout(20_000),
    });

    if (!resp.ok) {
      if (resp.status === 404 && !area.zip && area.city && area.state) {
        console.log(`  Redfin city page 404 for "${label}" — retrying via ZIP lookup`);
        const zipFallback = CITY_ZIP_FALLBACK[`${area.city.toLowerCase()},${area.state.toUpperCase()}`];
        if (zipFallback) return resolveRegionId({ ...area, zip: zipFallback });
      }
      console.log(`  Redfin page HTTP ${resp.status} for "${label}"`);
      return null;
    }

    const html = await resp.text();

    const result = extractRegionFromHtml(html, label, area.zip);
    if (result) {
      if (cacheLabel) setCache(`redfin:${CACHE_VERSION}:region:${cacheLabel}`, JSON.stringify(result));
      return result;
    }

    console.log(`  Redfin: could not find region_id in page for "${label}"`);
  } catch (err) {
    console.log(`  Redfin page fetch error for "${label}": ${err instanceof Error ? err.message : "Unknown"}`);
  }

  return null;
}

// ─── JSON API ─────────────────────────────────────────────────────

interface RedfinHome {
  mlsId?: { value?: string };
  mlsStatus?: string;
  price?: { value?: number };
  sqFt?: { value?: number };
  lotSize?: { value?: number };
  beds?: number;
  baths?: number;
  yearBuilt?: { value?: number };
  dom?: { value?: number };
  streetLine?: { value?: string };
  city?: string;
  state?: string;
  zip?: string;
  propertyType?: number;
  url?: string;
  latLong?: { value?: { latitude?: number; longitude?: number } };
  propertyId?: number;
  listingId?: number;
}

interface RedfinGisResponse {
  payload?: {
    homes?: RedfinHome[];
  };
}

function buildGisUrl(regionId: string, regionType: number, page: number): string {
  const params = new URLSearchParams({
    al: "1",
    num_homes: String(PAGE_SIZE),
    ord: "redfin-recommended-asc",
    page_number: String(page),
    region_id: regionId,
    region_type: String(regionType),
    sf: "1,2,3,5,6,7",
    status: "9",
    uipt: "1,2,3,4,5,6,7,8",
    v: "8",
  });
  return `${REDFIN_BASE}/stingray/api/gis?${params.toString()}`;
}

async function fetchGisPage(regionId: string, regionType: number, page: number): Promise<RedfinHome[]> {
  const url = buildGisUrl(regionId, regionType, page);
  const resp = await redfinFetch(url, {
    signal: AbortSignal.timeout(30_000),
  });

  if (!resp.ok) {
    if (resp.status === 403 || resp.status === 429) {
      backoffDomain(REDFIN_BASE);
      throw new Error(`HTTP ${resp.status} — rate limited`);
    }
    throw new Error(`HTTP ${resp.status}`);
  }

  const text = await resp.text();
  // Redfin JSON APIs return "{}&&" XSSI prefix
  const json = text.replace(/^\{\}&&/, "");
  const data: RedfinGisResponse = JSON.parse(json);
  return data.payload?.homes ?? [];
}

// ─── Home → OnMarketRecord mapping ────────────────────────────────

const PROPERTY_TYPE_MAP: Record<number, string> = {
  1: "Single Family",
  2: "Condo",
  3: "Townhouse",
  4: "Multi-Family",
  5: "Land",
  6: "Other",
  8: "Mobile",
};

function homeToRecord(home: RedfinHome, state: string): OnMarketRecord | null {
  const address = home.streetLine?.value;
  const city = home.city;
  if (!address || !city) return null;

  return {
    address,
    city,
    state: home.state || state,
    zip: home.zip || "",
    is_on_market: true,
    mls_list_price: home.price?.value && home.price.value > 0 ? home.price.value : undefined,
    listing_agent_name: undefined,
    listing_brokerage: undefined,
    listing_source: "redfin",
    listing_url: home.url ? `${REDFIN_BASE}${home.url}` : undefined,
    days_on_market: home.dom?.value ?? undefined,
    property_type: home.propertyType != null ? PROPERTY_TYPE_MAP[home.propertyType] : undefined,
    beds: home.beds ?? undefined,
    baths: home.baths ?? undefined,
    sqft: home.sqFt?.value ?? undefined,
    lot_sqft: home.lotSize?.value ?? undefined,
    year_built: home.yearBuilt?.value ?? undefined,
    observed_at: new Date().toISOString(),
    raw: {
      mlsNumber: home.mlsId?.value,
      mlsStatus: home.mlsStatus,
      propertyId: home.propertyId,
      listingId: home.listingId,
      latitude: home.latLong?.value?.latitude,
      longitude: home.latLong?.value?.longitude,
    },
  };
}

// ─── Retry Logic ────────────────────────────────────────────────

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

export class RedfinListingAdapter extends ListingAdapter {
  readonly source = "redfin" as const;

  canHandle(area: ListingSearchArea): boolean {
    return area.state.length === 2;
  }

  async *fetchListings(
    area: ListingSearchArea,
    onProgress?: (progress: ListingProgress) => void,
  ): AsyncGenerator<OnMarketRecord> {
    const areaLabel = area.zip ?? `${area.city}, ${area.state}`;
    const progress: ListingProgress = {
      source: "redfin",
      area: areaLabel,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // Check cache
    const cacheKey = `redfin:${CACHE_VERSION}:gis:${areaLabel}`;
    const cached = getCached(cacheKey, LISTING_CACHE_TTL);
    if (cached) {
      const records = JSON.parse(cached) as OnMarketRecord[];
      console.log(`  Redfin ${areaLabel}: ${records.length} from cache`);
      for (const record of records) {
        progress.total_found++;
        progress.total_processed++;
        yield record;
      }
      return;
    }

    // Step 1: Resolve region
    console.log(`  Redfin: resolving region for "${areaLabel}"...`);
    await waitForListingSlot(REDFIN_BASE);

    const region = await resolveRegionId(area);
    if (!region) {
      console.log(`  Redfin: could not resolve region for "${areaLabel}" — skipping`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    console.log(`  Redfin: resolved "${areaLabel}" -> region_id=${region.id} type=${region.type}`);

    // Step 2: Fetch listings — use ZIP-by-ZIP sweep for cities with known ZIP lists
    // to overcome the 350-per-region API cap.
    const cityKey = area.city && area.state
      ? `${area.city.toLowerCase()},${area.state.toUpperCase()}`
      : null;
    const zipRegionMap = cityKey ? CITY_ZIP_REGIONS[cityKey] : null;

    const allRecords: OnMarketRecord[] = [];
    const seenIds = new Set<number>();

    const fetchRegion = async (regionId: string, regionType: number, label: string): Promise<RedfinHome[]> => {
      await waitForListingSlot(REDFIN_BASE);
      try {
        const homes = await withRetry(() => fetchGisPage(regionId, regionType, 1), `redfin-gis:${label}`);
        resetDomainRate(REDFIN_BASE);
        return homes;
      } catch (err) {
        console.log(`  Redfin gis fetch failed (${label}): ${err instanceof Error ? err.message : "Unknown"}`);
        progress.errors++;
        return [];
      }
    };

    const regions: Array<{ id: string; type: number; label: string }> = zipRegionMap
      ? Object.entries(zipRegionMap).map(([zip, id]) => ({ id, type: 2, label: zip }))
      : [{ id: region.id, type: region.type, label: areaLabel }];

    if (zipRegionMap) {
      console.log(`  Redfin: sweeping ${regions.length} ZIPs for ${areaLabel}...`);
    }

    for (const r of regions) {
      const homes = await fetchRegion(r.id, r.type, r.label);
      for (const home of homes) {
        if (home.propertyId && seenIds.has(home.propertyId)) continue;
        if (home.propertyId) seenIds.add(home.propertyId);
        const record = homeToRecord(home, area.state);
        if (!record) continue;
        if (area.zip && record.zip && record.zip !== area.zip) continue;
        allRecords.push(record);
        progress.total_found++;
        progress.total_processed++;
        yield record;
      }
    }

    if (zipRegionMap) {
      console.log(`  Redfin: ZIP sweep complete — ${allRecords.length} unique listings`);
    }

    if (allRecords.length > 0) {
      setCache(cacheKey, JSON.stringify(allRecords));
    }

    onProgress?.(progress);
    console.log(`  Redfin ${areaLabel}: ${progress.total_found} found, ${progress.errors} errors`);
  }
}
