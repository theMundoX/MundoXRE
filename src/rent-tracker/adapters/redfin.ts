/**
 * Rent Tracker -- Redfin adapter. Uses Redfin's public stingray CSV download API.
 *
 * Strategy: Hit the /stingray/api/gis-csv endpoint which returns active listings
 * as CSV. This is the same endpoint that powers Redfin's "Download All" button.
 * No browser needed -- plain HTTP fetch.
 *
 * To resolve a ZIP code to a Redfin region_id, we hit their location-autocomplete
 * endpoint first.
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

const REDFIN_BASE = "https://www.redfin.com";
const LISTING_CACHE_TTL = 24 * 60 * 60 * 1000;
const MAX_RETRIES = 3;

// ─── HTTP Helpers ─────────────────────────────────────────────────

function getHeaders(): Record<string, string> {
  const stealth = getStealthConfig();
  return {
    "User-Agent": stealth.userAgent,
    "Accept": "text/csv,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.redfin.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
  };
}

// ─── Region ID Resolution ─────────────────────────────────────────

interface RegionResult {
  id: string;
  type: number; // 2=city, 6=zip, etc.
  name: string;
}

/**
 * Resolve a ZIP code or city+state to a Redfin region_id by fetching
 * the search page and extracting region info from the embedded HTML.
 *
 * Redfin's autocomplete endpoint blocks non-browser requests (403),
 * but the actual search pages embed region_id in their internal URLs.
 */
async function resolveRegionId(area: ListingSearchArea): Promise<RegionResult | null> {
  // Build the page URL
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
    const resp = await fetch(pageUrl, {
      headers: getHeaders(),
      signal: AbortSignal.timeout(20_000),
    });

    if (!resp.ok) {
      console.log(`  Redfin page HTTP ${resp.status} for "${label}"`);
      return null;
    }

    const html = await resp.text();

    // Strategy 1: Look for region_id in stingray API URLs embedded in the page.
    // Pattern: region_id=NNNNN&region_type=N
    // We want the one with region_type=2 (city) since that returns CSV data.
    const regionPairs = html.match(/region_id[=:](\d+).*?region_type[=:](\d+)/g);
    if (regionPairs) {
      // Collect all unique region_id + region_type pairs
      const seen = new Map<string, { id: string; type: number }>();
      for (const pair of regionPairs) {
        const idMatch = pair.match(/region_id[=:](\d+)/);
        const typeMatch = pair.match(/region_type[=:](\d+)/);
        if (idMatch && typeMatch) {
          const id = idMatch[1];
          const type = parseInt(typeMatch[1], 10);
          const key = `${id}:${type}`;
          if (!seen.has(key)) {
            seen.set(key, { id, type });
          }
        }
      }

      // Prefer type=2 (city-level) as it returns actual CSV data.
      // If ZIP search, the ZIP's own id with type=2 sometimes returns empty,
      // so prefer the non-ZIP numeric id with type=2.
      for (const [, entry] of seen) {
        if (entry.type === 2 && entry.id !== area.zip) {
          return { id: entry.id, type: entry.type, name: label };
        }
      }
      // Fallback: any type=2
      for (const [, entry] of seen) {
        if (entry.type === 2) {
          return { id: entry.id, type: entry.type, name: label };
        }
      }
      // Fallback: any type=6 (zip)
      for (const [, entry] of seen) {
        if (entry.type === 6) {
          return { id: entry.id, type: entry.type, name: label };
        }
      }
      // Last resort: first entry
      const first = seen.values().next().value;
      if (first) {
        return { id: first.id, type: first.type, name: label };
      }
    }

    // Strategy 2: Look for regionId in JavaScript data
    const regionIdMatch = html.match(/regionId[\"':\s=]+(\d{3,})/);
    if (regionIdMatch) {
      return { id: regionIdMatch[1], type: 2, name: label };
    }

    console.log(`  Redfin: could not find region_id in page for "${label}"`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown";
    console.log(`  Redfin page fetch error for "${label}": ${msg}`);
  }

  return null;
}

// ─── CSV Download ─────────────────────────────────────────────────

function buildCsvUrl(regionId: string, regionType: number): string {
  const params = new URLSearchParams({
    al: "1",
    market: "ohio", // This param is loosely used by Redfin; any value works
    num_homes: "350",
    ord: "redfin-recommended-asc",
    page_number: "1",
    region_id: regionId,
    region_type: String(regionType),
    sf: "1,2,3,5,6,7",
    status: "9", // active listings
    uipt: "1,2,3,4,5,6,7,8",
    v: "8",
  });

  return `${REDFIN_BASE}/stingray/api/gis-csv?${params.toString()}`;
}

interface CsvRow {
  [key: string]: string;
}

/**
 * Parse a CSV string into an array of objects keyed by header names.
 * Skips Redfin's disclaimer line that starts with a quoted string.
 */
function parseCsv(csv: string): CsvRow[] {
  const lines = csv.split("\n").filter((l) => l.trim());
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const rows: CsvRow[] = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip Redfin's MLS disclaimer line (starts with a quoted string that
    // doesn't match the expected number of columns)
    if (line.startsWith('"In accordance') || line.startsWith('"Some MLS')) {
      continue;
    }

    const values = parseCSVLine(line);
    if (values.length !== headers.length) continue;

    const row: CsvRow = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j].trim()] = values[j].trim();
    }
    rows.push(row);
  }

  return rows;
}

/**
 * Parse a single CSV line, handling quoted fields with commas.
 */
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++; // skip escaped quote
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

/**
 * Map Redfin CSV column names to our internal field names.
 * Redfin CSV headers (uppercase): SALE TYPE, SOLD DATE, PROPERTY TYPE, ADDRESS,
 * CITY, STATE OR PROVINCE, ZIP OR POSTAL CODE, PRICE, BEDS, BATHS, LOCATION,
 * SQUARE FEET, LOT SIZE, YEAR BUILT, DAYS ON MARKET, $/SQUARE FEET, HOA/MONTH,
 * STATUS, NEXT OPEN HOUSE START TIME, NEXT OPEN HOUSE END TIME, URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING),
 * SOURCE, MLS#, FAVORITE, INTERESTED, LATITUDE, LONGITUDE
 */
function csvRowToListing(row: CsvRow, state: string): OnMarketRecord | null {
  const address = row["ADDRESS"];
  const city = row["CITY"];
  if (!address || !city) return null;

  const priceStr = row["PRICE"]?.replace(/[$,]/g, "");
  const price = priceStr ? parseFloat(priceStr) : undefined;
  const sqftStr = row["SQUARE FEET"]?.replace(/,/g, "");
  const sqft = sqftStr ? parseFloat(sqftStr) : undefined;
  const lotStr = row["LOT SIZE"]?.replace(/,/g, "");
  const lotSqft = lotStr ? parseFloat(lotStr) : undefined;
  const yearStr = row["YEAR BUILT"];
  const yearBuilt = yearStr ? parseInt(yearStr, 10) : undefined;
  const domStr = row["DAYS ON MARKET"];
  const daysOnMarket = domStr ? parseInt(domStr, 10) : undefined;
  const bedsStr = row["BEDS"];
  const beds = bedsStr ? parseFloat(bedsStr) : undefined;
  const bathsStr = row["BATHS"];
  const baths = bathsStr ? parseFloat(bathsStr) : undefined;

  // The URL column has a very long header name; find any key containing "URL"
  let listingUrl: string | undefined;
  for (const key of Object.keys(row)) {
    if (key.startsWith("URL")) {
      listingUrl = row[key] || undefined;
      break;
    }
  }

  // Extract listing agent and brokerage from SOURCE field if present
  // Redfin CSV SOURCE format is typically "Redfin" or the brokerage/MLS
  const source = row["SOURCE"] || undefined;

  return {
    address,
    city,
    state: row["STATE OR PROVINCE"] || state,
    zip: row["ZIP OR POSTAL CODE"] || "",
    is_on_market: true,
    mls_list_price: price && price > 0 ? price : undefined,
    listing_agent_name: undefined, // CSV doesn't include agent name
    listing_brokerage: source,
    listing_source: "redfin",
    listing_url: listingUrl,
    days_on_market: daysOnMarket && !isNaN(daysOnMarket) ? daysOnMarket : undefined,
    property_type: row["PROPERTY TYPE"] || undefined,
    beds: beds && !isNaN(beds) ? beds : undefined,
    baths: baths && !isNaN(baths) ? baths : undefined,
    sqft: sqft && !isNaN(sqft) ? sqft : undefined,
    lot_sqft: lotSqft && !isNaN(lotSqft) ? lotSqft : undefined,
    year_built: yearBuilt && !isNaN(yearBuilt) ? yearBuilt : undefined,
    observed_at: new Date().toISOString(),
    raw: {
      mlsNumber: row["MLS#"] || undefined,
      saleType: row["SALE TYPE"] || undefined,
      status: row["STATUS"] || undefined,
      latitude: row["LATITUDE"] || undefined,
      longitude: row["LONGITUDE"] || undefined,
      hoaPerMonth: row["HOA/MONTH"] || undefined,
      pricePerSqft: row["$/SQUARE FEET"] || undefined,
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

    // Check cache first
    const cacheKey = `redfin:csv:${areaLabel}`;
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

    // Step 1: Resolve the search area to a Redfin region_id
    const query = area.zip ?? `${area.city}, ${area.state}`;

    console.log(`  Redfin: resolving region for "${query}"...`);
    await waitForListingSlot(REDFIN_BASE);

    const region = await resolveRegionId(area);
    if (!region) {
      console.log(`  Redfin: could not resolve region for "${query}" — skipping`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    console.log(`  Redfin: resolved "${query}" -> region_id=${region.id} type=${region.type} (${region.name})`);

    // Step 2: Download CSV
    const csvUrl = buildCsvUrl(region.id, region.type);
    console.log(`  Redfin: downloading CSV from gis-csv endpoint...`);
    await waitForListingSlot(REDFIN_BASE);

    let csvText: string;
    try {
      csvText = await withRetry(async () => {
        const resp = await fetch(csvUrl, {
          headers: getHeaders(),
          signal: AbortSignal.timeout(30_000),
        });

        if (!resp.ok) {
          if (resp.status === 403 || resp.status === 429) {
            backoffDomain(REDFIN_BASE);
            throw new Error(`HTTP ${resp.status} — rate limited or blocked`);
          }
          throw new Error(`HTTP ${resp.status}`);
        }

        resetDomainRate(REDFIN_BASE);
        return await resp.text();
      }, `redfin-csv:${areaLabel}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown";
      console.log(`  Redfin CSV download failed: ${msg}`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    // Step 3: Parse CSV into records
    const rows = parseCsv(csvText);
    console.log(`  Redfin: parsed ${rows.length} rows from CSV`);

    if (rows.length === 0) {
      console.log(`  Redfin: CSV was empty or unparseable`);
      // Log first 500 chars of response for debugging
      if (csvText.length > 0) {
        console.log(`  Response preview: ${csvText.substring(0, 500)}`);
      }
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    const records: OnMarketRecord[] = [];

    for (const row of rows) {
      const record = csvRowToListing(row, area.state);
      if (!record) continue;

      records.push(record);
      progress.total_found++;
      progress.total_processed++;
      yield record;
    }

    // Cache results
    if (records.length > 0) {
      setCache(cacheKey, JSON.stringify(records));
    }

    onProgress?.(progress);
    console.log(
      `  Redfin ${areaLabel}: ${progress.total_found} found, ${progress.total_processed} processed, ${progress.errors} errors`,
    );
  }
}
