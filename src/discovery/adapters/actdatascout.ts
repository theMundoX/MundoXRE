/**
 * ActDataScout Adapter — property assessor data for OK, AR, LA, PA, VA, CT, ME, MA.
 *
 * Site structure (discovered 2026-03-25):
 *   Search: POST /RealProperty/Search with CSRF token + CountyId + search fields
 *   Detail: POST /RealProperty/ParcelView with countyIdYearRpid
 *
 * Detail page fields: owner, mailing address, property type, tax district,
 * physical address, subdivision, block/lot, S-T-R, acres, legal description,
 * fair cash / assessed values, estimated taxes, deed transfers, year built,
 * sqft, stories, construction type, condition, beds, outbuildings.
 *
 * Discovery strategy: S-T-R (Section-Township-Range) search covers all parcels
 * systematically. Each county's land survey grid is finite and enumerable.
 */

import { chromium, type Page, type BrowserContext } from "playwright";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";
import { waitForSlot } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../../utils/stealth.js";
import { validateUrlBeforeScrape } from "../../utils/allowlist.js";
import { getResidentialProxy, reportProxyFailure, reportProxySuccess } from "../../utils/proxy.js";

const MAX_RETRIES = 3;
const RETRY_DELAY_BASE_MS = 5_000;

// ─── Browser Management ─────────────────────────────────────────────

async function createBrowserContext(): Promise<{ context: BrowserContext; close: () => Promise<void> }> {
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

// ─── Retry Logic ─────────────────────────────────────────────────────

async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  maxRetries = MAX_RETRIES,
): Promise<T> {
  let lastErr: Error | undefined;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err instanceof Error ? err : new Error(String(err));
      if (attempt < maxRetries) {
        const delay = RETRY_DELAY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
        console.log(`  Retry ${attempt}/${maxRetries} for ${label} in ${(delay / 1000).toFixed(1)}s: ${lastErr.message}`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ─── Search Results Parsing ──────────────────────────────────────────

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

/**
 * Submit a search using a persistent page. Navigates to search page,
 * fills form, submits, extracts results, then navigates back.
 */
async function submitSearchForm(
  page: Page,
  baseUrl: string,
  formId: string,
  fields: Record<string, string>,
): Promise<SearchResult[]> {
  await waitForSlot(baseUrl);

  try {
    // Navigate to the search page
    await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    // Fill form fields via JS (all forms exist in DOM, just hidden by tabs)
    const fieldJson = JSON.stringify(fields);
    await page.evaluate(`
      (() => {
        const form = document.querySelector("#${formId}");
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

    // Click submit button with force:true to bypass visibility check,
    // then wait for navigation to search results page
    const submitBtn = page.locator(`#${formId} button[type="submit"]`);
    await submitBtn.dispatchEvent("click");
    await page.waitForLoadState("networkidle", { timeout: 30_000 }).catch(() => {});
    await page.waitForTimeout(2000);

    // Extract results
    return await extractSearchResults(page);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown";
    if (!msg.includes("Timeout")) {
      console.log(`  Search error: ${msg}`);
    }
    return [];
  }
}

async function searchBySTR(
  page: Page,
  baseUrl: string,
  countyId: string,
  section: number,
  township: number,
  range: number,
): Promise<SearchResult[]> {
  const cacheKey = `actds:str:${countyId}:${section}-${township}-${range}`;
  const cached = getCached(cacheKey);
  if (cached) return JSON.parse(cached) as SearchResult[];

  const fields: Record<string, string> = {
    Township: String(township),
    Range: String(range),
  };
  if (section > 0) fields.Section = String(section);

  const results = await submitSearchForm(page, baseUrl, "RealFormSTRSearch", fields);

  if (results.length > 0) {
    setCache(cacheKey, JSON.stringify(results));
  }
  return results;
}

async function searchByAddress(
  page: Page,
  baseUrl: string,
  streetName: string,
): Promise<SearchResult[]> {
  const cacheKey = `actds:addr:${baseUrl}:${streetName}`;
  const cached = getCached(cacheKey);
  if (cached) return JSON.parse(cached) as SearchResult[];

  const results = await submitSearchForm(page, baseUrl, "RealFormAddressSearch", {
    StreetName: streetName,
  });

  if (results.length > 0) {
    setCache(cacheKey, JSON.stringify(results));
  }
  return results;
}

async function extractSearchResults(page: Page): Promise<SearchResult[]> {
  return await page.evaluate(`
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
}

// ─── Detail Page Parsing ─────────────────────────────────────────────

async function scrapePropertyDetail(
  page: Page,
  crpid: string,
  countyName: string,
  state: string,
): Promise<RawPropertyRecord | null> {
  const cacheKey = `actds:detail:${crpid}`;
  const cached = getCached(cacheKey);
  if (cached) return JSON.parse(cached) as RawPropertyRecord;

  await waitForSlot("https://www.actdatascout.com");

  try {
    // Submit form to ParcelView
    await page.evaluate(`
      (() => {
        const form = document.createElement("form");
        form.method = "POST";
        form.action = "/RealProperty/ParcelView";
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = "countyIdYearRpid";
        input.value = "${crpid}";
        form.appendChild(input);
        document.body.appendChild(form);
        form.submit();
      })()
    `);

    await page.waitForURL("**/ParcelView**", { timeout: 30_000 }).catch(() => {});
    await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
    await page.waitForTimeout(2000);

    const record = await page.evaluate(`
      (() => {
        const county = "${countyName}";
        const st = "${state}";
        const id = "${crpid}";

        const findLabelValue = (label) => {
          const dts = document.querySelectorAll("dt");
          for (const dt of dts) {
            if (dt.textContent?.trim().toLowerCase().includes(label.toLowerCase())) {
              const dd = dt.nextElementSibling;
              if (dd?.tagName === "DD") return dd.textContent?.trim() || "";
            }
          }
          const allText = document.body.innerText;
          const regex = new RegExp(label + "[:\\\\s]+([^\\\\n]+)", "i");
          const match = allText.match(regex);
          return match?.[1]?.trim() || "";
        };

        const ownerName = findLabelValue("Property Owner Name") || findLabelValue("Owner Name");
        const mailingAddr = findLabelValue("Mailing Address");
        const propType = findLabelValue("Type");
        const taxDistrict = findLabelValue("Tax District");
        const physicalAddr = findLabelValue("Physical Address");
        const subdivision = findLabelValue("Subdivision");
        const blockLot = findLabelValue("Block/Lot");
        const strVal = findLabelValue("S-T-R");
        const acres = findLabelValue("Size (Acres)") || findLabelValue("Acres");
        const legal = findLabelValue("Legal");

        const bodyText = document.body.innerText;

        let assessedValue;
        const assessedMatch = bodyText.match(/Taxable\\s*Assessed\\s*\\$?([\\d,]+)/i);
        if (assessedMatch) assessedValue = assessedMatch[1];

        let marketValue;
        const totalsRow = bodyText.match(/Totals\\s+([\\d,]+)\\s+([\\d,]+)\\s+([\\d,]+)/);
        if (totalsRow) marketValue = totalsRow[1];

        let estimatedTax;
        const taxMatch = bodyText.match(/Estimated\\s*Taxes[:\\s]*([\\d,]+)/i);
        if (taxMatch) estimatedTax = taxMatch[1];

        let yearBuilt, totalLivSqft, stories, construction, condition, beds, occupancy;

        const resCardMatch = bodyText.match(
          /Occupancy\\s+Story\\s+Construction\\s+Total Liv\\s+Grade\\s+Age\\s+Year Built\\s+Condition\\s+Beds\\s+([^\\n]+)/i
        );
        if (resCardMatch) {
          const parts = resCardMatch[1].trim().split(/\\s{2,}/);
          if (parts.length >= 7) {
            occupancy = parts[0];
            stories = parts[1];
            construction = parts[2];
            totalLivSqft = parts[3]?.replace(/[,\\s]/g, "");
            yearBuilt = parts[6];
            condition = parts[7] || undefined;
            beds = parts[8] || undefined;
          }
        }

        if (!yearBuilt) {
          const ybMatch = bodyText.match(/Year\\s*Built[:\\s]*(\\d{4})/i);
          if (ybMatch) yearBuilt = ybMatch[1];
        }

        let lastSaleDate, lastSalePrice, deedType;
        const deedMatch = bodyText.match(
          /Deed\\s*Date\\s+Book\\s+Page\\s+Deed\\s*Type\\s+Stamps\\s+Est\\.\\s*Sale\\s+Grantor\\s+([^\\n]+)/i
        );
        if (deedMatch) {
          const parts = deedMatch[1].trim().split(/\\s{2,}/);
          if (parts.length >= 5) {
            lastSaleDate = parts[0];
            deedType = parts[3];
            const saleStr = parts[5] || parts[4];
            if (saleStr) lastSalePrice = saleStr.replace(/[\\$,\\s]/g, "");
          }
        }

        let city = county;
        let zip = "";
        if (mailingAddr) {
          const zipMatch = mailingAddr.match(/(\\d{5})(-\\d{4})?/);
          if (zipMatch) zip = zipMatch[1];
          const cityMatch = mailingAddr.match(/([A-Z][A-Z\\s]+),\\s*[A-Z]{2}/);
          if (cityMatch) city = cityMatch[1].trim();
        }

        const storyNum = stories === "ONE" ? 1 : stories === "TWO" ? 2 : stories === "THREE" ? 3 :
                         stories ? parseInt(stories) || undefined : undefined;

        return {
          parcel_id: "",
          address: physicalAddr,
          city: city,
          state: st,
          zip: zip,
          owner_name: ownerName || undefined,
          property_type: propType || undefined,
          assessed_value: assessedValue ? parseFloat(assessedValue.replace(/[,]/g, "")) || undefined : undefined,
          market_value: marketValue ? parseFloat(marketValue.replace(/[,]/g, "")) || undefined : undefined,
          property_tax: estimatedTax ? parseFloat(estimatedTax.replace(/[,]/g, "")) || undefined : undefined,
          year_built: yearBuilt ? parseInt(yearBuilt) || undefined : undefined,
          total_sqft: totalLivSqft ? parseInt(totalLivSqft) || undefined : undefined,
          total_units: undefined,
          stories: storyNum,
          last_sale_price: lastSalePrice && lastSalePrice !== "0" ? parseFloat(lastSalePrice) || undefined : undefined,
          last_sale_date: lastSaleDate || undefined,
          legal_description: legal || undefined,
          assessor_url: "https://www.actdatascout.com/RealProperty/ParcelView",
          raw: { crpid: id, subdivision, blockLot, str: strVal, acres, taxDistrict, construction, condition, beds, occupancy, deedType, mailingAddr },
        };
      })()
    `) as unknown as RawPropertyRecord;

    if (record && (record.address || record.owner_name)) {
      setCache(cacheKey, JSON.stringify(record));
      return record;
    }
  } catch (err) {
    console.error(`  Detail error for ${crpid}:`, err instanceof Error ? err.message : "Unknown");
  }

  return null;
}

// ─── S-T-R Grid Discovery ────────────────────────────────────────────

/**
 * Known S-T-R ranges for Oklahoma counties.
 * Comanche County spans approximately:
 *   Townships: 1N-4N, 1S-3S
 *   Ranges: 9W-16W
 *   Sections: 1-36 per township/range
 */
const COUNTY_STR_GRIDS: Record<string, { townships: number[]; ranges: number[]; sections: number[] }> = {
  "40031": { // Comanche County
    townships: [1, 2, 3, 4, -1, -2, -3], // positive = North, negative = South
    ranges: [9, 10, 11, 12, 13, 14, 15, 16],
    sections: Array.from({ length: 36 }, (_, i) => i + 1),
  },
};

// Default grid for unknown counties — covers common OK ranges
const DEFAULT_STR_GRID = {
  townships: [1, 2, 3, 4, -1, -2, -3],
  ranges: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
  sections: Array.from({ length: 36 }, (_, i) => i + 1),
};

// ─── Adapter ─────────────────────────────────────────────────────────

export class ActDataScoutAdapter extends AssessorAdapter {
  readonly platform = "actdatascout";

  canHandle(config: CountyConfig): boolean {
    return (
      config.platform === "actdatascout" ||
      config.alt_platform === "actdatascout" ||
      config.base_url.includes("actdatascout.com")
    );
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const baseUrl =
      config.platform === "actdatascout"
        ? config.base_url
        : config.alt_url ?? config.base_url;

    const validation = validateUrlBeforeScrape(baseUrl);
    if (!validation.allowed) {
      console.error(`  Blocked: ${validation.reason}`);
      return;
    }

    const countyId = `${config.state_fips}${config.county_fips}`;
    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    const { context, close } = await createBrowserContext();

    try {
      const page = await context.newPage();
      await page.addInitScript(STEALTH_INIT_SCRIPT);
      page.setDefaultTimeout(30_000);

      const grid = COUNTY_STR_GRIDS[countyId] || DEFAULT_STR_GRID;
      const seenCrpids = new Set<string>();
      let totalCombos = grid.townships.length * grid.ranges.length;
      let completedCombos = 0;

      console.log(`  Scanning ${grid.townships.length} townships x ${grid.ranges.length} ranges`);

      for (const township of grid.townships) {
        for (const range of grid.ranges) {
          completedCombos++;

          // Search by township+range (leave section blank to get all sections)
          try {
            const results = await withRetry(
              () => searchBySTR(page, baseUrl, countyId, 0, Math.abs(township), range),
              `STR T${township}R${range}`,
            );

            if (results.length === 0) continue;

            console.log(`  T${township >= 0 ? township + "N" : Math.abs(township) + "S"} R${range}W: ${results.length} parcels (${completedCombos}/${totalCombos} combos)`);

            for (const result of results) {
              if (!result.crpid || seenCrpids.has(result.crpid)) continue;
              seenCrpids.add(result.crpid);

              progress.total_found++;

              // Fetch full detail for this property
              try {
                const detail = await withRetry(
                  () => scrapePropertyDetail(page, result.crpid, config.name, config.state),
                  `detail ${result.crpid}`,
                );

                if (detail) {
                  // Merge search result data into detail
                  detail.parcel_id = result.parcel || detail.parcel_id;
                  if (!detail.address && result.address) detail.address = result.address;
                  if (!detail.city && result.subdivision) detail.city = result.subdivision;
                  if (!detail.owner_name && result.ownerName) detail.owner_name = result.ownerName;

                  progress.total_processed++;
                  onProgress?.(progress);
                  yield detail;
                } else {
                  // Fall back to search result data only
                  const fallback: RawPropertyRecord = {
                    parcel_id: result.parcel,
                    address: result.address,
                    city: result.subdivision || config.name,
                    state: config.state,
                    zip: "",
                    owner_name: result.ownerName || result.businessName || undefined,
                    legal_description: result.legal || undefined,
                    assessor_url: baseUrl,
                    raw: result as unknown as Record<string, unknown>,
                  };
                  progress.total_processed++;
                  onProgress?.(progress);
                  yield fallback;
                }
              } catch (err) {
                console.error(`  Detail failed for ${result.crpid}:`, err instanceof Error ? err.message : "Unknown");
                progress.errors++;

                // Still yield basic data from search results
                const fallback: RawPropertyRecord = {
                  parcel_id: result.parcel,
                  address: result.address,
                  city: result.subdivision || config.name,
                  state: config.state,
                  zip: "",
                  owner_name: result.ownerName || result.businessName || undefined,
                  legal_description: result.legal || undefined,
                  assessor_url: baseUrl,
                  raw: result as unknown as Record<string, unknown>,
                };
                progress.total_processed++;
                onProgress?.(progress);
                yield fallback;
              }

              // Progress reporting
              if (progress.total_processed % 50 === 0) {
                console.log(`  Progress: ${progress.total_processed} processed, ${progress.errors} errors, ${seenCrpids.size} unique`);
              }
            }
          } catch (err) {
            console.error(`  STR search failed T${township}R${range}:`, err instanceof Error ? err.message : "Unknown");
            progress.errors++;

            // If we get too many consecutive errors, the browser may be stale
            if (progress.errors > 10 && progress.errors > progress.total_processed * 0.5) {
              console.log("  Too many errors — restarting browser");
              await close();
              const newCtx = await createBrowserContext();
              // Can't reassign const — break and let caller restart
              break;
            }
          }
        }
      }
    } finally {
      await close();
    }

    console.log(
      `  ${config.name}: ${progress.total_found} found, ${progress.total_processed} processed, ${progress.errors} errors`,
    );
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    // County info page shows total parcels — hardcode known values
    const KNOWN_COUNTS: Record<string, number> = {
      "40031": 57_081, // Comanche County
    };
    const countyId = `${config.state_fips}${config.county_fips}`;
    return KNOWN_COUNTS[countyId] ?? null;
  }
}
