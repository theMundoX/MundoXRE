/**
 * AppFolio Property Website Scraper
 *
 * Scrapes actual advertised rental rates from AppFolio-hosted property listing pages.
 * AppFolio is a property management platform used by smaller/mid-size operators.
 *
 * URL patterns:
 *   https://{property-slug}.appfolio.com/listings
 *   https://{property-slug}.appfolio.com/listings?1702702748
 *
 * AppFolio listing pages show available units in a straightforward table/card layout:
 *   - Address, unit number
 *   - Beds, baths, sqft
 *   - Rent amount
 *   - Available date
 *
 * Data extracted:
 *   - Unit listings: beds, baths, sqft, rent, availability date
 *   - Maps to RentSnapshot type
 */

import { chromium, type Browser, type BrowserContext } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../utils/rate-limiter.js";
import { validateUrlBeforeScrape } from "../utils/allowlist.js";
import { getResidentialProxy } from "../utils/proxy.js";
import type { RentSnapshot } from "../db/queries.js";
import type { ScrapedFloorplan, ScrapedPropertyData } from "./rentcafe.js";

// ─── Extraction Script ──────────────────────────────────────────────

/**
 * In-page extraction for AppFolio listing pages.
 *
 * AppFolio renders listings in one of these patterns:
 *   1. .listing-item cards with structured data
 *   2. Table rows with unit details
 *   3. JSON-LD structured data in script tags
 *   4. Generic card/row containers
 */
const APPFOLIO_EXTRACT_SCRIPT = `
(() => {
  const getText = (sel) => {
    const el = document.querySelector(sel);
    return el ? el.textContent.trim() : "";
  };

  const getAll = (sel) => {
    return Array.from(document.querySelectorAll(sel)).map(el => el.textContent.trim());
  };

  const bodyText = document.body.innerText || "";

  // ─── Property name ────────────────────────────────────────────
  const titleText = document.title || "";
  const h1Text = getText("h1, .property-name, [class*='company-name'], [class*='brand']");
  const propertyName = h1Text || titleText.split("|")[0]?.split(" - ")[0]?.trim() || "";

  // ─── Address (from page header if available) ──────────────────
  const addressEl = document.querySelector("[class*='address'], [itemprop='address'], address");
  const address = addressEl ? addressEl.textContent.trim().replace(/\\s+/g, " ") : "";

  // ─── Phone ────────────────────────────────────────────────────
  let phone = "";
  const phoneEl = document.querySelector("a[href^='tel:'], [class*='phone']");
  if (phoneEl) phone = phoneEl.textContent.trim();

  // ─── Try JSON-LD first ────────────────────────────────────────
  const floorplans = [];
  let foundJson = false;

  const jsonLdScripts = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of jsonLdScripts) {
    try {
      const data = JSON.parse(s.textContent || "");
      const items = Array.isArray(data) ? data : [data];
      for (const item of items) {
        if (item["@type"] === "Apartment" || item["@type"] === "RealEstateListing" || item["@type"] === "Product") {
          const offers = item.offers || item.offer || {};
          const rent = parseInt(offers.price || offers.lowPrice || "0") || 0;
          const beds = parseInt(item.numberOfBedrooms || item.bedrooms || "0") || 0;
          const baths = parseFloat(item.numberOfBathroomsTotal || item.bathrooms || "1") || 1;
          const sqft = parseInt(item.floorSize?.value || item.sqft || "0") || 0;
          const name = item.name || item.description || "";

          if (rent > 0 || sqft > 0) {
            floorplans.push({
              name: (name || (beds === 0 ? "Studio" : beds + " Bed")).substring(0, 100),
              beds, baths,
              sqft_min: sqft, sqft_max: sqft,
              rent_min: rent, rent_max: rent,
              available_count: 1, deposit: 0,
            });
            foundJson = true;
          }
        }
      }
    } catch {}
  }

  // ─── DOM extraction ───────────────────────────────────────────
  if (!foundJson || floorplans.length === 0) {
    // Strategy 1: AppFolio listing items
    const listingSelectors = [
      ".listing-item", ".listing-card", "[class*='listing']",
      ".js-listing-item", ".listable-item",
      ".property-listing", "[data-listing-id]",
    ];

    let items = [];
    for (const sel of listingSelectors) {
      items = Array.from(document.querySelectorAll(sel));
      if (items.length > 0) break;
    }

    // Strategy 2: Table rows
    if (items.length === 0) {
      items = Array.from(document.querySelectorAll("tr")).filter(tr => {
        const text = tr.textContent || "";
        return /(\\d+\\s*bed|studio)/i.test(text) && /\\$\\d{3,}/.test(text);
      });
    }

    // Strategy 3: Generic containers
    if (items.length === 0) {
      const allContainers = document.querySelectorAll("div, article, section, li");
      for (const c of allContainers) {
        const t = c.textContent || "";
        if (/(studio|\\d\\s*bed)/i.test(t) && /\\$\\d{3,}/.test(t) && t.length < 1200) {
          items.push(c);
        }
      }
      // Remove nested duplicates
      items = items.filter((el, i) => {
        for (let j = 0; j < items.length; j++) {
          if (i !== j && items[j].contains(el) && items[j] !== el) return false;
        }
        return true;
      });
    }

    for (const item of items) {
      const text = item.textContent || "";

      let beds = -1;
      const bedMatch = text.match(/(\\d)\\s*(?:bed(?:room)?s?|br|BD|Bed)/i) || text.match(/studio/i);
      if (bedMatch) beds = bedMatch[0].toLowerCase().includes("studio") ? 0 : parseInt(bedMatch[1]) || 0;

      let baths = 1;
      const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*(?:bath(?:room)?s?|ba|BA|Bath)/i);
      if (bathMatch) baths = parseFloat(bathMatch[1]) || 1;

      let sqftMin = 0, sqftMax = 0;
      const sqftRangeMatch = text.match(/(\\d{3,5})\\s*(?:-|to|\u2013)\\s*(\\d{3,5})\\s*(?:sq|sf|sqft|sq\\.?\\s*ft)/i);
      const sqftSingle = text.match(/(\\d{3,5})\\s*(?:sq|sf|sqft|sq\\.?\\s*ft)/i);
      if (sqftRangeMatch) {
        sqftMin = parseInt(sqftRangeMatch[1]) || 0;
        sqftMax = parseInt(sqftRangeMatch[2]) || sqftMin;
      } else if (sqftSingle) {
        sqftMin = parseInt(sqftSingle[1]) || 0;
        sqftMax = sqftMin;
      }

      let rentMin = 0, rentMax = 0;
      const rentRange = text.match(/\\$(\\d[\\d,]{2,5})\\s*(?:-|to|\u2013)\\s*\\$(\\d[\\d,]{2,5})/);
      const rentSingle = text.match(/\\$(\\d[\\d,]{2,5})(?:\\s*\\/\\s*(?:mo|month))?/i);
      if (rentRange) {
        rentMin = parseInt(rentRange[1].replace(",", "")) || 0;
        rentMax = parseInt(rentRange[2].replace(",", "")) || rentMin;
      } else if (rentSingle) {
        rentMin = parseInt(rentSingle[1].replace(",", "")) || 0;
        rentMax = rentMin;
      }

      let deposit = 0;
      const depMatch = text.match(/deposit[:\\s]*\\$(\\d{2,5})/i);
      if (depMatch) deposit = parseInt(depMatch[1]) || 0;

      // AppFolio lists individual units, so available_count is 1 per listing
      let available = 1;

      // Try to get unit address/name
      const nameEl = item.querySelector("h2, h3, h4, h5, [class*='address'], [class*='name'], [class*='title'], strong, b");
      let name = nameEl ? nameEl.textContent.trim() : "";
      if (!name || name.length > 100) name = beds === 0 ? "Studio" : beds + " Bed / " + baths + " Bath";

      if (beds >= 0 && (rentMin > 0 || sqftMin > 0)) {
        floorplans.push({
          name: name.substring(0, 100),
          beds, baths, sqft_min: sqftMin, sqft_max: sqftMax,
          rent_min: rentMin, rent_max: rentMax,
          available_count: available, deposit,
        });
      }
    }
  }

  // ─── Deduplicate ──────────────────────────────────────────────
  const seen = new Set();
  const uniqueFps = floorplans.filter(fp => {
    const key = fp.name + "|" + fp.beds + "|" + fp.baths + "|" + fp.rent_min;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // ─── Fees (AppFolio pages rarely show fees, but try) ──────────
  let appFee = 0, petDeposit = 0, petMonthly = 0;
  const appMatch = bodyText.match(/application\\s*fee[s]?[:\\s]*\\$(\\d+)/i);
  if (appMatch) appFee = parseInt(appMatch[1]) || 0;
  const petDepMatch = bodyText.match(/pet\\s*deposit[:\\s]*\\$(\\d+)/i);
  if (petDepMatch) petDeposit = parseInt(petDepMatch[1]) || 0;
  const petMoMatch = bodyText.match(/pet\\s*(?:rent|monthly)[:\\s]*\\$(\\d+)/i);
  if (petMoMatch) petMonthly = parseInt(petMoMatch[1]) || 0;

  return {
    property_name: propertyName.substring(0, 200),
    total_units: 0,
    year_built: 0,
    address: address.substring(0, 300),
    city: "",
    state: "",
    zip: "",
    phone,
    website_url: "",
    floorplans: uniqueFps,
    amenities: [],
    fees: { application_fee: appFee, admin_fee: 0, pet_deposit: petDeposit, pet_monthly: petMonthly, parking: 0 },
    concession_text: "",
    scrape_date: new Date().toISOString().split("T")[0],
  };
})()
`;

// ─── Browser Helpers ────────────────────────────────────────────────

function buildLaunchOptions(): Parameters<typeof chromium.launch>[0] {
  const opts: Parameters<typeof chromium.launch>[0] = { headless: true };
  const proxyUrl = getResidentialProxy();
  if (proxyUrl) {
    try {
      const parsed = new URL(proxyUrl);
      opts.proxy = {
        server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
        username: parsed.username || undefined,
        password: parsed.password || undefined,
      };
    } catch { /* launch without proxy */ }
  }
  return opts;
}

async function createStealthContext(browser: Browser): Promise<BrowserContext> {
  const stealth = getStealthConfig();
  return browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });
}

// ─── AppFolio Scraper ───────────────────────────────────────────────

/**
 * Scrape an AppFolio property listing page for rental data.
 * Accepts an optional shared browser to avoid launching one per property.
 */
export async function scrapeAppFolio(
  url: string,
  sharedBrowser?: Browser,
): Promise<ScrapedPropertyData | null> {
  const validation = validateUrlBeforeScrape(url);
  if (!validation.allowed) {
    console.log(`  Blocked: ${validation.reason}`);
    return null;
  }

  // Ensure we're hitting the /listings page
  const listingsUrl = url.includes("/listings") ? url : url.replace(/\/$/, "") + "/listings";

  await waitForSlot(listingsUrl);

  const ownBrowser = !sharedBrowser;
  const browser = sharedBrowser ?? await chromium.launch(buildLaunchOptions());
  const context = await createStealthContext(browser);
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  try {
    await page.goto(listingsUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(3000);

    // Some AppFolio pages need scrolling to load all listings
    try {
      await page.evaluate(`window.scrollTo(0, document.body.scrollHeight)`);
      await page.waitForTimeout(1500);
    } catch { /* scroll failed, continue */ }

    // Extract data
    const data = (await page.evaluate(APPFOLIO_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;

    if (data) {
      data.website_url = url;
      resetDomainRate(listingsUrl);
    }
    return data;
  } catch (err) {
    backoffDomain(listingsUrl);
    console.error(`  Scrape error for ${listingsUrl}:`, err instanceof Error ? err.message : "Unknown");
    return null;
  } finally {
    await context.close();
    if (ownBrowser) await browser.close();
  }
}

/**
 * Convert scraped AppFolio data to RentSnapshot records ready for DB insertion.
 */
export function toRentSnapshots(
  propertyId: number,
  websiteId: number | undefined,
  data: ScrapedPropertyData,
): RentSnapshot[] {
  const today = data.scrape_date || new Date().toISOString().split("T")[0];
  const snapshots: RentSnapshot[] = [];

  for (const fp of data.floorplans) {
    if (fp.rent_min <= 0) continue;

    const sqft = fp.sqft_min || fp.sqft_max || undefined;
    const askingPsf = sqft && sqft > 0 ? Math.round((fp.rent_min / sqft) * 100) / 100 : undefined;

    snapshots.push({
      property_id: propertyId,
      website_id: websiteId,
      observed_at: today,
      beds: fp.beds,
      baths: fp.baths,
      sqft,
      asking_rent: fp.rent_min,
      effective_rent: fp.rent_min,
      asking_psf: askingPsf,
      deposit: fp.deposit || undefined,
      available_count: fp.available_count || undefined,
      raw: {
        source: "appfolio",
        rent_max: fp.rent_max,
        sqft_max: fp.sqft_max,
        floor_plan_name: fp.name,
        property_name: data.property_name,
      },
    });
  }

  return snapshots;
}
