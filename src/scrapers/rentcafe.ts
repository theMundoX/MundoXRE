/**
 * RentCafe Property Website Scraper
 *
 * Scrapes actual advertised rental rates from RentCafe-hosted property websites.
 * RentCafe (by Yardi) hosts thousands of apartment leasing sites.
 *
 * URL patterns:
 *   https://{property-slug}.rentcafe.com
 *   https://{property-slug}.rentcafe.com/floorplans
 *
 * Data extracted:
 *   - Property name, total units, year built
 *   - Floor plans: name, beds, baths, sqft, rent range
 *   - Available units per floor plan
 *   - Amenities (building + unit level)
 *   - Fees (application, admin, pet, parking)
 *   - Concessions / specials
 */

import { chromium, type Page, type Browser, type BrowserContext } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../utils/rate-limiter.js";
import { validateUrlBeforeScrape } from "../utils/allowlist.js";
import { getResidentialProxy } from "../utils/proxy.js";
import type { RentSnapshot } from "../db/queries.js";

// ─── Types ───────────────────────────────────────────────────────────

export interface ScrapedFloorplan {
  name: string;
  beds: number;
  baths: number;
  sqft_min: number;
  sqft_max: number;
  rent_min: number;
  rent_max: number;
  available_count: number;
  deposit: number;
}

export interface ScrapedPropertyData {
  property_name: string;
  total_units: number;
  year_built: number;
  address: string;
  city: string;
  state: string;
  zip: string;
  phone: string;
  website_url: string;
  floorplans: ScrapedFloorplan[];
  amenities: string[];
  fees: {
    application_fee: number;
    admin_fee: number;
    pet_deposit: number;
    pet_monthly: number;
    parking: number;
  };
  concession_text: string;
  scrape_date: string;
}

// ─── Extraction Script ──────────────────────────────────────────────

/**
 * In-page extraction script for RentCafe sites.
 * Handles multiple layout variations:
 *   1. New RentCafe layout with .fp-info elements
 *   2. Generic floorplan card layout
 *   3. Fallback: any container with bed/bath/rent text
 */
const RENTCAFE_EXTRACT_SCRIPT = `
(() => {
  const getText = (sel) => {
    const el = document.querySelector(sel);
    return el ? el.textContent.trim() : "";
  };

  const getAll = (sel) => {
    return Array.from(document.querySelectorAll(sel)).map(el => el.textContent.trim());
  };

  // Property name — prefer title tag for RentCafe (more reliable)
  const titleText = document.title || "";
  const h1Text = getText("h1, .property-name, .community-name, [class*='property-title']");
  const propertyName = (titleText.includes("RentCafe") ? titleText.split(",")[0]?.split(" - ")[0]?.split("|")[0]?.trim() : "")
    || h1Text || titleText.split("|")[0]?.trim() || "";

  // Total units
  const bodyText = document.body.innerText || "";
  let totalUnits = 0;
  const unitMatch = bodyText.match(/(\\d+)\\s*units?\\s*available/i);
  if (unitMatch) totalUnits = parseInt(unitMatch[1]) || 0;
  if (!totalUnits) {
    const unitMatch2 = bodyText.match(/(\\d+)[-\\s](?:unit|apartment|home|residence)/i);
    if (unitMatch2) totalUnits = parseInt(unitMatch2[1]) || 0;
  }

  // Year built
  let yearBuilt = 0;
  const ybMatch = bodyText.match(/(?:built|constructed|established)\\s*(?:in\\s*)?(\\d{4})/i);
  if (ybMatch) yearBuilt = parseInt(ybMatch[1]) || 0;

  // Address
  const addressEl = document.querySelector(".property-address, [class*='address'], [class*='Address'], address");
  const address = addressEl ? addressEl.textContent.trim().replace(/\\s+/g, " ") : "";

  // Phone
  let phone = "";
  const phoneEl = document.querySelector("a[href^='tel:'], [class*='phone']");
  if (phoneEl) phone = phoneEl.textContent.trim();

  // ─── Floor plans ─────────────────────────────────────────────
  const floorplans = [];

  // ──── STRATEGY 1: RentCafe new layout — .fp-info elements ────
  const fpInfoEls = document.querySelectorAll(".fp-info");
  if (fpInfoEls.length > 0) {
    for (const fpInfo of fpInfoEls) {
      const text = fpInfo.textContent?.trim().replace(/\\s+/g, " ") || "";

      const nameEl = fpInfo.querySelector(".fp-name, b.fp-name, h2, h3, h4, h5, strong, b");
      let name = nameEl ? nameEl.textContent.trim() : "";

      const infoList = fpInfo.querySelector(".fp-info-list");
      const infoText = infoList ? infoList.textContent.trim() : text;

      let beds = -1;
      const bedMatch = infoText.match(/(\\d)\\s*Beds?/i) || infoText.match(/studio/i);
      if (bedMatch) beds = bedMatch[0].toLowerCase().includes("studio") ? 0 : parseInt(bedMatch[1]) || 0;

      let baths = 1;
      const bathMatch = infoText.match(/(\\d+(?:\\.\\d+)?)\\s*Baths?/i);
      if (bathMatch) baths = parseFloat(bathMatch[1]) || 1;

      let sqftMin = 0, sqftMax = 0;
      const sqftRangeMatch = infoText.match(/(\\d{3,5})\\s*[-\u2013]\\s*(\\d{3,5})\\s*Sqft/i);
      const sqftSingle = infoText.match(/(\\d{3,5})\\s*Sqft/i);
      if (sqftRangeMatch) {
        sqftMin = parseInt(sqftRangeMatch[1]) || 0;
        sqftMax = parseInt(sqftRangeMatch[2]) || sqftMin;
      } else if (sqftSingle) {
        sqftMin = parseInt(sqftSingle[1]) || 0;
        sqftMax = sqftMin;
      }

      let rentMin = 0, rentMax = 0;
      const priceEl = fpInfo.querySelector(".fp-price-info, [class*='price'], [class*='rent']");
      const priceText = priceEl ? priceEl.textContent.trim() : text;
      const rentRange = priceText.match(/\\$(\\d[\\d,]{2,5})\\s*[-\u2013]\\s*\\$(\\d[\\d,]{2,5})/);
      const rentSingle = priceText.match(/\\$(\\d[\\d,]{2,5})/);
      if (rentRange) {
        rentMin = parseInt(rentRange[1].replace(",", "")) || 0;
        rentMax = parseInt(rentRange[2].replace(",", "")) || rentMin;
      } else if (rentSingle) {
        rentMin = parseInt(rentSingle[1].replace(",", "")) || 0;
        rentMax = rentMin;
      }

      let available = 0;
      const fpItem = fpInfo.closest(".fp-item, [class*='fp-item']") || fpInfo.parentElement;
      if (fpItem) {
        const unitRows = fpItem.querySelectorAll(".fp-unit, tr.fp-unit");
        available = unitRows.length;
      }

      if (!name) name = beds === 0 ? "Studio" : beds + " Bed / " + baths + " Bath";

      if (beds >= 0 && (rentMin > 0 || sqftMin > 0)) {
        floorplans.push({
          name: name.substring(0, 100),
          beds, baths, sqft_min: sqftMin, sqft_max: sqftMax,
          rent_min: rentMin, rent_max: rentMax,
          available_count: available, deposit: 0,
        });
      }
    }
  }

  // ──── STRATEGY 2: Generic floorplan cards ────────────────────
  if (floorplans.length === 0) {
    const fpSelectors = [
      ".floorplan-card", ".fp-card", "[class*='floorplan']", "[class*='floor-plan']",
      "[class*='FloorPlan']", ".unit-type", "tr[class*='fp']",
      "[class*='unitType']", "[class*='pricingRow']", "[data-unit-type]",
      ".floorplan", ".floor-plan-card", ".pricing-row",
    ];

    let fpElements = [];
    for (const sel of fpSelectors) {
      fpElements = Array.from(document.querySelectorAll(sel));
      if (fpElements.length > 0) break;
    }

    // Strategy 3: any container with bed/bath/rent info
    if (fpElements.length === 0) {
      const allContainers = document.querySelectorAll("div, article, section, tr");
      for (const c of allContainers) {
        const t = c.textContent || "";
        if (/(studio|\\d\\s*bed)/i.test(t) && /\\$\\d{3,}/.test(t) && t.length < 1000) {
          fpElements.push(c);
        }
      }
      // Remove nested duplicates
      fpElements = fpElements.filter((el, i) => {
        for (let j = 0; j < fpElements.length; j++) {
          if (i !== j && fpElements[j].contains(el) && fpElements[j] !== el) return false;
        }
        return true;
      });
    }

    for (const fp of fpElements) {
      const text = fp.textContent || "";

      let beds = -1;
      const bedMatch = text.match(/(\\d)\\s*(?:bed(?:room)?s?|br|BD)/i) || text.match(/studio/i);
      if (bedMatch) beds = bedMatch[0].toLowerCase().includes("studio") ? 0 : parseInt(bedMatch[1]) || 0;

      let baths = 1;
      const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*(?:bath(?:room)?s?|ba|BA)/i);
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
      const rentSingle = text.match(/\\$(\\d[\\d,]{2,5})/);
      if (rentRange) {
        rentMin = parseInt(rentRange[1].replace(",", "")) || 0;
        rentMax = parseInt(rentRange[2].replace(",", "")) || rentMin;
      } else if (rentSingle) {
        rentMin = parseInt(rentSingle[1].replace(",", "")) || 0;
        rentMax = rentMin;
      }

      let available = 0;
      const availMatch = text.match(/(\\d+)\\s*(?:available|avail)/i);
      if (availMatch) available = parseInt(availMatch[1]) || 0;

      let deposit = 0;
      const depMatch = text.match(/deposit[:\\s]*\\$(\\d{2,5})/i);
      if (depMatch) deposit = parseInt(depMatch[1]) || 0;

      const nameEl = fp.querySelector("h2, h3, h4, h5, .fp-name, .floorplan-name, [class*='name'], .title");
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

  // ─── Deduplicate ───────────────────────────────────────────────
  const seen = new Set();
  const uniqueFps = floorplans.filter(fp => {
    const key = fp.name + "|" + fp.beds + "|" + fp.baths + "|" + fp.rent_min;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // ─── Amenities ────────────────────────────────────────────────
  const amenities = getAll(
    ".amenity, [class*='amenity'] li, .feature-list li, [class*='feature'] li, " +
    "[class*='Amenity'] li, .amenity-list li"
  ).filter(Boolean).slice(0, 50);

  // ─── Fees ─────────────────────────────────────────────────────
  let appFee = 0, adminFee = 0, petDeposit = 0, petMonthly = 0, parking = 0;
  const appMatch = bodyText.match(/application\\s*fee[s]?[:\\s]*\\$(\\d+)/i);
  if (appMatch) appFee = parseInt(appMatch[1]) || 0;
  const admMatch = bodyText.match(/admin(?:istration)?\\s*fee[:\\s]*\\$(\\d+)/i);
  if (admMatch) adminFee = parseInt(admMatch[1]) || 0;
  const petDepMatch = bodyText.match(/pet\\s*deposit[:\\s]*\\$(\\d+)/i);
  if (petDepMatch) petDeposit = parseInt(petDepMatch[1]) || 0;
  const petMoMatch = bodyText.match(/pet\\s*(?:rent|monthly)[:\\s]*\\$(\\d+)/i);
  if (petMoMatch) petMonthly = parseInt(petMoMatch[1]) || 0;
  const parkMatch = bodyText.match(/parking[:\\s]*\\$(\\d+)/i);
  if (parkMatch) parking = parseInt(parkMatch[1]) || 0;

  // ─── Concessions ──────────────────────────────────────────────
  let concession = "";
  const specEl = document.querySelector(
    ".special, .concession, [class*='special'], [class*='promo'], [class*='Special'], [class*='Promo']"
  );
  if (specEl) concession = specEl.textContent.trim();
  if (!concession) {
    const concMatch = bodyText.match(/(\\d+\\s*(?:weeks?|months?)\\s*free[^.]*)/i)
      || bodyText.match(/(move[- ]in\\s*special[^.]*)/i)
      || bodyText.match(/(\\$\\d+\\s*off[^.]*)/i);
    if (concMatch) concession = concMatch[1].trim();
  }

  return {
    property_name: propertyName.substring(0, 200),
    total_units: totalUnits,
    year_built: yearBuilt,
    address: address.substring(0, 300),
    city: "",
    state: "",
    zip: "",
    phone,
    website_url: "",
    floorplans: uniqueFps,
    amenities,
    fees: { application_fee: appFee, admin_fee: adminFee, pet_deposit: petDeposit, pet_monthly: petMonthly, parking },
    concession_text: concession.substring(0, 500),
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
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });
  return context;
}

// ─── RentCafe Scraper ────────────────────────────────────────────────

/**
 * Scrape a RentCafe property website for rental data.
 * Accepts an optional shared browser to avoid launching one per property.
 */
export async function scrapeRentCafe(
  url: string,
  sharedBrowser?: Browser,
): Promise<ScrapedPropertyData | null> {
  const validation = validateUrlBeforeScrape(url);
  if (!validation.allowed) {
    console.log(`  Blocked: ${validation.reason}`);
    return null;
  }

  await waitForSlot(url);

  const ownBrowser = !sharedBrowser;
  const browser = sharedBrowser ?? await chromium.launch(buildLaunchOptions());
  const context = await createStealthContext(browser);
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(3000);

    // Try to click the Floor Plans / Pricing tab
    try {
      const tabSelectors = [
        'a[href*="floorplan"]', 'a[href*="floor-plan"]', 'a[href*="pricing"]',
        '[class*="floorplan-tab"]', '[class*="pricing-tab"]',
        'button:has-text("Floor Plans")', 'a:has-text("Floor Plans")',
        'a:has-text("Pricing")', 'button:has-text("Pricing")',
      ];
      for (const sel of tabSelectors) {
        const tab = page.locator(sel).first();
        if ((await tab.count()) > 0) {
          await tab.click({ timeout: 3000 }).catch(() => {});
          await page.waitForTimeout(2000);
          break;
        }
      }
    } catch { /* tab not found — pricing might be on main page */ }

    // Extract data
    const data = (await page.evaluate(RENTCAFE_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;

    // If no floorplans, try /floorplans subpage
    if (data && data.floorplans.length === 0) {
      const fpUrl = url.replace(/\/$/, "") + "/floorplans";
      try {
        await page.goto(fpUrl, { waitUntil: "domcontentloaded", timeout: 15_000 });
        await page.waitForTimeout(3000);
        const fpData = (await page.evaluate(RENTCAFE_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;
        if (fpData && fpData.floorplans.length > 0) {
          fpData.website_url = url;
          resetDomainRate(url);
          return fpData;
        }
      } catch { /* floorplans page doesn't exist */ }
    }

    if (data) {
      data.website_url = url;
      resetDomainRate(url);
    }
    return data;
  } catch (err) {
    backoffDomain(url);
    console.error(`  Scrape error for ${url}:`, err instanceof Error ? err.message : "Unknown");
    return null;
  } finally {
    await context.close();
    if (ownBrowser) await browser.close();
  }
}

/**
 * Convert scraped RentCafe data to RentSnapshot records ready for DB insertion.
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
      concession_text: data.concession_text || undefined,
      raw: {
        source: "rentcafe",
        rent_max: fp.rent_max,
        sqft_max: fp.sqft_max,
        floor_plan_name: fp.name,
        property_name: data.property_name,
      },
    });
  }

  return snapshots;
}
