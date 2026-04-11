/**
 * Entrata Property Website Scraper
 *
 * Scrapes actual advertised rental rates from Entrata-hosted property websites.
 * Entrata is one of the largest property management software platforms.
 *
 * URL patterns:
 *   https://{property-slug}.entrata.com
 *   https://{property-slug}.entrata.com/floorplans
 *
 * Data extracted:
 *   - Property name, address
 *   - Floor plans: name, beds, baths, sqft, rent range
 *   - Available units per floor plan
 *   - Availability dates
 *   - Specials / concessions
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
 * In-page extraction for Entrata property sites.
 *
 * Entrata sites typically render floorplans in one of these patterns:
 *   1. .floor-plan-card / .floorplan-card elements
 *   2. .unit-type-row / .unit-row table rows
 *   3. React-rendered JSON embedded in page script tags
 *   4. Generic containers with bed/bath/rent text
 */
const ENTRATA_EXTRACT_SCRIPT = `
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
  const h1Text = getText("h1, .property-name, .community-name, [class*='propertyName'], [class*='community-name']");
  const propertyName = h1Text
    || titleText.split("|")[0]?.split(" - ")[0]?.split(",")[0]?.trim()
    || "";

  // ─── Address ──────────────────────────────────────────────────
  const addressEl = document.querySelector(
    ".property-address, [class*='address'], [class*='Address'], [itemprop='address'], address"
  );
  const address = addressEl ? addressEl.textContent.trim().replace(/\\s+/g, " ") : "";

  // ─── Phone ────────────────────────────────────────────────────
  let phone = "";
  const phoneEl = document.querySelector("a[href^='tel:'], [class*='phone'], [itemprop='telephone']");
  if (phoneEl) phone = phoneEl.textContent.trim();

  // ─── Total units / Year built ─────────────────────────────────
  let totalUnits = 0;
  const unitMatch = bodyText.match(/(\\d+)[-\\s](?:unit|apartment|home|residence)/i);
  if (unitMatch) totalUnits = parseInt(unitMatch[1]) || 0;

  let yearBuilt = 0;
  const ybMatch = bodyText.match(/(?:built|constructed|established)\\s*(?:in\\s*)?(\\d{4})/i);
  if (ybMatch) yearBuilt = parseInt(ybMatch[1]) || 0;

  // ─── Try to extract from embedded JSON (Entrata React apps) ───
  const floorplans = [];
  let foundJson = false;

  // Look for __NEXT_DATA__ or similar embedded data
  const scripts = document.querySelectorAll("script");
  for (const s of scripts) {
    const content = s.textContent || "";

    // Pattern: JSON with floorplan/unit data
    if (content.includes("floorPlan") || content.includes("floor_plan") || content.includes("unitType")) {
      try {
        // Try to find a JSON object with floor plan arrays
        const jsonMatches = content.match(/\\[\\{[^\\[]*"(?:bed|BR|bedroom|unitType|floorPlan)"[^\\]]*\\}\\]/g);
        if (jsonMatches) {
          for (const match of jsonMatches) {
            try {
              const arr = JSON.parse(match);
              for (const item of arr) {
                const beds = item.beds ?? item.bedrooms ?? item.BR ?? item.bed_count ?? -1;
                const baths = item.baths ?? item.bathrooms ?? item.BA ?? item.bath_count ?? 1;
                const sqft = item.sqft ?? item.squareFeet ?? item.square_feet ?? item.area ?? 0;
                const rent = item.rent ?? item.price ?? item.asking_rent ?? item.minRent ?? item.min_rent ?? 0;
                const rentMax = item.rentMax ?? item.maxRent ?? item.max_rent ?? item.maxPrice ?? rent;
                const name = item.name ?? item.floorPlanName ?? item.unitType ?? item.title ?? "";
                const available = item.availableCount ?? item.available ?? item.units_available ?? 0;

                if (beds >= 0 && (rent > 0 || sqft > 0)) {
                  floorplans.push({
                    name: (name || (beds === 0 ? "Studio" : beds + " Bed")).substring(0, 100),
                    beds: parseInt(beds) || 0,
                    baths: parseFloat(baths) || 1,
                    sqft_min: parseInt(sqft) || 0,
                    sqft_max: parseInt(sqft) || 0,
                    rent_min: parseInt(rent) || 0,
                    rent_max: parseInt(rentMax) || parseInt(rent) || 0,
                    available_count: parseInt(available) || 0,
                    deposit: 0,
                  });
                  foundJson = true;
                }
              }
            } catch {}
          }
        }
      } catch {}
    }
  }

  // ─── DOM extraction strategies ────────────────────────────────
  if (!foundJson || floorplans.length === 0) {
    // Strategy 1: Entrata floorplan cards
    const cardSelectors = [
      ".floor-plan-card", ".floorplan-card", "[class*='floorplan']", "[class*='FloorPlan']",
      "[class*='floor-plan']", ".unit-type-card", "[class*='unitType']",
      ".fp-card", "[data-floorplan]", "[data-unit-type]",
      ".pricing-card", "[class*='pricing']", "[class*='Pricing']",
    ];

    let fpElements = [];
    for (const sel of cardSelectors) {
      fpElements = Array.from(document.querySelectorAll(sel));
      if (fpElements.length > 0) break;
    }

    // Strategy 2: Table rows
    if (fpElements.length === 0) {
      const rowSelectors = [
        "tr[class*='unit']", "tr[class*='floor']", "tr[class*='pricing']",
        ".unit-row", ".unit-type-row",
      ];
      for (const sel of rowSelectors) {
        fpElements = Array.from(document.querySelectorAll(sel));
        if (fpElements.length > 0) break;
      }
    }

    // Strategy 3: Generic containers with rent data
    if (fpElements.length === 0) {
      const allContainers = document.querySelectorAll("div, article, section, li");
      for (const c of allContainers) {
        const t = c.textContent || "";
        if (/(studio|\\d\\s*bed)/i.test(t) && /\\$\\d{3,}/.test(t) && t.length < 1200) {
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
      const rentSingle = text.match(/(?:from\\s*)?\\$(\\d[\\d,]{2,5})/i);
      if (rentRange) {
        rentMin = parseInt(rentRange[1].replace(",", "")) || 0;
        rentMax = parseInt(rentRange[2].replace(",", "")) || rentMin;
      } else if (rentSingle) {
        rentMin = parseInt(rentSingle[1].replace(",", "")) || 0;
        rentMax = rentMin;
      }

      let available = 0;
      const availMatch = text.match(/(\\d+)\\s*(?:available|avail|unit)/i);
      if (availMatch) available = parseInt(availMatch[1]) || 0;

      let deposit = 0;
      const depMatch = text.match(/deposit[:\\s]*\\$(\\d{2,5})/i);
      if (depMatch) deposit = parseInt(depMatch[1]) || 0;

      const nameEl = fp.querySelector("h2, h3, h4, h5, [class*='name'], [class*='Name'], .title, strong");
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

  // ─── Amenities ────────────────────────────────────────────────
  const amenities = getAll(
    ".amenity, [class*='amenity'] li, [class*='Amenity'] li, .feature-list li, [class*='feature'] li"
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

  // ─── Concessions / Specials ───────────────────────────────────
  let concession = "";
  const specEl = document.querySelector(
    ".special, .concession, [class*='special'], [class*='promo'], [class*='Special'], " +
    "[class*='Promo'], [class*='offer'], [class*='Offer'], [class*='incentive']"
  );
  if (specEl) concession = specEl.textContent.trim();
  if (!concession) {
    const concMatch = bodyText.match(/(\\d+\\s*(?:weeks?|months?)\\s*free[^.]*)/i)
      || bodyText.match(/(move[- ]in\\s*special[^.]*)/i)
      || bodyText.match(/(\\$\\d+\\s*off[^.]*)/i)
      || bodyText.match(/(look\\s*&?\\s*lease\\s*special[^.]*)/i);
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
  return browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });
}

// ─── Entrata Scraper ────────────────────────────────────────────────

/**
 * Scrape an Entrata property website for rental data.
 * Accepts an optional shared browser to avoid launching one per property.
 */
export async function scrapeEntrata(
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

    // Entrata sites often have a "Floor Plans" or "Apartments" nav link
    try {
      const navSelectors = [
        'a[href*="floorplan"]', 'a[href*="floor-plan"]', 'a[href*="pricing"]',
        'a[href*="apartments"]', 'a[href*="units"]',
        'a:has-text("Floor Plans")', 'a:has-text("Apartments")',
        'a:has-text("Pricing")', 'a:has-text("Floorplans")',
        'button:has-text("Floor Plans")', 'button:has-text("View Floor Plans")',
      ];
      for (const sel of navSelectors) {
        const link = page.locator(sel).first();
        if ((await link.count()) > 0) {
          await link.click({ timeout: 3000 }).catch(() => {});
          await page.waitForTimeout(2000);
          break;
        }
      }
    } catch { /* nav link not found */ }

    // Extract data
    const data = (await page.evaluate(ENTRATA_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;

    // If no floorplans, try /floorplans path
    if (data && data.floorplans.length === 0) {
      const fpUrl = url.replace(/\/$/, "") + "/floorplans";
      try {
        await page.goto(fpUrl, { waitUntil: "domcontentloaded", timeout: 15_000 });
        await page.waitForTimeout(3000);
        const fpData = (await page.evaluate(ENTRATA_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;
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
 * Convert scraped Entrata data to RentSnapshot records ready for DB insertion.
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
        source: "entrata",
        rent_max: fp.rent_max,
        sqft_max: fp.sqft_max,
        floor_plan_name: fp.name,
        property_name: data.property_name,
      },
    });
  }

  return snapshots;
}
