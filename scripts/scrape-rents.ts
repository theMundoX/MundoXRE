#!/usr/bin/env tsx
/**
 * Rent Scraper — scrapes actual rental rates from property websites.
 *
 * Targets RentCafe pages (property marketing sites hosted by Yardi)
 * and generic property leasing websites.
 *
 * Usage:
 *   npx tsx scripts/scrape-rents.ts
 *   npx tsx scripts/scrape-rents.ts --city=Lawton --state=OK
 *   npx tsx scripts/scrape-rents.ts --county_id=3 --limit=20
 *   npx tsx scripts/scrape-rents.ts --url=https://some-property.rentcafe.com
 *   npx tsx scripts/scrape-rents.ts --discover  (discover + scrape in one pass)
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { chromium, type Page, type Browser, type BrowserContext } from "playwright";
import { waitForSlot, backoffDomain, resetDomainRate } from "../src/utils/rate-limiter.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";
import { initProxies, getResidentialProxy } from "../src/utils/proxy.js";

initProxies();

// ─── Config ──────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY!;

const args = process.argv.slice(2);
const argCity = args.find((a) => a.startsWith("--city="))?.split("=")[1];
const argState = args.find((a) => a.startsWith("--state="))?.split("=")[1];
const countyId = parseInt(args.find((a) => a.startsWith("--county_id="))?.split("=")[1] || "3");
const limit = parseInt(args.find((a) => a.startsWith("--limit="))?.split("=")[1] || "50");
const singleUrl = args.find((a) => a.startsWith("--url="))?.split("=").slice(1).join("=");
const discoverMode = args.includes("--discover");

// ─── Types ───────────────────────────────────────────────────────────

interface ScrapedFloorplan {
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

interface ScrapedData {
  property_name: string;
  total_units: number;
  year_built: number;
  address: string;
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

interface WebsiteRow {
  id: number;
  property_id: number;
  url: string;
  platform: string | null;
  last_scraped_at: string | null;
}

// ─── Browser Pool ────────────────────────────────────────────────────

let sharedBrowser: Browser | null = null;

async function getBrowser(): Promise<Browser> {
  if (sharedBrowser && sharedBrowser.isConnected()) return sharedBrowser;

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
    } catch { /* launch without proxy */ }
  }

  sharedBrowser = await chromium.launch(launchOpts);
  return sharedBrowser;
}

async function closeBrowser() {
  if (sharedBrowser) {
    await sharedBrowser.close().catch(() => {});
    sharedBrowser = null;
  }
}

// ─── RentCafe Scraper ────────────────────────────────────────────────

/**
 * Extract rental data from a page using generic selectors that work
 * across RentCafe, Entrata, AppFolio, and generic property sites.
 */
const EXTRACT_SCRIPT = `
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
  let totalUnits = 0;
  const bodyText = document.body.innerText || "";
  const unitMatch = bodyText.match(/(\\d+)\\s*units?\\s*available/i);
  if (unitMatch) totalUnits = parseInt(unitMatch[1]) || 0;
  // Also try "X-unit" pattern
  if (!totalUnits) {
    const unitMatch2 = bodyText.match(/(\\d+)[-\\s](?:unit|apartment|home|residence)/i);
    if (unitMatch2) totalUnits = parseInt(unitMatch2[1]) || 0;
  }

  // Year built
  let yearBuilt = 0;
  const ybMatch = bodyText.match(/(?:built|constructed|established)\\s*(?:in\\s*)?(\\d{4})/i);
  if (ybMatch) yearBuilt = parseInt(ybMatch[1]) || 0;

  // Address from page
  const addressEl = document.querySelector(".property-address, [class*='address'], [class*='Address'], address");
  const address = addressEl ? addressEl.textContent.trim().replace(/\\s+/g, " ") : "";

  // ─── Floor plans ─────────────────────────────────────────────
  const floorplans = [];

  // ──── STRATEGY 1: RentCafe new layout — .fp-info elements ────
  const fpInfoEls = document.querySelectorAll(".fp-info");
  if (fpInfoEls.length > 0) {
    for (const fpInfo of fpInfoEls) {
      const text = fpInfo.textContent?.trim().replace(/\\s+/g, " ") || "";

      // Name: from .fp-name or first bold element
      const nameEl = fpInfo.querySelector(".fp-name, b.fp-name, h2, h3, h4, h5, strong, b");
      let name = "";
      if (nameEl) {
        name = nameEl.textContent.trim();
      }
      // Fallback: first word(s) before bed/bath info
      if (!name) {
        const nameMatch = text.match(/^([A-Z0-9][A-Za-z0-9.\\- ]{0,30}?)\\s*\\d\\s*Bed/);
        if (nameMatch) name = nameMatch[1].trim();
      }

      // Bed/Bath/Sqft from the info list
      const infoList = fpInfo.querySelector(".fp-info-list");
      const infoText = infoList ? infoList.textContent.trim() : text;

      let beds = -1;
      const bedMatch = infoText.match(/(\\d)\\s*Beds?/i) || infoText.match(/studio/i);
      if (bedMatch) beds = bedMatch[0].toLowerCase().includes("studio") ? 0 : parseInt(bedMatch[1]) || 0;

      let baths = 1;
      const bathMatch = infoText.match(/(\\d+(?:\\.\\d+)?)\\s*Baths?/i);
      if (bathMatch) baths = parseFloat(bathMatch[1]) || 1;

      let sqftMin = 0, sqftMax = 0;
      const sqftRangeMatch = infoText.match(/(\\d{3,5})\\s*[-–]\\s*(\\d{3,5})\\s*Sqft/i);
      const sqftSingle = infoText.match(/(\\d{3,5})\\s*Sqft/i);
      if (sqftRangeMatch) {
        sqftMin = parseInt(sqftRangeMatch[1]) || 0;
        sqftMax = parseInt(sqftRangeMatch[2]) || sqftMin;
      } else if (sqftSingle) {
        sqftMin = parseInt(sqftSingle[1]) || 0;
        sqftMax = sqftMin;
      }

      // Rent — prefer .fp-price-info element, fallback to text parsing
      let rentMin = 0, rentMax = 0;
      const priceEl = fpInfo.querySelector(".fp-price-info, [class*='price'], [class*='rent']");
      const priceText = priceEl ? priceEl.textContent.trim() : text;
      const rentRange = priceText.match(/\\$(\\d[\\d,]{2,5})\\s*[-–]\\s*\\$(\\d[\\d,]{2,5})/);
      const rentSingle = priceText.match(/\\$(\\d[\\d,]{2,5})/);
      if (rentRange) {
        rentMin = parseInt(rentRange[1].replace(",", "")) || 0;
        rentMax = parseInt(rentRange[2].replace(",", "")) || rentMin;
      } else if (rentSingle) {
        rentMin = parseInt(rentSingle[1].replace(",", "")) || 0;
        rentMax = rentMin;
      }

      // Available count: from the parent fp-item's unit table
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
          beds,
          baths,
          sqft_min: sqftMin,
          sqft_max: sqftMax,
          rent_min: rentMin,
          rent_max: rentMax,
          available_count: available,
          deposit: 0,
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

    // Strategy 3: Look for any container with bed/bath/rent info
    if (fpElements.length === 0) {
      const allContainers = document.querySelectorAll("div, article, section, tr");
      for (const c of allContainers) {
        const t = c.textContent || "";
        if (/(studio|\\d\\s*bed)/i.test(t) && /\\$\\d{3,}/.test(t) && t.length < 1000) {
          fpElements.push(c);
        }
      }
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
      const sqftRangeMatch = text.match(/(\\d{3,5})\\s*(?:-|to|–)\\s*(\\d{3,5})\\s*(?:sq|sf|sqft|sq\\.?\\s*ft)/i);
      const sqftSingle = text.match(/(\\d{3,5})\\s*(?:sq|sf|sqft|sq\\.?\\s*ft)/i);
      if (sqftRangeMatch) {
        sqftMin = parseInt(sqftRangeMatch[1]) || 0;
        sqftMax = parseInt(sqftRangeMatch[2]) || sqftMin;
      } else if (sqftSingle) {
        sqftMin = parseInt(sqftSingle[1]) || 0;
        sqftMax = sqftMin;
      }

      let rentMin = 0, rentMax = 0;
      const rentRange = text.match(/\\$(\\d[\\d,]{2,5})\\s*(?:-|to|–)\\s*\\$(\\d[\\d,]{2,5})/);
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
          beds,
          baths,
          sqft_min: sqftMin,
          sqft_max: sqftMax,
          rent_min: rentMin,
          rent_max: rentMax,
          available_count: available,
          deposit,
        });
      }
    }
  }

  // ─── Deduplicate floorplans ──────────────────────────────────
  const seen = new Set();
  const uniqueFps = floorplans.filter(fp => {
    const key = fp.name + "|" + fp.beds + "|" + fp.baths + "|" + fp.rent_min;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // ─── Amenities ───────────────────────────────────────────────
  const amenities = getAll(
    ".amenity, [class*='amenity'] li, .feature-list li, [class*='feature'] li, " +
    "[class*='Amenity'] li, .amenity-list li"
  ).filter(Boolean).slice(0, 50);

  // ─── Fees ────────────────────────────────────────────────────
  let appFee = 0, adminFee = 0, petDeposit = 0, petMonthly = 0, parking = 0;
  const feeSection = bodyText;
  const appMatch = feeSection.match(/application\\s*fee[s]?[:\\s]*\\$(\\d+)/i);
  if (appMatch) appFee = parseInt(appMatch[1]) || 0;
  const admMatch = feeSection.match(/admin(?:istration)?\\s*fee[:\\s]*\\$(\\d+)/i);
  if (admMatch) adminFee = parseInt(admMatch[1]) || 0;
  // Security deposit
  const secDepMatch = feeSection.match(/(?:security\\s*)?deposit[:\\s]*\\$(\\d+)/i);
  if (secDepMatch) {
    // Check if this is pet deposit or security deposit
    const depositVal = parseInt(secDepMatch[1]) || 0;
    if (feeSection.match(/pet\\s*deposit[:\\s]*\\$/i)) {
      const petDepMatch2 = feeSection.match(/pet\\s*deposit[:\\s]*\\$(\\d+)/i);
      if (petDepMatch2) petDeposit = parseInt(petDepMatch2[1]) || 0;
    }
  }
  const petMoMatch = feeSection.match(/pet\\s*(?:rent|monthly)[:\\s]*\\$(\\d+)/i);
  if (petMoMatch) petMonthly = parseInt(petMoMatch[1]) || 0;
  const parkMatch = feeSection.match(/parking[:\\s]*\\$(\\d+)/i);
  if (parkMatch) parking = parseInt(parkMatch[1]) || 0;

  // ─── Concessions ─────────────────────────────────────────────
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
    floorplans: uniqueFps,
    amenities,
    fees: {
      application_fee: appFee,
      admin_fee: adminFee,
      pet_deposit: petDeposit,
      pet_monthly: petMonthly,
      parking,
    },
    concession_text: concession.substring(0, 500),
    scrape_date: new Date().toISOString().split("T")[0],
  };
})()
`;

/**
 * Scrape a single property URL (RentCafe or generic).
 */
async function scrapePropertyUrl(url: string): Promise<ScrapedData | null> {
  await waitForSlot(url);

  const browser = await getBrowser();
  const stealth = getStealthConfig();

  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });

  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  try {
    // Navigate to the page
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(3000);

    // For RentCafe, try to click the "Floor Plans" or "Pricing" tab
    const isRentCafe = url.includes("rentcafe.com");
    if (isRentCafe) {
      try {
        // Try clicking floorplans/pricing tab
        const tabSelectors = [
          'a[href*="floorplan"]',
          'a[href*="floor-plan"]',
          'a[href*="pricing"]',
          '[class*="floorplan-tab"]',
          '[class*="pricing-tab"]',
          'button:has-text("Floor Plans")',
          'a:has-text("Floor Plans")',
          'a:has-text("Pricing")',
          'button:has-text("Pricing")',
        ];

        for (const sel of tabSelectors) {
          const tab = page.locator(sel).first();
          if ((await tab.count()) > 0) {
            await tab.click({ timeout: 3000 }).catch(() => {});
            await page.waitForTimeout(2000);
            break;
          }
        }
      } catch {
        // Tab not found — pricing might be on the main page
      }
    }

    // Extract data
    const data = (await page.evaluate(EXTRACT_SCRIPT)) as ScrapedData | null;

    if (data && data.floorplans.length === 0 && isRentCafe) {
      // Try the /floorplans subpage
      const fpUrl = url.replace(/\/$/, "") + "/floorplans";
      try {
        await page.goto(fpUrl, { waitUntil: "domcontentloaded", timeout: 15_000 });
        await page.waitForTimeout(3000);
        const fpData = (await page.evaluate(EXTRACT_SCRIPT)) as ScrapedData | null;
        if (fpData && fpData.floorplans.length > 0) {
          resetDomainRate(url);
          return fpData;
        }
      } catch {
        // Floorplans page doesn't exist
      }
    }

    if (data) {
      resetDomainRate(url);
    }
    return data;
  } catch (err) {
    backoffDomain(url);
    console.error(`  Scrape error for ${url}:`, err instanceof Error ? err.message : "Unknown");
    return null;
  } finally {
    await context.close();
  }
}

// ─── Database Operations ─────────────────────────────────────────────

async function getWebsitesToScrape(db: SupabaseClient): Promise<WebsiteRow[]> {
  const staleDate = new Date();
  staleDate.setDate(staleDate.getDate() - 7); // Rescrape after 7 days

  let query = db
    .from("property_websites")
    .select("id, property_id, url, platform, last_scraped_at")
    .eq("active", true)
    .or(`last_scraped_at.is.null,last_scraped_at.lt.${staleDate.toISOString()}`)
    .order("last_scraped_at", { ascending: true, nullsFirst: true })
    .limit(limit);

  // Filter by platform preference: RentCafe first
  const { data, error } = await query;
  if (error) throw new Error(`Failed to query websites: ${error.message}`);
  return (data ?? []) as WebsiteRow[];
}

async function getWebsitesForCounty(db: SupabaseClient, countyId: number): Promise<WebsiteRow[]> {
  const staleDate = new Date();
  staleDate.setDate(staleDate.getDate() - 7);

  // Join with properties to filter by county
  const { data, error } = await db
    .from("property_websites")
    .select("id, property_id, url, platform, last_scraped_at, properties!inner(county_id)")
    .eq("active", true)
    .eq("properties.county_id", countyId)
    .or(`last_scraped_at.is.null,last_scraped_at.lt.${staleDate.toISOString()}`)
    .order("last_scraped_at", { ascending: true, nullsFirst: true })
    .limit(limit);

  if (error) {
    // Fallback: just get all active websites
    console.log(`  County filter error (${error.message}), falling back to all websites`);
    return getWebsitesToScrape(db);
  }

  return (data ?? []).map((d: any) => ({
    id: d.id,
    property_id: d.property_id,
    url: d.url,
    platform: d.platform,
    last_scraped_at: d.last_scraped_at,
  }));
}

async function saveScrapedData(
  db: SupabaseClient,
  propertyId: number,
  websiteId: number,
  data: ScrapedData,
) {
  const today = data.scrape_date;

  // Update property info if we learned something
  const propUpdate: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
  };
  if (data.total_units > 0) propUpdate.total_units = data.total_units;
  if (data.year_built > 1800) propUpdate.year_built = data.year_built;
  if (Object.keys(propUpdate).length > 1) {
    await db.from("properties").update(propUpdate).eq("id", propertyId);
  }

  // Save floorplans + rent snapshots
  for (const fp of data.floorplans) {
    // Upsert floorplan
    const { data: fpRow, error: fpErr } = await db
      .from("floorplans")
      .upsert(
        {
          property_id: propertyId,
          name: fp.name,
          beds: fp.beds,
          baths: fp.baths,
          sqft: fp.sqft_min || fp.sqft_max || null,
          estimated_count: fp.available_count || null,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "property_id,name" },
      )
      .select("id")
      .single();

    const floorplanId = fpRow?.id;

    // Insert rent snapshot
    if (fp.rent_min > 0) {
      const sqft = fp.sqft_min || fp.sqft_max || null;
      const askingPsf = sqft && sqft > 0 ? Math.round((fp.rent_min / sqft) * 100) : null;

      await db.from("rent_snapshots").insert({
        property_id: propertyId,
        floorplan_id: floorplanId || null,
        website_id: websiteId,
        observed_at: today,
        beds: fp.beds,
        baths: fp.baths,
        sqft,
        asking_rent: fp.rent_min,
        effective_rent: fp.rent_min, // Same unless concession
        asking_psf: askingPsf,
        deposit: fp.deposit || null,
        available_count: fp.available_count || null,
        concession_text: data.concession_text || null,
        raw: {
          source: "scraped",
          rent_max: fp.rent_max,
          sqft_max: fp.sqft_max,
          floor_plan_name: fp.name,
          property_name: data.property_name,
        },
      });
    }
  }

  // Save amenities
  if (data.amenities.length > 0) {
    const amenityRecords = data.amenities.map((a) => ({
      property_id: propertyId,
      scope: "building" as const,
      amenity: a,
      present: true,
      observed_at: today,
    }));

    await db
      .from("amenities")
      .upsert(amenityRecords, { onConflict: "property_id,scope,amenity" });
  }

  // Save fees
  const fees = data.fees;
  if (fees.application_fee > 0 || fees.pet_deposit > 0 || fees.admin_fee > 0) {
    await db.from("fee_schedules").insert({
      property_id: propertyId,
      observed_at: today,
      app_fee: fees.application_fee || null,
      admin_fee: fees.admin_fee || null,
      pet_deposit: fees.pet_deposit || null,
      pet_monthly: fees.pet_monthly || null,
      parking_surface: fees.parking || null,
    });
  }

  // Mark website as scraped
  await db
    .from("property_websites")
    .update({
      last_scraped_at: new Date().toISOString(),
      scrape_success: true,
    })
    .eq("id", websiteId);
}

async function markScrapeFailure(db: SupabaseClient, websiteId: number) {
  await db
    .from("property_websites")
    .update({
      last_scraped_at: new Date().toISOString(),
      scrape_success: false,
    })
    .eq("id", websiteId);
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE: Rent Scraper");
  console.log("=".repeat(50));

  const db = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

  // Single URL mode
  if (singleUrl) {
    console.log(`Scraping single URL: ${singleUrl}\n`);
    const data = await scrapePropertyUrl(singleUrl);

    if (data) {
      console.log(`\nProperty: ${data.property_name}`);
      console.log(`Address: ${data.address}`);
      console.log(`Units: ${data.total_units}, Year Built: ${data.year_built}`);
      console.log(`Floorplans found: ${data.floorplans.length}`);
      console.log(`Concessions: ${data.concession_text || "None"}`);
      console.log();

      for (const fp of data.floorplans) {
        console.log(
          `  ${fp.name}: ${fp.beds}bd/${fp.baths}ba, ${fp.sqft_min}-${fp.sqft_max} sqft, ` +
            `$${fp.rent_min}-$${fp.rent_max}/mo, ${fp.available_count} avail`,
        );
      }

      if (data.amenities.length > 0) {
        console.log(`\nAmenities: ${data.amenities.join(", ")}`);
      }

      if (data.fees.application_fee || data.fees.pet_deposit) {
        console.log(
          `\nFees: App=$${data.fees.application_fee}, Admin=$${data.fees.admin_fee}, ` +
            `Pet Dep=$${data.fees.pet_deposit}, Pet Mo=$${data.fees.pet_monthly}`,
        );
      }
    } else {
      console.log("No data extracted from URL.");
    }

    await closeBrowser();
    return;
  }

  // Batch mode: scrape all pending websites
  let websites: WebsiteRow[];

  if (countyId) {
    console.log(`County ID: ${countyId}`);
    websites = await getWebsitesForCounty(db, countyId);
  } else {
    websites = await getWebsitesToScrape(db);
  }

  console.log(`Found ${websites.length} websites to scrape\n`);

  if (websites.length === 0) {
    if (discoverMode) {
      console.log("No websites found. Run discover-websites.ts first.");
    } else {
      console.log("No websites pending scrape. All up to date or none discovered yet.");
      console.log("Run: npx tsx scripts/discover-websites.ts --city=Lawton --state=OK");
    }
    await closeBrowser();
    return;
  }

  let stats = {
    total: websites.length,
    scraped: 0,
    floorplans: 0,
    failed: 0,
    noData: 0,
  };

  for (let i = 0; i < websites.length; i++) {
    const web = websites[i];
    const progress = `[${i + 1}/${websites.length}]`;

    // Skip non-safe platforms
    if (web.platform === "apartments_com" || web.platform === "zillow") {
      console.log(`${progress} SKIP ${web.url} (blocked platform: ${web.platform})`);
      continue;
    }

    console.log(`${progress} Scraping: ${web.url}`);

    try {
      const data = await scrapePropertyUrl(web.url);

      if (data && data.floorplans.length > 0) {
        await saveScrapedData(db, web.property_id, web.id, data);
        stats.scraped++;
        stats.floorplans += data.floorplans.length;
        console.log(
          `  OK: ${data.property_name} — ${data.floorplans.length} plans, ` +
            `$${data.floorplans[0]?.rent_min || "?"}-$${data.floorplans[data.floorplans.length - 1]?.rent_max || "?"}/mo`,
        );
      } else if (data) {
        console.log(`  WARN: Page loaded but no floorplans found (${data.property_name})`);
        await markScrapeFailure(db, web.id);
        stats.noData++;
      } else {
        console.log("  FAIL: No data extracted");
        await markScrapeFailure(db, web.id);
        stats.failed++;
      }
    } catch (err) {
      console.error(`  ERROR: ${err instanceof Error ? err.message : "Unknown"}`);
      await markScrapeFailure(db, web.id);
      stats.failed++;
    }
  }

  await closeBrowser();

  console.log("\n" + "=".repeat(50));
  console.log("Scrape Summary");
  console.log("=".repeat(50));
  console.log(`  Total websites:     ${stats.total}`);
  console.log(`  Successfully scraped: ${stats.scraped}`);
  console.log(`  Floorplans found:   ${stats.floorplans}`);
  console.log(`  No data found:      ${stats.noData}`);
  console.log(`  Failed:             ${stats.failed}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  closeBrowser().finally(() => process.exit(1));
});
