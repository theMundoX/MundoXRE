/**
 * Direct Property Website Scraper
 *
 * Scrapes official apartment/property websites that are not hosted on a known
 * leasing platform. This is intentionally generic: it avoids blocked
 * aggregators, visits only the supplied property site and obvious floorplan
 * pages, and extracts advertised floorplan/rent text already rendered publicly.
 */

import { chromium, type Browser, type BrowserContext } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../utils/rate-limiter.js";
import { isDomainBlocked } from "../utils/allowlist.js";
import type { RentSnapshot } from "../db/queries.js";
import type { ScrapedFloorplan, ScrapedPropertyData } from "./rentcafe.js";

const DIRECT_EXTRACT_SCRIPT = `
(() => {
  const bodyText = document.body?.innerText || "";
  const textOf = (el) => (el?.textContent || "").replace(/\\s+/g, " ").trim();
  const titleText = document.title || "";
  const h1 = textOf(document.querySelector("h1, [class*='property-name'], [class*='community-name'], [class*='PropertyName']"));
  const propertyName = h1 || titleText.split("|")[0]?.split(" - ")[0]?.trim() || "";
  const address = textOf(document.querySelector("address, [class*='address'], [class*='Address'], [itemprop='address']"));
  const phone = textOf(document.querySelector("a[href^='tel:'], [class*='phone'], [itemprop='telephone']"));

  let totalUnits = 0;
  const totalMatch = bodyText.match(/(\\d{2,4})\\s*(?:unit|apartment|residence|home)s?\\b/i);
  if (totalMatch) totalUnits = parseInt(totalMatch[1], 10) || 0;

  let yearBuilt = 0;
  const yearMatch = bodyText.match(/(?:built|constructed|opened|established)\\s*(?:in\\s*)?(19\\d{2}|20\\d{2})/i);
  if (yearMatch) yearBuilt = parseInt(yearMatch[1], 10) || 0;

  const parseMoney = (value) => {
    const parsed = parseInt(String(value || "").replace(/[^0-9]/g, ""), 10);
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const parseNumber = (value) => {
    const parsed = parseInt(String(value || "").replace(/[^0-9]/g, ""), 10);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  const floorplans = [];
  const seen = new Set();
  const selectors = [
    "[class*='floorplan']", "[class*='floor-plan']", "[class*='FloorPlan']",
    "[class*='apartment']", "[class*='Apartment']", "[class*='unit-card']",
    "[class*='unitCard']", "[class*='pricing']", "[class*='Pricing']",
    "[data-floorplan]", "[data-floor-plan]", "article", "tr"
  ];

  const candidates = [];
  for (const sel of selectors) {
    for (const el of Array.from(document.querySelectorAll(sel))) {
      const text = textOf(el);
      if (text.length < 15 || text.length > 1400) continue;
      if (/sort by|clear filters|minimum|maximum|show available only/i.test(text)) continue;
      if (!/\\$\\s*[0-9][0-9,]{2,5}/.test(text)) continue;
      if (!/(studio|\\b\\d+(?:\\.\\d+)?\\s*(?:bed|br|bd)|\\b\\d+x\\d\\b)/i.test(text)) continue;
      candidates.push({ el, text });
    }
  }

  if (candidates.length === 0) {
    const chunks = bodyText.split(/\\n{2,}/).map(t => t.replace(/\\s+/g, " ").trim());
    for (const text of chunks) {
      if (text.length < 15 || text.length > 700) continue;
      if (/\\$\\s*\\d{3,5}/.test(text) && /(studio|\\b\\d+(?:\\.\\d+)?\\s*(?:bed|br|bd)|\\b\\d+x\\d\\b)/i.test(text)) {
        candidates.push({ el: null, text });
      }
    }
  }

  for (const { el, text } of candidates) {
    const key = text.toLowerCase().replace(/[^a-z0-9$]/g, "").slice(0, 160);
    if (seen.has(key)) continue;
    seen.add(key);

    let beds = -1;
    if (/studio/i.test(text)) beds = 0;
    const bedMatch = text.match(/\\b(\\d+(?:\\.\\d+)?)\\s*(?:bed|br|bd)\\b/i) || text.match(/\\b(\\d+)x\\d+\\b/i);
    if (bedMatch) beds = Math.round(parseFloat(bedMatch[1]));

    let baths = 1;
    const bathMatch = text.match(/\\b(\\d+(?:\\.\\d+)?)\\s*(?:bath|ba)\\b/i) || text.match(/\\b\\d+x(\\d+(?:\\.\\d+)?)\\b/i);
    if (bathMatch) baths = parseFloat(bathMatch[1]) || 1;

    const rentRange = text.match(/\\$\\s*([0-9][0-9,]{2,5})\\s*(?:-|to|–|—)\\s*\\$?\\s*([0-9][0-9,]{2,5})/i);
    const rentSingle = text.match(/\\$\\s*([0-9][0-9,]{2,5})/);
    const rentMin = rentRange ? parseMoney(rentRange[1]) : rentSingle ? parseMoney(rentSingle[1]) : 0;
    const rentMax = rentRange ? parseMoney(rentRange[2]) : rentMin;

    const sqftRange = text.match(/\\b([0-9][0-9,]{2,4})\\s*(?:-|to|–|—)\\s*([0-9][0-9,]{2,4})\\s*(?:sq\\.?\\s*ft|sf|sqft)/i);
    const sqftSingle = text.match(/\\b([0-9][0-9,]{2,4})\\s*(?:sq\\.?\\s*ft|sf|sqft)/i);
    const sqftMin = sqftRange ? parseNumber(sqftRange[1]) : sqftSingle ? parseNumber(sqftSingle[1]) : 0;
    const sqftMax = sqftRange ? parseNumber(sqftRange[2]) : sqftMin;

    const availableMatch = text.match(/(\\d+)\\s*(?:available|units? available|apartments? available)/i);
    const available = availableMatch ? parseInt(availableMatch[1], 10) || 0 : 0;

    let name = "";
    if (el) {
      name = textOf(el.querySelector("h2, h3, h4, [class*='name'], [class*='Name'], strong"));
    }
    if (!name) name = beds === 0 ? "Studio" : beds > 0 ? beds + " Bed / " + baths + " Bath" : "Floorplan";

    if (beds >= 0 && rentMin >= 300 && rentMin <= 20000) {
      floorplans.push({
        name: name.slice(0, 100),
        beds,
        baths,
        sqft_min: sqftMin,
        sqft_max: sqftMax,
        rent_min: rentMin,
        rent_max: rentMax || rentMin,
        available_count: available,
        deposit: 0,
      });
    }
  }

  if (floorplans.length === 0) {
    const lines = bodyText.split(/\\n+/).map(line => line.replace(/\\s+/g, " ").trim()).filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      const current = lines[i] || "";
      const next = lines[i + 1] || "";
      const currentHasBed = /(studio|\\b\\d+(?:\\.\\d+)?\\s*(?:bed|bd|br)\\b)/i.test(current);
      const nextHasBed = /(studio|\\b\\d+(?:\\.\\d+)?\\s*(?:bed|bd|br)\\b)/i.test(next);
      if (!currentHasBed && !nextHasBed) continue;
      if (/^(bedrooms?|bathrooms?|floor plans?|min(?:imum)?|max(?:imum)?|price|any|show available only|clear filters|see results)$/i.test(current)) continue;

      const windowLines = lines.slice(i, i + 6);
      const text = windowLines.join(" ");
      if (!/(studio|\\b\\d+(?:\\.\\d+)?\\s*(?:bed|bd|br)\\b)/i.test(text)) continue;
      if (!/\\$\\s*[0-9][0-9,]{2,5}/.test(text)) continue;

      let beds = -1;
      if (/studio/i.test(text)) beds = 0;
      const bedMatch = text.match(/\\b(\\d+(?:\\.\\d+)?)\\s*(?:bed|bd|br)\\b/i);
      if (bedMatch) beds = Math.round(parseFloat(bedMatch[1]));

      let baths = 1;
      const bathMatch = text.match(/\\b(\\d+(?:\\.\\d+)?)\\s*(?:bath|bth|ba)\\b/i);
      if (bathMatch) baths = parseFloat(bathMatch[1]) || 1;

      const sqftMatch = text.match(/\\b([0-9][0-9,]{2,4})\\s*(?:sq\\.?\\s*ft|sf|sqft)/i);
      const sqft = sqftMatch ? parseNumber(sqftMatch[1]) : 0;

      const rentRange = text.match(/\\$\\s*([0-9][0-9,]{2,5})\\s*(?:-|to|–|—)\\s*\\$?\\s*([0-9][0-9,]{2,5})/i);
      const rentSingle = text.match(/\\$\\s*([0-9][0-9,]{2,5})/);
      const rentMin = rentRange ? parseMoney(rentRange[1]) : rentSingle ? parseMoney(rentSingle[1]) : 0;
      const rentMax = rentRange ? parseMoney(rentRange[2]) : rentMin;
      if (beds < 0 || rentMin < 300 || rentMin > 20000) continue;

      const prior = lines[Math.max(0, i - 1)] || "";
      const lineName = currentHasBed ? prior : current;
      const name = (/\\$|bed|bath|bth|sq\\.?\\s*ft|starting|available|minimum|maximum|filter|price|any|apply/i.test(lineName) ? "" : lineName) || (beds === 0 ? "Studio" : beds + " Bed / " + baths + " Bath");
      const key = name + "|" + beds + "|" + baths + "|" + sqft + "|" + rentMin;
      if (seen.has(key)) continue;
      seen.add(key);
      floorplans.push({
        name: name.slice(0, 100),
        beds,
        baths,
        sqft_min: sqft,
        sqft_max: sqft,
        rent_min: rentMin,
        rent_max: rentMax || rentMin,
        available_count: /view\\s+(\\d+)\\s+apartments?/i.test(text) ? parseInt(text.match(/view\\s+(\\d+)\\s+apartments?/i)[1], 10) || 0 : 0,
        deposit: 0,
      });
    }
  }

  const amenities = Array.from(new Set((bodyText.match(/\\b(pool|fitness center|garage|parking|laundry|pet friendly|dog park|clubhouse|business center|elevator|balcony|patio|storage)\\b/gi) || [])
    .map(a => a.toLowerCase()))).slice(0, 30);
  const concession = (bodyText.match(/(?:special|concession|limited time|one month free|weeks? free|waived)[^\\n.]{0,180}/i) || [""])[0];
  const dedupedFloorplans = Array.from(
    new Map(floorplans.map(fp => [fp.name + "|" + fp.beds + "|" + fp.baths + "|" + fp.sqft_min + "|" + fp.rent_min, fp])).values()
  );

  return {
    property_name: propertyName,
    total_units: totalUnits,
    year_built: yearBuilt,
    address,
    city: "",
    state: "",
    zip: "",
    phone,
    website_url: location.href,
    floorplans: dedupedFloorplans,
    amenities,
    fees: { application_fee: 0, admin_fee: 0, pet_deposit: 0, pet_monthly: 0, parking: 0 },
    concession_text: concession,
    scrape_date: new Date().toISOString().slice(0, 10),
  };
})()
`;

let ownedBrowser: Browser | null = null;

async function getBrowser(sharedBrowser?: Browser): Promise<Browser> {
  if (sharedBrowser) return sharedBrowser;
  if (ownedBrowser?.isConnected()) return ownedBrowser;

  const launchOpts: Parameters<typeof chromium.launch>[0] = { headless: true };
  ownedBrowser = await chromium.launch(launchOpts);
  return ownedBrowser;
}

function sameOrigin(baseUrl: string, candidate: string): boolean {
  try {
    return new URL(baseUrl).origin === new URL(candidate).origin;
  } catch {
    return false;
  }
}

function likelyFloorplanLink(text: string, href: string): boolean {
  const value = `${text} ${href}`.toLowerCase();
  return /(floor|availability|available|apartments|layouts|pricing|rates|units|listing|listings|appfolio|sightmap)/.test(value);
}

async function collectCandidateUrls(pageUrl: string, context: BrowserContext): Promise<string[]> {
  const page = await context.newPage();
  await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 12_000 });
  await page.waitForTimeout(4000);
  const links = await page.evaluate(() => Array.from(document.querySelectorAll("a[href]")).map((a) => ({
    href: (a as HTMLAnchorElement).href,
    text: (a.textContent || "").trim(),
  })));
  const iframeUrls = await page.evaluate(() => Array.from(document.querySelectorAll("iframe[src]"))
    .map((iframe) => (iframe as HTMLIFrameElement).src)
    .filter(Boolean));
  await page.close().catch(() => {});

  const urls = new Set<string>([pageUrl]);
  const priorityUrls = new Set<string>();
  const path = new URL(pageUrl).pathname.toLowerCase();
  if (!/(floor|avail|apartments)/i.test(path)) {
    for (const suffix of ["/floor-plans", "/floorplans", "/apartments", "/availability"]) {
      urls.add(pageUrl.replace(/\/$/, "") + suffix);
    }
  }
  for (const link of links) {
    if (!isDomainBlocked(link.href) && (sameOrigin(pageUrl, link.href) || likelyFloorplanLink(link.text, link.href)) && likelyFloorplanLink(link.text, link.href)) {
      urls.add(link.href.split("#")[0]);
    }
  }
  for (const iframeUrl of iframeUrls) {
    if (!isDomainBlocked(iframeUrl) && likelyFloorplanLink("", iframeUrl)) {
      priorityUrls.add(iframeUrl.split("#")[0]);
    }
  }
  return [...priorityUrls, ...urls].slice(0, 12);
}

export async function scrapeDirectPropertySite(
  url: string,
  sharedBrowser?: Browser,
): Promise<ScrapedPropertyData | null> {
  if (isDomainBlocked(url)) {
    console.log("  Blocked: domain is on the blocklist.");
    return null;
  }

  await waitForSlot(url);
  const browser = await getBrowser(sharedBrowser);
  const context = await browser.newContext(getStealthConfig());
  await context.addInitScript(STEALTH_INIT_SCRIPT);

  try {
    const candidateUrls = await collectCandidateUrls(url, context);
    let best: ScrapedPropertyData | null = null;

    for (const candidate of candidateUrls) {
      if (isDomainBlocked(candidate)) continue;
      const page = await context.newPage();
      try {
        await page.goto(candidate, { waitUntil: "domcontentloaded", timeout: 12_000 });
        await page.waitForTimeout(2000);
        const data = (await page.evaluate(DIRECT_EXTRACT_SCRIPT)) as ScrapedPropertyData | null;
        if (data) {
          data.website_url = url;
          if (!best || data.floorplans.length > best.floorplans.length) best = data;
        }
      } finally {
        await page.close().catch(() => {});
      }
    }

    resetDomainRate(url);
    return best;
  } catch (err) {
    backoffDomain(url);
    console.error(`  Direct scrape error: ${err instanceof Error ? err.message : err}`);
    return null;
  } finally {
    await context.close().catch(() => {});
    if (!sharedBrowser && ownedBrowser) {
      await ownedBrowser.close().catch(() => {});
      ownedBrowser = null;
    }
  }
}

export function toRentSnapshots(
  propertyId: number,
  websiteId: number | undefined,
  data: ScrapedPropertyData,
): RentSnapshot[] {
  const today = data.scrape_date || new Date().toISOString().split("T")[0];
  return data.floorplans
    .filter((fp: ScrapedFloorplan) => fp.rent_min > 0)
    .map((fp: ScrapedFloorplan) => {
      const sqft = fp.sqft_min || fp.sqft_max || undefined;
      const askingPsf = sqft && sqft > 0 ? Math.round((fp.rent_min / sqft) * 100) / 100 : undefined;
      return {
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
          source: "direct",
          rent_max: fp.rent_max,
          sqft_max: fp.sqft_max,
          floor_plan_name: fp.name,
          property_name: data.property_name,
        },
      };
    });
}
