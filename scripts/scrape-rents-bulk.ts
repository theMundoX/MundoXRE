#!/usr/bin/env tsx
/**
 * Rent Tracker — Bulk Rent Scraper. Scrapes actual rental rates from property websites at scale.
 *
 * Queries the properties table for entries with a website field set,
 * detects the platform (RentCafe, Entrata, AppFolio), and runs the
 * appropriate scraper. Results are stored as RentSnapshot records.
 *
 * Usage:
 *   npx tsx scripts/scrape-rents-bulk.ts --state TX --platform rentcafe
 *   npx tsx scripts/scrape-rents-bulk.ts --state TX                     (all platforms)
 *   npx tsx scripts/scrape-rents-bulk.ts --state OK --limit 10
 *   npx tsx scripts/scrape-rents-bulk.ts --city Dallas --state TX
 *   npx tsx scripts/scrape-rents-bulk.ts --county_id 3
 *   npx tsx scripts/scrape-rents-bulk.ts --dry-run --state TX           (preview without scraping)
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { chromium, type Browser } from "playwright";
import { initProxies } from "../src/utils/proxy.js";
import { getStealthConfig } from "../src/utils/stealth.js";
import {
  detectPlatform,
  getScraperForUrl,
  getSupportedPlatforms,
  type PlatformId,
} from "../src/scrapers/rent-registry.js";

initProxies();

// ─── Config ──────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment.");
  process.exit(1);
}

// ─── CLI Arguments ──────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=").slice(1).join("=");
}

const argState = getArg("state")?.toUpperCase();
const argCity = getArg("city");
const argCountyId = getArg("county_id") ? parseInt(getArg("county_id")!) : undefined;
const argPlatform = getArg("platform")?.toLowerCase() as PlatformId | undefined;
const argLimit = parseInt(getArg("limit") || "100");
const argStaledays = parseInt(getArg("stale_days") || "7");
const isDryRun = args.includes("--dry-run");

if (argPlatform && !getSupportedPlatforms().includes(argPlatform)) {
  console.error(`Unknown platform: ${argPlatform}`);
  console.error(`Supported: ${getSupportedPlatforms().join(", ")}`);
  process.exit(1);
}

// ─── Types ───────────────────────────────────────────────────────────

interface PropertyWebsite {
  id: number;
  property_id: number;
  url: string;
  platform: string | null;
  last_scraped_at: string | null;
}

interface PropertyRow {
  id: number;
  address: string;
  city: string;
  state_code: string;
  website: string | null;
}

function websitePriority(url: string): number {
  const path = (() => {
    try {
      return new URL(url).pathname.toLowerCase();
    } catch {
      return url.toLowerCase();
    }
  })();

  if (/\/floor-plan\//i.test(path)) return 10;
  if (/floor-plans|floorplans|apartments|availability/i.test(path)) return 100;
  return 50;
}

function chooseBestWebsitePerProperty(websites: PropertyWebsite[]): PropertyWebsite[] {
  const best = new Map<number, PropertyWebsite>();
  for (const website of websites) {
    const current = best.get(website.property_id);
    if (!current || websitePriority(website.url) > websitePriority(current.url)) {
      best.set(website.property_id, website);
    }
  }
  return [...best.values()];
}

// ─── Database Queries ───────────────────────────────────────────────

/**
 * Query properties that have a website field and match filters.
 * Returns from property_websites table if it exists, otherwise
 * falls back to properties.website field.
 */
async function getWebsitesToScrape(db: SupabaseClient): Promise<PropertyWebsite[]> {
  const staleDate = new Date();
  staleDate.setDate(staleDate.getDate() - argStaledays);

  // Try property_websites table first (structured)
  try {
    let query = db
      .from("property_websites")
      .select("id, property_id, url, platform, last_scraped_at, properties!inner(state_code, city)")
      .eq("active", true)
      .or(`last_scraped_at.is.null,last_scraped_at.lt.${staleDate.toISOString()}`)
      .order("last_scraped_at", { ascending: true, nullsFirst: true })
      .limit(argLimit);

    if (argState) {
      query = query.eq("properties.state_code", argState);
    }
    if (argCity) {
      query = query.ilike("properties.city", `%${argCity}%`);
    }
    if (argCountyId) {
      query = query.eq("properties.county_id", argCountyId);
    }

    const { data, error } = await query;

    if (!error && data && data.length > 0) {
      let websites = data.map((d: any) => ({
        id: d.id,
        property_id: d.property_id,
        url: d.url,
        platform: d.platform,
        last_scraped_at: d.last_scraped_at,
      }));

      // Filter by platform if specified
      if (argPlatform) {
        websites = websites.filter((w) => {
          const detected = w.platform || detectPlatform(w.url);
          return detected === argPlatform;
        });
      }

      return chooseBestWebsitePerProperty(websites);
    }
  } catch {
    // property_websites table might not exist, fall back
  }

  // Fallback: use properties.website field
  console.log("  (Using properties.website field as fallback)");

  let query = db
    .from("properties")
    .select("id, address, city, state_code, website")
    .not("website", "is", null)
    .neq("website", "")
    .order("id")
    .limit(argLimit);

  if (argState) {
    query = query.eq("state_code", argState);
  }
  if (argCity) {
    query = query.ilike("city", `%${argCity}%`);
  }
  if (argCountyId) {
    query = query.eq("county_id", argCountyId);
  }

  const { data, error } = await query;
  if (error) throw new Error(`Failed to query properties: ${error.message}`);

  let properties = (data ?? []) as PropertyRow[];

  // Filter by platform
  if (argPlatform) {
    properties = properties.filter((p) => {
      if (!p.website) return false;
      return detectPlatform(p.website) === argPlatform;
    });
  } else {
    // Filter to only supported platforms
    properties = properties.filter((p) => {
      if (!p.website) return false;
      return detectPlatform(p.website) !== "unknown";
    });
  }

  // Map to PropertyWebsite shape (without real website_id)
  return chooseBestWebsitePerProperty(properties.map((p) => ({
    id: 0, // No website table row
    property_id: p.id,
    url: p.website!,
    platform: detectPlatform(p.website!),
    last_scraped_at: null,
  })));
}

/**
 * Save scraped data to the database.
 */
async function saveScrapedData(
  db: SupabaseClient,
  propertyId: number,
  websiteId: number,
  scraperEntry: ReturnType<typeof getScraperForUrl>,
  data: NonNullable<Awaited<ReturnType<NonNullable<typeof scraperEntry>["scrape"]>>>,
) {
  const today = data.scrape_date;

  // Update property info if we learned something
  const propUpdate: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
  };
  if (data.total_units > 0) propUpdate.total_units = data.total_units;
  if (data.year_built > 1800) propUpdate.year_built = data.year_built;
  if (Object.keys(propUpdate).length > 1) {
    const { error } = await db.from("properties").update(propUpdate).eq("id", propertyId);
    if (error) throw new Error(`Failed to update property ${propertyId}: ${error.message}`);
  }

  // Save floorplans + rent snapshots
  for (const fp of data.floorplans) {
    const sqft = fp.sqft_min || fp.sqft_max || null;
    const wholeBaths = Math.floor(fp.baths || 0);
    const halfBaths = fp.baths - wholeBaths >= 0.5 ? 1 : 0;
    const floorplanName = /^(studio|floorplan|\d+ bed \/)/i.test(fp.name)
      ? `${fp.name} ${sqft ? `${sqft}sf ` : ""}${fp.rent_min ? `$${fp.rent_min}` : ""}`.trim()
      : fp.name;

    // Upsert floorplan
    const { data: fpRow, error: floorplanError } = await db
      .from("floorplans")
      .upsert(
        {
          property_id: propertyId,
          name: floorplanName,
          beds: fp.beds,
          baths: wholeBaths,
          half_baths: halfBaths,
          sqft,
          estimated_count: fp.available_count || null,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "property_id,name" },
      )
      .select("id")
      .single();
    if (floorplanError) {
      throw new Error(`Failed to upsert floorplan ${floorplanName}: ${floorplanError.message}`);
    }

    const floorplanId = fpRow?.id;

    // Insert rent snapshot
    if (fp.rent_min > 0) {
      let deleteQuery = db
        .from("rent_snapshots")
        .delete()
        .eq("property_id", propertyId)
        .eq("observed_at", today)
        .eq("asking_rent", fp.rent_min);
      if (floorplanId) {
        deleteQuery = deleteQuery.eq("floorplan_id", floorplanId);
      } else {
        deleteQuery = deleteQuery.is("floorplan_id", null);
      }
      const { error: deleteSnapshotError } = await deleteQuery;
      if (deleteSnapshotError) {
        throw new Error(`Failed to replace existing rent snapshot for ${floorplanName}: ${deleteSnapshotError.message}`);
      }

      const { error: snapshotError } = await db.from("rent_snapshots").insert({
        property_id: propertyId,
        floorplan_id: floorplanId || null,
        website_id: websiteId || null,
        observed_at: today,
        beds: fp.beds,
        baths: wholeBaths,
        sqft,
        asking_rent: fp.rent_min,
        effective_rent: fp.rent_min,
        asking_psf: null,
        deposit: fp.deposit || null,
        available_count: fp.available_count || null,
        concession_text: data.concession_text || null,
        raw: {
          source: scraperEntry?.platform || "unknown",
          rent_max: fp.rent_max,
          sqft_max: fp.sqft_max,
          floor_plan_name: fp.name,
          baths_decimal: fp.baths,
          property_name: data.property_name,
        },
      });
      if (snapshotError) {
        throw new Error(`Failed to insert rent snapshot for ${floorplanName}: ${snapshotError.message}`);
      }
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

    const { error } = await db
      .from("amenities")
      .upsert(amenityRecords, { onConflict: "property_id,scope,amenity" });
    if (error) throw new Error(`Failed to upsert amenities: ${error.message}`);
  }

  // Save fees
  const fees = data.fees;
  if (fees.application_fee > 0 || fees.pet_deposit > 0 || fees.admin_fee > 0) {
    const { error } = await db.from("fee_schedules").insert({
      property_id: propertyId,
      observed_at: today,
      app_fee: fees.application_fee || null,
      admin_fee: fees.admin_fee || null,
      pet_deposit: fees.pet_deposit || null,
      pet_monthly: fees.pet_monthly || null,
      parking_surface: fees.parking || null,
    });
    if (error) throw new Error(`Failed to insert fee schedule: ${error.message}`);
  }

  // Mark website as scraped (if using property_websites table)
  if (websiteId > 0) {
    await db
      .from("property_websites")
      .update({
        last_scraped_at: new Date().toISOString(),
        scrape_success: true,
      })
      .eq("id", websiteId);
  }
}

async function markScrapeFailure(db: SupabaseClient, websiteId: number) {
  if (websiteId <= 0) return;
  await db
    .from("property_websites")
    .update({
      last_scraped_at: new Date().toISOString(),
      scrape_success: false,
    })
    .eq("id", websiteId);
}

// ─── Browser Pool ───────────────────────────────────────────────────

let sharedBrowser: Browser | null = null;

async function getBrowser(): Promise<Browser> {
  if (sharedBrowser && sharedBrowser.isConnected()) return sharedBrowser;

  const launchOpts: Parameters<typeof chromium.launch>[0] = { headless: true };

  sharedBrowser = await chromium.launch(launchOpts);
  return sharedBrowser;
}

async function closeBrowser() {
  if (sharedBrowser) {
    await sharedBrowser.close().catch(() => {});
    sharedBrowser = null;
  }
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("Rent Tracker — Bulk Rent Scraper");
  console.log("=".repeat(60));
  console.log(`  State:     ${argState || "all"}`);
  console.log(`  City:      ${argCity || "all"}`);
  console.log(`  County ID: ${argCountyId || "all"}`);
  console.log(`  Platform:  ${argPlatform || "all supported"}`);
  console.log(`  Limit:     ${argLimit}`);
  console.log(`  Stale:     ${argStaledays} days`);
  console.log(`  Dry run:   ${isDryRun}`);
  console.log("=".repeat(60));

  const db = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: { persistSession: false },
  });

  // Get websites to scrape
  const websites = await getWebsitesToScrape(db);

  // Group by platform for reporting
  const byPlatform = new Map<string, number>();
  for (const w of websites) {
    const platform = w.platform || detectPlatform(w.url);
    byPlatform.set(platform, (byPlatform.get(platform) || 0) + 1);
  }

  console.log(`\nFound ${websites.length} websites to scrape:`);
  for (const [platform, count] of byPlatform) {
    console.log(`  ${platform}: ${count}`);
  }
  console.log();

  if (websites.length === 0) {
    console.log("No websites found matching filters. Nothing to do.");
    console.log("Ensure properties have a website field or property_websites entries.");
    return;
  }

  if (isDryRun) {
    console.log("DRY RUN — listing URLs that would be scraped:\n");
    for (const w of websites) {
      const platform = w.platform || detectPlatform(w.url);
      console.log(`  [${platform}] ${w.url} (property_id=${w.property_id})`);
    }
    console.log(`\nTotal: ${websites.length} URLs`);
    return;
  }

  // Launch shared browser
  const browser = await getBrowser();

  const stats = {
    total: websites.length,
    scraped: 0,
    floorplans: 0,
    snapshots: 0,
    noData: 0,
    failed: 0,
    skipped: 0,
  };

  const errors: Array<{ url: string; error: string }> = [];
  const startTime = Date.now();

  for (let i = 0; i < websites.length; i++) {
    const web = websites[i];
    const platform = web.platform || detectPlatform(web.url);
    const progress = `[${i + 1}/${websites.length}]`;

    // Get the appropriate scraper
    const scraperEntry = getScraperForUrl(web.url);
    if (!scraperEntry) {
      console.log(`${progress} SKIP ${web.url} (unsupported platform: ${platform})`);
      stats.skipped++;
      continue;
    }

    console.log(`${progress} [${scraperEntry.platform}] Scraping: ${web.url}`);

    try {
      const data = await scraperEntry.scrape(web.url, scraperEntry.platform === "direct" ? undefined : browser);

      if (data && data.floorplans.length > 0) {
        await saveScrapedData(db, web.property_id, web.id, scraperEntry, data);
        stats.scraped++;
        stats.floorplans += data.floorplans.length;
        stats.snapshots += data.floorplans.filter((fp) => fp.rent_min > 0).length;

        const rentRange = data.floorplans
          .filter((fp) => fp.rent_min > 0)
          .map((fp) => fp.rent_min);
        const minRent = rentRange.length > 0 ? Math.min(...rentRange) : 0;
        const maxRent = rentRange.length > 0 ? Math.max(...data.floorplans.map((fp) => fp.rent_max)) : 0;

        console.log(
          `  OK: ${data.property_name || "?"} — ${data.floorplans.length} plans, ` +
            `$${minRent}-$${maxRent}/mo` +
            (data.concession_text ? ` [Special: ${data.concession_text.substring(0, 60)}]` : ""),
        );
      } else if (data) {
        console.log(`  WARN: Page loaded but no floorplans found (${data.property_name || "?"})`);
        await markScrapeFailure(db, web.id);
        stats.noData++;
      } else {
        console.log("  FAIL: No data extracted");
        await markScrapeFailure(db, web.id);
        stats.failed++;
        errors.push({ url: web.url, error: "No data extracted" });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      console.error(`  ERROR: ${msg}`);
      await markScrapeFailure(db, web.id);
      stats.failed++;
      errors.push({ url: web.url, error: msg });
    }
  }

  await closeBrowser();

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log("\n" + "=".repeat(60));
  console.log("Rent Tracker — Scrape Summary");
  console.log("=".repeat(60));
  console.log(`  Total websites:       ${stats.total}`);
  console.log(`  Successfully scraped: ${stats.scraped}`);
  console.log(`  Floorplans found:     ${stats.floorplans}`);
  console.log(`  Rent snapshots:       ${stats.snapshots}`);
  console.log(`  No data found:        ${stats.noData}`);
  console.log(`  Failed:               ${stats.failed}`);
  console.log(`  Skipped:              ${stats.skipped}`);
  console.log(`  Elapsed:              ${elapsed}s`);

  if (errors.length > 0) {
    console.log(`\nErrors (${errors.length}):`);
    for (const e of errors.slice(0, 20)) {
      console.log(`  ${e.url}: ${e.error}`);
    }
    if (errors.length > 20) {
      console.log(`  ... and ${errors.length - 20} more`);
    }
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  closeBrowser().finally(() => process.exit(1));
});
