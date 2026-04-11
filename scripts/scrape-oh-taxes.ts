#!/usr/bin/env tsx
/**
 * Scrape actual annual property tax amounts for Fairfield County, OH.
 *
 * Uses Beacon (Schneider Corp) property reports to extract real tax bills.
 * Beacon is protected by Cloudflare, so we use Playwright with stealth config.
 *
 * The Beacon property report URL pattern:
 *   https://beacon.schneidercorp.com/Application.aspx?AppID=1131&LayerID=28628
 *     &PageTypeID=4&PageID=11901&KeyValue={PARCEL_ID}
 *
 * Tax data appears in the "Tax Bill" or "Tax History" sections of the report.
 *
 * Usage:
 *   npx tsx scripts/scrape-oh-taxes.ts
 *   npx tsx scripts/scrape-oh-taxes.ts --limit=50
 *   npx tsx scripts/scrape-oh-taxes.ts --limit=10 --dry-run
 *   npx tsx scripts/scrape-oh-taxes.ts --parcel=0360017000   (single parcel test)
 *   npx tsx scripts/scrape-oh-taxes.ts --offset=100 --limit=200
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium, type Page, type BrowserContext } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../src/utils/rate-limiter.js";

// ─── Config ──────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// Beacon Fairfield County OH constants
const BEACON_BASE = "https://beacon.schneidercorp.com";
const BEACON_REPORT_URL = `${BEACON_BASE}/Application.aspx?AppID=1131&LayerID=28628&PageTypeID=4&PageID=11901`;

// ─── CLI Arguments ──────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=").slice(1).join("=");
}

const argLimit = parseInt(getArg("limit") || "50");
const argOffset = parseInt(getArg("offset") || "0");
const argParcel = getArg("parcel");
const isDryRun = args.includes("--dry-run");
const isHeaded = args.includes("--headed");
const BATCH_SIZE = 10; // process in batches of 10

// ─── Types ──────────────────────────────────────────────────────────

interface PropertyRow {
  id: number;
  parcel_id: string;
  address: string;
  city: string;
  annual_tax: number | null;
}

interface TaxResult {
  annualTax: number;   // in cents
  taxYear: number;
}

// ─── Tax Extraction ─────────────────────────────────────────────────

/**
 * Extract annual tax amount from Beacon property report page.
 * Looks for tax data in multiple possible page sections:
 * - "Tax Bill" section with total amount
 * - "Tax History" table with yearly totals
 * - "Levies" section with tax breakdown
 */
async function extractTaxFromPage(page: Page): Promise<TaxResult | null> {
  try {
    // Wait for the page content to load (Beacon uses dynamic rendering)
    await page.waitForSelector(".report-container, .module-container, #report-content, .detail-page", {
      timeout: 15_000,
    }).catch(() => {});

    // Give Beacon's JS a moment to render all sections
    await page.waitForTimeout(3000);

    // Strategy 1: Look for "Annual Taxes" or "Total Tax" in the page
    const taxData = await page.evaluate(() => {
      const text = document.body.innerText;

      // Common patterns in Beacon tax reports:
      // "Total Tax: $3,247.00"
      // "Annual Tax: $3,247.00"
      // "Tax Billed: $3,247.00"
      // "Total: $3,247.00" (in tax bill section)
      // Also look in tables for tax amounts

      let annualTax: number | null = null;
      let taxYear: number | null = null;

      // Pattern 1: Look for explicit "Total Tax" or "Annual Tax" labels
      const totalTaxPatterns = [
        /(?:Total\s+Tax|Annual\s+Tax(?:es)?|Tax\s+Billed|Total\s+Due|Grand\s+Total|Net\s+Tax)\s*[:$]\s*\$?([\d,]+\.?\d*)/gi,
        /\$\s*([\d,]+\.?\d*)\s*(?:total\s+tax|annual|per\s+year)/gi,
      ];

      for (const pattern of totalTaxPatterns) {
        const matches = [...text.matchAll(pattern)];
        if (matches.length > 0) {
          // Take the largest match (likely the full annual amount)
          for (const m of matches) {
            const val = parseFloat(m[1].replace(/,/g, ""));
            if (val > 0 && (annualTax === null || val > annualTax)) {
              annualTax = val;
            }
          }
        }
      }

      // Pattern 2: Look in tables for tax amounts
      if (annualTax === null) {
        const tables = document.querySelectorAll("table");
        for (const table of tables) {
          const headerText = (table.previousElementSibling?.textContent || "").toLowerCase();
          const caption = (table.querySelector("caption")?.textContent || "").toLowerCase();
          const tableText = (headerText + " " + caption).toLowerCase();

          if (tableText.includes("tax") || tableText.includes("levy") || tableText.includes("bill")) {
            // Look for a "Total" row in this table
            const rows = table.querySelectorAll("tr");
            for (const row of rows) {
              const cells = Array.from(row.querySelectorAll("td, th"));
              const rowText = cells.map(c => c.textContent?.trim() || "").join(" ");

              if (/total|annual|net\s+tax|grand/i.test(rowText)) {
                const amountMatch = rowText.match(/\$\s*([\d,]+\.?\d*)/);
                if (amountMatch) {
                  const val = parseFloat(amountMatch[1].replace(/,/g, ""));
                  if (val > 100 && val < 500_000) { // sanity check
                    annualTax = val;
                  }
                }
              }
            }
          }
        }
      }

      // Pattern 3: Scan all elements with class names suggesting tax data
      if (annualTax === null) {
        const taxElements = document.querySelectorAll(
          '[class*="tax"], [class*="Tax"], [id*="tax"], [id*="Tax"], ' +
          '[class*="total"], [class*="Total"], [class*="amount"], [class*="Amount"]'
        );
        for (const el of taxElements) {
          const elText = el.textContent || "";
          const amountMatch = elText.match(/\$\s*([\d,]+\.?\d*)/);
          if (amountMatch) {
            const val = parseFloat(amountMatch[1].replace(/,/g, ""));
            if (val > 100 && val < 500_000 && (annualTax === null || val > annualTax)) {
              annualTax = val;
            }
          }
        }
      }

      // Pattern 4: Find tax history section and get the most recent year
      if (annualTax === null) {
        // Look for any dollar amount preceded by a year (2023, 2024, 2025)
        const yearTaxPattern = /20(2[0-6])\s+.*?\$\s*([\d,]+\.?\d*)/g;
        const yearMatches = [...text.matchAll(yearTaxPattern)];
        if (yearMatches.length > 0) {
          // Sort by year descending and take the most recent
          yearMatches.sort((a, b) => parseInt(b[1]) - parseInt(a[1]));
          const val = parseFloat(yearMatches[0][2].replace(/,/g, ""));
          if (val > 100 && val < 500_000) {
            annualTax = val;
            taxYear = 2000 + parseInt(yearMatches[0][1]);
          }
        }
      }

      // Try to find tax year
      if (taxYear === null) {
        const yearMatch = text.match(/(?:Tax\s+Year|Fiscal\s+Year|Year)\s*[:]\s*(\d{4})/i);
        if (yearMatch) taxYear = parseInt(yearMatch[1]);
        else {
          // Default to current or previous year
          const currentYear = new Date().getFullYear();
          taxYear = currentYear - 1;
        }
      }

      // Pattern 5: If we still have nothing, look for the "Payments" section
      // which sometimes shows the total annual payment
      if (annualTax === null) {
        const paymentsSection = text.match(/Payments[\s\S]*?(?:Total|Amount)\s*\$?\s*([\d,]+\.?\d*)/i);
        if (paymentsSection) {
          const val = parseFloat(paymentsSection[1].replace(/,/g, ""));
          if (val > 100 && val < 500_000) {
            annualTax = val;
          }
        }
      }

      // Pattern 6: Sum up half-year payments (Ohio bills semi-annually)
      if (annualTax === null) {
        const halfPatterns = [
          /(?:1st\s+Half|First\s+Half|Half\s+1)\s*[:$]\s*\$?([\d,]+\.?\d*)/gi,
          /(?:2nd\s+Half|Second\s+Half|Half\s+2)\s*[:$]\s*\$?([\d,]+\.?\d*)/gi,
        ];
        let halfTotal = 0;
        let foundHalves = 0;
        for (const pattern of halfPatterns) {
          const m = pattern.exec(text);
          if (m) {
            halfTotal += parseFloat(m[1].replace(/,/g, ""));
            foundHalves++;
          }
        }
        if (foundHalves === 2 && halfTotal > 100) {
          annualTax = halfTotal;
        } else if (foundHalves === 1 && halfTotal > 50) {
          // Only found one half; double it as estimate
          annualTax = halfTotal * 2;
        }
      }

      return { annualTax, taxYear };
    });

    if (taxData.annualTax !== null && taxData.annualTax > 0) {
      return {
        annualTax: Math.round(taxData.annualTax * 100), // convert to cents
        taxYear: taxData.taxYear || new Date().getFullYear() - 1,
      };
    }

    return null;
  } catch (err) {
    return null;
  }
}

// ─── Browser Setup ──────────────────────────────────────────────────

async function createBrowserContext() {
  const browser = await chromium.launch({
    headless: !isHeaded,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox",
      "--disable-dev-shm-usage",
    ],
  });

  const stealth = getStealthConfig();
  const ctx = await browser.newContext({
    ...stealth,
    // Use Eastern timezone for Ohio
    timezoneId: "America/New_York",
  });

  await ctx.addInitScript(STEALTH_INIT_SCRIPT);

  return { browser, ctx };
}

// ─── Cloudflare Handler ─────────────────────────────────────────────

/**
 * Navigate to a Beacon page, handling Cloudflare challenge if present.
 * Returns true if page loaded successfully.
 */
async function navigateWithCloudflare(page: Page, url: string): Promise<boolean> {
  await waitForSlot(BEACON_BASE);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });

    // Check for Cloudflare challenge
    const title = await page.title();
    if (title.includes("Just a moment") || title.includes("Cloudflare")) {
      console.log("    [CF] Cloudflare challenge detected, waiting...");
      // Wait for the challenge to resolve (up to 30 seconds)
      try {
        await page.waitForFunction(
          () => !document.title.includes("Just a moment"),
          { timeout: 30_000 },
        );
        // Additional wait for page to fully render after challenge
        await page.waitForTimeout(3000);
        return true;
      } catch {
        console.log("    [CF] Challenge timeout — may need manual intervention");
        return false;
      }
    }

    // Check for "No data" or error pages
    const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 500) || "");
    if (bodyText.includes("No data available") || bodyText.includes("not found") || bodyText.includes("Error")) {
      return false;
    }

    return true;
  } catch (err) {
    const msg = (err as Error).message;
    if (msg.includes("timeout")) {
      backoffDomain(BEACON_BASE);
      console.log("    [TIMEOUT] Page load timed out");
    }
    return false;
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Fairfield County OH Annual Tax Scraper\n");
  console.log(`  Mode: ${isDryRun ? "DRY RUN" : "LIVE"}`);
  console.log(`  Limit: ${argLimit}`);
  if (argParcel) console.log(`  Single parcel: ${argParcel}`);
  console.log();

  // ── Get Fairfield County ID ──
  const { data: county } = await db.from("counties")
    .select("id")
    .eq("county_name", "Fairfield")
    .eq("state_code", "OH")
    .single();

  if (!county) {
    console.error("Fairfield County OH not found in counties table.");
    process.exit(1);
  }
  console.log(`  County ID: ${county.id}`);

  // ── Fetch properties to scrape ──
  let query = db.from("properties")
    .select("id, parcel_id, address, city, annual_tax")
    .eq("county_id", county.id)
    .not("parcel_id", "is", null);

  if (argParcel) {
    query = query.eq("parcel_id", argParcel);
  } else {
    // Prioritize properties without tax data
    query = query.is("annual_tax", null)
      .range(argOffset, argOffset + argLimit - 1);
  }

  const { data: properties, error: fetchErr } = await query;
  if (fetchErr) {
    console.error("Failed to fetch properties:", fetchErr.message);
    process.exit(1);
  }

  if (!properties || properties.length === 0) {
    console.log("  No properties to scrape (all have annual_tax set, or none match filter).");
    return;
  }

  console.log(`  Properties to scrape: ${properties.length}\n`);

  if (isDryRun) {
    console.log("  DRY RUN — would scrape these parcels:");
    for (const p of properties.slice(0, 20)) {
      console.log(`    ${p.parcel_id} — ${p.address}, ${p.city}`);
    }
    if (properties.length > 20) console.log(`    ... and ${properties.length - 20} more`);
    return;
  }

  // ── Launch browser ──
  const { browser, ctx } = await createBrowserContext();

  let scraped = 0;
  let updated = 0;
  let failed = 0;
  let noData = 0;

  try {
    // First, warm up the browser by loading the Beacon homepage
    // This helps with Cloudflare challenge solving
    const warmupPage = await ctx.newPage();
    console.log("  Warming up browser (loading Beacon homepage)...");
    await warmupPage.goto(`${BEACON_BASE}/Application.aspx?AppID=1131&LayerID=28628&PageTypeID=2&PageID=11899`, {
      waitUntil: "domcontentloaded",
      timeout: 45_000,
    }).catch(() => {});

    // Wait for Cloudflare challenge on warmup
    try {
      await warmupPage.waitForFunction(
        () => !document.title.includes("Just a moment"),
        { timeout: 45_000 },
      );
      console.log("  Cloudflare challenge passed.\n");
      resetDomainRate(BEACON_BASE);
    } catch {
      console.log("  WARNING: Cloudflare challenge may not have resolved.");
      console.log("  Continuing anyway — individual pages may fail.\n");
    }
    await warmupPage.waitForTimeout(2000);
    await warmupPage.close();

    // Process properties in batches
    for (let i = 0; i < properties.length; i += BATCH_SIZE) {
      const batch = properties.slice(i, i + BATCH_SIZE);
      console.log(`  Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(properties.length / BATCH_SIZE)} (${batch.length} properties)`);

      for (const prop of batch) {
        scraped++;
        const parcelId = prop.parcel_id;
        const reportUrl = `${BEACON_REPORT_URL}&KeyValue=${encodeURIComponent(parcelId)}`;

        process.stdout.write(`  [${scraped}/${properties.length}] ${parcelId} — ${prop.address}... `);

        const page = await ctx.newPage();

        try {
          const loaded = await navigateWithCloudflare(page, reportUrl);
          if (!loaded) {
            console.log("SKIP (page failed to load)");
            failed++;
            await page.close();
            continue;
          }

          const taxResult = await extractTaxFromPage(page);

          if (taxResult) {
            const taxDollars = (taxResult.annualTax / 100).toFixed(2);
            console.log(`$${taxDollars}/yr (${taxResult.taxYear})`);

            // Update database
            const { error: updateErr } = await db.from("properties")
              .update({
                annual_tax: taxResult.annualTax,
                tax_year: taxResult.taxYear,
              })
              .eq("id", prop.id);

            if (updateErr) {
              console.log(`    UPDATE ERROR: ${updateErr.message}`);
              failed++;
            } else {
              updated++;
            }
          } else {
            console.log("NO TAX DATA");
            noData++;
          }
        } catch (err) {
          console.log(`ERROR: ${(err as Error).message.slice(0, 60)}`);
          failed++;
        } finally {
          await page.close();
        }

        // Small human-like delay between requests
        await new Promise(r => setTimeout(r, 1000 + Math.random() * 2000));
      }

      // Pause between batches
      if (i + BATCH_SIZE < properties.length) {
        const pauseMs = 3000 + Math.random() * 5000;
        console.log(`  Batch pause: ${(pauseMs / 1000).toFixed(1)}s\n`);
        await new Promise(r => setTimeout(r, pauseMs));
      }
    }
  } finally {
    await browser.close();
  }

  // ── Summary ──
  console.log("\n─── Summary ────────────────────────────────────");
  console.log(`  Total scraped:  ${scraped}`);
  console.log(`  Updated in DB:  ${updated}`);
  console.log(`  No tax data:    ${noData}`);
  console.log(`  Failed:         ${failed}`);
  console.log("─────────────────────────────────────────────────\n");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
