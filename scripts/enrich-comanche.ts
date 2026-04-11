#!/usr/bin/env tsx
/**
 * Enrich Comanche County properties via ActDataScout.
 * Uses Name search (A-Z) to get bulk results, matching by parcel ID.
 * Much faster than searching one parcel at a time.
 */
import "dotenv/config";
import { chromium, type Page } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";
import { waitForSlot } from "../src/utils/rate-limiter.js";
import { getCached, setCache } from "../src/utils/cache.js";

const BASE_URL = "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche";
const LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
);

interface ResultRow {
  rpid: string;
  parcel: string;
  owner: string;
  business: string;
  address: string;
  str: string;
  subdivision: string;
  legal: string;
  acres: string;
}

async function searchByLetter(page: Page, letter: string): Promise<ResultRow[]> {
  const cacheKey = `actds:name:comanche:${letter}`;
  const cached = getCached(cacheKey);
  if (cached) return JSON.parse(cached) as ResultRow[];

  await waitForSlot(BASE_URL);

  // Navigate to search page
  await page.goto(BASE_URL, { waitUntil: "networkidle", timeout: 30_000 });
  await page.waitForTimeout(1000);

  // Name tab should be default, fill last name
  await page.fill("#LastName", letter);
  await page.waitForTimeout(300);
  await page.click("#RPNameSubmit");
  await page.waitForTimeout(4000);

  // Show 100 entries per page
  try {
    await page.selectOption("select[name*='_length']", "100");
    await page.waitForTimeout(2000);
  } catch {
    // Default pagination
  }

  // Extract all pages
  const allRows: ResultRow[] = [];
  let pageNum = 0;
  const MAX_PAGES = 200;

  while (pageNum < MAX_PAGES) {
    const rows = await page.evaluate(`
      (() => {
        const results = [];
        const trs = document.querySelectorAll("table tbody tr");
        for (const tr of trs) {
          const cells = tr.querySelectorAll("td");
          // 11 columns: Actions, CRPID, RPID, Parcel#, OwnerName, BusinessName, MultiAddress, STR, Subdivision, Legal, Acres
          // Skip metadata rows (only have 2 cells)
          if (cells.length >= 10) {
            results.push({
              rpid: cells[2]?.textContent?.trim() || "",
              parcel: cells[3]?.textContent?.trim() || "",
              owner: cells[4]?.textContent?.trim() || "",
              business: cells[5]?.textContent?.trim() || "",
              address: cells[6]?.textContent?.trim() || "",
              str: cells[7]?.textContent?.trim() || "",
              subdivision: cells[8]?.textContent?.trim() || "",
              legal: cells[9]?.textContent?.trim() || "",
              acres: cells[10]?.textContent?.trim() || "0",
            });
          }
        }
        return results;
      })()
    `) as ResultRow[];

    allRows.push(...rows);
    pageNum++;

    // Check for Next button
    const hasNext = await page.evaluate(`
      (() => {
        const next = document.querySelector(".next:not(.disabled), .paginate_button.next:not(.disabled)");
        return !!next;
      })()
    `) as boolean;

    if (!hasNext) break;

    await page.click(".next:not(.disabled), .paginate_button.next:not(.disabled)");
    await page.waitForTimeout(1500);
  }

  setCache(cacheKey, JSON.stringify(allRows));
  return allRows;
}

async function main() {
  // Build a map of existing parcels that need enrichment
  const { data: properties, error } = await db
    .from("properties")
    .select("id, parcel_id")
    .eq("county_id", 3)
    .not("parcel_id", "is", null);

  if (error || !properties) {
    console.error("Failed to fetch properties.");
    process.exit(1);
  }

  const parcelMap = new Map<string, number>();
  for (const p of properties) {
    if (p.parcel_id) parcelMap.set(p.parcel_id, p.id as number);
  }
  console.log(`${parcelMap.size} parcels in DB to match against\n`);

  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
  });
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  let totalFound = 0;
  let matched = 0;
  let updated = 0;

  for (const letter of LETTERS) {
    console.log(`Letter ${letter}...`);

    try {
      const rows = await searchByLetter(page, letter);
      totalFound += rows.length;
      console.log(`  ${rows.length} results`);

      // Match rows to our DB records and update
      const updates: Array<{ id: number; data: Record<string, unknown> }> = [];

      for (const row of rows) {
        const dbId = parcelMap.get(row.parcel);
        if (dbId && row.address) {
          matched++;
          updates.push({
            id: dbId,
            data: {
              address: row.address.toUpperCase(),
              city: "LAWTON",
              owner_name: row.owner || undefined,
              updated_at: new Date().toISOString(),
            },
          });
        }
      }

      // Batch update
      for (const u of updates) {
        const { error: updateError } = await db
          .from("properties")
          .update(u.data)
          .eq("id", u.id);
        if (!updateError) updated++;
      }

      console.log(`  Matched: ${updates.length}, Updated: ${updated}`);
    } catch (err) {
      console.error(`  Error: ${err instanceof Error ? err.message : "unknown"}`);
    }
  }

  await context.close();
  await browser.close();

  console.log(`\n── Done ──`);
  console.log(`Total from ActDataScout: ${totalFound}`);
  console.log(`Matched to DB: ${matched}`);
  console.log(`Updated: ${updated}`);
}

main().catch(() => {
  console.error("Enrichment failed.");
  process.exit(1);
});
