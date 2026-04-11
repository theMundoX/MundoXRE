/**
 * OKTaxRolls Adapter — handles Oklahoma counties using oktaxrolls.com
 *
 * This is a TAX ROLL site (payment data), not a full assessor database.
 * It provides: Tax ID, Owner Name, Property ID, Type, Base Tax, Total Due.
 * These parcel IDs can be cross-referenced with assessor data for full details.
 *
 * The site uses DataTables with client-side rendering, so we need Playwright.
 */

import { chromium } from "playwright";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";
import { waitForSlot } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../../utils/stealth.js";
import { validateUrlBeforeScrape } from "../../utils/allowlist.js";

const SEARCH_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");
const MAX_ROWS_PER_LETTER = 10_000;

interface TaxRow {
  year: string;
  tax_id: string;
  owner_name: string;
  property_id: string;
  type: string;
  base_tax: string;
  total_due: string;
}

export class OKTaxRollsAdapter extends AssessorAdapter {
  readonly platform = "oktaxrolls";

  canHandle(config: CountyConfig): boolean {
    return (
      config.platform === "oktaxrolls" ||
      config.alt_platform === "oktaxrolls" ||
      config.base_url.includes("oktaxrolls.com")
    );
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const baseUrl =
      config.platform === "oktaxrolls" ? config.base_url : config.alt_url ?? config.base_url;

    const validation = validateUrlBeforeScrape(baseUrl);
    if (!validation.allowed) {
      console.error(`  Blocked: ${validation.reason}`);
      return;
    }

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // Launch browser
    const stealth = getStealthConfig();
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      userAgent: stealth.userAgent,
      viewport: stealth.viewport,
      locale: stealth.locale,
      timezoneId: stealth.timezoneId,
    });

    try {
      const page = await context.newPage();
      await page.addInitScript(STEALTH_INIT_SCRIPT);

      for (const letter of SEARCH_LETTERS) {
        try {
          // Check cache for this letter
          const cacheKey = `${baseUrl}?letter=${letter}`;
          const cached = getCached(cacheKey);

          let rows: TaxRow[];

          if (cached) {
            rows = JSON.parse(cached) as TaxRow[];
          } else {
            await waitForSlot(baseUrl);

            // Navigate to search page
            await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30_000 });

            // Fill in search form — search by owner last name
            await page.selectOption("#tax_info_sel", "View By Owner Name");
            await page.waitForTimeout(500);

            // Fill last name field
            const lastNameInput = page.locator("#last_name, input[name='last_name']");
            if (await lastNameInput.isVisible()) {
              await lastNameInput.fill(letter);
            } else {
              // Try business/owner name field
              const ownerInput = page.locator("#owner_name, input[name='owner_name']");
              if (await ownerInput.isVisible()) {
                await ownerInput.fill(letter);
              }
            }

            // Submit search
            await page.click("#Search, input[type='submit']");

            // Wait for DataTable to load
            await page.waitForSelector("#tms_datatable tbody tr", { timeout: 15_000 }).catch(() => null);
            await page.waitForTimeout(2000);

            // Try to show max records per page (100)
            try {
              await page.selectOption("select[name='tms_datatable_length']", "100");
              await page.waitForTimeout(2000);
            } catch {
              // Default pagination is fine
            }

            // Extract all pages of the DataTable
            rows = [];
            let pageNum = 0;
            const MAX_PAGES = 100;

            while (pageNum < MAX_PAGES) {
              // Extract current page rows
              const pageRows = await page.evaluate(`
                (() => {
                  const results = [];
                  const trs = document.querySelectorAll("#tms_datatable tbody tr");
                  for (const tr of trs) {
                    const cells = tr.querySelectorAll("td");
                    if (cells.length >= 7) {
                      results.push({
                        year: cells[0]?.textContent?.trim() ?? "",
                        tax_id: cells[1]?.textContent?.trim() ?? "",
                        owner_name: cells[2]?.textContent?.trim() ?? "",
                        property_id: cells[3]?.textContent?.trim() ?? "",
                        type: cells[4]?.textContent?.trim() ?? "",
                        base_tax: cells[5]?.textContent?.trim() ?? "",
                        total_due: cells[6]?.textContent?.trim() ?? "",
                      });
                    }
                  }
                  return results;
                })()
              `) as TaxRow[];

              rows.push(...pageRows);
              pageNum++;

              // Check if there's a "Next" button that's not disabled
              const hasNext = await page.evaluate(`
                (() => {
                  const nextBtn = document.querySelector("#tms_datatable_next, .dataTables_paginate .next");
                  if (!nextBtn) return false;
                  return !nextBtn.classList.contains("disabled") && !nextBtn.classList.contains("ui-state-disabled");
                })()
              `) as boolean;

              if (!hasNext) break;

              // Click next page
              await page.click("#tms_datatable_next, .dataTables_paginate .next");
              await page.waitForTimeout(1500);
            }

            // Cache the results
            setCache(cacheKey, JSON.stringify(rows));
          }

          // Convert rows to property records
          for (const row of rows.slice(0, MAX_ROWS_PER_LETTER)) {
            if (!row.property_id && !row.owner_name) continue;

            const record: RawPropertyRecord = {
              parcel_id: row.property_id || row.tax_id,
              address: "PENDING ENRICHMENT",
              city: config.name,
              state: config.state,
              zip: "",
              owner_name: row.owner_name || undefined,
              property_type: row.type || undefined,
              assessed_value: undefined, // Comes from ActDataScout, not tax rolls
              property_tax: row.base_tax
                ? Math.round(parseFloat(row.base_tax.replace(/[$,]/g, "")) * 100) || undefined
                : undefined,
              assessor_url: baseUrl,
              raw: row as unknown as Record<string, unknown>,
            };

            progress.total_found++;
            progress.total_processed++;
            onProgress?.(progress);
            yield record;
          }

          console.log(`    Letter ${letter}: ${rows.length} records`);
        } catch (err) {
          console.error(`  Error on letter ${letter}:`, err instanceof Error ? err.message : "Unknown");
          progress.errors++;
        }
      }
    } finally {
      await context.close();
      await browser.close();
    }

    console.log(
      `  ${config.name}: ${progress.total_found} properties found, ${progress.errors} errors`,
    );
  }
}
