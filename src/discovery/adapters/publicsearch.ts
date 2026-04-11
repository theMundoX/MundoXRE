/**
 * PublicSearch.us Official Records Adapter
 *
 * Scrapes Texas county clerk official records (deeds, mortgages, liens)
 * from the publicsearch.us platform used by Dallas, Denton, Tarrant, and others.
 *
 * Approach:
 *   1. Navigate to county's publicsearch.us URL
 *   2. Select "Property Records" department
 *   3. Search by date range or name
 *   4. Scrape rendered results table
 *
 * Confirmed working:
 *   Dallas:  https://dallas.tx.publicsearch.us/
 *   Denton:  https://denton.tx.publicsearch.us/
 *   Tarrant: https://tarrant.tx.publicsearch.us/ (if available)
 */

import { chromium, type Browser, type BrowserContext, type Page } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../../utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";
import type { RecorderDocument, RecorderProgress } from "./landmark-web.js";

export { type RecorderDocument, type RecorderProgress };

export interface PublicSearchCountyConfig {
  county_name: string;
  state: string;
  base_url: string;
  county_id: number;
}

export class PublicSearchAdapter {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private maxRetries = 3;

  async init(): Promise<void> {
    this.browser = await chromium.launch({
      headless: true,
      args: ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    });
    const stealth = getStealthConfig();
    this.context = await this.browser.newContext(stealth);
    await this.context.addInitScript(STEALTH_INIT_SCRIPT);
  }

  async close(): Promise<void> {
    await this.context?.close();
    await this.browser?.close();
    this.context = null;
    this.browser = null;
  }

  /**
   * Search by date range and yield documents.
   * Uses URL-based navigation for each date to avoid SPA state issues.
   */
  async *fetchDocuments(
    config: PublicSearchCountyConfig,
    startDate: string,
    endDate: string,
    onProgress?: (progress: RecorderProgress) => void,
  ): AsyncGenerator<RecorderDocument> {
    if (!this.context) throw new Error("Call init() first.");

    const page = await this.context.newPage();
    const progress: RecorderProgress = {
      county: config.county_name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      current_date: startDate,
      started_at: new Date(),
    };

    try {
      const start = new Date(startDate);
      const end = new Date(endDate);
      const current = new Date(start);

      while (current <= end) {
        const dateStr = current.toISOString().split("T")[0];
        progress.current_date = dateStr;

        let retries = 0;
        let success = false;

        while (retries < this.maxRetries && !success) {
          try {
            const docs = await this.searchDate(page, config, dateStr);
            for (const doc of docs) {
              progress.total_found++;
              progress.total_processed++;
              yield doc;
            }
            success = true;
            resetDomainRate(config.base_url);

            if (docs.length > 0) {
              console.log(`  ${dateStr}: ${docs.length} documents`);
            }
          } catch (err) {
            retries++;
            progress.errors++;
            const msg = (err as Error).message;
            console.error(`  Error ${dateStr} (attempt ${retries}): ${msg.substring(0, 80)}`);
            backoffDomain(config.base_url);
          }
        }

        onProgress?.(progress);
        current.setDate(current.getDate() + 1);
      }
    } finally {
      await page.close();
    }
  }

  /**
   * Search a single date by navigating to the results URL directly.
   */
  private async searchDate(
    page: Page,
    config: PublicSearchCountyConfig,
    date: string,
  ): Promise<RecorderDocument[]> {
    // Navigate directly to search results for this date
    const url = `${config.base_url}results?department=RP&limit=250&offset=0&recordedDateRange=custom&recordedDateFrom=${date}&recordedDateTo=${date}&searchOcrText=false&searchType=quickSearch`;

    await waitForSlot(config.base_url);
    await page.goto(url, { waitUntil: "networkidle", timeout: 30_000 });

    // Wait for results table to render
    try {
      await page.waitForSelector("table tbody tr", { timeout: 15_000 });
    } catch {
      // No results for this day
      return [];
    }
    await page.waitForTimeout(2000);

    // Extract data from the rendered table
    const rawRows = await page.evaluate(() => {
      const rows = document.querySelectorAll("table tbody tr");
      return Array.from(rows).map((r) => {
        const cells = Array.from(r.querySelectorAll("td"));
        return cells.map((c) => c.textContent?.trim() || "");
      });
    });

    // Parse rows
    // publicsearch.us column layout: [empty, empty, empty, Grantor, Grantee, DocType, RecordedDate, DocNumber, Book/Vol/Page, Town, LegalDescription]
    const docs: RecorderDocument[] = [];
    for (const row of rawRows) {
      if (row.length < 6) continue;

      // Skip leading empty columns
      const nonEmpty = row.filter((c) => c.length > 0);
      if (nonEmpty.length < 4) continue;

      // Find date column to anchor
      const dateIdx = row.findIndex((c) => /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(c));
      let grantor: string, grantee: string, docType: string, recordDate: string;
      let docNumber: string, bookPage: string, town: string, legal: string;

      if (dateIdx >= 3) {
        grantor = row[dateIdx - 3] || "";
        grantee = row[dateIdx - 2] || "";
        docType = row[dateIdx - 1] || "";
        recordDate = this.parseDate(row[dateIdx]) || date;
        docNumber = row[dateIdx + 1] || "";
        bookPage = row[dateIdx + 2] || "";
        town = row[dateIdx + 3] || "";
        legal = row[dateIdx + 4] || "";
      } else {
        // Fallback: use nonEmpty array
        grantor = nonEmpty[0] || "";
        grantee = nonEmpty[1] || "";
        docType = nonEmpty[2] || "";
        recordDate = this.parseDate(nonEmpty[3]) || date;
        docNumber = nonEmpty[4] || "";
        bookPage = nonEmpty[5] || "";
        town = nonEmpty[6] || "";
        legal = nonEmpty[7] || "";
      }

      if (!docType) continue;

      docs.push({
        document_type: docType.toUpperCase().trim(),
        recording_date: recordDate,
        instrument_number: docNumber || undefined,
        book_page: bookPage !== "--/--/--" ? bookPage || undefined : undefined,
        grantor: grantor.toUpperCase().trim(),
        grantee: grantee.toUpperCase().trim(),
        legal_description: [town, legal].filter(Boolean).join(" — ") || undefined,
        source_url: config.base_url,
        raw: { cells: row, town },
      });
    }

    // Handle pagination — check if there are more results
    const totalText = await page.evaluate(() => {
      const el = document.querySelector("[class*='result-count'], [class*='total']");
      return el?.textContent?.trim() || "";
    });

    // If there are more than 250 results, fetch additional pages
    if (docs.length >= 250) {
      let offset = 250;
      while (true) {
        const nextUrl = `${config.base_url}results?department=RP&limit=250&offset=${offset}&recordedDateRange=custom&recordedDateFrom=${date}&recordedDateTo=${date}&searchOcrText=false&searchType=quickSearch`;
        await waitForSlot(config.base_url);
        await page.goto(nextUrl, { waitUntil: "networkidle", timeout: 30_000 });

        try {
          await page.waitForSelector("table tbody tr", { timeout: 10_000 });
        } catch {
          break;
        }
        await page.waitForTimeout(1000);

        const moreRows = await page.evaluate(() => {
          const rows = document.querySelectorAll("table tbody tr");
          return Array.from(rows).map((r) => {
            const cells = Array.from(r.querySelectorAll("td"));
            return cells.map((c) => c.textContent?.trim() || "");
          });
        });

        if (moreRows.length === 0) break;

        for (const row of moreRows) {
          if (row.length < 4) continue;
          docs.push({
            document_type: (row[2] || "").toUpperCase().trim(),
            recording_date: this.parseDate(row[3]) || date,
            instrument_number: row[4] || undefined,
            book_page: row[5] !== "--/--/--" ? row[5] || undefined : undefined,
            grantor: (row[0] || "").toUpperCase().trim(),
            grantee: (row[1] || "").toUpperCase().trim(),
            legal_description: [row[6], row[7]].filter(Boolean).join(" — ") || undefined,
            source_url: config.base_url,
            raw: { cells: row, town: row[6] },
          });
        }

        offset += 250;
        if (moreRows.length < 250) break;
      }
    }

    return docs;
  }

  private parseDate(s: string): string | undefined {
    // Handle MM/DD/YYYY
    const m1 = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (m1) return `${m1[3]}-${m1[1].padStart(2, "0")}-${m1[2].padStart(2, "0")}`;
    // Handle YYYY-MM-DD (already in ISO format)
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    return undefined;
  }
}

// ─── County Registry ────────────────────────────────────────────────

export const PUBLICSEARCH_COUNTIES: PublicSearchCountyConfig[] = [
  // ── Texas (confirmed working) ──────────────────────────────────────
  { county_name: "Dallas", state: "TX", base_url: "https://dallas.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Denton", state: "TX", base_url: "https://denton.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Tarrant", state: "TX", base_url: "https://tarrant.tx.publicsearch.us/", county_id: 0 },
  // ── Texas (discovered via web search — same platform) ─────────────
  { county_name: "Cameron", state: "TX", base_url: "https://cameron.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Hidalgo", state: "TX", base_url: "https://hidalgo.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Jefferson", state: "TX", base_url: "https://jefferson.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Brazos", state: "TX", base_url: "https://brazos.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Johnson", state: "TX", base_url: "https://johnson.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Wilson", state: "TX", base_url: "https://wilson.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Kendall", state: "TX", base_url: "https://kendall.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Nacogdoches", state: "TX", base_url: "https://nacogdoches.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Ellis", state: "TX", base_url: "https://ellis.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Rockwall", state: "TX", base_url: "https://rockwall.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Kaufman", state: "TX", base_url: "https://kaufman.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Hood", state: "TX", base_url: "https://hood.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Parker", state: "TX", base_url: "https://parker.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Wise", state: "TX", base_url: "https://wise.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Hunt", state: "TX", base_url: "https://hunt.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Henderson", state: "TX", base_url: "https://henderson.tx.publicsearch.us/", county_id: 0 },
  { county_name: "Navarro", state: "TX", base_url: "https://navarro.tx.publicsearch.us/", county_id: 0 },
];
