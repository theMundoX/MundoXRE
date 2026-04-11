/**
 * Landmark Web Official Records Adapter (by Pioneer Technology Group / Catalis)
 *
 * Scrapes Florida Clerk of Court official records (deeds, mortgages, liens)
 * from the Landmark Web platform used by many Florida counties.
 *
 * Approach:
 *   1. Accept disclaimer via SetDisclaimer()
 *   2. Click "Record Date Search" tab to make it visible
 *   3. Fill date range and submit
 *   4. Read paginated JSON from /Search/GetSearchResults
 *
 * Confirmed working (no captcha):
 *   Levy: https://online.levyclerk.com/landmarkweb
 *
 * Additional counties tested by background agent — check LANDMARK_COUNTIES below.
 */

import { chromium, type Browser, type BrowserContext, type Page } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../../utils/stealth.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";

// ─── Types ──────────────────────────────────────────────────────────

export interface RecorderDocument {
  document_type: string;
  recording_date: string;
  instrument_number?: string;
  book_page?: string;
  consideration?: number;
  grantor: string;
  grantee: string;
  legal_description?: string;
  doc_id?: string;
  source_url: string;
  raw: Record<string, unknown>;
}

export interface LandmarkCountyConfig {
  county_name: string;
  state: string;
  base_url: string;
  path_prefix: string;   // e.g. "/landmarkweb", "/LandMarkWeb", ""
  county_id: number;
}

export interface RecorderProgress {
  county: string;
  total_found: number;
  total_processed: number;
  errors: number;
  current_date: string;
  started_at: Date;
}

// ─── Adapter ────────────────────────────────────────────────────────

export class LandmarkWebAdapter {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private maxRetries = 3;

  async init(): Promise<void> {
    this.browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage",
      ],
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
   * Accept disclaimer and navigate to the search page.
   */
  private async setupSession(page: Page, config: LandmarkCountyConfig): Promise<void> {
    await waitForSlot(config.base_url);
    await page.goto(`${config.base_url}${config.path_prefix}`, {
      waitUntil: "networkidle",
      timeout: 30_000,
    });

    // Accept disclaimer — try multiple approaches since LandmarkWeb versions vary
    try {
      // Method 1: Try JS SetDisclaimer() first — works even when button is hidden/in a modal
      const hasSetDisclaimer = await page.evaluate(() => typeof (window as any).SetDisclaimer === "function");
      if (hasSetDisclaimer) {
        await page.evaluate(() => (window as any).SetDisclaimer());
        await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
      } else {
        // Method 2: Click visible disclaimer accept button
        const acceptBtn = page.locator('input[type="button"][value*="Accept"], button:has-text("Accept"), input[onclick*="SetDisclaimer"], #btnAcceptTerms, .btn-accept, #idAcceptYes');
        const visibleBtn = acceptBtn.filter({ visible: true });
        const visibleCount = await visibleBtn.count();
        if (visibleCount > 0) {
          await visibleBtn.first().click({ timeout: 5000 });
          await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
        } else if (await acceptBtn.count() > 0) {
          // Force-dispatch click on hidden element (e.g. #idAcceptYes not yet visible)
          await acceptBtn.first().dispatchEvent("click");
          await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
        } else {
          // Method 3: v2 portal — click "Accept" link by text (new LandmarkWeb design)
          const aAccept = page.locator('a:has-text("Accept")');
          if (await aAccept.count() > 0) {
            await aAccept.first().dispatchEvent("click");
            await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
          } else {
            // Method 4: Direct form manipulation
            await page.evaluate(() => {
              const disclaimerDiv = document.getElementById("disclaimer") || document.querySelector('[class*="disclaimer"]');
              if (disclaimerDiv) (disclaimerDiv as HTMLElement).style.display = "none";
              const searchDiv = document.getElementById("mainsearch") || document.querySelector('[class*="search"]');
              if (searchDiv) (searchDiv as HTMLElement).style.display = "block";
            });
            await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
          }
        }
      }
    } catch {
      // If all disclaimer methods fail, try navigating directly to search
      await page.goto(`${config.base_url}${config.path_prefix}/Search/Index`, {
        waitUntil: "networkidle",
        timeout: 15_000,
      }).catch(() => {});
    }
    // Ensure page is fully settled before any evaluate() calls
    await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
  }

  /**
   * Execute a Record Date search for a single day and return results.
   * Uses response interception to capture the GetSearchResults JSON/HTML
   * since DataTables rendering is unreliable in headless mode.
   */
  /** Track whether this portal has a Consideration search tab */
  private hasConsiderationTab: boolean | null = null;

  private async searchDate(
    page: Page,
    config: LandmarkCountyConfig,
    date: string,
  ): Promise<RecorderDocument[]> {
    const [year, month, day] = date.split("-");
    const dateFormatted = `${month}/${day}/${year}`;

    // Check if Consideration tab exists (first time only)
    if (this.hasConsiderationTab === null) {
      await page.waitForLoadState("domcontentloaded").catch(() => {});
      this.hasConsiderationTab = await page.evaluate(() => {
        const navs = document.querySelectorAll(".searchNav");
        return Array.from(navs).some(
          n => n.textContent?.trim()?.includes("Consideration") && (n as HTMLElement).offsetHeight > 0,
        );
      });
    }

    // Prefer Consideration search — returns dollar amounts in column [4]
    if (this.hasConsiderationTab) {
      return this.searchByConsideration(page, config, dateFormatted, date);
    }

    // Fallback to Record Date search (no dollar amounts)
    return this.searchByRecordDate(page, config, dateFormatted, date);
  }

  /**
   * Search using the Consideration tab — returns all fields PLUS dollar amount.
   * Uses $1 - $999,999,999 range to capture everything with any consideration.
   * Documents with $0 consideration (assignments, satisfactions) won't appear,
   * so we also run a Record Date search to catch those.
   */
  private async searchByConsideration(
    page: Page,
    config: LandmarkCountyConfig,
    dateFormatted: string,
    isoDate: string,
  ): Promise<RecorderDocument[]> {
    // Ensure page is settled before any evaluate()
    await page.waitForLoadState("domcontentloaded", { timeout: 10_000 }).catch(() => {});
    // Click Consideration tab
    await page.evaluate(() => {
      const navs = document.querySelectorAll(".searchNav");
      for (const nav of navs) {
        if (nav.textContent?.trim()?.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
          (nav as HTMLElement).click();
          break;
        }
      }
    });
    await page.waitForTimeout(500);

    // Fill consideration range and dates
    try {
      await page.fill("#lowerBound", "1", { timeout: 3000 });
      await page.fill("#upperBound", "999999999", { timeout: 3000 });
      await page.fill("#beginDate-Consideration", dateFormatted, { timeout: 3000 });
      await page.fill("#endDate-Consideration", dateFormatted, { timeout: 3000 });
    } catch {
      await page.evaluate((d) => {
        const lb = document.getElementById("lowerBound") as HTMLInputElement;
        const ub = document.getElementById("upperBound") as HTMLInputElement;
        const begin = document.getElementById("beginDate-Consideration") as HTMLInputElement;
        const end = document.getElementById("endDate-Consideration") as HTMLInputElement;
        if (lb) lb.value = "1";
        if (ub) ub.value = "999999999";
        if (begin) begin.value = d;
        if (end) end.value = d;
      }, dateFormatted);
    }

    // Set max records
    try {
      const sel = page.locator("#numberOfRecords-Consideration");
      if (await sel.count() > 0 && await sel.isVisible({ timeout: 2000 })) {
        await sel.selectOption("10000").catch(() =>
          sel.selectOption("5000").catch(() =>
            sel.selectOption("200").catch(() => {}),
          ),
        );
      }
    } catch {}

    // Intercept response — handles both old (GetSearchResults) and new (GetDocumentList) LandmarkWeb API
    const resultPromise = new Promise<string>((resolve) => {
      const handler = async (resp: any) => {
        const url = resp.url();
        if (url.includes("GetSearchResults") || url.includes("GetDocumentList")) {
          try { resolve(await resp.text()); } catch { resolve(""); }
          page.off("response", handler);
        }
      };
      page.on("response", handler);
      setTimeout(() => resolve(""), 25_000);
    });

    // Submit
    await waitForSlot(config.base_url);
    try {
      await page.click("#submit-Consideration", { timeout: 5000 });
    } catch {
      await page.evaluate(() => {
        const btn = document.getElementById("submit-Consideration");
        if (btn) (btn as HTMLElement).click();
      });
    }

    const responseBody = await resultPromise;

    // Parse — Consideration search returns amount in column [4]
    let docs: RecorderDocument[] = [];
    if (responseBody && responseBody.trimStart().startsWith("{")) {
      docs = this.parseFromJson(responseBody, isoDate, config.base_url, true);
    } else if (responseBody && responseBody.length >= 100) {
      docs = await this.parseFromHtml(page, responseBody, isoDate, config.base_url);
    }

    // Skip the Record Date fallback for now — it doubles the time per day.
    // Documents with $0 consideration (assignments, satisfactions) can be caught
    // in a separate Record Date pass later.
    return docs;
  }

  /**
   * Search using the Record Date tab — standard search without dollar amounts.
   */
  private async searchByRecordDate(
    page: Page,
    config: LandmarkCountyConfig,
    dateFormatted: string,
    isoDate: string,
  ): Promise<RecorderDocument[]> {
    // Ensure page is settled before any evaluate()
    await page.waitForLoadState("domcontentloaded", { timeout: 10_000 }).catch(() => {});

    // Click Record Date Search tab — handles both old (.searchNav) and new (link text) LandmarkWeb versions
    const hasOldNav = await page.evaluate(() => document.querySelectorAll(".searchNav").length > 0);
    if (hasOldNav) {
      // Old LandmarkWeb version: click the .searchNav tab
      await page.evaluate(() => {
        const navs = document.querySelectorAll(".searchNav");
        for (const nav of navs) {
          if (nav.textContent?.trim() === "Record Date Search" && (nav as HTMLElement).offsetHeight > 0) {
            (nav as HTMLElement).click();
            break;
          }
        }
      });
    } else {
      // New LandmarkWeb version (v2): click the "Record Date" link in the sidebar/nav
      const rdLink = page.locator('a:has-text("Record Date"):not(:has-text("Record Date Search"))');
      const rdCount = await rdLink.count();
      if (rdCount > 0) {
        await rdLink.first().click({ timeout: 5000 }).catch(() => {});
        // Wait for the Record Date form to load
        await page.waitForSelector("#beginDate-RecordDate", { timeout: 8000 }).catch(() => {});
      }
    }
    await page.waitForTimeout(500);

    // Fill dates
    try {
      await page.fill("#beginDate-RecordDate", dateFormatted, { timeout: 3000 });
      await page.fill("#endDate-RecordDate", dateFormatted, { timeout: 3000 });
    } catch {
      await page.evaluate((d) => {
        const begin = document.getElementById("beginDate-RecordDate") as HTMLInputElement;
        const end = document.getElementById("endDate-RecordDate") as HTMLInputElement;
        if (begin) begin.value = d;
        if (end) end.value = d;
      }, dateFormatted);
    }

    // Set max records
    try {
      const recordsSelect = page.locator("#numberOfRecords-RecordDate");
      if (await recordsSelect.count() > 0 && await recordsSelect.isVisible({ timeout: 2000 })) {
        await recordsSelect.selectOption("10000").catch(() =>
          recordsSelect.selectOption("5000").catch(() =>
            recordsSelect.selectOption("200").catch(() => {}),
          ),
        );
      }
    } catch {}

    // Intercept response — handles both old (GetSearchResults) and new (GetDocumentList) LandmarkWeb API
    const resultPromise = new Promise<string>((resolve) => {
      const handler = async (resp: any) => {
        const url = resp.url();
        if (url.includes("GetSearchResults") || url.includes("GetDocumentList")) {
          try { resolve(await resp.text()); } catch { resolve(""); }
          page.off("response", handler);
        }
      };
      page.on("response", handler);
      setTimeout(() => resolve(""), 25_000);
    });

    // Submit
    await waitForSlot(config.base_url);
    try {
      await page.click("#submit-RecordDate", { timeout: 5000 });
    } catch {
      await page.evaluate(() => {
        const btn = document.getElementById("submit-RecordDate");
        if (btn) (btn as HTMLElement).click();
        else {
          // New version: look for Submit button by text
          const btns = document.querySelectorAll("button, input[type='button'], a.btn");
          for (const b of btns) {
            if ((b as HTMLElement).textContent?.trim() === "Submit") {
              (b as HTMLElement).click();
              break;
            }
          }
        }
      });
    }

    const resultsHtml = await resultPromise;
    if (!resultsHtml || resultsHtml.length < 100) {
      try {
        await page.waitForFunction(
          () => {
            const t = document.getElementById("resultsTable");
            return t && t.querySelectorAll("tbody tr").length > 0;
          },
          { timeout: 15_000 },
        );
        await page.waitForTimeout(2000);
        return this.parseFromDOM(page, isoDate);
      } catch {
        return [];
      }
    }

    if (resultsHtml.trimStart().startsWith("{")) {
      return this.parseFromJson(resultsHtml, isoDate, config.base_url, false);
    }
    return this.parseFromHtml(page, resultsHtml, isoDate, config.base_url);
  }

  /**
   * Parse results from DataTables JSON response.
   * Format: {"draw":"1","recordsTotal":N,"data":[{"0":"...","1":"...",...},...]}
   * Column mapping: [5]=Grantor, [6]=Grantee, [7]=Date, [8]=DocType, [9]=DocSubType,
   *                  [10]=Book, [11]=Page, [12]=CFN/InstrumentNumber, [13]=Legal
   */
  private parseFromJson(
    jsonStr: string,
    searchDate: string,
    sourceUrl: string,
    hasConsiderationColumn = false,
  ): RecorderDocument[] {
    try {
      const data = JSON.parse(jsonStr);
      if (!data.data || !Array.isArray(data.data)) return [];

      const docs: RecorderDocument[] = [];
      for (const row of data.data) {
        // Strip HTML tags and cleanup markers from cell values
        const strip = (val: string | undefined): string => {
          if (!val) return "";
          return val
            .replace(/<[^>]+>/g, "")
            .replace(/&amp;/g, "&")
            .replace(/&lt;/g, "<")
            .replace(/&gt;/g, ">")
            .replace(/nobreak_\s*/g, "")
            .replace(/unclickable_/g, "")
            .replace(/hidden_\S*/g, "")
            .trim();
        };

        // Extract columns — use numeric keys
        const cols: string[] = [];
        for (let i = 0; i < 30; i++) {
          cols[i] = strip(row[String(i)]);
        }

        // Find the date column dynamically (MM/DD/YYYY)
        const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
        if (dateIdx < 0) continue;

        const grantor = cols[dateIdx - 2] || "";
        const grantee = cols[dateIdx - 1] || "";
        const dateStr = cols[dateIdx];
        const docType = cols[dateIdx + 1] || "";
        const book = cols[dateIdx + 3] || "";
        const pg = cols[dateIdx + 4] || "";
        const cfn = cols[dateIdx + 5] || "";
        const legal = cols[dateIdx + 6] || "";

        if (!docType) continue;

        // Parse date from MM/DD/YYYY to YYYY-MM-DD
        const dm = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4})/);
        const recordDate = dm ? `${dm[3]}-${dm[1]}-${dm[2]}` : searchDate;

        // Extract consideration amount
        let consideration: number | undefined;
        if (hasConsiderationColumn) {
          // Consideration search returns dollar amount in the column BEFORE grantor
          // Column layout: [4]=$amount, [5]=grantor, [6]=grantee, [7]=date, ...
          // dateIdx-3 is the consideration column
          const amtCol = cols[dateIdx - 3] || "";
          const amtClean = amtCol.replace(/[$,]/g, "").trim();
          if (amtClean && !isNaN(parseFloat(amtClean))) {
            consideration = parseFloat(amtClean);
          }
        }
        // Fallback: try to extract from legal description
        if (!consideration) {
          const amtMatch = legal.match(/\$[\d,]+\.?\d*/);
          if (amtMatch) {
            consideration = parseFloat(amtMatch[0].replace(/[$,]/g, ""));
          }
        }

        docs.push({
          document_type: docType.toUpperCase().trim(),
          recording_date: recordDate,
          instrument_number: cfn || undefined,
          book_page: book && pg ? `${book}/${pg}` : undefined,
          consideration,
          grantor: grantor.toUpperCase().trim(),
          grantee: grantee.toUpperCase().trim(),
          legal_description: legal || undefined,
          source_url: sourceUrl,
          raw: { cols },
        });
      }
      return docs;
    } catch {
      return [];
    }
  }

  /**
   * Parse results from the intercepted GetSearchResults HTML response.
   * This is more reliable than reading the DOM since DataTables may not render.
   */
  private async parseFromHtml(
    page: Page,
    html: string,
    searchDate: string,
    sourceUrl: string,
  ): Promise<RecorderDocument[]> {
    const docs = await page.evaluate(
      ({ html, searchDate, sourceUrl }) => {
        const parser = new DOMParser();
        const doc = parser.parseFromString(html, "text/html");
        const rows = doc.querySelectorAll("tr");
        const results: any[] = [];

        for (const row of rows) {
          const cells = Array.from(row.querySelectorAll("td"));
          if (cells.length < 6) continue;
          const texts = cells.map((c) => c.textContent?.trim() || "");

          // Find date column
          const dateIdx = texts.findIndex((c) => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
          if (dateIdx < 0) continue;

          const grantor = texts[dateIdx - 2] || "";
          const grantee = texts[dateIdx - 1] || "";
          const dateStr = texts[dateIdx] || "";
          const docType = texts[dateIdx + 1] || "";
          const book = texts[dateIdx + 3] || "";
          const pg = texts[dateIdx + 4] || "";
          const cfn = texts[dateIdx + 5] || "";
          const legal = texts[dateIdx + 6] || "";

          // Parse date from MM/DD/YYYY to YYYY-MM-DD
          const dm = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4})/);
          const recordDate = dm ? `${dm[3]}-${dm[1]}-${dm[2]}` : searchDate;

          results.push({
            document_type: docType.toUpperCase().trim(),
            recording_date: recordDate,
            instrument_number: cfn || undefined,
            book_page: book && pg ? `${book}/${pg}` : undefined,
            grantor: grantor.toUpperCase().trim(),
            grantee: grantee.toUpperCase().trim(),
            legal_description: legal || undefined,
            source_url: sourceUrl,
            raw: { cells: texts },
          });
        }
        return results;
      },
      { html, searchDate, sourceUrl },
    );

    return docs as RecorderDocument[];
  }

  /**
   * Fallback: parse from the rendered DOM (works for Levy and similar).
   */
  private async parseFromDOM(page: Page, date: string): Promise<RecorderDocument[]> {
    const rawRows = await page.evaluate(() => {
      const rows = document.querySelectorAll("#resultsTable tbody tr");
      return Array.from(rows).map((r) => {
        const cells = Array.from(r.querySelectorAll("td"));
        return cells.map((c) => c.textContent?.trim() || "");
      });
    });

    const docs: RecorderDocument[] = [];
    for (const row of rawRows) {
      if (row.length < 6) continue;
      const dateIdx = row.findIndex((c) => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
      if (dateIdx < 0) continue;

      const book = row[dateIdx + 3] || "";
      const pagNum = row[dateIdx + 4] || "";

      docs.push({
        document_type: (row[dateIdx + 1] || "").toUpperCase().trim(),
        recording_date: this.parseDate(row[dateIdx]) || date,
        instrument_number: row[dateIdx + 5] || undefined,
        book_page: book && pagNum ? `${book}/${pagNum}` : undefined,
        grantor: (row[dateIdx - 2] || "").toUpperCase().trim(),
        grantee: (row[dateIdx - 1] || "").toUpperCase().trim(),
        legal_description: row[dateIdx + 6] || undefined,
        source_url: "",
        raw: { cells: row },
      });
    }
    return docs;
  }

  /**
   * Search by date range and yield documents.
   */
  async *fetchDocuments(
    config: LandmarkCountyConfig,
    startDate: string,
    endDate: string,
    onProgress?: (progress: RecorderProgress) => void,
  ): AsyncGenerator<RecorderDocument> {
    if (!this.context) throw new Error("Call init() first.");

    // Reset per-county state so each portal is probed independently
    this.hasConsiderationTab = null;

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
      await this.setupSession(page, config);

      const start = new Date(startDate);
      const end = new Date(endDate);
      const current = new Date(start);

      while (current <= end) {
        const dateStr = current.toISOString().split("T")[0];
        progress.current_date = dateStr;

        let retries = 0;
        let success = false;

        while (retries < this.maxRetries && !success) {
          // Abort immediately if browser was closed (e.g., by timeout handler)
          if (!this.browser) throw new Error("Browser closed — aborting county");

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

            // Try re-establishing session on failure
            // If browser was closed (timeout), abort immediately
            if (!this.browser) throw new Error("Browser closed — aborting county");

            if (retries < this.maxRetries) {
              try {
                await this.setupSession(page, config);
              } catch {}
            }
          }
        }

        onProgress?.(progress);
        current.setDate(current.getDate() + 1);
      }
    } finally {
      await page.close();
    }
  }

  private parseDate(s: string): string | undefined {
    const m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (!m) return undefined;
    return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  }
}

// ─── County Registry ────────────────────────────────────────────────

export const LANDMARK_COUNTIES: LandmarkCountyConfig[] = [
  // ── Confirmed working (tested 2026-03-26) ─────────────
  { county_name: "Levy", state: "FL", base_url: "https://online.levyclerk.com", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Martin", state: "FL", base_url: "http://or.martinclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Walton", state: "FL", base_url: "https://orsearch.clerkofcourts.co.walton.fl.us", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Citrus", state: "FL", base_url: "https://search.citrusclerk.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  // ── Discovered 2026-03-27 — ready for ingestion ──────────────
  { county_name: "Brevard", state: "FL", base_url: "https://officialrecords.brevardclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Clay", state: "FL", base_url: "https://landmark.clayclerk.com", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Hernando", state: "FL", base_url: "https://or.hernandoclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Lee", state: "FL", base_url: "https://or.leeclerk.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Manatee", state: "FL", base_url: "https://records.manateeclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Osceola", state: "FL", base_url: "https://or.osceolacounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Pasco", state: "FL", base_url: "https://or.pascocounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  // ── Discovered 2026-03-27 — additional FL counties ──────────────
  { county_name: "Escambia", state: "FL", base_url: "https://dory.escambiaclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Flagler", state: "FL", base_url: "https://records.flaglerclerk.gov", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Indian River", state: "FL", base_url: "https://ori.indian-river.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Okaloosa", state: "FL", base_url: "https://clerkapps.okaloosaclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "St. Johns", state: "FL", base_url: "https://doris.clk.co.st-johns.fl.us", path_prefix: "/LandmarkWebSJC", county_id: 0 },
  { county_name: "Wakulla", state: "FL", base_url: "http://www.wakullaclerk.com", path_prefix: "/landmarkweb", county_id: 0 },
  // ── Non-FL Landmark counties — discovered 2026-03-27 ──────────────
  { county_name: "Jefferson", state: "AL", base_url: "https://landmarkweb.jccal.org", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Adams", state: "CO", base_url: "https://recording.adcogov.org", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Chaffee", state: "CO", base_url: "https://recorder.chaffeerecordings.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Douglas", state: "CO", base_url: "https://apps.douglas.co.us", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Larimer", state: "CO", base_url: "https://records.larimer.org", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Mesa", state: "CO", base_url: "https://landmark.mesacounty.us", path_prefix: "/Landmarkweb", county_id: 0 },
  { county_name: "Montrose", state: "CO", base_url: "https://landmarkweb.montrosecounty.net", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Sussex", state: "DE", base_url: "https://deeds.sussexcountyde.gov", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Douglas", state: "KS", base_url: "https://landmark.douglascountyks.org", path_prefix: "/landmarkweb", county_id: 0 },
  { county_name: "Elko", state: "NV", base_url: "https://records.elkocountynv.net", path_prefix: "/Landmark", county_id: 0 },
  { county_name: "Clark", state: "WA", base_url: "https://e-docs.clark.wa.gov", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "King", state: "WA", base_url: "https://recordsearch.kingcounty.gov", path_prefix: "/LandmarkWeb", county_id: 0 },
  // ── Broken portals (redirect away from LandmarkWeb) ──────────────
  // { county_name: "Broward", state: "FL", base_url: "https://or.browardclerk.org", path_prefix: "/LandmarkWeb", county_id: 0 },  // redirects to PageNotFound
  // { county_name: "Duval", state: "FL", base_url: "https://officialrecords.duvalclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },  // redirects to ViewNotFound
  // { county_name: "Leon", state: "FL", base_url: "https://www.leonclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },  // redirects to cvweb (different platform)
  // { county_name: "Palm Beach", state: "FL", base_url: "https://or.palmbeachcounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },  // redirects to third-party
];
