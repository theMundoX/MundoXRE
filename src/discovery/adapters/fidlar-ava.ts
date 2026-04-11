/**
 * Fidlar AVA (AVID) Recorder Adapter
 *
 * Scrapes county recorder official records from the Fidlar AVA platform.
 * AVA is an Angular SPA with a clean JSON API (ScrapRelay.WebService.Ava/breeze).
 *
 * Key features:
 * - ConsiderationAmount in search results (no extra lookups needed)
 * - Date range search
 * - Document type filtering
 * - Pagination via ResultAccessCode
 *
 * Coverage: 60+ counties in AR, IA, IL, IN, ME, MI, NH, OH, TX, WA
 *
 * API endpoint: {base}/ScrapRelay.WebService.Ava/breeze/Search
 * Auth: Bearer token from {base}/ScrapRelay.WebService.Ava/token
 *
 * Auth strategy:
 * - The token endpoint is WAF-protected and rejects plain fetch requests.
 * - A lightweight Playwright session loads the SPA page once per host to
 *   obtain the Bearer token via response interception.
 * - All subsequent API calls (Search, GetMoreResults) use pure fetch()
 *   with the Bearer token, giving ~10x speedup over full browser automation.
 * - Tokens are cached per base_url and reused for 10 minutes.
 */

import { waitForSlot } from "../../utils/rate-limiter.js";
import type { RecorderDocument, RecorderProgress } from "./landmark-web.js";

export { type RecorderDocument, type RecorderProgress };

export interface FidlarCountyConfig {
  county_name: string;
  state: string;
  base_url: string;  // e.g. "https://ava.fidlar.com/OHFairfield/AvaWeb/"
  county_id: number;
}

interface AvaDocResult {
  Id: number;
  DocumentType: string;
  RecordedDateTime: string;
  DocumentName: string;  // Document number
  ConsiderationAmount: number;
  Book: string;
  Page?: string;
  AssociatedDocuments: unknown[];
  DocumentDate: string;
  Fees: unknown[];
  Legals: Array<{ Id: number; LegalType: string; Description: string; Notes: string | null }>;
  LegalSummary: string;
  Names: Array<{ Name: string; Type: string }>; // Type: "Grantor" or "Grantee"
}

interface AvaSearchResponse {
  ResultAccessCode: string;
  ResultId: number;
  TotalResults: number;
  ViewableResults: number;
  DocResults: AvaDocResult[];
}

/** Cached Bearer token for a given county base URL */
interface TokenCache {
  bearerToken: string;
  obtainedAt: number;
}

export class FidlarAvaAdapter {
  /** Cache tokens per base_url so we don't re-auth for every call */
  private tokenCache = new Map<string, TokenCache>();

  /** Playwright browser — launched lazily, only for token acquisition */
  private browser: import("playwright").Browser | null = null;
  private browserContext: import("playwright").BrowserContext | null = null;

  async init(): Promise<void> {
    // No-op — browser launched lazily only when needed for token acquisition.
  }

  async close(): Promise<void> {
    if (this.browserContext) {
      await this.browserContext.close();
      this.browserContext = null;
    }
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
    this.tokenCache.clear();
  }

  /**
   * Derive the API base URL from the AvaWeb URL.
   * AvaWeb: https://ava.fidlar.com/OHFairfield/AvaWeb/
   * API:    https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/
   */
  private getApiBase(config: FidlarCountyConfig): string {
    return config.base_url.replace("/AvaWeb/", "/ScrapRelay.WebService.Ava/");
  }

  /**
   * Obtain a Bearer token for a county.
   *
   * First checks the cache. If expired or missing, tries pure fetch to the
   * token endpoint (works on some Fidlar instances without WAF). Falls back
   * to a lightweight Playwright session that loads the SPA and intercepts
   * the token response.
   */
  private async getToken(config: FidlarCountyConfig): Promise<string> {
    const cached = this.tokenCache.get(config.base_url);
    if (cached && Date.now() - cached.obtainedAt < 10 * 60 * 1000) {
      return cached.bearerToken;
    }

    const apiBase = this.getApiBase(config);

    // Strategy 1: Try pure fetch to token endpoint (some instances lack WAF)
    try {
      const resp = await fetch(`${apiBase}token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "grant_type=client_credentials",
      });
      if (resp.ok) {
        const text = await resp.text();
        if (text.includes("access_token")) {
          const data = JSON.parse(text) as { access_token: string };
          this.tokenCache.set(config.base_url, {
            bearerToken: data.access_token,
            obtainedAt: Date.now(),
          });
          return data.access_token;
        }
      }
    } catch {
      // Token endpoint blocked or unreachable — fall through
    }

    // Strategy 2: Use Playwright to load the SPA and intercept the token
    return this.getTokenViaPlaywright(config);
  }

  /**
   * Launch a minimal Playwright session to load the SPA and intercept the
   * Bearer token from the /token response. The browser instance is reused
   * across counties to amortize startup cost.
   */
  private async getTokenViaPlaywright(config: FidlarCountyConfig): Promise<string> {
    if (!this.browser) {
      const { chromium } = await import("playwright");
      const { getStealthConfig, STEALTH_INIT_SCRIPT } = await import("../../utils/stealth.js");
      this.browser = await chromium.launch({
        headless: true,
        args: ["--no-sandbox", "--disable-dev-shm-usage"],
      });
      const stealth = getStealthConfig();
      this.browserContext = await this.browser.newContext(stealth);
      await this.browserContext.addInitScript(STEALTH_INIT_SCRIPT);
    }

    const page = await this.browserContext!.newPage();
    try {
      let token = "";

      page.on("response", async (resp) => {
        if (resp.url().includes("/token") && resp.status() === 200) {
          try {
            const data = await resp.json() as { access_token?: string };
            if (data.access_token) token = data.access_token;
          } catch { /* not JSON */ }
        }
      });

      await page.goto(config.base_url, { waitUntil: "networkidle", timeout: 30000 });
      // Give the SPA a moment to complete its token request
      await page.waitForTimeout(2000);

      if (!token) {
        throw new Error(`No Bearer token intercepted for ${config.county_name}`);
      }

      this.tokenCache.set(config.base_url, {
        bearerToken: token,
        obtainedAt: Date.now(),
      });
      return token;
    } finally {
      await page.close();
    }
  }

  /**
   * Search by parcel ID (TaxId). Returns all documents for a specific property.
   * This is the reliable way to link recorder data to properties.
   */
  async *fetchByParcel(
    config: FidlarCountyConfig,
    parcelId: string,
    onProgress?: (progress: RecorderProgress) => void,
  ): AsyncGenerator<RecorderDocument> {
    yield* this.fetchDocuments(config, "01/01/1990", "12/31/2026", onProgress, { TaxId: parcelId });
  }

  async *fetchDocuments(
    config: FidlarCountyConfig,
    startDate: string,
    endDate: string,
    onProgress?: (progress: RecorderProgress) => void,
    overrides?: Partial<Record<string, string>>,
  ): AsyncGenerator<RecorderDocument> {
    const apiBase = this.getApiBase(config);
    const progress: RecorderProgress = {
      county: config.county_name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      current_date: startDate,
      started_at: new Date(),
    };

    // Obtain Bearer token
    let token: string;
    try {
      token = await this.getToken(config);
    } catch (err: any) {
      console.error(`  [fidlar] Auth failed for ${config.county_name}: ${err.message?.slice(0, 200)}`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    const headers = {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    };

    // Build search body
    const searchBody = {
      FirstName: "",
      LastBusinessName: "",
      StartDate: startDate,
      EndDate: endDate,
      DocumentName: "",
      DocumentType: "",
      SubdivisionName: "",
      SubdivisionLot: "",
      SubdivisionBlock: "",
      MunicipalityName: "",
      TractSection: "",
      TractTownship: "",
      TractRange: "",
      TractQuarter: "",
      TractQuarterQuarter: "",
      Book: "",
      Page: "",
      LotOfRecord: "",
      BlockOfRecord: "",
      AddressNumber: "",
      AddressDirection: "",
      AddressStreetName: "",
      TaxId: "",
      ...overrides,
    };

    await waitForSlot(config.base_url);

    // POST to search endpoint via fetch
    let data: AvaSearchResponse;
    try {
      const searchResp = await fetch(`${apiBase}breeze/Search`, {
        method: "POST",
        headers,
        body: JSON.stringify(searchBody),
      });

      const contentType = searchResp.headers.get("content-type") || "";
      if (searchResp.status === 401 || contentType.includes("text/html")) {
        // Token expired — clear cache and retry once
        this.tokenCache.delete(config.base_url);
        token = await this.getToken(config);
        const retryHeaders = { ...headers, Authorization: `Bearer ${token}` };
        const retryResp = await fetch(`${apiBase}breeze/Search`, {
          method: "POST",
          headers: retryHeaders,
          body: JSON.stringify(searchBody),
        });
        const retryContentType = retryResp.headers.get("content-type") || "";
        if (!retryResp.ok || retryContentType.includes("text/html")) {
          console.error(`  [fidlar] Search retry failed for ${config.county_name}: HTTP ${retryResp.status} (${retryContentType.slice(0, 30)})`);
          progress.errors++;
          onProgress?.(progress);
          return;
        }
        // Update headers for pagination calls
        headers["Authorization"] = `Bearer ${token}`;
        data = await retryResp.json() as AvaSearchResponse;
      } else if (!searchResp.ok) {
        console.error(`  [fidlar] Search failed for ${config.county_name}: HTTP ${searchResp.status}`);
        progress.errors++;
        onProgress?.(progress);
        return;
      } else {
        data = await searchResp.json() as AvaSearchResponse;
      }
    } catch (err: any) {
      console.error(`  [fidlar] Search error for ${config.county_name}: ${err.message?.slice(0, 200)}`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    progress.total_found = data.TotalResults;
    progress.current_date = `${startDate} to ${endDate}`;
    onProgress?.(progress);

    // Collect all doc results across pages
    let allDocs: AvaDocResult[] = [...data.DocResults];
    let fetched = allDocs.length;
    const resultAccessCode = data.ResultAccessCode;

    // Paginate if there are more results
    if (data.TotalResults > fetched && resultAccessCode) {
      const PAGE_SIZE = 1500;

      while (fetched < data.TotalResults) {
        const nextStart = fetched;
        const nextEnd = Math.min(fetched + PAGE_SIZE, data.TotalResults);
        console.log(`  Fetching page: ${nextStart + 1}-${nextEnd} of ${data.TotalResults}...`);

        await waitForSlot(config.base_url);

        try {
          const moreResp = await fetch(`${apiBase}breeze/GetMoreResults`, {
            method: "POST",
            headers,
            body: JSON.stringify({
              ResultAccessCode: resultAccessCode,
              StartIndex: nextStart,
              EndIndex: nextEnd,
            }),
          });

          if (!moreResp.ok) {
            console.error(`  Pagination: HTTP ${moreResp.status} at index ${nextStart}, stopping.`);
            progress.errors++;
            break;
          }

          const moreText = await moreResp.text();
          if (!moreText || (!moreText.startsWith("{") && !moreText.startsWith("["))) {
            console.error(`  Pagination: unexpected response at index ${nextStart}, stopping.`);
            break;
          }

          const moreData = JSON.parse(moreText) as AvaSearchResponse | AvaDocResult[];
          const moreDocs = Array.isArray(moreData) ? moreData : moreData.DocResults;

          if (!moreDocs || moreDocs.length === 0) {
            console.log(`  Pagination: no more results at index ${nextStart}.`);
            break;
          }

          allDocs = allDocs.concat(moreDocs);
          fetched += moreDocs.length;
        } catch (err: any) {
          console.error(`  Pagination error at index ${nextStart}: ${err.message?.slice(0, 100)}`);
          progress.errors++;
          break;
        }
      }

      console.log(`  Total fetched: ${allDocs.length} of ${data.TotalResults}`);
    }

    // Process all results
    for (const doc of allDocs) {
      const grantors = doc.Names?.filter(n => n.Type === "Grantor").map(n => n.Name) || [];
      const grantees = doc.Names?.filter(n => n.Type === "Grantee").map(n => n.Name) || [];

      // Parse date
      const dateMatch = doc.RecordedDateTime.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      const recordDate = dateMatch
        ? `${dateMatch[3]}-${dateMatch[1].padStart(2, "0")}-${dateMatch[2].padStart(2, "0")}`
        : startDate;

      const legal = doc.Legals?.map(l => l.Description).join("; ") || doc.LegalSummary || "";

      yield {
        document_type: doc.DocumentType.toUpperCase().trim(),
        recording_date: recordDate,
        instrument_number: doc.DocumentName || undefined,
        book_page: doc.Book && doc.Page ? `${doc.Book}/${doc.Page}` : (doc.Book || undefined),
        consideration: doc.ConsiderationAmount > 0 ? doc.ConsiderationAmount : undefined,
        grantor: grantors.join("; ").toUpperCase().trim(),
        grantee: grantees.join("; ").toUpperCase().trim(),
        legal_description: legal || undefined,
        source_url: config.base_url,
        raw: {
          id: doc.Id,
          fees: doc.Fees,
          documentDate: doc.DocumentDate,
          names: doc.Names,
        },
      };

      progress.total_processed++;
    }

    onProgress?.(progress);
  }
}

// ─── County Registry ────────────────────────────────────────────────

export const FIDLAR_AVA_COUNTIES: FidlarCountyConfig[] = [
  // Arkansas
  { county_name: "Saline", state: "AR", base_url: "https://ava.fidlar.com/ARSaline/AvaWeb/", county_id: 0 },
  // Illinois — discovered 2026-03-27
  { county_name: "Kendall", state: "IL", base_url: "https://ilkendall.fidlar.com/ILKendall/AvaWeb/", county_id: 0 },
  { county_name: "McHenry", state: "IL", base_url: "https://rep4laredo.fidlar.com/ILMcHenry/AvaWeb/", county_id: 0 },
  { county_name: "St. Clair", state: "IL", base_url: "https://ilstclair.fidlar.com/ILStClair/AvaWeb/", county_id: 0 },
  { county_name: "Will", state: "IL", base_url: "https://ilwill.fidlar.com/ILWill/AvaWeb/", county_id: 0 },
  // Iowa
  { county_name: "Black Hawk", state: "IA", base_url: "https://ava.fidlar.com/IABlackHawk/AvaWeb/", county_id: 0 },
  { county_name: "Boone", state: "IA", base_url: "https://ava.fidlar.com/IABoone/AvaWeb/", county_id: 0 },
  { county_name: "Calhoun", state: "IA", base_url: "https://ava.fidlar.com/IACalhoun/AvaWeb/", county_id: 0 },
  { county_name: "Clayton", state: "IA", base_url: "https://ava.fidlar.com/IAClayton/AvaWeb/", county_id: 0 },
  { county_name: "Dallas", state: "IA", base_url: "https://iadallas.fidlar.com/IADallas/AvaWeb/", county_id: 0 },
  { county_name: "Jasper", state: "IA", base_url: "https://ava.fidlar.com/IAJasper/AvaWeb/", county_id: 0 },
  { county_name: "Linn", state: "IA", base_url: "https://ava.fidlar.com/IALinn/AvaWeb/", county_id: 0 },
  { county_name: "Scott", state: "IA", base_url: "https://ava.fidlar.com/IAScott/AvaWeb/", county_id: 0 },
  // Maine — discovered 2026-03-27
  { county_name: "Sagadahoc", state: "ME", base_url: "https://mesagadahoc.fidlar.com/MESagadahoc/AvaWeb/", county_id: 0 },
  // Michigan
  { county_name: "Antrim", state: "MI", base_url: "https://ava.fidlar.com/MIAntrim/AvaWeb/", county_id: 0 },
  { county_name: "Oakland", state: "MI", base_url: "https://ava.fidlar.com/MIOakland/AvaWeb/", county_id: 0 },
  // New Hampshire
  { county_name: "Belknap", state: "NH", base_url: "https://ava.fidlar.com/NHBelknap/AvaWeb/", county_id: 0 },
  { county_name: "Carroll", state: "NH", base_url: "https://ava.fidlar.com/NHCarroll/AvaWeb/", county_id: 0 },
  { county_name: "Cheshire", state: "NH", base_url: "https://ava.fidlar.com/NHCheshire/AvaWeb/", county_id: 0 },
  { county_name: "Coos", state: "NH", base_url: "https://nhcoos.fidlar.com/NHCoos/AvaWeb/", county_id: 0 },
  { county_name: "Grafton", state: "NH", base_url: "https://ava.fidlar.com/NHGrafton/AvaWeb/", county_id: 0 },
  { county_name: "Hillsborough", state: "NH", base_url: "https://ava.fidlar.com/NHHillsborough/AvaWeb/", county_id: 0 },
  { county_name: "Merrimack", state: "NH", base_url: "https://ava.fidlar.com/NHMerrimack/AvaWeb/", county_id: 0 },
  { county_name: "Rockingham", state: "NH", base_url: "https://ava.fidlar.com/NHRockingham/AvaWeb/", county_id: 0 },
  { county_name: "Strafford", state: "NH", base_url: "https://ava.fidlar.com/NHStrafford/AvaWeb/", county_id: 0 },
  { county_name: "Sullivan", state: "NH", base_url: "https://ava.fidlar.com/NHSullivan/AvaWeb/", county_id: 0 },
  // Ohio — confirmed working (ScrapRelay API on ava.fidlar.com)
  { county_name: "Fairfield", state: "OH", base_url: "https://ava.fidlar.com/OHFairfield/AvaWeb/", county_id: 0 },
  { county_name: "Geauga", state: "OH", base_url: "https://ava.fidlar.com/OHGeauga/AvaWeb/", county_id: 0 },
  { county_name: "Paulding", state: "OH", base_url: "https://ava.fidlar.com/OHPaulding/AvaWeb/", county_id: 0 },
  { county_name: "Wyandot", state: "OH", base_url: "https://ava.fidlar.com/OHWyandot/AvaWeb/", county_id: 0 },
  // Ohio — BROKEN (ScrapRelay endpoint returns 404 — migrated to unknown platform):
  //   Athens (ohathens.fidlar.com), Defiance (ohdefiance), Hocking (ohhocking),
  //   Jackson (ohjackson), Scioto (ohscioto), Vinton (ohvinton),
  //   Darke/Lake (rep2laredo), Mahoning (rep5laredo), Marion (rep3laredo),
  //   Perry (oh3laredo), Warren (ohwarren)
  // Texas
  { county_name: "Austin", state: "TX", base_url: "https://ava.fidlar.com/TXAustin/AvaWeb/", county_id: 0 },
  { county_name: "Fannin", state: "TX", base_url: "https://ava.fidlar.com/TXFannin/AvaWeb/", county_id: 0 },
  { county_name: "Galveston", state: "TX", base_url: "https://ava.fidlar.com/TXGalveston/AvaWeb/", county_id: 0 },
  { county_name: "Kerr", state: "TX", base_url: "https://ava.fidlar.com/TXKerr/AvaWeb/", county_id: 0 },
  { county_name: "Panola", state: "TX", base_url: "https://ava.fidlar.com/TXPanola/AvaWeb/", county_id: 0 },
  // Washington
  { county_name: "Yakima", state: "WA", base_url: "https://ava.fidlar.com/WAYakima/AvaWeb/", county_id: 0 },
];
