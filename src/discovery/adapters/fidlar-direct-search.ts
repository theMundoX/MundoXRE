/**
 * Fidlar DirectSearch Adapter
 *
 * Covers Indiana counties with free anonymous access (and any other
 * counties using the DirectSearch platform).
 *
 * Key differences from AVA:
 * - API base URL is fetched from appConfig.json at the base_url
 * - Simpler search body: { docType, startDate, endDate }
 * - Anonymous cap: 200 results per search (vs AVA's 1500)
 * - Must recursively split date windows when TotalResults > ViewableResults
 *
 * Free Indiana counties confirmed:
 *   Marion (Indianapolis), Allen (Fort Wayne), St. Joseph (South Bend),
 *   Porter (Valparaiso), Floyd (New Albany)
 */

import type { RecorderDocument, RecorderProgress } from "./landmark-web.js";

export { type RecorderDocument, type RecorderProgress };

export interface DirectSearchCountyConfig {
  county_name: string;
  state: string;
  /** URL to the DirectSearch SPA root, e.g. "https://inmarion.fidlar.com/INMarion/DirectSearch/" */
  base_url: string;
  county_id: number;
  /** Optional: pre-resolved webApiBase. Filled in automatically from appConfig.json. */
  webApiBase?: string;
}

interface DirectSearchResponse {
  ResultAccessCode: string;
  TotalResults: number;
  ViewableResults: number;
  DocResults: DirectSearchDoc[];
}

interface DirectSearchDoc {
  Id: number;
  DocumentType: string;
  RecordedDateTime: string;  // e.g. "01/15/2024 00:00:00"
  DocumentName: string;      // instrument/document number
  ConsiderationAmount: number;
  Book: string;
  Page: string;
  LegalSummary: string;
  Party1: string;            // grantor (borrower)
  Party2: string;            // grantee (lender)
  Parties?: Array<{ Name: string; AdditionalName: string | null; PartyTypeId: number }>;
  Legals?: Array<{ Description: string }>;
}

/**
 * NOTE: The `docType` search parameter is completely ignored by the DirectSearch API.
 * Every search returns ALL document types recorded in the date range. Document type
 * filtering must be done client-side on the DocumentType field in DocResults.
 *
 * Typical daily volumes for Marion County, IN (~biggest market):
 *   All doc types combined: ~400-800/busy day
 *   MORTGAGE only: ~30-80/day
 *   Anonymous cap: 200/search
 *
 * The 200-cap only affects completeness when ALL-types daily volume exceeds 200.
 * Since mortgage/lien types are ~15-25% of all docs, we typically stay under the cap.
 */

/** Minimum window size in milliseconds before we give up splitting (1 day) */
const MIN_WINDOW_MS = 24 * 60 * 60 * 1000;

/** Max results the anonymous API will return per search */
const ANON_CAP = 200;

function toApiDate(d: Date): string {
  // ISO date only: "YYYY-MM-DD"
  return d.toISOString().split("T")[0];
}

function parseRecordDate(raw: string): string {
  // "01/15/2024 00:00:00" → "2024-01-15"
  const m = raw?.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  return m ? `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}` : "";
}

function parseStreetAddress(address: string): { number: string; street: string } | null {
  const match = address.trim().match(/^(\d+)\s+(.+)$/);
  if (!match) return null;
  const street = match[2]
    .replace(/^(N|S|E|W|NE|NW|SE|SW)\s+/i, "")
    .replace(/\s+(STREET|ST|AVENUE|AVE|BOULEVARD|BLVD|ROAD|RD|DRIVE|DR|COURT|CT|LANE|LN|WAY|PLACE|PL|CIRCLE|CIR|TERRACE|TER)\s*$/i, "")
    .trim();
  if (!street) return null;
  return { number: match[1], street };
}

export class FidlarDirectSearchAdapter {
  /** Cache: base_url → { webApiBase, token, obtainedAt } */
  private cache = new Map<string, { webApiBase: string; token: string; obtainedAt: number }>();

  private async resolveApiBase(config: DirectSearchCountyConfig): Promise<string> {
    if (config.webApiBase) return config.webApiBase;

    const cached = this.cache.get(config.base_url);
    if (cached) return cached.webApiBase;

    const cfgUrl = config.base_url.replace(/\/?$/, "/") + "appConfig.json";
    const resp = await fetch(cfgUrl, { signal: AbortSignal.timeout(15000) });
    if (!resp.ok) throw new Error(`appConfig.json fetch failed: HTTP ${resp.status} for ${cfgUrl}`);
    const cfg = await resp.json() as { webApiBase: string };
    if (!cfg.webApiBase) throw new Error(`No webApiBase in appConfig.json for ${config.county_name}`);
    return cfg.webApiBase;
  }

  private async getToken(webApiBase: string, countyBaseUrl: string): Promise<string> {
    const cached = this.cache.get(countyBaseUrl);
    if (cached && Date.now() - cached.obtainedAt < 9 * 60 * 1000) {
      return cached.token;
    }

    const resp = await fetch(webApiBase + "token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: "grant_type=password&username=anonymous&password=anonymous",
      signal: AbortSignal.timeout(15000),
    });
    if (!resp.ok) throw new Error(`Token failed: HTTP ${resp.status}`);
    const data = await resp.json() as { access_token: string };
    if (!data.access_token) throw new Error("No access_token in token response");

    this.cache.set(countyBaseUrl, {
      webApiBase,
      token: data.access_token,
      obtainedAt: Date.now(),
    });
    return data.access_token;
  }

  private async search(
    webApiBase: string,
    token: string,
    docType: string,
    startDate: string,
    endDate: string,
  ): Promise<DirectSearchResponse> {
    return this.searchWithRetry(webApiBase, token, { docType, startDate, endDate });
  }

  private async searchWithRetry(
    webApiBase: string,
    token: string,
    body: Record<string, string>,
    retries = 3,
  ): Promise<DirectSearchResponse> {
    const delays = [0, 15_000, 45_000]; // immediate, 15s, 45s
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        const delay = delays[Math.min(attempt - 1, delays.length - 1)];
        await new Promise(r => setTimeout(r, delay));
        // Refresh token on retry — the server may have invalidated our session
        try {
          token = await this.getToken(webApiBase, webApiBase);
        } catch { /* ignore token refresh errors */ }
      }
      try {
        const resp = await fetch(webApiBase + "breeze/Search", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(60000),
        });
        if (!resp.ok) throw new Error(`Search failed: HTTP ${resp.status}`);
        return resp.json() as Promise<DirectSearchResponse>;
      } catch (err: any) {
        if (attempt === retries) throw err;
        // Network-level error (fetch failed, ECONNRESET) — retry after delay
      }
    }
    throw new Error("Search failed after all retries");
  }

  private async searchByBusinessName(
    webApiBase: string,
    token: string,
    businessName: string,
    startDate?: string,
    endDate?: string,
  ): Promise<DirectSearchResponse> {
    const body: Record<string, string> = { LastBusinessName: businessName };
    if (startDate) body["StartDate"] = startDate;
    if (endDate)   body["EndDate"]   = endDate;
    return this.searchWithRetry(webApiBase, token, body);
  }

  private async searchByAddress(
    webApiBase: string,
    token: string,
    addressNumber: string,
    streetName: string,
    startDate?: string,
    endDate?: string,
  ): Promise<DirectSearchResponse> {
    const body: Record<string, string> = {
      AddressNumber: addressNumber,
      AddressStreetName: streetName,
    };
    if (startDate) body["StartDate"] = startDate;
    if (endDate) body["EndDate"] = endDate;
    return this.searchWithRetry(webApiBase, token, body);
  }

  private async searchByTaxId(
    webApiBase: string,
    token: string,
    taxId: string,
    startDate?: string,
    endDate?: string,
  ): Promise<DirectSearchResponse> {
    const body: Record<string, string> = { TaxId: taxId };
    if (startDate) body["StartDate"] = startDate;
    if (endDate) body["EndDate"] = endDate;
    return this.searchWithRetry(webApiBase, token, body);
  }

  async fetchAddressDocuments(
    config: DirectSearchCountyConfig,
    address: string,
    startDate = "2000-01-01",
    endDate = new Date().toISOString().slice(0, 10),
    docTypeFilter: string[] = ["MORTGAGE", "LIEN", "JUDGMENT", "MECHANIC"],
  ): Promise<RecorderDocument[]> {
    const parsed = parseStreetAddress(address);
    if (!parsed) return [];

    const webApiBase = await this.resolveApiBase(config);
    const token = await this.getToken(webApiBase, config.base_url);
    const data = await this.searchByAddress(webApiBase, token, parsed.number, parsed.street, startDate, endDate);
    const out: RecorderDocument[] = [];

    for (const raw of data.DocResults ?? []) {
      if (docTypeFilter.length > 0) {
        const rawType = (raw.DocumentType ?? "").toUpperCase();
        const keep = docTypeFilter.some(f => rawType.includes(f.toUpperCase()));
        if (!keep) continue;
      }
      const doc = this.mapDoc(raw, config.base_url);
      if (doc) out.push(doc);
    }
    return out;
  }

  async fetchTaxIdDocuments(
    config: DirectSearchCountyConfig,
    taxId: string,
    startDate = "2000-01-01",
    endDate = new Date().toISOString().slice(0, 10),
    docTypeFilter: string[] = ["MORTGAGE", "LIEN", "JUDGMENT", "MECHANIC"],
  ): Promise<RecorderDocument[]> {
    const cleaned = taxId.trim();
    if (!cleaned) return [];

    const webApiBase = await this.resolveApiBase(config);
    const token = await this.getToken(webApiBase, config.base_url);
    const data = await this.searchByTaxId(webApiBase, token, cleaned, startDate, endDate);
    const out: RecorderDocument[] = [];

    for (const raw of data.DocResults ?? []) {
      if (docTypeFilter.length > 0) {
        const rawType = (raw.DocumentType ?? "").toUpperCase();
        const keep = docTypeFilter.some(f => rawType.includes(f.toUpperCase()));
        if (!keep) continue;
      }
      const doc = this.mapDoc(raw, config.base_url);
      if (doc) out.push(doc);
    }
    return out;
  }

  /**
   * Fetch all recorded documents for a specific business entity name.
   * The anonymous cap (200) applies per request, so large entities (>200 docs)
   * are split by year until each window fits within the cap.
   */
  async *fetchByBusinessName(
    config: DirectSearchCountyConfig,
    businessName: string,
    fromYear = 2000,
    toYear = new Date().getFullYear(),
  ): AsyncGenerator<RecorderDocument> {
    const webApiBase = await this.resolveApiBase(config);
    const token = await this.getToken(webApiBase, config.base_url);

    for (let year = fromYear; year <= toYear; year++) {
      const startDate = `${year}-01-01`;
      const endDate   = `${year}-12-31`;
      let data: DirectSearchResponse;
      try {
        data = await this.searchByBusinessName(webApiBase, token, businessName, startDate, endDate);
      } catch (err: any) {
        console.error(`  [by-name] ${businessName} ${year}: ${err.message?.slice(0, 80)}`);
        continue;
      }

      const total    = data.TotalResults ?? 0;
      const viewable = data.ViewableResults ?? 0;
      if (total === 0) continue;

      if (total > ANON_CAP && total > viewable) {
        // Split into quarters for high-volume entities
        const quarters = [
          [`${year}-01-01`, `${year}-03-31`],
          [`${year}-04-01`, `${year}-06-30`],
          [`${year}-07-01`, `${year}-09-30`],
          [`${year}-10-01`, `${year}-12-31`],
        ];
        for (const [qStart, qEnd] of quarters) {
          let qData: DirectSearchResponse;
          try {
            qData = await this.searchByBusinessName(webApiBase, token, businessName, qStart, qEnd);
          } catch { continue; }
          for (const raw of qData.DocResults ?? []) {
            const doc = this.mapDoc(raw, config.base_url);
            if (doc) yield doc;
          }
        }
      } else {
        for (const raw of data.DocResults ?? []) {
          const doc = this.mapDoc(raw, config.base_url);
          if (doc) yield doc;
        }
      }
    }
  }

  /**
   * Recursively fetch all documents in a date window, splitting in half
   * whenever TotalResults exceeds the 200-result anonymous cap.
   *
   * Minimum window: 1 day. If a single day still exceeds 200, we take the
   * 200 we can get and log a warning — we cannot go narrower without datetime
   * precision in the API.
   */
  private async *fetchWindow(
    webApiBase: string,
    token: string,
    docType: string,
    start: Date,
    end: Date,
    progress: RecorderProgress,
    depth = 0,
  ): AsyncGenerator<DirectSearchDoc> {
    const startStr = toApiDate(start);
    const endStr = toApiDate(end);

    let data: DirectSearchResponse;
    try {
      data = await this.search(webApiBase, token, docType, startStr, endStr);
    } catch (err: any) {
      console.error(`  [direct-search] search error (${startStr}→${endStr}): ${err.message?.slice(0, 100)}`);
      progress.errors++;
      return;
    }

    const total = data.TotalResults ?? 0;
    const viewable = data.ViewableResults ?? 0;

    if (total === 0) return;

    // All results fit in the cap — yield them
    if (total <= ANON_CAP || total <= viewable) {
      progress.total_found += total;
      for (const doc of data.DocResults ?? []) {
        yield doc;
      }
      return;
    }

    // Can we split further?
    const windowMs = end.getTime() - start.getTime();
    if (windowMs <= MIN_WINDOW_MS || startStr === endStr) {
      // Minimum window reached — take what we can get and warn
      console.warn(
        `  [direct-search] WARNING: ${progress.county} ${startStr} has ${total} results but only ${viewable} viewable (anon cap). Taking first ${viewable}.`,
      );
      progress.total_found += viewable;
      for (const doc of data.DocResults ?? []) {
        yield doc;
      }
      return;
    }

    // Split window in half and recurse
    const midMs = Math.floor((start.getTime() + end.getTime()) / 2);
    const mid = new Date(midMs);

    // Left half: start → mid
    yield* this.fetchWindow(webApiBase, token, docType, start, mid, progress, depth + 1);

    // Right half: start of the day AFTER mid → end
    // Use UTC date math to avoid local-timezone overshoot bugs with setDate()
    const midDateStr = toApiDate(mid);
    const midPlus1 = new Date(midDateStr + "T00:00:00Z");
    midPlus1.setUTCDate(midPlus1.getUTCDate() + 1);
    if (midPlus1 <= end) {
      yield* this.fetchWindow(webApiBase, token, docType, midPlus1, end, progress, depth + 1);
    }
  }

  /**
   * Fetch documents for a county in the given date range.
   * The API returns ALL document types regardless of any filter — client-side
   * filtering is applied via the `docTypeFilter` set.
   *
   * @param docTypeFilter - Uppercase substring patterns to keep (e.g. ["MORTGAGE", "DEED", "LIEN"]).
   *   A document is kept if its DocumentType contains any of these strings.
   *   Pass an empty array to keep everything.
   */
  async *fetchDocuments(
    config: DirectSearchCountyConfig,
    startDate: string,
    endDate: string,
    docTypeFilter: string[] = ["MORTGAGE", "DEED", "LIEN", "JUDGMENT"],
    onProgress?: (progress: RecorderProgress) => void,
  ): AsyncGenerator<RecorderDocument> {
    const progress: RecorderProgress = {
      county: config.county_name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      current_date: `${startDate} to ${endDate}`,
      started_at: new Date(),
    };

    let webApiBase: string;
    try {
      webApiBase = await this.resolveApiBase(config);
    } catch (err: any) {
      console.error(`  [direct-search] Failed to resolve API base for ${config.county_name}: ${err.message}`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    let token: string;
    try {
      token = await this.getToken(webApiBase, config.base_url);
    } catch (err: any) {
      console.error(`  [direct-search] Auth failed for ${config.county_name}: ${err.message}`);
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    const start = new Date(startDate + "T00:00:00Z");
    const end = new Date(endDate + "T23:59:59Z");

    // Iterate over monthly chunks to keep windows manageable
    let chunkStart = new Date(start);
    while (chunkStart <= end) {
      const chunkEnd = new Date(chunkStart);
      chunkEnd.setUTCMonth(chunkEnd.getUTCMonth() + 1);
      chunkEnd.setUTCDate(chunkEnd.getUTCDate() - 1);
      if (chunkEnd > end) chunkEnd.setTime(end.getTime());

      progress.current_date = toApiDate(chunkStart);
      onProgress?.(progress);

      // Single search for all doc types; filter client-side
      for await (const raw of this.fetchWindow(webApiBase, token, "", chunkStart, chunkEnd, progress)) {
        // Client-side filter: keep only doc types matching the filter list
        if (docTypeFilter.length > 0) {
          const rawType = (raw.DocumentType ?? "").toUpperCase();
          const keep = docTypeFilter.some(f => rawType.includes(f.toUpperCase()));
          if (!keep) continue;
        }
        const doc = this.mapDoc(raw, config.base_url);
        if (doc) {
          yield doc;
          progress.total_processed++;
        }
      }

      // Advance to next month
      chunkStart = new Date(chunkEnd);
      chunkStart.setUTCDate(chunkStart.getUTCDate() + 1);
    }

    onProgress?.(progress);
  }

  private mapDoc(raw: DirectSearchDoc, sourceUrl: string): RecorderDocument | null {
    // Prefer Parties[] array with Name+AdditionalName; fall back to Party1/Party2 fields
    const parties = raw.Parties ?? [];
    const partyName = (p: { Name: string; AdditionalName: string | null }) =>
      [p.Name, p.AdditionalName].filter(Boolean).join(" ").trim();
    const grantors = parties.filter(p => p.PartyTypeId === 1).map(partyName).filter(Boolean);
    const grantees = parties.filter(p => p.PartyTypeId === 2).map(partyName).filter(Boolean);
    if (grantors.length === 0 && raw.Party1) grantors.push(raw.Party1.trim());
    if (grantees.length === 0 && raw.Party2) grantees.push(raw.Party2.trim());

    const recordDate = parseRecordDate(raw.RecordedDateTime);
    if (!recordDate) return null;

    const legal = raw.Legals?.map(l => l.Description).join("; ") || raw.LegalSummary || "";

    return {
      document_type: (raw.DocumentType ?? "").toUpperCase().trim(),
      recording_date: recordDate,
      instrument_number: raw.DocumentName || undefined,
      book_page: raw.Book && raw.Page ? `${raw.Book}/${raw.Page}` : undefined,
      consideration: raw.ConsiderationAmount > 0 ? raw.ConsiderationAmount : undefined,
      grantor: grantors.join("; ").toUpperCase().trim(),
      grantee: grantees.join("; ").toUpperCase().trim(),
      legal_description: legal || undefined,
      source_url: sourceUrl,
      raw: { id: raw.Id },
    };
  }
}

// ─── County Registry ─────────────────────────────────────────────────────────

export interface DirectSearchCountyDef extends DirectSearchCountyConfig {
  /** FIPS codes for DB lookup */
  state_fips: string;
  county_fips: string;
}

export const DIRECT_SEARCH_COUNTIES: DirectSearchCountyDef[] = [
  // Indiana — confirmed free anonymous access
  {
    county_name: "Marion",
    state: "IN",
    state_fips: "18",
    county_fips: "097",
    base_url: "https://inmarion.fidlar.com/INMarion/DirectSearch/",
    county_id: 0,
    // NOTE: 400-800 total docs/day; anonymous cap of 200 means ~50% coverage.
    // Captures first 200 recorded each morning. IP rotation confirmed useless
    // (same results regardless of token). Full coverage requires paid auth.
  },
  {
    county_name: "Allen",
    state: "IN",
    state_fips: "18",
    county_fips: "003",
    base_url: "https://inallen.fidlar.com/INAllen/DirectSearch/",
    county_id: 0,
    // NOTE: ~175-260 docs/day; many days under 200 cap = near-complete coverage.
    // Back-fill: npx tsx scripts/ingest-indiana-recorder.ts --county=Allen --from=2008-01-01 --to=2025-12-31
  },
  {
    county_name: "St. Joseph",
    state: "IN",
    state_fips: "18",
    county_fips: "141",
    base_url: "https://instjoseph.fidlar.com/INStJoseph/DirectSearch/",
    county_id: 0,
    // Back-fill: npx tsx scripts/ingest-indiana-recorder.ts --county=St.+Joseph --from=2008-01-01 --to=2025-12-31
  },
  {
    county_name: "Porter",
    state: "IN",
    state_fips: "18",
    county_fips: "127",
    base_url: "https://inporter.fidlar.com/INPorter/DirectSearch/",
    county_id: 0,
    // Back-fill: npx tsx scripts/ingest-indiana-recorder.ts --county=Porter --from=2008-01-01 --to=2025-12-31
  },
  // Floyd County (043): DirectSearch platform confirmed but anonymous search
  // returns ResultAction=2 (pay-per-search). Added to paid access list.
];

/**
 * Paid-access Indiana counties (DirectSearch platform, per-search fees):
 *   Floyd (043) — https://infloyd.fidlar.com/INFloyd/DirectSearch/
 *     ResultAction=2 means billing is active; anonymous token valid but no free searches.
 */
