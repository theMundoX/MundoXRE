/**
 * Fidlar AVA Direct API Adapter (no browser needed)
 *
 * Uses the ScrapRelay.WebService.Ava REST API directly.
 * Auth: anonymous/anonymous bearer token (free, no registration)
 *
 * Returns up to 1,500 results per search including:
 * - ConsiderationAmount (actual lien amount)
 * - Party1/Party2/Parties (grantor/grantee names)
 * - CanViewImage (whether document PDF is free)
 * - ImagePageCount
 * - TapestryLink (direct document link)
 * - Legal descriptions
 *
 * Coverage: 28 counties in AR, IA, MI, NH, OH, TX, WA
 */

import type { RecorderDocument, RecorderProgress } from "./landmark-web.js";

export { type RecorderDocument, type RecorderProgress };

export interface FidlarCountyConfig {
  county_name: string;
  state: string;
  base_url: string;
  county_id: number;
}

export class FidlarAvaApiAdapter {
  private getApiBase(config: FidlarCountyConfig): string {
    return config.base_url.replace("/AvaWeb/", "/ScrapRelay.WebService.Ava/");
  }

  private async getToken(apiBase: string): Promise<string> {
    const resp = await fetch(apiBase + "token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: "grant_type=password&username=anonymous&password=anonymous",
    });
    if (!resp.ok) throw new Error(`Token failed: ${resp.status}`);
    const data = await resp.json();
    return data.access_token;
  }

  async *fetchDocuments(
    config: FidlarCountyConfig,
    startDate: string,
    endDate: string,
    onProgress?: (progress: RecorderProgress) => void,
  ): AsyncGenerator<RecorderDocument> {
    const apiBase = this.getApiBase(config);
    const progress: RecorderProgress = {
      county: config.county_name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      current_date: `${startDate} to ${endDate}`,
      started_at: new Date(),
    };

    let token: string;
    try {
      token = await this.getToken(apiBase);
    } catch (err) {
      progress.errors++;
      onProgress?.(progress);
      return;
    }

    // The API returns up to 1,500 results per search.
    // For larger date ranges, split into weekly chunks.
    const start = new Date(startDate);
    const end = new Date(endDate);
    const chunkDays = 7;

    let current = new Date(start);
    while (current <= end) {
      const chunkEnd = new Date(current);
      chunkEnd.setDate(chunkEnd.getDate() + chunkDays - 1);
      if (chunkEnd > end) chunkEnd.setTime(end.getTime());

      const chunkStart = current.toISOString().split("T")[0];
      const chunkEndStr = chunkEnd.toISOString().split("T")[0];
      progress.current_date = chunkStart;

      try {
        const resp = await fetch(apiBase + "breeze/Search", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`,
          },
          body: JSON.stringify({
            FirstName: "", LastBusinessName: "",
            StartDate: chunkStart, EndDate: chunkEndStr,
            DocumentName: "", DocumentType: "",
            SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
            MunicipalityName: "",
            TractSection: "", TractTownship: "", TractRange: "",
            TractQuarter: "", TractQuarterQuarter: "",
            Book: "", Page: "",
            LotOfRecord: "", BlockOfRecord: "",
            AddressNumber: "", AddressDirection: "", AddressStreetName: "",
            TaxId: "",
          }),
        });

        if (!resp.ok) {
          // Token might have expired — refresh
          try {
            token = await this.getToken(apiBase);
            // Retry
            const retry = await fetch(apiBase + "breeze/Search", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${token}`,
              },
              body: JSON.stringify({
                FirstName: "", LastBusinessName: "",
                StartDate: chunkStart, EndDate: chunkEndStr,
                DocumentName: "", DocumentType: "",
                SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
                MunicipalityName: "",
                TractSection: "", TractTownship: "", TractRange: "",
                TractQuarter: "", TractQuarterQuarter: "",
                Book: "", Page: "",
                LotOfRecord: "", BlockOfRecord: "",
                AddressNumber: "", AddressDirection: "", AddressStreetName: "",
                TaxId: "",
              }),
            });
            if (!retry.ok) {
              progress.errors++;
              onProgress?.(progress);
              current.setDate(current.getDate() + chunkDays);
              continue;
            }
            const retryData = await retry.json();
            yield* this.processResults(retryData, config, progress);
          } catch {
            progress.errors++;
          }
          current.setDate(current.getDate() + chunkDays);
          continue;
        }

        const data = await resp.json();
        progress.total_found += data.TotalResults || 0;
        yield* this.processResults(data, config, progress);
      } catch (err) {
        progress.errors++;
      }

      onProgress?.(progress);
      current.setDate(current.getDate() + chunkDays);
    }

    onProgress?.(progress);
  }

  private *processResults(
    data: any,
    config: FidlarCountyConfig,
    progress: RecorderProgress,
  ): Generator<RecorderDocument> {
    if (!data.DocResults) return;

    for (const doc of data.DocResults) {
      // Extract parties
      const parties = doc.Parties || [];
      const grantors = parties
        .filter((p: any) => p.PartyTypeId === 1)
        .map((p: any) => p.Name)
        .filter(Boolean);
      const grantees = parties
        .filter((p: any) => p.PartyTypeId === 2)
        .map((p: any) => p.Name)
        .filter(Boolean);

      // Fallback to Party1/Party2 if Parties array is empty
      if (grantors.length === 0 && doc.Party1) grantors.push(doc.Party1.trim());
      if (grantees.length === 0 && doc.Party2) grantees.push(doc.Party2.trim());

      // Parse date
      const dateMatch = doc.RecordedDateTime?.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      const recordDate = dateMatch
        ? `${dateMatch[3]}-${dateMatch[1].padStart(2, "0")}-${dateMatch[2].padStart(2, "0")}`
        : "";

      const legal = doc.Legals?.map((l: any) => l.Description).join("; ") || doc.LegalSummary || "";

      yield {
        document_type: (doc.DocumentType || "").toUpperCase().trim(),
        recording_date: recordDate,
        instrument_number: doc.DocumentName || undefined,
        book_page: doc.Book && doc.Page ? `${doc.Book}/${doc.Page}` : undefined,
        consideration: doc.ConsiderationAmount > 0 ? doc.ConsiderationAmount : undefined,
        grantor: grantors.join("; ").toUpperCase().trim(),
        grantee: grantees.join("; ").toUpperCase().trim(),
        legal_description: legal || undefined,
        source_url: config.base_url,
        raw: {
          id: doc.Id,
          canViewImage: doc.CanViewImage,
          imagePageCount: doc.ImagePageCount,
          tapestryLink: doc.TapestryLink,
          referenceNumber: doc.ReferenceNumber,
          returnTo: doc.ReturnTo,
        },
      };

      progress.total_processed++;
    }
  }
}

// Re-export county list from the browser-based adapter
export { FIDLAR_AVA_COUNTIES } from "./fidlar-ava.js";
