#!/usr/bin/env tsx
/**
 * MXRE — Hamilton County, OH Recorder Ingest (Acclaim)
 *
 * Source: Hamilton County Recorder — Acclaim Web
 *   https://acclaim-web.hamiltoncountyohio.gov/AcclaimWebLive
 *
 * Session-based ASP.NET app; no login required.
 * Flow: GET session → POST search criteria → POST paginated results.
 *
 * Doc types ingested: DEED, MORTGAGE, MECHANICS LIEN, FEDERAL TAX LIEN,
 *                     CHILD SUPPORT LIEN, JUDGMENT
 *
 * ~9,000 recordings/month total; 2,185/week.
 * No consideration amount in index (embedded in image only).
 *
 * Usage:
 *   npx tsx scripts/ingest-hamilton-oh-recorder.ts
 *   npx tsx scripts/ingest-hamilton-oh-recorder.ts --start=2024-01-01 --end=2025-12-31
 *   npx tsx scripts/ingest-hamilton-oh-recorder.ts --county=Hamilton
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL = "https://acclaim-web.hamiltoncountyohio.gov/AcclaimWebLive";
const PAGE_SIZE = 500;
const BATCH_SIZE = 500;
const RATE_LIMIT_MS = 400;

// ─── Date range defaults (last 90 days) ──────────────────────────
const args = process.argv.slice(2);
const today = new Date();
const defaultEnd = today.toISOString().split("T")[0];
const defaultStart = (() => {
  const d = new Date();
  d.setDate(d.getDate() - 90);
  return d.toISOString().split("T")[0];
})();
const startArg = args.find(a => a.startsWith("--start="))?.split("=")[1] || defaultStart;
const endArg = args.find(a => a.startsWith("--end="))?.split("=")[1] || defaultEnd;

// ─── Helpers ──────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

/** Iterate over 1-month chunks between start and end dates */
function* monthRanges(start: string, end: string): Generator<{ from: string; to: string }> {
  const startDate = new Date(start + "T00:00:00Z");
  const endDate = new Date(end + "T00:00:00Z");
  let cursor = new Date(startDate);
  while (cursor <= endDate) {
    const monthStart = new Date(cursor);
    const nextMonth = new Date(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1);
    const monthEnd = new Date(nextMonth.getTime() - 86400000); // last day of month
    const clampedEnd = monthEnd > endDate ? endDate : monthEnd;
    yield {
      from: monthStart.toISOString().split("T")[0],
      to: clampedEnd.toISOString().split("T")[0],
    };
    cursor = nextMonth;
  }
}

/** Format date as MM/DD/YYYY for Acclaim */
function fmtDate(iso: string): string {
  const [y, m, d] = iso.split("-");
  return `${m}/${d}/${y}`;
}

/** Classify document type */
function classifyDocType(desc: string): string {
  const d = (desc || "").toUpperCase();
  if (d.includes("MORTGAGE") || d.includes("MTG")) return "mortgage";
  if (d.includes("QUIT CLAIM") || d.includes("QCD")) return "quit_claim_deed";
  if (d.includes("DEED")) return "deed";
  if (d.includes("MECHANIC") || d.includes("ML")) return "mechanics_lien";
  if (d.includes("FEDERAL TAX") || d.includes("IRS") || d.includes("FED")) return "federal_tax_lien";
  if (d.includes("TAX LIEN") || d.includes("STATE TAX")) return "state_tax_lien";
  if (d.includes("JUDGMENT") || d.includes("JUDG")) return "judgment";
  if (d.includes("LIEN")) return "lien";
  if (d.includes("RELEASE") || d.includes("DISCHARGE") || d.includes("SATISFACTION")) return "release";
  if (d.includes("ASSIGNMENT")) return "assignment";
  return "other";
}

// ─── Session management ───────────────────────────────────────────

interface Session {
  cookie: string;
}

async function getSession(): Promise<Session> {
  // Step 1: GET homepage to obtain ASP.NET session cookie
  const homeRes = await fetch(`${BASE_URL}`, {
    redirect: "follow",
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
  });
  if (!homeRes.ok) throw new Error(`Home GET failed: HTTP ${homeRes.status}`);

  const setCookies = homeRes.headers.getSetCookie?.() || [];
  const cookie = setCookies
    .map(c => c.split(";")[0])
    .filter(c => c.includes("="))
    .join("; ");

  if (!cookie) throw new Error("No session cookie obtained");

  // Step 2: GET the search page to initialize session
  await fetch(`${BASE_URL}/Search/SearchTypeDocType`, {
    headers: {
      "Cookie": cookie,
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    },
  });

  return { cookie };
}

// ─── Search ────────────────────────────────────────────────────────

interface AcclaimRecord {
  TransactionItemId: number;
  InstrumentNumber: string;
  RecordDate: string;
  BookPage: string;
  DocTypeDescription: string;
  DirectName: string;   // grantor (borrower / seller)
  IndirectName: string; // grantee (lender / buyer)
  DocLegalDescription: string;
  ParcelNumber: string | null;
}

interface SearchResponse {
  Data: AcclaimRecord[];
  Total: number;
  Errors: unknown | null;
}

async function postSearch(
  session: Session,
  fromDate: string,
  toDate: string,
): Promise<void> {
  const fromFmt = fmtDate(fromDate);
  const toFmt = fmtDate(toDate);

  // Doc type IDs: 49=DEED, 68=MORTGAGE, 65=MECHANICS LIEN, 56=FEDERAL TAX LIEN, 47=CHILD SUPPORT LIEN, 205=JUDGMENT
  const docTypeIds = ["49", "68", "65", "56", "47", "205"];
  const historyObj = JSON.stringify({
    DocTypesList: { BE: docTypeIds.join(",") },
    DateRange: { BE: "SpecificDateRange" },
    FromDatePicker: { BE: fromFmt },
    ToDatePicker: { BE: toFmt },
  });

  const body = [
    ...docTypeIds.map(id => `DocTypesList=${id}`),
    "DocTypesGroupList=All%7CAll",
    "DateRange=SpecificDateRange",
    `DateFrom=${encodeURIComponent(fromFmt)}`,
    `DateTo=${encodeURIComponent(toFmt)}`,
    "IsRegisteredLand=false",
    `HistoryObject=${encodeURIComponent(historyObj)}`,
  ].join("&");

  const res = await fetch(`${BASE_URL}/Search/SearchTypeDoctype`, {
    method: "POST",
    headers: {
      "Cookie": session.cookie,
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    },
    body,
  });

  if (!res.ok) throw new Error(`Search POST failed: HTTP ${res.status}`);
}

async function fetchResults(
  session: Session,
  page: number,
): Promise<SearchResponse> {
  const body = [
    `SearchResultGrid-page=${page}`,
    `SearchResultGrid-pageSize=${PAGE_SIZE}`,
    "SearchResultGrid-sort=",
    "SearchResultGrid-group=",
    "SearchResultGrid-filter=",
  ].join("&");

  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}/Search/GetSearchResults`, {
        method: "POST",
        headers: {
          "Cookie": session.cookie,
          "Content-Type": "application/x-www-form-urlencoded",
          "X-Requested-With": "XMLHttpRequest",
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        body,
        signal: AbortSignal.timeout(30000),
      });
      if (!res.ok) throw new Error(`GetSearchResults HTTP ${res.status}`);
      const json = await res.json() as SearchResponse;
      return json;
    } catch (err: unknown) {
      if (attempt === 3) throw err;
      await sleep(2000 * (attempt + 1));
    }
  }
  return { Data: [], Total: 0, Errors: null };
}

// ─── Dedup: get existing instrument numbers ───────────────────────

async function getExistingInstruments(
  instruments: string[],
): Promise<Set<string>> {
  const existing = new Set<string>();
  for (let i = 0; i < instruments.length; i += 200) {
    const chunk = instruments.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .ilike("source_url", "%acclaim-web.hamiltoncountyohio.gov%")
      .in("document_number", chunk);
    if (data) for (const r of data) existing.add(r.document_number);
  }
  return existing;
}

// ─── Main ─────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Hamilton County, OH Recorder Ingest (Acclaim)");
  console.log("═".repeat(60));
  console.log(`Date range: ${startArg} to ${endArg}\n`);

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Hamilton").eq("state_code", "OH").single();
  if (!county) { console.error("Hamilton County, OH not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  const SOURCE_URL = `${BASE_URL}/`;
  console.log(`County ID: ${COUNTY_ID}\n`);

  let totalInserted = 0, totalDupes = 0, totalErrors = 0;

  for (const range of monthRanges(startArg, endArg)) {
    process.stdout.write(`\n  ${range.from} → ${range.to}:`);

    let session: Session;
    try {
      session = await getSession();
    } catch (err: any) {
      console.error(`\n  Failed to get session: ${err.message}`);
      continue;
    }

    // POST search criteria
    try {
      await postSearch(session, range.from, range.to);
    } catch (err: any) {
      console.error(`\n  Search POST failed: ${err.message}`);
      continue;
    }

    // Paginate results
    let page = 1;
    let total = -1;
    let monthInserted = 0;

    while (true) {
      let result: SearchResponse;
      try {
        result = await fetchResults(session, page);
      } catch (err: any) {
        console.error(`\n  fetchResults p${page} failed: ${err.message}`);
        break;
      }

      if (page === 1) {
        total = result.Total;
        process.stdout.write(` ${total} records`);
      }

      if (!result.Data || result.Data.length === 0) break;

      // Dedup check
      const instruments = result.Data
        .map(r => r.InstrumentNumber)
        .filter(Boolean);
      const existingSet = await getExistingInstruments(instruments);

      const batch: Array<Record<string, unknown>> = [];
      for (const rec of result.Data) {
        if (!rec.InstrumentNumber) continue;
        if (existingSet.has(rec.InstrumentNumber)) { totalDupes++; continue; }

        // Parse record date: "2026/03/20" → "2026-03-20"
        let recordDate: string | null = null;
        if (rec.RecordDate) {
          recordDate = rec.RecordDate.replace(/\//g, "-");
        }

        // Book/page: "15627/1" → "15627-1"
        const bookPage = rec.BookPage?.replace(/\//g, "-") || null;

        const docType = classifyDocType(rec.DocTypeDescription);

        batch.push({
          property_id: null,
          document_number: rec.InstrumentNumber,
          document_type: docType,
          recording_date: recordDate,
          borrower_name: rec.DirectName?.trim().slice(0, 500) || null,  // grantor
          lender_name: rec.IndirectName?.trim().slice(0, 500) || null,  // grantee
          book_page: bookPage,
          loan_amount: null, // not available in index
          original_amount: null,
          interest_rate_type: "unknown",
          source_url: SOURCE_URL,
        });
      }

      // Insert batch (dedup already handled above via getExistingInstruments)
      for (let i = 0; i < batch.length; i += BATCH_SIZE) {
        const chunk = batch.slice(i, i + BATCH_SIZE);
        const { error } = await db.from("mortgage_records").insert(chunk);
        if (error) {
          for (const record of chunk) {
            const { error: e2 } = await db.from("mortgage_records").insert(record);
            if (e2) {
              if (totalErrors < 5) console.error(`\n  Error: ${JSON.stringify(e2).slice(0, 120)}`);
              totalErrors++;
            } else {
              totalInserted++;
              monthInserted++;
            }
          }
        } else {
          totalInserted += chunk.length;
          monthInserted += chunk.length;
        }
      }

      process.stdout.write(` | ${totalInserted} ins`);

      const maxPage = Math.ceil(total / PAGE_SIZE);
      if (page >= maxPage || result.Data.length < PAGE_SIZE) break;
      page++;
      await sleep(RATE_LIMIT_MS);
    }
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${totalInserted.toLocaleString()} inserted, ${totalDupes.toLocaleString()} dupes, ${totalErrors} errors`);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
