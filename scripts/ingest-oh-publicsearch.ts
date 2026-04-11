#!/usr/bin/env tsx
/**
 * MXRE — Ohio PublicSearch.us Recorder Ingest
 *
 * Ingests deeds, mortgages, liens from PublicSearch.us for Ohio counties.
 * Coverage:
 *   - Butler County (Cincinnati suburbs): butler.oh.publicsearch.us
 *   - Franklin County (Columbus): franklin.oh.publicsearch.us
 *   - Cuyahoga County (Cleveland): cuyahoga.oh.publicsearch.us
 *   - Stark County (Canton): stark.oh.publicsearch.us
 *
 * NOT on PublicSearch.us (subdomains ENOTFOUND):
 *   - Warren → Laredo (paid)
 *   - Lake, Mahoning, Summit, Lorain → unknown platform
 *
 * Ohio is a full-disclosure state — consideration amounts are available.
 * Document types: mortgage, deed, quit claim deed, lien, judgment, release
 *
 * Usage:
 *   npx tsx scripts/ingest-oh-publicsearch.ts
 *   npx tsx scripts/ingest-oh-publicsearch.ts --county=Butler
 *   npx tsx scripts/ingest-oh-publicsearch.ts --start=2024-01-01 --end=2025-12-31
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import WebSocket from "ws";
import https from "node:https";
import crypto from "node:crypto";

// ─── Config ──────────────────────────────────────────────────────

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PAGE_SIZE = 250;
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 6;
const WS_TIMEOUT_MS = 60000;

interface CountyConfig {
  name: string;
  subdomain: string;
  state_code: string;
  /** Document type codes to ingest (null = all) */
  docCodes: string[] | null;
}

const COUNTIES: CountyConfig[] = [
  {
    name: "Butler",
    subdomain: "butler",
    state_code: "OH",
    docCodes: null, // ingest all doc types
  },
  {
    name: "Franklin",
    subdomain: "franklin",
    state_code: "OH",
    docCodes: null,
  },
  {
    name: "Cuyahoga",
    subdomain: "cuyahoga",
    state_code: "OH",
    docCodes: null,
  },
  // Warren → Laredo (paid), Lake/Mahoning/Summit/Lorain → unknown platform
  {
    name: "Stark",
    subdomain: "stark",
    state_code: "OH",
    docCodes: null,
  },
];

// ─── CLI Args ────────────────────────────────────────────────────

const args = process.argv.slice(2);
const countyFilter = args.find(a => a.startsWith("--county="))?.split("=")[1];

// Default: last 90 days
const defaultEnd = new Date().toISOString().split("T")[0];
const defaultStart = (() => {
  const d = new Date();
  d.setDate(d.getDate() - 90);
  return d.toISOString().split("T")[0];
})();
const startArg = args.find(a => a.startsWith("--start="))?.split("=")[1] || defaultStart;
const endArg = args.find(a => a.startsWith("--end="))?.split("=")[1] || defaultEnd;

// ─── Helpers ─────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function* monthRanges(start: string, end: string): Generator<{ from: string; to: string }> {
  const startDate = new Date(start + "T00:00:00Z");
  const endDate = new Date(end + "T00:00:00Z");

  let cursor = new Date(startDate);
  while (cursor <= endDate) {
    const monthStart = new Date(cursor);
    const monthEnd = new Date(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 0);
    const clampedEnd = monthEnd > endDate ? endDate : monthEnd;

    yield {
      from: monthStart.toISOString().split("T")[0],
      to: clampedEnd.toISOString().split("T")[0],
    };

    cursor = new Date(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1);
  }
}

function parseRecordedDate(dateStr: string): string | null {
  if (!dateStr) return null;
  const parts = dateStr.split("/");
  if (parts.length !== 3) return null;
  const [month, day, year] = parts;
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}

function classifyDocType(docTypeCode: string, docType: string): string {
  const code = (docTypeCode || "").toUpperCase();
  const type = (docType || "").toUpperCase();

  if (code.includes("MORT") || code === "M" || type.includes("MORTGAGE")) return "mortgage";
  if (code === "DOT" || code.includes("DEED OF TRUST") || type.includes("DEED OF TRUST")) return "deed_of_trust";
  if (code.includes("WD") || code === "DEED" || type.includes("WARRANTY DEED")) return "deed";
  if (code === "QC" || code === "QCD" || type.includes("QUIT CLAIM")) return "deed";
  if (code.includes("LN") || code.includes("LIEN") || type.includes("LIEN")) return "lien";
  if (code.includes("JDG") || code.includes("JUDG") || type.includes("JUDGMENT")) return "judgment";
  if (code.includes("REL") || code.includes("RELEASE") || type.includes("RELEASE") || type.includes("SATISFACTION")) return "satisfaction";
  if (code.includes("ASSIGN") || type.includes("ASSIGNMENT")) return "assignment";
  return code.toLowerCase().replace(/[^a-z0-9_]/g, "_") || "other";
}

// ─── WebSocket Session ────────────────────────────────────────────

interface SessionInfo {
  cookies: string;
  ort: string;
}

async function getSession(host: string): Promise<SessionInfo> {
  return new Promise((resolve, reject) => {
    https.get(`https://${host}/`, (res) => {
      let body = "";
      const cookies = res.headers["set-cookie"] || [];
      const cookieStr = cookies.map((c: string) => c.split(";")[0]).join("; ");

      res.on("data", (d: Buffer) => body += d);
      res.on("end", () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error("Failed to get __ort token from " + host));
        resolve({ cookies: cookieStr, ort });
      });
      res.on("error", reject);
    }).on("error", reject);
  });
}

interface PublicSearchDoc {
  instrumentNumber: string;
  docType: string;
  docTypeCode: string;
  recordedDate: string;
  grantor: string[];
  grantee: string[];
  bookVolumePage: string;
  considerationAmount?: number;
}

interface SearchResult {
  total: number;
  documents: PublicSearchDoc[];
}

/**
 * Fetch all pages for a date range using a single persistent WebSocket connection.
 * The server keeps the connection open after responding — reusing it avoids
 * per-connection rate limits that cause timeouts when a new WS is opened per page.
 */
async function fetchAllPages(
  host: string,
  session: SessionInfo,
  dateRange: string,
  county: CountyConfig,
): Promise<SearchResult[]> {
  // Try up to MAX_RETRIES times to open a connection and drain all pages
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fetchAllPagesOnce(host, session, dateRange, county);
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(5000 * attempt, 30000);
      console.warn(`    Retry ${attempt}/${MAX_RETRIES}: ${err.message} (wait ${delay}ms)`);
      await sleep(delay);
      try {
        Object.assign(session, await getSession(host));
        console.warn(`    Session refreshed`);
      } catch { /* keep old session */ }
    }
  }
  throw new Error("Should not reach here");
}

async function fetchAllPagesOnce(
  host: string,
  session: SessionInfo,
  dateRange: string,
  county: CountyConfig,
): Promise<SearchResult[]> {
  // Open one WS connection and send all page queries sequentially on it.
  // Each sendNextPage() call fires a query; the message handler resolves
  // the current pending promise, then fires the next query.
  const pages: SearchResult[] = [];

  await new Promise<void>((resolve, reject) => {
    const ws = new WebSocket(`wss://${host}/ws`, {
      headers: { Cookie: session.cookies, Origin: `https://${host}` },
    });

    let totalRecords = -1;
    let currentOffset = 0;
    let pageTimeout: ReturnType<typeof setTimeout> | null = null;
    let settled = false;

    function armTimeout() {
      if (pageTimeout) clearTimeout(pageTimeout);
      pageTimeout = setTimeout(() => {
        pageTimeout = null;
        if (!settled) { settled = true; ws.terminate(); reject(new Error("WebSocket timeout")); }
      }, WS_TIMEOUT_MS);
    }

    function sendNextPage(offset: number) {
      const query: Record<string, unknown> = {
        limit: String(PAGE_SIZE),
        offset: String(offset),
        department: "RP",
        recordedDateRange: dateRange,
        searchOcrText: false,
        searchType: "quickSearch",
      };
      if (county.docCodes) query._docTypes = county.docCodes;
      armTimeout();
      ws.send(JSON.stringify({
        type: "@kofile/FETCH_DOCUMENTS/v4",
        payload: { query, workspaceID: crypto.randomUUID().substring(0, 20) },
        authToken: session.ort,
        ip: "",
        correlationId: crypto.randomUUID(),
        sync: true,
      }));
    }

    ws.on("open", () => sendNextPage(0));

    ws.on("message", (data: Buffer) => {
      try {
        const parsed = JSON.parse(data.toString());
        if (parsed.type === "@kofile/FETCH_DOCUMENTS_FULFILLED/v6") {
          if (pageTimeout) { clearTimeout(pageTimeout); pageTimeout = null; }

          const byOrder: number[] = parsed.payload?.data?.byOrder || [];
          const byHash: Record<string, PublicSearchDoc> = parsed.payload?.data?.byHash || {};
          const documents: PublicSearchDoc[] = byOrder.map((id: number) => byHash[id]).filter(Boolean);
          const result: SearchResult = {
            total: parsed.payload?.meta?.numRecords || 0,
            documents,
          };

          if (totalRecords < 0) totalRecords = result.total;
          pages.push(result);
          currentOffset += PAGE_SIZE;

          if (currentOffset >= totalRecords || documents.length === 0) {
            settled = true;
            ws.close();
            resolve();
          } else {
            // More pages — send next query on the same connection
            sendNextPage(currentOffset);
          }
        } else if (parsed.type === "@kofile/FETCH_DOCUMENTS_REJECTED/v1") {
          if (pageTimeout) { clearTimeout(pageTimeout); pageTimeout = null; }
          settled = true;
          ws.close();
          reject(new Error(`Search rejected: ${JSON.stringify(parsed.payload?.errors || "unknown")}`));
        }
      } catch { /* ignore non-matching */ }
    });

    ws.on("error", (err: Error) => {
      if (pageTimeout) { clearTimeout(pageTimeout); pageTimeout = null; }
      if (!settled) { settled = true; reject(err); }
    });

    ws.on("close", (code: number) => {
      if (pageTimeout) { clearTimeout(pageTimeout); pageTimeout = null; }
      if (!settled) {
        settled = true;
        reject(new Error(`WS closed unexpectedly (code ${code}) after ${pages.length} pages`));
      }
    });
  });

  return pages;
}

async function getExistingDocNumbers(docNumbers: string[], sourceUrl: string): Promise<Set<string>> {
  const existing = new Set<string>();
  if (docNumbers.length === 0) return existing;

  for (let i = 0; i < docNumbers.length; i += 200) {
    const chunk = docNumbers.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .eq("source_url", sourceUrl)
      .in("document_number", chunk);
    if (data) {
      for (const row of data) existing.add(row.document_number);
    }
  }
  return existing;
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Ohio PublicSearch.us Recorder Ingest");
  console.log("═".repeat(60));
  console.log(`Date range: ${startArg} to ${endArg}`);
  console.log(`Counties: ${countyFilter || "all (Butler, Franklin, Cuyahoga, Warren, Lake, Mahoning, Summit, Lorain, Stark)"}\n`);

  const counties = countyFilter
    ? COUNTIES.filter(c => c.name.toLowerCase() === countyFilter.toLowerCase())
    : COUNTIES;

  if (counties.length === 0) {
    console.error(`County "${countyFilter}" not found. Use: Butler, Franklin, Cuyahoga, Stark`);
    process.exit(1);
  }

  // Look up county IDs
  const countyIds: Record<string, number> = {};
  for (const county of counties) {
    const { data } = await db.from("counties")
      .select("id")
      .eq("county_name", county.name)
      .eq("state_code", county.state_code)
      .single();
    if (data) {
      countyIds[county.name] = data.id;
      console.log(`  ✓ ${county.name} County, OH: DB id=${data.id}`);
    } else {
      console.warn(`  ⚠ ${county.name} County, OH not in DB — property linking will be skipped`);
    }
  }

  let grandInserted = 0;
  let grandDupes = 0;
  let grandErrors = 0;

  for (const county of counties) {
    const host = `${county.subdomain}.oh.publicsearch.us`;
    const sourceUrl = `https://${host}/`;

    console.log(`\n${"═".repeat(60)}`);
    console.log(`  ${county.name} County, OH — ${host}`);
    console.log(`${"═".repeat(60)}`);

    let countyInserted = 0;
    let countyDupes = 0;
    let countyErrors = 0;

    let session: SessionInfo;
    try {
      session = await getSession(host);
      console.log("  Session acquired ✓");
    } catch (err: any) {
      console.error(`  Failed to get session: ${err.message}`);
      continue;
    }

    for (const range of monthRanges(startArg, endArg)) {
      const dateRange = `${range.from},${range.to}`;
      process.stdout.write(`\n  ${range.from} → ${range.to}:`);

      try {
        // Fetch all pages for this month over a single persistent WS connection
        const allPages = await fetchAllPages(host, session, dateRange, county);
        const totalRecords = allPages[0]?.total ?? 0;
        process.stdout.write(` ${totalRecords} records`);

        for (const page of allPages) {
          if (page.documents.length === 0) continue;

          const docNumbers = page.documents.map(d => d.instrumentNumber).filter(Boolean);
          const existingDocs = await getExistingDocNumbers(docNumbers, sourceUrl);
          const batch: Array<Record<string, unknown>> = [];

          for (const doc of page.documents) {
            const docNumber = doc.instrumentNumber;
            if (!docNumber || existingDocs.has(docNumber)) {
              countyDupes++;
              continue;
            }

            let bookPage: string | null = null;
            if (doc.bookVolumePage && doc.bookVolumePage !== "--/--/--") {
              const parts = doc.bookVolumePage.split("/");
              if (parts.length >= 3 && parts[0] !== "--") {
                bookPage = `${parts[0]}-${parts[2]}`;
              }
            }

            const borrowerName = doc.grantor?.filter((s: string) => s).join("; ") || null;
            const lenderName = doc.grantee?.filter((s: string) => s).join("; ") || null;
            const documentType = classifyDocType(doc.docTypeCode, doc.docType);
            const loanAmount = doc.considerationAmount && doc.considerationAmount > 0
              ? doc.considerationAmount : null;

            batch.push({
              property_id: null,
              document_type: documentType,
              recording_date: parseRecordedDate(doc.recordedDate),
              loan_amount: loanAmount,
              original_amount: loanAmount,
              borrower_name: borrowerName?.slice(0, 500),
              lender_name: lenderName?.slice(0, 500),
              document_number: docNumber,
              book_page: bookPage,
              source_url: sourceUrl,
              interest_rate_type: "unknown",
            });
          }

          if (batch.length > 0) {
            const { error } = await db.from("mortgage_records").insert(batch);
            if (error) {
              for (const record of batch) {
                const { error: e2 } = await db.from("mortgage_records").insert(record);
                if (e2) {
                  if (countyErrors < 5) console.error(`\n  Single error: ${JSON.stringify(e2).slice(0, 150)}`);
                  countyErrors++;
                } else {
                  countyInserted++;
                }
              }
            } else {
              countyInserted += batch.length;
            }
          }
        }

        process.stdout.write(` | ${countyInserted} ins`);
      } catch (err: any) {
        console.error(`\n  Error for ${range.from}→${range.to}: ${err.message}`);
        countyErrors++;
      }

      await sleep(RATE_LIMIT_MS);
    }

    console.log(`\n\n  ${county.name} County TOTAL: ${countyInserted} inserted, ${countyDupes} dupes, ${countyErrors} errors`);
    grandInserted += countyInserted;
    grandDupes += countyDupes;
    grandErrors += countyErrors;
  }

  console.log(`\n${"═".repeat(60)}`);
  console.log(`GRAND TOTAL: ${grandInserted} inserted, ${grandDupes} dupes, ${grandErrors} errors`);
  console.log("Done.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
