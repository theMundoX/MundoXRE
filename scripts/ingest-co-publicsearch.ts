#!/usr/bin/env tsx
/**
 * MXRE — Colorado PublicSearch.us Deed of Trust Ingest
 *
 * Scrapes deed of trust (DOT) recordings from PublicSearch.us via WebSocket API
 * for Denver, Arapahoe, and Boulder counties. Loads into mortgage_records table.
 *
 * Colorado is non-disclosure — no loan amounts in the index.
 *
 * Usage:
 *   npx tsx scripts/ingest-co-publicsearch.ts
 *   npx tsx scripts/ingest-co-publicsearch.ts --county=Denver
 *   npx tsx scripts/ingest-co-publicsearch.ts --start=2025-06-01 --end=2025-12-31
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

const BATCH_SIZE = 250;
const PAGE_SIZE = 250;
const RATE_LIMIT_MS = 500;
const MAX_RETRIES = 3;
const WS_TIMEOUT_MS = 45000;

interface CountyConfig {
  name: string;
  subdomain: string;
  state_code: string;
  /** Doc type codes for deed of trust in this county's system */
  dotCodes: string[];
  /** If true, filter must happen client-side (API doesn't support _docTypes) */
  clientFilter: boolean;
}

const COUNTIES: CountyConfig[] = [
  { name: "Denver", subdomain: "denver", state_code: "CO", dotCodes: ["DOT"], clientFilter: false },
  { name: "Arapahoe", subdomain: "arapahoe", state_code: "CO", dotCodes: ["DT"], clientFilter: true },
  { name: "Boulder", subdomain: "boulder", state_code: "CO", dotCodes: ["DT", "MORTGAGE"], clientFilter: true },
];

// ─── CLI Args ────────────────────────────────────────────────────

const args = process.argv.slice(2);
const countyFilter = args.find(a => a.startsWith("--county="))?.split("=")[1];
const startArg = args.find(a => a.startsWith("--start="))?.split("=")[1] || "2024-01-01";
const endArg = args.find(a => a.startsWith("--end="))?.split("=")[1] || "2026-03-29";

// ─── Helpers ─────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Generate month ranges between start and end dates */
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

/** Format date string like "1/31/2025" to "2025-01-31" */
function parseRecordedDate(dateStr: string): string | null {
  if (!dateStr) return null;
  const parts = dateStr.split("/");
  if (parts.length !== 3) return null;
  const [month, day, year] = parts;
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}

// ─── WebSocket Client ────────────────────────────────────────────

interface SessionInfo {
  cookies: string;
  ort: string;
}

async function getSession(host: string): Promise<SessionInfo> {
  return new Promise((resolve, reject) => {
    https.get(`https://${host}/`, (res) => {
      let body = "";
      const cookies = res.headers["set-cookie"] || [];
      const cookieStr = cookies.map(c => c.split(";")[0]).join("; ");

      res.on("data", d => body += d);
      res.on("end", () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error("Failed to get __ort token"));
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
}

interface SearchResult {
  total: number;
  documents: PublicSearchDoc[];
}

async function fetchPage(
  host: string,
  session: SessionInfo,
  dateRange: string,
  offset: number,
  county: CountyConfig,
): Promise<SearchResult> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`wss://${host}/ws`, {
      headers: {
        Cookie: session.cookies,
        Origin: `https://${host}`,
      },
    });

    const timeout = setTimeout(() => {
      ws.close();
      reject(new Error("WebSocket timeout"));
    }, WS_TIMEOUT_MS);

    ws.on("open", () => {
      const query: Record<string, unknown> = {
        limit: String(PAGE_SIZE),
        offset: String(offset),
        department: "RP",
        recordedDateRange: dateRange,
        searchOcrText: false,
        searchType: "quickSearch",
      };

      // Some counties support server-side doc type filtering
      if (!county.clientFilter) {
        query._docTypes = county.dotCodes;
      }

      const msg = {
        type: "@kofile/FETCH_DOCUMENTS/v4",
        payload: {
          query,
          workspaceID: crypto.randomUUID().substring(0, 20),
        },
        authToken: session.ort,
        ip: "",
        correlationId: crypto.randomUUID(),
        sync: true,
      };
      ws.send(JSON.stringify(msg));
    });

    ws.on("message", (data) => {
      try {
        const parsed = JSON.parse(data.toString());
        if (parsed.type === "@kofile/FETCH_DOCUMENTS_FULFILLED/v6") {
          clearTimeout(timeout);
          const payload = parsed.payload;
          const documents: PublicSearchDoc[] = [];
          const byOrder: number[] = payload.data?.byOrder || [];
          const byHash: Record<string, PublicSearchDoc> = payload.data?.byHash || {};

          for (const id of byOrder) {
            if (byHash[id]) documents.push(byHash[id]);
          }

          ws.close();
          resolve({
            total: payload.meta?.numRecords || 0,
            documents,
          });
        } else if (parsed.type === "@kofile/FETCH_DOCUMENTS_REJECTED/v1") {
          clearTimeout(timeout);
          ws.close();
          reject(new Error(`Search rejected: ${JSON.stringify(parsed.payload?.errors || "unknown")}`));
        }
      } catch (e) {
        // Ignore non-matching messages
      }
    });

    ws.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });

    ws.on("close", (code) => {
      clearTimeout(timeout);
      if (code !== 1000 && code !== 1005) {
        reject(new Error(`WS closed with code ${code}`));
      }
    });
  });
}

async function fetchPageWithRetry(
  host: string,
  session: SessionInfo,
  dateRange: string,
  offset: number,
  county: CountyConfig,
): Promise<SearchResult> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fetchPage(host, session, dateRange, offset, county);
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      console.warn(`    Retry ${attempt}/${MAX_RETRIES}: ${err.message}`);
      await sleep(3000 * attempt);
      // Always refresh session on retry
      try {
        const newSession = await getSession(host);
        Object.assign(session, newSession);
        console.warn(`    Session refreshed`);
      } catch { /* keep old session */ }
    }
  }
  throw new Error("Should not reach here");
}

// ─── Dedup ───────────────────────────────────────────────────────

async function getExistingDocNumbers(docNumbers: string[], sourceUrl: string): Promise<Set<string>> {
  const existing = new Set<string>();
  if (docNumbers.length === 0) return existing;

  // Query in chunks of 200 to avoid URL length limits
  for (let i = 0; i < docNumbers.length; i += 200) {
    const chunk = docNumbers.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .eq("source_url", sourceUrl)
      .in("document_number", chunk);
    if (data) {
      for (const row of data) {
        existing.add(row.document_number);
      }
    }
  }
  return existing;
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Colorado PublicSearch.us Deed of Trust Ingest\n");
  console.log(`Date range: ${startArg} to ${endArg}`);
  console.log(`Counties: ${countyFilter || "all (Denver, Arapahoe, Boulder)"}\n`);

  const counties = countyFilter
    ? COUNTIES.filter(c => c.name.toLowerCase() === countyFilter.toLowerCase())
    : COUNTIES;

  if (counties.length === 0) {
    console.error(`County "${countyFilter}" not found. Use: Denver, Arapahoe, or Boulder`);
    process.exit(1);
  }

  // Look up county IDs (for property linking)
  const countyIds: Record<string, number> = {};
  for (const county of counties) {
    const { data } = await db.from("counties")
      .select("id")
      .eq("county_name", county.name)
      .eq("state_code", county.state_code)
      .single();
    if (data) {
      countyIds[county.name] = data.id;
    } else {
      console.warn(`  County ${county.name}, ${county.state_code} not found in DB — will skip property linking`);
    }
  }

  let grandTotal = 0;
  let grandDupes = 0;
  let grandErrors = 0;

  for (const county of counties) {
    const host = `${county.subdomain}.co.publicsearch.us`;
    console.log(`\n${"═".repeat(60)}`);
    console.log(`  ${county.name} County, CO — ${host}`);
    console.log(`${"═".repeat(60)}`);

    let countyInserted = 0;
    let countyDupes = 0;
    let countyErrors = 0;

    // Get session
    let session: SessionInfo;
    try {
      session = await getSession(host);
      console.log("  Session acquired");
    } catch (err: any) {
      console.error(`  Failed to get session: ${err.message}`);
      continue;
    }

    for (const range of monthRanges(startArg, endArg)) {
      const dateRange = `${range.from},${range.to}`;
      console.log(`\n  --- ${range.from} to ${range.to} ---`);

      let offset = 0;
      let total = -1;
      let monthInserted = 0;

      while (true) {
        try {
          const result = await fetchPageWithRetry(host, session, dateRange, offset, county);
          if (total < 0) {
            total = result.total;
            if (county.clientFilter) {
              console.log(`  Found ${total} total records (filtering client-side for ${county.dotCodes.join(",")})`);
            } else {
              console.log(`  Found ${total} deeds of trust`);
            }
          }

          if (result.documents.length === 0) break;

          // Client-side doc type filtering if needed
          const docs = county.clientFilter
            ? result.documents.filter(d => county.dotCodes.includes(d.docTypeCode))
            : result.documents;

          const sourceUrl = `https://${host}/`;
          const docNumbers = docs.map(d => d.instrumentNumber).filter(Boolean);
          const existingDocs = await getExistingDocNumbers(docNumbers, sourceUrl);

          const batch: Array<Record<string, unknown>> = [];

          for (const doc of docs) {
            const docNumber = doc.instrumentNumber;

            // Check for duplicates
            if (existingDocs.has(docNumber)) {
              countyDupes++;
              continue;
            }

            // Parse book/page from bookVolumePage format: "BOOK/VOL/PAGE" or "--/--/--"
            let bookPage: string | null = null;
            if (doc.bookVolumePage && doc.bookVolumePage !== "--/--/--") {
              const parts = doc.bookVolumePage.split("/");
              if (parts.length >= 3 && parts[0] !== "--" && parts[2] !== "--") {
                bookPage = `${parts[0]}-${parts[2]}`;
              }
            }

            // For deed of trust: grantor = borrower signs it, grantee = lender/trustee
            // But in PublicSearch: grantor is listed first (the borrower), grantee is the lender/trustee
            const borrowerName = doc.grantor?.filter(s => s).join("; ") || null;
            const lenderName = doc.grantee?.filter(s => s).join("; ") || null;

            batch.push({
              property_id: null,
              document_type: "deed_of_trust",
              recording_date: parseRecordedDate(doc.recordedDate),
              loan_amount: null, // CO non-disclosure
              original_amount: null,
              borrower_name: borrowerName?.slice(0, 500),
              lender_name: lenderName?.slice(0, 500),
              document_number: docNumber,
              book_page: bookPage,
              source_url: sourceUrl,
            });
          }

          // Insert batch
          if (batch.length > 0) {
            const { error } = await db.from("mortgage_records").insert(batch);
            if (error) {
              console.error(`\n  Batch error: ${JSON.stringify(error).slice(0,200)}`);
              console.error(`  Sample record: ${JSON.stringify(batch[0]).slice(0,300)}`);
              // Retry individually for partial dupes
              for (const record of batch) {
                const { error: e2 } = await db.from("mortgage_records").insert(record);
                if (e2) {
                  if (countyErrors < 3) console.error(`  Single error: ${JSON.stringify(e2).slice(0,200)}`);
                  countyErrors++;
                } else {
                  countyInserted++;
                  monthInserted++;
                }
              }
            } else {
              countyInserted += batch.length;
              monthInserted += batch.length;
            }
          }

          offset += PAGE_SIZE;

          // Progress every 1000 records
          if ((countyInserted + countyDupes) % 1000 < PAGE_SIZE) {
            process.stdout.write(`\r  Progress: ${countyInserted} inserted, ${countyDupes} dupes, ${countyErrors} errors`);
          }

          if (offset >= total) break;

          // Rate limit
          await sleep(RATE_LIMIT_MS);

          // Refresh session every 20 pages to avoid token expiry
          if (offset % (PAGE_SIZE * 20) === 0) {
            try {
              session = await getSession(host);
            } catch { /* keep old session */ }
          }
        } catch (err: any) {
          console.error(`\n  Error at offset ${offset}: ${err.message}`);
          countyErrors++;
          break;
        }
      }

      console.log(`\n  ${range.from}: ${monthInserted} inserted`);
    }

    console.log(`\n  ${county.name} totals: ${countyInserted} inserted, ${countyDupes} dupes, ${countyErrors} errors`);
    grandTotal += countyInserted;
    grandDupes += countyDupes;
    grandErrors += countyErrors;
  }

  // ─── Link records to properties ────────────────────────────────

  console.log("\n\nLinking records to properties...");
  let totalLinked = 0;

  for (const county of counties) {
    const countyId = countyIds[county.name];
    const host = `${county.subdomain}.co.publicsearch.us`;
    const sourceUrl = `https://${host}/`;

    if (!countyId) {
      console.log(`  ${county.name}: no county_id, skipping linking`);
      continue;
    }

    // Get unlinked records for this county
    const { data: records, error: fetchErr } = await db.from("mortgage_records")
      .select("id, borrower_name, lender_name")
      .is("property_id", null)
      .eq("source_url", sourceUrl)
      .limit(10000);

    if (fetchErr || !records || records.length === 0) {
      console.log(`  ${county.name}: no unlinked records`);
      continue;
    }

    const { count: propCount } = await db.from("properties")
      .select("*", { count: "exact", head: true })
      .eq("county_id", countyId);

    if (!propCount || propCount < 10) {
      console.log(`  ${county.name}: only ${propCount} properties, skipping linking`);
      continue;
    }

    console.log(`  ${county.name}: linking ${records.length} records against ${propCount} properties...`);
    let linked = 0;

    for (const rec of records) {
      // Try borrower_name (grantor in DOT = borrower/property owner)
      const name = (rec.borrower_name || "").trim().toUpperCase();
      if (!name || name.length < 3) continue;

      // Skip if it looks like a company
      if (/\b(LLC|INC|CORP|BANK|MORTGAGE|CREDIT UNION|TRUST CO|NATIONAL|FEDERAL|LENDING|LOAN|FINANCIAL|SERVIC|ASSOC|INSURANCE|SAVINGS)\b/.test(name)) continue;

      // Extract name parts
      const nameParts = name.replace(/[,;]/g, " ").replace(/\s+/g, " ").trim()
        .split(" ").filter(p => p.length > 2 && !["THE", "AND", "FOR", "JR", "SR", "III", "II"].includes(p));

      if (nameParts.length === 0) continue;

      // Search by first name part (usually last name)
      const { data: properties } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", countyId)
        .ilike("owner_name", `%${nameParts[0]}%`)
        .limit(20);

      if (!properties || properties.length === 0) continue;

      // Score matches
      let bestMatch: { id: number; score: number } | null = null;
      for (const prop of properties) {
        const ownerUpper = (prop.owner_name || "").toUpperCase();
        let score = 0;
        for (const part of nameParts) {
          if (ownerUpper.includes(part)) score++;
        }
        const minRequired = Math.min(2, nameParts.length);
        if (score >= minRequired && (!bestMatch || score > bestMatch.score)) {
          bestMatch = { id: prop.id, score };
        }
      }

      if (bestMatch) {
        await db.from("mortgage_records").update({ property_id: bestMatch.id }).eq("id", rec.id);
        linked++;
        totalLinked++;
      }
    }

    console.log(`  ${county.name}: linked ${linked} / ${records.length}`);
  }

  // ─── Refresh materialized views ────────────────────────────────

  console.log("\nRefreshing materialized views...");
  for (const view of ["county_lien_counts", "county_stats_mv"]) {
    try {
      const { error } = await db.rpc("exec_sql", { sql_text: `REFRESH MATERIALIZED VIEW ${view}` });
      if (error) {
        // Fallback to direct fetch
        await fetch(`${process.env.SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
          method: "POST",
          headers: {
            apikey: process.env.SUPABASE_SERVICE_KEY!,
            Authorization: `Bearer ${process.env.SUPABASE_SERVICE_KEY!}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ sql_text: `REFRESH MATERIALIZED VIEW ${view}` }),
        });
      }
      console.log(`  ${view}: refreshed`);
    } catch (err: any) {
      console.warn(`  ${view}: failed (${err.message})`);
    }
  }

  // ─── Summary ───────────────────────────────────────────────────

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  Grand total inserted: ${grandTotal}`);
  console.log(`  Duplicates skipped:   ${grandDupes}`);
  console.log(`  Errors:               ${grandErrors}`);
  console.log(`  Records linked:       ${totalLinked}`);
  console.log(`  DB total records:     ${count}`);
  console.log(`${"═".repeat(60)}`);
}

main().catch(console.error);
