#!/usr/bin/env tsx
/**
 * MXRE — Montgomery County, OH Recorder Ingest (RISS)
 *
 * Source: Montgomery County Recorder RISS (ColdFusion)
 *   https://riss.mcrecorder.org/
 *
 * Flow: GET index.cfm (session) -> POST disclaimer -> POST wildcard name search by day.
 * No login required. Max 1250 records/query; daily windows stay under limit (~478/day avg).
 *
 * Usage:
 *   npx tsx scripts/ingest-montgomery-oh-recorder.ts
 *   npx tsx scripts/ingest-montgomery-oh-recorder.ts --start=2020-01-01 --end=2026-04-07
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(
  process.env.SUPABASE_URL as string,
  process.env.SUPABASE_SERVICE_KEY as string,
  { auth: { persistSession: false } },
);

const BASE_URL = "https://riss.mcrecorder.org";
const RATE_LIMIT_MS = 800;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const args = process.argv.slice(2);
const today = new Date().toISOString().split("T")[0];
const defaultStart = (() => {
  const d = new Date();
  d.setDate(d.getDate() - 90);
  return d.toISOString().split("T")[0];
})();
const startArg = args.find(a => a.startsWith("--start="))?.split("=")[1] || defaultStart;
const endArg   = args.find(a => a.startsWith("--end="))?.split("=")[1]   || today;

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

function* dayRanges(start: string, end: string): Generator<string> {
  const endDate = new Date(end + "T00:00:00Z");
  let cursor = new Date(start + "T00:00:00Z");
  while (cursor <= endDate) {
    yield cursor.toISOString().split("T")[0];
    cursor = new Date(cursor.getTime() + 86400000);
  }
}

function classifyDocType(desc: string): string {
  const d = (desc || "").toUpperCase();
  if (d.includes("RELEASE") || d.includes("SATISFACTION") || d.includes("DISCHARGE")) return "release";
  if (d.includes("ASSIGNMENT")) return "assignment";
  if (d.includes("MODIFICATION") || d.includes("EXTENSION") || d.includes("AMENDMENT")) return "modification";
  if (d.includes("MORTGAGE") || d.includes("DEED OF TRUST")) return "mortgage";
  if (d.includes("MECHANIC") || d.includes("MATERIALMAN")) return "mechanics_lien";
  if (d.includes("FEDERAL TAX") || d.includes("IRS")) return "federal_tax_lien";
  if (d.includes("STATE TAX") || d.includes("OHIO TAX")) return "state_tax_lien";
  if (d.includes("TAX LIEN")) return "state_tax_lien";
  if (d.includes("JUDGMENT") || d.includes("JUDGEMENT")) return "judgment";
  if (d.includes("LIEN")) return "lien";
  if (d.includes("QUIT CLAIM")) return "quit_claim_deed";
  if (d.includes("DEED")) return "deed";
  return "other";
}

function stripHtml(s: string): string {
  return s.replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&#x[0-9a-f]+;/gi, "").replace(/\s+/g, " ").trim();
}

function parseDate(mmddyyyy: string): string | null {
  const m = mmddyyyy.trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[1]}-${m[2]}`;
}

// Session

interface Session { cookies: string; }

async function getSession(): Promise<Session> {
  const r = await fetch(`${BASE_URL}/index.cfm`, {
    redirect: "follow",
    headers: { "User-Agent": "Mozilla/5.0 (compatible; MXRE/1.0)" },
  });
  if (!r.ok) throw new Error(`RISS init: HTTP ${r.status}`);

  const cookies = (r.headers.getSetCookie?.() || [])
    .map(c => c.split(";")[0]).filter(c => c.includes("=")).join("; ");
  if (!cookies) throw new Error("No cookies from RISS");

  await fetch(`${BASE_URL}/document_search/byname.cfm`, {
    method: "POST",
    headers: {
      "Cookie": cookies,
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": "Mozilla/5.0 (compatible; MXRE/1.0)",
    },
    body: "u_disclaimer=on",
  });
  return { cookies };
}

// Parse

interface RissRecord {
  instrumentNumber: string;
  docMasterKey: string;
  docType: string;
  docTypeRaw: string;
  fileDate: string;
  grantorName: string;
  granteeName: string;
  legalDescription: string;
  referenceInstrument: string;
}

function parseResults(html: string): RissRecord[] {
  const records: RissRecord[] = [];
  const tbodyMatch = html.match(/<tbody>([\s\S]*?)<\/tbody>/i);
  if (!tbodyMatch) return records;

  const rows = tbodyMatch[1].split(/<TR[\s>]/i).slice(1);
  for (const row of rows) {
    const tds = [...row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map(m => m[1]);
    if (tds.length < 7) continue;

    const docTypeRaw = stripHtml(tds[1]).replace(/\s*Mortgages\s*$/i, "").trim();
    const fileDate   = parseDate(stripHtml(tds[2]).trim());
    if (!fileDate) continue;

    const matchName   = stripHtml(tds[3]).trim();
    const partiesRaw  = tds[4].replace(/<br\s*\/?>/gi, "\n").replace(/<[^>]+>/g, "");
    const parts       = partiesRaw.split(/[\n/]/).map(s => s.trim()).filter(Boolean);
    const grantorName = parts[0] || matchName;
    const granteeName = parts.slice(1).join(" / ").trim();

    const legalDescription = stripHtml(tds[5]).trim();

    const instrMatch = tds[6].match(/type="submit"[^>]*value="([^"]+)"/i);
    const instrumentNumber = instrMatch ? instrMatch[1].trim() : "";
    if (!instrumentNumber) continue;

    const mkMatch = tds[6].match(/name="docmasterkey"\s+value="([^"]+)"/i);
    const docMasterKey = mkMatch ? mkMatch[1].trim() : "";

    const referenceInstrument = stripHtml(tds[7] || "").trim();

    records.push({
      instrumentNumber, docMasterKey,
      docType: classifyDocType(docTypeRaw), docTypeRaw,
      fileDate, grantorName, granteeName, legalDescription, referenceInstrument,
    });
  }
  return records;
}

async function fetchPage(session: Session, date: string, lastNamePrefix: string): Promise<{ records: RissRecord[]; overflow: boolean }> {
  const body = new URLSearchParams({
    LastName: lastNamePrefix === "%" ? "%" : `${lastNamePrefix}%`,
    LastName_Search: lastNamePrefix === "%" ? "Contains" : "BeginWith",
    FirstName: "", FirstName_Search: "BeginWith",
    Side: "%", IndexType: "MTG",
    StartDate: date, EndDate: date,
    Sort: "CAST([FileDate] as DATE) ASC",
  });

  const res = await fetch(`${BASE_URL}/name_search/all_matches.cfm`, {
    method: "POST",
    headers: {
      "Cookie": session.cookies,
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": "Mozilla/5.0 (compatible; MXRE/1.0)",
      "Referer": `${BASE_URL}/document_search/byname.cfm`,
    },
    body: body.toString(),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const html = await res.text();
  if (html.includes("Session has Timed Out")) throw new Error("SESSION_EXPIRED");
  const overflow = html.includes("Record Count Exceeds");
  return { records: overflow ? [] : parseResults(html), overflow };
}

async function fetchDay(session: Session, date: string): Promise<RissRecord[]> {
  const { records, overflow } = await fetchPage(session, date, "%");
  if (!overflow) return records;

  // Overflow: split A-Z to stay under 1250 limit per bucket
  const allRecords = new Map<string, RissRecord>();
  for (const letter of "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split("")) {
    await sleep(500);
    const { records: sub, overflow: subOvf } = await fetchPage(session, date, letter);
    if (subOvf) process.stdout.write(`(${letter}:OVF!) `);
    for (const r of sub) allRecords.set(r.instrumentNumber, r);
  }
  return Array.from(allRecords.values());
}

// DB

async function getExistingDocNumbers(docNumbers: string[]): Promise<Set<string>> {
  const existing = new Set<string>();
  for (let i = 0; i < docNumbers.length; i += 500) {
    const chunk = docNumbers.slice(i, i + 500);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .ilike("source_url", "%riss.mcrecorder.org%")
      .in("document_number", chunk);
    for (const row of (data || [])) existing.add(row.document_number);
  }
  return existing;
}

async function insertBatch(records: RissRecord[]): Promise<{ inserted: number; dupes: number; errors: number }> {
  if (!records.length) return { inserted: 0, dupes: 0, errors: 0 };

  // Pre-check existing to avoid duplicates
  const allNums = records.map(r => r.instrumentNumber);
  const existing = await getExistingDocNumbers(allNums);

  const newRecords = records.filter(r => !existing.has(r.instrumentNumber));
  const dupes = records.length - newRecords.length;

  if (!newRecords.length) return { inserted: 0, dupes, errors: 0 };

  const rows = newRecords.map(r => ({
    property_id: null,
    document_type: r.docType,
    recording_date: r.fileDate,
    borrower_name: r.grantorName.slice(0, 500),
    lender_name: r.granteeName.slice(0, 500),
    document_number: r.instrumentNumber,
    book_page: null,
    loan_amount: null,
    original_amount: null,
    interest_rate_type: "unknown",
    source_url: BASE_URL + "/",
  }));

  let inserted = 0, errors = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const { error } = await db.from("mortgage_records").insert(batch);
    if (error) {
      // Try one by one to skip any single bad row
      for (const row of batch) {
        const { error: e2 } = await db.from("mortgage_records").insert(row);
        if (e2) { errors++; }
        else { inserted++; }
      }
    } else {
      inserted += batch.length;
    }
  }
  return { inserted, dupes, errors };
}

// Main

async function main() {
  console.log("MXRE - Montgomery County, OH RISS Recorder Ingest");
  console.log("=".repeat(60));
  console.log(`Date range: ${startArg} to ${endArg}`);

  console.log("  Montgomery County, OH: riss.mcrecorder.org");

  let session = await getSession();
  console.log("  Session acquired\n");
  console.log("=".repeat(60));
  console.log(`  Montgomery County, OH - riss.mcrecorder.org`);
  console.log("=".repeat(60));

  let totalInserted = 0, totalDupes = 0, totalErrors = 0;

  for (const day of dayRanges(startArg, endArg)) {
    process.stdout.write(`  ${day}: `);

    let records: RissRecord[] = [];
    let retries = 0;

    while (retries <= MAX_RETRIES) {
      try {
        records = await fetchDay(session, day);
        break;
      } catch (err: any) {
        retries++;
        if (retries > MAX_RETRIES) {
          process.stdout.write(`\n  Error for ${day}: ${err.message}\n`);
          totalErrors++;
          break;
        }
        const wait = retries * 6000;
        process.stdout.write(`\n    Retry ${retries}/${MAX_RETRIES}: ${err.message} (wait ${wait}ms)\n  `);
        await sleep(wait);
        try { session = await getSession(); } catch {}
      }
    }

    if (records.length > 0) {
      const { inserted, dupes, errors } = await insertBatch(records);
      totalInserted += inserted;
      totalDupes    += dupes;
      totalErrors   += errors;
      process.stdout.write(`${records.length} records | ${totalInserted} ins\n`);
    } else if (retries <= MAX_RETRIES) {
      process.stdout.write("0 records\n");
    }

    await sleep(RATE_LIMIT_MS);
  }

  console.log("\n" + "=".repeat(60));
  console.log(`GRAND TOTAL: ${totalInserted} inserted, ${totalDupes} dupes, ${totalErrors} errors`);
  console.log("Done.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
