#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium, type Page } from "playwright";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(firstEnv("SUPABASE_URL")!, firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY")!, {
  auth: { persistSession: false },
});

const args = process.argv.slice(2);
const arg = (name: string) => args.find((a) => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const DRY_RUN = args.includes("--dry-run");
const LIMIT = Math.max(1, Number.parseInt(arg("limit") ?? "10", 10) || 10);
const DELAY_MS = Math.max(500, Number.parseInt(arg("delay-ms") ?? "2500", 10) || 2500);
const STATE = "FL";
const CITY = "PANAMA CITY BEACH";
const TARGET_ZIPS = ["32407", "32408", "32413"];
const COUNTY_FIPS = "12005";
const ATTEMPT_PROVIDER = "bay_county_recorder";
const ATTEMPT_REASON = "debt_status_name_search";

type CandidateProperty = {
  id: number;
  address: string | null;
  owner_name: string | null;
  legal_description: string | null;
};

type ParsedRow = {
  grantor: string;
  grantee: string;
  date: string;
  documentType: string;
  bookPage: string | null;
  documentNumber: string | null;
  legalDescription: string | null;
  documentId: string | null;
  raw: Record<string, unknown>;
};

function clean(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function stripHtml(value: unknown): string {
  return clean(value)
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/nobreak_\s*/g, "")
    .replace(/unclickable_/g, "")
    .replace(/hidden_legalfield_/g, "")
    .replace(/^hidden_/, "")
    .trim();
}

function normalize(value: unknown): string {
  return clean(value).toUpperCase().replace(/[^A-Z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeLegal(value: unknown): string {
  return normalize(value)
    .replace(/\bOR(?:B)?\s+\d+\s+P(?:AGE)?\s+\d+\b/g, " ")
    .replace(/\bORB\s+\d+\b/g, " ")
    .replace(/\bP\s+\d+\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function ownerSearchNames(owner: string | null): string[] {
  const raw = clean(owner).toUpperCase();
  const normalized = normalize(owner);
  if (!normalized || normalized.length < 4) return [];
  const names = new Set<string>();
  const withoutSuffixes = normalized.replace(/\b(LLC|INC|LTD|CORP|CORPORATION|COMPANY|CO|TRUST|TRUSTEE)\b/g, " ").replace(/\s+/g, " ").trim();
  if (withoutSuffixes) names.add(withoutSuffixes);
  names.add(normalized);

  const commaRoot = raw.includes(",") ? normalize(raw.split(",")[0]) : "";
  if (commaRoot.length >= 4) names.add(commaRoot);
  const firstToken = withoutSuffixes.split(" ").find((part) => part.length >= 4);
  if (firstToken) names.add(firstToken);

  return [...names].slice(0, 3);
}

function ownerTokens(owner: string | null): string[] {
  return normalize(owner)
    .replace(/\b(AND|OR|THE|LLC|INC|LTD|CORP|CORPORATION|COMPANY|CO|TRUST|TRUSTEE|ETAL|CO TRUSTEE)\b/g, " ")
    .split(" ")
    .filter((part) => part.length >= 3);
}

function classify(rawType: string): { document_type: string; debtRelevant: boolean; loan_type?: string } {
  const upper = normalize(rawType);
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", debtRelevant: true, loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase" };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction", debtRelevant: true };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment", debtRelevant: true };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return { document_type: "lien", debtRelevant: true };
  return { document_type: rawType.toLowerCase(), debtRelevant: false };
}

function matchesProperty(row: ParsedRow, property: CandidateProperty): boolean {
  const legal = normalizeLegal(property.legal_description);
  const rowLegal = normalizeLegal(row.legalDescription);
  if (legal.length >= 12 && rowLegal.includes(legal.slice(0, Math.min(legal.length, 35)))) return true;

  const tokens = ownerTokens(property.owner_name);
  if (tokens.length === 0) return false;
  const grantor = normalize(row.grantor);
  const matched = tokens.filter((token) => grantor.includes(token)).length;
  return matched >= Math.min(2, tokens.length);
}

function parseDate(value: string): string | null {
  const match = value.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!match) return null;
  return `${match[3]}-${match[1].padStart(2, "0")}-${match[2].padStart(2, "0")}`;
}

function parseRows(responseText: string): ParsedRow[] {
  if (!responseText.trim().startsWith("{")) return [];
  const json = JSON.parse(responseText);
  const rows = Array.isArray(json.data) ? json.data : [];
  return rows.map((row: Record<string, unknown>) => {
    const date = stripHtml(row[7]);
    const book = stripHtml(row[10]);
    const page = stripHtml(row[11]);
    const legal = stripHtml(row[13]);
    const documentId = stripHtml(row[25]) || null;
    return {
      grantor: stripHtml(row[5]),
      grantee: stripHtml(row[6]),
      date,
      documentType: stripHtml(row[8]),
      bookPage: book || page ? `${book}/${page}` : null,
      documentNumber: stripHtml(row[12]) || documentId,
      legalDescription: legal || null,
      documentId,
      raw: row,
    };
  }).filter((row: ParsedRow) => row.date && row.documentType);
}

async function candidates(): Promise<CandidateProperty[]> {
  const where = `
    l.is_on_market = true
    and l.state_code = '${STATE}'
    and (upper(coalesce(l.city,'')) = '${CITY}' or l.zip = any(array[${TARGET_ZIPS.map((zip) => `'${zip}'`).join(",")}]))
  `;
  const query = `
    with ap as (
      select distinct p.id, p.address, p.owner_name, p.legal_description
        from listing_signals l
        join properties p on p.id = l.property_id
       where ${where}
         and p.owner_name is not null
    ),
    covered as (
      select distinct property_id from mortgage_records where property_id in (select id from ap)
      union
      select distinct property_id from realestateapi_property_details
       where property_id in (select id from ap)
         and status = 'ok'
         and response_body <> '{}'::jsonb
      union
      select distinct property_id from property_enrichment_queue
       where property_id in (select id from ap)
         and provider = '${ATTEMPT_PROVIDER}'
         and reason = '${ATTEMPT_REASON}'
         and status = 'completed'
    )
    select id, address, owner_name, legal_description
      from ap
     where id not in (select property_id from covered)
     order by case when legal_description is not null then 0 else 1 end, owner_name
     limit ${LIMIT};
  `;
  const pgUrl = (firstEnv("MXRE_PG_URL") ?? "").replace(/\/$/, "");
  const response = await fetch(pgUrl.endsWith("/pg/query") ? pgUrl : `${pgUrl}/pg/query`, {
    method: "POST",
    headers: {
      apikey: firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY") ?? "",
      Authorization: `Bearer ${firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY") ?? ""}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

async function saveAttempt(property: CandidateProperty, outcome: "matched" | "no_data", rowsSeen: number, matches: number): Promise<void> {
  if (DRY_RUN) return;
  const { error } = await db.from("property_enrichment_queue").upsert({
    property_id: property.id,
    provider: ATTEMPT_PROVIDER,
    reason: ATTEMPT_REASON,
    status: "completed",
    priority: 500,
    attempts: 1,
    completed_at: new Date().toISOString(),
    last_error: `${outcome}: Bay County Official Records name search rows_seen=${rowsSeen}; debt_matches=${matches}`,
    updated_at: new Date().toISOString(),
  }, { onConflict: "property_id,provider,reason" });
  if (error) console.log(`  attempt marker skipped: ${error.message}`);
}

async function setupPage(page: Page) {
  await page.goto("https://records2.baycoclerk.com/recording/", { waitUntil: "networkidle", timeout: 30_000 });
  await page.evaluate(() => {
    if (typeof (globalThis as { SetDisclaimer?: () => void }).SetDisclaimer === "function") {
      (globalThis as { SetDisclaimer: () => void }).SetDisclaimer();
    }
  });
  await page.waitForSelector("#submit-Name", { timeout: 15_000 });
}

async function searchName(page: Page, name: string): Promise<ParsedRow[]> {
  await page.selectOption("#partyType", "0").catch(() => {});
  await page.selectOption("#matchType-Name", "0").catch(() => {});
  await page.fill("#name-Name", name);
  await page.fill("#beginDate-Name", "01/01/1987").catch(() => {});
  await page.fill("#endDate-Name", new Date().toLocaleDateString("en-US")).catch(() => {});

  const resultPromise = new Promise<string>((resolve) => {
    const timer = setTimeout(() => {
      page.off("response", handler);
      resolve("");
    }, 30_000);
    const handler = async (resp: { url: () => string; text: () => Promise<string> }) => {
      const url = resp.url();
      if (url.includes("GetSearchResults") || url.includes("GetDocumentList")) {
        clearTimeout(timer);
        page.off("response", handler);
        resolve(await resp.text().catch(() => ""));
      }
    };
    page.on("response", handler);
  });

  await page.click("#submit-Name", { timeout: 5_000 });
  const text = await resultPromise;
  return text ? parseRows(text) : [];
}

async function duplicate(row: ParsedRow): Promise<boolean> {
  let query = db.from("mortgage_records").select("id").eq("source_url", "https://records2.baycoclerk.com").limit(1);
  if (row.documentNumber) query = query.eq("document_number", row.documentNumber);
  else if (row.bookPage) query = query.eq("book_page", row.bookPage);
  else return false;
  const { data, error } = await query;
  if (error) throw error;
  return Boolean(data?.length);
}

async function insertMatch(row: ParsedRow, property: CandidateProperty): Promise<boolean> {
  const type = classify(row.documentType);
  const recordingDate = parseDate(row.date);
  if (!recordingDate || !type.debtRelevant) return false;
  if (await duplicate(row)) return false;

  const record: Record<string, unknown> = {
    property_id: property.id,
    document_type: type.document_type,
    recording_date: recordingDate,
    lender_name: row.grantee.slice(0, 500),
    borrower_name: row.grantor.slice(0, 500),
    grantee_name: row.grantee.slice(0, 500),
    document_number: row.documentNumber,
    book_page: row.bookPage,
    source_url: "https://records2.baycoclerk.com",
    loan_type: type.loan_type,
    county_fips: COUNTY_FIPS,
    legal_description: row.legalDescription,
    raw: {
      bayNameSearch: {
        source: "Bay County Official Records Name Search",
        market: "panama-city-beach-fl",
        observedAt: new Date().toISOString(),
      },
      sourceRow: row.raw,
    },
  };
  const { error } = await db.from("mortgage_records").insert(record);
  if (error) {
    if (!String(error.message).toLowerCase().includes("duplicate")) console.log(`  insert skipped: ${error.message}`);
    return false;
  }
  return true;
}

async function main() {
  console.log("MXRE - Bay County targeted recorder name search");
  console.log(`Limit ${LIMIT}; delay ${DELAY_MS}ms; dry run ${DRY_RUN}`);
  const props = await candidates();
  console.log(`Candidates loaded: ${props.length}`);

  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
  const page = await browser.newPage();
  let searched = 0;
  let rowsSeen = 0;
  let matchedRows = 0;
  let inserted = 0;
  try {
    await setupPage(page);
    for (const property of props) {
      const searches = ownerSearchNames(property.owner_name);
      if (searches.length === 0) continue;
      await page.waitForTimeout(DELAY_MS);
      searched++;
      console.log(`  [${searched}/${props.length}] ${property.owner_name} -> ${searches.join(" | ")}`);
      const seen = new Set<string>();
      const rows: ParsedRow[] = [];
      for (const search of searches) {
        const searchRows = await searchName(page, search);
        for (const row of searchRows) {
          const key = row.documentNumber ?? row.bookPage ?? `${row.date}:${row.documentType}:${row.grantor}:${row.grantee}`;
          if (seen.has(key)) continue;
          seen.add(key);
          rows.push(row);
        }
        if (searchRows.some((row) => classify(row.documentType).debtRelevant && matchesProperty(row, property))) break;
        await page.waitForTimeout(Math.max(500, Math.floor(DELAY_MS / 2)));
      }
      rowsSeen += rows.length;
      const matches = rows.filter((row) => classify(row.documentType).debtRelevant && matchesProperty(row, property));
      matchedRows += matches.length;
      console.log(`    rows ${rows.length}; debt matches ${matches.length}`);
      for (const match of matches) {
        if (!DRY_RUN && await insertMatch(match, property)) inserted++;
      }
      await saveAttempt(property, matches.length > 0 ? "matched" : "no_data", rows.length, matches.length);
    }
  } finally {
    await browser.close();
  }
  console.log(JSON.stringify({ searched, rows_seen: rowsSeen, matched_rows: matchedRows, inserted, dry_run: DRY_RUN }, null, 2));
}

main().catch((error) => {
  console.error("Fatal Bay name-search enrichment error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
