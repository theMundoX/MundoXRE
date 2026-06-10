#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { LandmarkWebAdapter, type RecorderDocument } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(firstEnv("SUPABASE_URL")!, firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY")!, {
  auth: { persistSession: false },
});

const BAY_CONFIG = {
  county_name: "Bay",
  state: "FL",
  base_url: "https://records2.baycoclerk.com",
  path_prefix: "/recording",
  county_id: 2338923,
};
const STATE = "FL";
const CITY = "PANAMA CITY BEACH";
const TARGET_ZIPS = ["32407", "32408", "32413"];
const COUNTY_FIPS = "12005";

const args = process.argv.slice(2);
const arg = (name: string) => args.find((a) => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const DRY_RUN = args.includes("--dry-run");
const LIMIT = Math.max(0, Number.parseInt(arg("limit") ?? "0", 10) || 0);
const DAYS = Math.max(1, Number.parseInt(arg("days") ?? "90", 10) || 90);
const toDate = arg("to") ?? new Date().toISOString().slice(0, 10);
const fromDate = arg("from") ?? new Date(Date.parse(`${toDate}T00:00:00Z`) - DAYS * 86_400_000).toISOString().slice(0, 10);

type ActiveProperty = {
  id: number;
  owner_name: string | null;
  legal_description: string | null;
};

function clean(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeLegal(value: unknown): string {
  return clean(value)
    .toUpperCase()
    .replace(/\bOR(?:B)?\s+\d+\s+P(?:AGE)?\s+\d+\b/g, " ")
    .replace(/\bORB\s+\d+\b/g, " ")
    .replace(/\bP\s+\d+\b/g, " ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePerson(value: unknown): string {
  return clean(value)
    .toUpperCase()
    .replace(/\b(AND|OR|THE|TRUST|TRUSTEE|LLC|INC|LTD|CORP|CORPORATION|COMPANY|CO)\b/g, " ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(value: string): string[] {
  return value.split(" ").filter((part) => part.length >= 3);
}

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string; debtRelevant: boolean } {
  const upper = rawType.toUpperCase();
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase", debtRelevant: true };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction", debtRelevant: true };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment", debtRelevant: true };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return { document_type: "lien", debtRelevant: true };
  if (upper.includes("DEED")) return { document_type: "deed", deed_type: upper.includes("QUIT") ? "quitclaim" : undefined, debtRelevant: false };
  return { document_type: upper.toLowerCase(), debtRelevant: false };
}

function chooseUnique(values: number[]): number | null {
  const unique = [...new Set(values)];
  return unique.length === 1 ? unique[0] : null;
}

async function activeProperties(): Promise<ActiveProperty[]> {
  const ids = new Set<number>();
  let from = 0;
  while (true) {
    const { data, error } = await db
      .from("listing_signals")
      .select("property_id")
      .eq("is_on_market", true)
      .eq("state_code", STATE)
      .or(`city.eq.${CITY},zip.in.(${TARGET_ZIPS.join(",")})`)
      .not("property_id", "is", null)
      .range(from, from + 999);
    if (error) throw error;
    for (const row of data ?? []) ids.add(Number(row.property_id));
    if (!data || data.length < 1000) break;
    from += 1000;
  }

  const rows: ActiveProperty[] = [];
  const idList = [...ids];
  for (let i = 0; i < idList.length; i += 500) {
    const { data, error } = await db
      .from("properties")
      .select("id,owner_name,legal_description")
      .in("id", idList.slice(i, i + 500));
    if (error) throw error;
    rows.push(...((data ?? []) as ActiveProperty[]));
  }
  return rows;
}

function buildIndex(properties: ActiveProperty[]) {
  const legal = new Map<string, number[]>();
  const owner = new Map<string, number[]>();
  for (const property of properties) {
    const legalKey = normalizeLegal(property.legal_description);
    if (legalKey.length >= 12) legal.set(legalKey, [...(legal.get(legalKey) ?? []), property.id]);
    const ownerKey = normalizePerson(property.owner_name);
    if (ownerKey.length >= 5) owner.set(ownerKey, [...(owner.get(ownerKey) ?? []), property.id]);
  }
  return { legal, owner, properties };
}

function matchProperty(doc: RecorderDocument, index: ReturnType<typeof buildIndex>): { propertyId: number | null; method: string } {
  const legalKey = normalizeLegal(doc.legal_description);
  if (legalKey.length >= 12) {
    const exactLegal = chooseUnique(index.legal.get(legalKey) ?? []);
    if (exactLegal) return { propertyId: exactLegal, method: "exact_legal_description" };
  }

  const grantorKey = normalizePerson(doc.grantor);
  if (grantorKey.length >= 5) {
    const exactOwner = chooseUnique(index.owner.get(grantorKey) ?? []);
    if (exactOwner) return { propertyId: exactOwner, method: "exact_borrower_owner" };

    const grantorTokens = new Set(tokens(grantorKey));
    const scored: Array<{ id: number; score: number }> = [];
    for (const property of index.properties) {
      const ownerKey = normalizePerson(property.owner_name);
      const ownerTokens = tokens(ownerKey);
      if (ownerTokens.length < 2) continue;
      const score = ownerTokens.filter((token) => grantorTokens.has(token)).length;
      if (score >= Math.min(3, ownerTokens.length)) scored.push({ id: property.id, score });
    }
    scored.sort((a, b) => b.score - a.score);
    if (scored.length === 1 || (scored[0] && scored[1] && scored[0].score > scored[1].score)) {
      return { propertyId: scored[0].id, method: "borrower_owner_token_match" };
    }
  }

  return { propertyId: null, method: "no_safe_match" };
}

async function duplicateExists(doc: RecorderDocument): Promise<boolean> {
  let query = db.from("mortgage_records").select("id").eq("source_url", doc.source_url).limit(1);
  if (doc.instrument_number) query = query.eq("document_number", doc.instrument_number);
  else if (doc.book_page) query = query.eq("book_page", doc.book_page);
  else return false;
  const { data, error } = await query;
  if (error) throw error;
  return Boolean(data?.length);
}

async function insertRecord(record: Record<string, unknown>): Promise<boolean> {
  const { error } = await db.from("mortgage_records").insert(record);
  if (error) {
    if (!String(error.message).toLowerCase().includes("duplicate")) {
      console.log(`  insert skipped: ${error.message}`);
    }
    return false;
  }
  return true;
}

async function main() {
  console.log("MXRE - Bay County FL recorder ingest");
  console.log(`Date range: ${fromDate} to ${toDate}; limit ${LIMIT || "none"}; dry run ${DRY_RUN}`);

  const index = buildIndex(await activeProperties());
  console.log(`Active Panama property index: ${index.properties.length}`);

  const adapter = new LandmarkWebAdapter();
  await adapter.init();
  let seen = 0;
  let debtRelevant = 0;
  let linked = 0;
  let inserted = 0;
  let duplicates = 0;
  const matchMethods = new Map<string, number>();

  try {
    for await (const doc of adapter.fetchDocuments(BAY_CONFIG, fromDate, toDate, undefined, (progress) => {
      process.stdout.write(`\r  ${progress.current_date} | found=${progress.total_found} | inserted=${inserted} | linked=${linked} | dupes=${duplicates}   `);
    })) {
      seen++;
      if (LIMIT && seen > LIMIT) break;
      const classified = classifyDocType(doc.document_type);
      if (!classified.debtRelevant) continue;
      debtRelevant++;
      if (await duplicateExists(doc)) {
        duplicates++;
        continue;
      }
      const match = matchProperty(doc, index);
      matchMethods.set(match.method, (matchMethods.get(match.method) ?? 0) + 1);
      if (match.propertyId) linked++;

      const record: Record<string, unknown> = {
        property_id: match.propertyId,
        document_type: classified.document_type,
        recording_date: doc.recording_date,
        loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
        original_amount: doc.consideration ? Math.round(doc.consideration) : null,
        lender_name: doc.grantee?.slice(0, 500),
        borrower_name: doc.grantor?.slice(0, 500),
        grantee_name: doc.grantee?.slice(0, 500),
        document_number: doc.instrument_number,
        book_page: doc.book_page,
        source_url: doc.source_url,
        loan_type: classified.loan_type,
        deed_type: classified.deed_type,
        county_fips: COUNTY_FIPS,
        legal_description: doc.legal_description,
        raw: {
          ...doc.raw,
          bayRecorderMatch: {
            method: match.method,
            market: "panama-city-beach-fl",
            observedAt: new Date().toISOString(),
          },
        },
      };

      if (classified.document_type === "mortgage" && doc.consideration && doc.recording_date) {
        Object.assign(record, computeMortgageFields({
          originalAmount: doc.consideration,
          recordingDate: doc.recording_date,
        }));
      }

      if (!DRY_RUN && await insertRecord(record)) inserted++;
    }
  } finally {
    await adapter.close();
  }

  console.log();
  console.log(JSON.stringify({
    seen,
    debt_relevant: debtRelevant,
    inserted,
    linked,
    duplicates,
    dry_run: DRY_RUN,
    match_methods: Object.fromEntries(matchMethods),
  }, null, 2));
}

main().catch((error) => {
  console.error("Fatal Bay recorder ingest error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
