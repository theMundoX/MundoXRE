#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "5000", 10));
const DRY_RUN = process.argv.includes("--dry-run");
const arg = (name: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const STATE = arg("state")?.toUpperCase();
const CITY = arg("city")?.toUpperCase();

type ListingRow = {
  id: number;
  listing_url: string | null;
  listing_source: string | null;
  raw: Record<string, unknown> | null;
};

type ScoreResult = {
  score: number | null;
  status: "positive" | "negative" | "no_data";
  terms: string[];
  negativeTerms: string[];
  rateText: string | null;
  source: string | null;
};

const POSITIVE_PATTERNS: Array<[string, RegExp]> = [
  ["seller_financing", /\b(seller|sellers)\s+(financ\w+|carry|will\s+carry|carryback)\b/i],
  ["seller_finance", /\bseller\s+finance\b/i],
  ["owner_financing", /\b(owner|owners)\s+(financ\w+|carry|will\s+carry|carryback)\b/i],
  ["owner_finance", /\bowner\s+finance\b/i],
  ["subject_to", /\b(subject\s+to|sub[\s-]?to|sub\s*2)\b/i],
  ["creative_financing", /\bcreative\s+financ\w+\b/i],
  ["contract_for_deed", /\b(contract\s+for\s+deed|land\s+contract)\b/i],
  ["seller_terms", /\b(seller|owner)\s+(terms|carry\s+terms)\b/i],
];

const NEGATIVE_PATTERNS: Array<[string, RegExp]> = [
  ["no_seller_financing", /\b(no|not|won'?t|will\s+not|cannot|can't|seller\s+will\s+not)\s+(?:consider\s+)?(?:seller\s+)?financ\w+\b/i],
  ["no_owner_financing", /\b(no|not|won'?t|will\s+not|cannot|can't|owner\s+will\s+not)\s+(?:consider\s+)?(?:owner\s+)?financ\w+\b/i],
  ["no_creative_financing", /\b(no|not|won'?t|will\s+not|cannot|can't)\s+(?:creative\s+)?financ\w+\b/i],
  ["no_subject_to", /\b(no|not|won'?t|will\s+not|cannot|can't)\s+(?:subject\s+to|sub[\s-]?to|sub\s*2)\b/i],
  ["cash_or_conventional_only", /\b(cash|conventional)\s+(?:or\s+cash\s+)?only\b/i],
];

const DESCRIPTION_KEYS = [
  "description",
  "publicRemarks",
  "public_remarks",
  "remarks",
  "listingRemarks",
  "marketingRemarks",
  "propertyDescription",
  "seoDescription",
];

function textValues(value: unknown, path = ""): Array<{ path: string; text: string }> {
  const out: Array<{ path: string; text: string }> = [];
  if (!value) return out;
  if (typeof value === "string") {
    if (value.length >= 20) out.push({ path, text: value });
    return out;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => out.push(...textValues(item, `${path}[${index}]`)));
    return out;
  }
  if (typeof value === "object") {
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      const childPath = path ? `${path}.${key}` : key;
      if (DESCRIPTION_KEYS.some(k => k.toLowerCase() === key.toLowerCase())) {
        out.push(...textValues(child, childPath));
      } else if (typeof child === "object" && child !== null) {
        out.push(...textValues(child, childPath));
      }
    }
  }
  return out;
}

function normalizeText(text: string): string {
  return text.replace(/\\u0026/g, "&").replace(/\s+/g, " ").trim();
}

export function scoreCreativeFinance(raw: Record<string, unknown> | null): ScoreResult {
  const descriptions = textValues(raw).map(item => ({ ...item, text: normalizeText(item.text) }));
  if (descriptions.length === 0) {
    return { score: null, status: "no_data", terms: [], negativeTerms: [], rateText: null, source: null };
  }

  const joined = descriptions.map(d => d.text).join(" \n ");
  const negativeTerms = NEGATIVE_PATTERNS.filter(([, pattern]) => pattern.test(joined)).map(([name]) => name);
  if (negativeTerms.length > 0) {
    return { score: 0, status: "negative", terms: [], negativeTerms, rateText: null, source: descriptions[0].path };
  }

  const terms = POSITIVE_PATTERNS.filter(([, pattern]) => pattern.test(joined)).map(([name]) => name);
  if (terms.length === 0) {
    return { score: null, status: "no_data", terms: [], negativeTerms: [], rateText: null, source: descriptions[0].path };
  }

  const rateText = joined.match(/\b\d{1,2}(?:\.\d{1,2})?\s*%\b(?:\s*(?:interest|rate|seller|owner|financ\w+))?/i)?.[0] ?? null;
  const score = Math.min(100, 55 + (terms.length * 12) + (rateText ? 15 : 0));
  return { score, status: "positive", terms, negativeTerms: [], rateText, source: descriptions[0].path };
}

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

function sqlString(value: string | null): string {
  if (value == null) return "null";
  return `'${value.replace(/'/g, "''")}'`;
}

function sqlTextArray(values: string[]): string {
  if (values.length === 0) return "array[]::text[]";
  return `array[${values.map(sqlString).join(",")}]::text[]`;
}

async function main() {
  console.log("MXRE - Creative finance signal scorer");
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Limit: ${LIMIT}`);
  if (STATE || CITY) console.log(`Market filter: ${CITY ?? "all cities"}, ${STATE ?? "all states"}`);

  const filters = [
    STATE ? `state_code = ${sqlString(STATE)}` : null,
    CITY ? `upper(coalesce(city,'')) = ${sqlString(CITY)}` : null,
  ].filter(Boolean).join("\n      and ");

  const rows = await pg(`
    select id, listing_url, listing_source, raw
    from listing_signals
    where is_on_market = true
      and raw is not null
      ${filters ? `and ${filters}` : ""}
    order by last_seen_at desc nulls last
    limit ${LIMIT};
  `) as ListingRow[];

  let positive = 0;
  let negative = 0;
  let noData = 0;
  const updates: string[] = [];

  for (const row of rows) {
    const scored = scoreCreativeFinance(row.raw);
    if (scored.status === "positive") positive++;
    if (scored.status === "negative") negative++;
    if (scored.status === "no_data") noData++;
    if (scored.status === "no_data") continue;

    updates.push(`
      update listing_signals
         set creative_finance_score = ${scored.score ?? "null"},
             creative_finance_status = ${sqlString(scored.status)},
             creative_finance_terms = ${sqlTextArray(scored.terms)},
             creative_finance_negative_terms = ${sqlTextArray(scored.negativeTerms)},
             creative_finance_rate_text = ${sqlString(scored.rateText)},
             creative_finance_source = ${sqlString(scored.source)},
             creative_finance_observed_at = now(),
             updated_at = now()
       where id = ${row.id};
    `);
  }

  if (!DRY_RUN && updates.length > 0) {
    for (let i = 0; i < updates.length; i += 100) {
      await pg(updates.slice(i, i + 100).join("\n"));
    }
  }

  console.log(JSON.stringify({
    scanned: rows.length,
    positive,
    negative,
    no_data: noData,
    updated: DRY_RUN ? 0 : updates.length,
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
