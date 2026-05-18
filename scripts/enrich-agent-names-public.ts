#!/usr/bin/env tsx
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const basePgUrl = (firstEnv("MXRE_PG_URL") || firstEnv("SUPABASE_URL") || "").replace(/\/$/, "");
const PG_URL = basePgUrl.endsWith("/pg/query") ? basePgUrl : `${basePgUrl}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY") ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "25", 10));
const DELAY_MS = Math.max(0, parseInt(process.argv.find(a => a.startsWith("--delay-ms="))?.split("=")[1] ?? "800", 10));
const DRY_RUN = process.argv.includes("--dry-run");
const DEBUG = process.argv.includes("--debug");
const arg = (name: string) => process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const ROW_ID = arg("id");
const STATE = (arg("state") ?? "IN").toUpperCase();
const CITY = (arg("city") ?? "INDIANAPOLIS").toUpperCase();

type ListingRow = {
  id: number;
  address: string;
  city: string | null;
  state_code: string | null;
  zip: string | null;
  listing_url: string | null;
  listing_source: string | null;
};

type SearchResult = {
  url: string;
  text: string;
  engine: string;
};

type AgentHit = {
  agentName: string;
  brokerage: string | null;
  sourceUrl: string;
  sourceEngine: string;
  confidence: "public_address_snippet_exact" | "public_builder_snippet";
  evidence: string;
};

const knownBrokerages = [
  "Highgarden Real Estate",
  "M/I Homes",
  "M/I Homes of Indiana",
  "M/I Homes of Indiana, L.P.",
  "Berkshire Hathaway HomeServices Indiana Realty",
  "Compass Indiana, LLC",
  "Opendoor Brokerage LLC",
  "Trelora Realty",
  "Keller Williams Indy Metro",
  "F.C. Tucker",
  "CENTURY 21 Scheetz",
  "Trueblood Real Estate",
  "eXp Realty",
  "RE/MAX",
  "DRH Realty of Indiana, LLC",
  "Matlock Realty Group",
  "Mike Watkins Real Estate Group",
];

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function decodeHtml(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;|&#x27;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&#x2F;/gi, "/")
    .replace(/&#x3A;/gi, ":")
    .replace(/&#x3D;/gi, "=");
}

function cleanText(value: string): string {
  return decodeHtml(value)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeAddress(value: string): string {
  return value
    .toLowerCase()
    .replace(/\b(drive)\b/g, "dr")
    .replace(/\b(lane)\b/g, "ln")
    .replace(/\b(street)\b/g, "st")
    .replace(/\b(avenue)\b/g, "ave")
    .replace(/\b(road)\b/g, "rd")
    .replace(/\b(court)\b/g, "ct")
    .replace(/\b(way)\b/g, "way")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function exactAddressInText(row: ListingRow, text: string): boolean {
  const haystack = normalizeAddress(text);
  const address = normalizeAddress(row.address);
  if (address && haystack.includes(address)) return true;
  const parts = address.split(" ");
  return parts.length >= 3 && haystack.includes(parts.slice(0, Math.min(4, parts.length)).join(" "));
}

function normalizeSearchResultUrl(value: string): string | null {
  const decoded = decodeHtml(value).trim();
  if (!decoded) return null;
  try {
    const url = new URL(decoded, "https://www.bing.com");
    const target = url.searchParams.get("u") ?? url.searchParams.get("url");
    if (target) {
      if (/^a1[a-z0-9_-]+$/i.test(target)) {
        try {
          return normalizeSearchResultUrl(Buffer.from(target.slice(2), "base64url").toString("utf8"));
        } catch {
          return null;
        }
      }
      return normalizeSearchResultUrl(target);
    }
    url.hash = "";
    return /^https?:$/i.test(url.protocol) ? url.toString() : null;
  } catch {
    return null;
  }
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, {
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,text/plain",
        "accept-language": "en-US,en;q=0.9",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(12_000),
    });
    if (!response.ok) return null;
    return response.text();
  } catch {
    return null;
  }
}

function extractResultsFromBing(html: string): SearchResult[] {
  const decodedHtml = decodeHtml(html);
  const results: SearchResult[] = [];
  for (const match of decodedHtml.matchAll(/<li[^>]+class=["'][^"']*\bb_algo\b[^"']*["'][\s\S]*?<\/li>/gi)) {
    const block = match[0];
    const href = block.match(/<a[^>]+href=["']([^"']+)["']/i)?.[1];
    const url = href ? normalizeSearchResultUrl(href) : null;
    if (!url) continue;
    results.push({ url, text: cleanText(block), engine: "bing" });
  }
  return results.filter(result => !/bing\.com|microsoft\.com|facebook\.com|instagram\.com|linkedin\.com/i.test(result.url)).slice(0, 8);
}

function extractResultsFromStartpage(html: string): SearchResult[] {
  const decodedHtml = decodeHtml(html);
  const results: SearchResult[] = [];
  for (const match of decodedHtml.matchAll(/<a[^>]+href=["']([^"']+)["'][^>]*>/gi)) {
    const href = decodeHtml(match[1]);
    let url: string | null = null;
    try {
      const parsed = new URL(href, "https://www.startpage.com");
      const target = parsed.searchParams.get("url") ?? parsed.searchParams.get("u") ?? parsed.searchParams.get("q");
      url = target ? decodeURIComponent(target) : parsed.toString();
    } catch {
      url = normalizeSearchResultUrl(href);
    }
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const block = decodedHtml.slice(Math.max(0, match.index ?? 0), Math.min(decodedHtml.length, (match.index ?? 0) + 2200));
    results.push({ url, text: cleanText(block), engine: "startpage" });
  }
  return [...new Map(results
    .filter(result => !/startpage\.com|bing\.com|microsoft\.com|google\.com|facebook\.com|instagram\.com|linkedin\.com/i.test(result.url))
    .map(result => [result.url, result])).values()].slice(0, 8);
}

function parsePersonName(value: string): string | null {
  const cleaned = value
    .replace(/\b(listing|agent|realtor|brokered|presented|by|with|from|email|contact|new|construction|built)\b/gi, " ")
    .replace(/[^A-Za-z.' -]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const match = cleaned.match(/\b([A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){1,3})\b/);
  if (!match) return null;
  const name = match[1].replace(/\s+/g, " ").trim();
  if (
    /^(Find A|Real Estate|New Construction|Indianapolis Real|Listing Agent|M I Homes|Quick Move-In Homes)$/i.test(name)
    || /\b(Dr|Drive|Ln|Lane|St|Street|Ave|Road|Rd|Ct|Court|Way|Blvd|Indianapolis|Indiana|Homes?|Properties|Real Estate|Construction|Broker|Realty)\b/i.test(name)
    || /\b(School|Elementary|Middle|High|Christian|Community|Neighborhood|Subdivision|Preserve|Run|Move-In|Source|County|Zillow|Plan)\b/i.test(name)
  ) {
    return null;
  }
  return name;
}

function extractBrokerage(text: string): string | null {
  for (const brokerage of knownBrokerages) {
    if (new RegExp(brokerage.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+"), "i").test(text)) return brokerage;
  }
  const brokered = text.match(/Brokered by\s+([^.;|]{3,90})/i)?.[1]?.trim();
  if (brokered) return brokered.replace(/\s+/g, " ");
  return null;
}

function extractAgentHit(row: ListingRow, result: SearchResult): AgentHit | null {
  if (!exactAddressInText(row, result.text)) return null;
  const text = result.text.replace(/\s+/g, " ");
  const addressIndex = normalizeAddress(text).indexOf(normalizeAddress(row.address).split(" ").slice(0, 4).join(" "));
  const evidence = addressIndex >= 0 ? text.slice(Math.max(0, addressIndex - 220), addressIndex + 700) : text.slice(0, 900);
  const brokerage = extractBrokerage(evidence) ?? extractBrokerage(text);

  const patterns = [
    /(?:Listed by|Listing by|Presented by|Agent:?)\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})(?:\s+(?:with|at|of)\s+([^.;|]{3,90}))?/i,
    /(?:Email Agent|Contact Agent)\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})(?:\s+([^.;|]{3,90}))?/i,
  ];
  for (const pattern of patterns) {
    const match = evidence.match(pattern);
    const name = match ? parsePersonName(match[1]) : null;
    if (name) {
      return {
        agentName: name,
        brokerage: brokerage ?? match?.[2]?.replace(/\s+/g, " ").trim() ?? null,
        sourceUrl: result.url,
        sourceEngine: result.engine,
        confidence: "public_address_snippet_exact",
        evidence: evidence.slice(0, 800),
      };
    }
  }

  return null;
}

async function searchAddress(row: ListingRow): Promise<AgentHit | null> {
  const queries = [
    `"${row.address}" "${row.city ?? CITY}" listing agent`,
    `"${row.address}" "${row.zip ?? ""}" realtor`,
    `"${row.address}" "${row.city ?? CITY}" homes.com`,
    `"${row.address}" "${row.city ?? CITY}" zillow`,
    `"${row.address}" "${row.city ?? CITY}" realtor.com`,
  ].filter((query, index, all) => query && all.indexOf(query) === index);

  for (const query of queries) {
    const bingHtml = await fetchText(`https://www.bing.com/search?q=${encodeURIComponent(query)}`);
    const bingResults = bingHtml ? extractResultsFromBing(bingHtml) : [];
    for (const result of bingResults) {
      const hit = extractAgentHit(row, result);
      if (hit) return hit;
    }
    if (DELAY_MS) await sleep(DELAY_MS);

    const startpageHtml = await fetchText(`https://www.startpage.com/sp/search?query=${encodeURIComponent(query)}`);
    const startpageResults = startpageHtml ? extractResultsFromStartpage(startpageHtml) : [];
    for (const result of startpageResults) {
      const hit = extractAgentHit(row, result);
      if (hit) return hit;
    }
    if (DELAY_MS) await sleep(DELAY_MS);
  }

  return null;
}

function splitName(name: string): { first: string | null; last: string | null } {
  const clean = name.replace(/\s+/g, " ").trim();
  if (!clean || clean.includes("/")) return { first: clean || null, last: null };
  const parts = clean.split(" ");
  return { first: parts[0] ?? null, last: parts.length > 1 ? parts.slice(1).join(" ") : null };
}

async function saveHit(row: ListingRow, hit: AgentHit): Promise<number> {
  const name = splitName(hit.agentName);
  const rawPatch = {
    publicAgentName: {
      name: hit.agentName,
      brokerage: hit.brokerage,
      sourceUrl: hit.sourceUrl,
      sourceEngine: hit.sourceEngine,
      confidence: hit.confidence,
      evidence: hit.evidence,
      observedAt: new Date().toISOString(),
    },
  };
  const updated = await pg(`
    with updated as (
      update listing_signals
         set listing_agent_name = coalesce(nullif(listing_agent_name,''), ${sql(hit.agentName)}),
             listing_agent_first_name = coalesce(nullif(listing_agent_first_name,''), ${sql(name.first)}),
             listing_agent_last_name = coalesce(nullif(listing_agent_last_name,''), ${sql(name.last)}),
             listing_brokerage = coalesce(nullif(listing_brokerage,''), ${sql(hit.brokerage)}),
             agent_contact_source = coalesce(agent_contact_source, 'public_address_search'),
             agent_contact_confidence = coalesce(agent_contact_confidence, ${sql(hit.confidence)}),
             raw = coalesce(raw, '{}'::jsonb) || ${sql(JSON.stringify(rawPatch))}::jsonb,
             updated_at = now()
       where id = ${row.id}
         and is_on_market = true
         and nullif(listing_agent_name,'') is null
       returning id
    )
    select count(*)::int as updated from updated;
  `);
  return Number(updated?.[0]?.updated ?? 0);
}

async function saveAttempt(row: ListingRow, reason: string): Promise<void> {
  const rawPatch = {
    publicAgentNameAttempt: {
      status: "no_verified_agent_name",
      reason: reason.slice(0, 500),
      observedAt: new Date().toISOString(),
    },
  };
  await pg(`
    update listing_signals
       set raw = coalesce(raw, '{}'::jsonb) || ${sql(JSON.stringify(rawPatch))}::jsonb,
           updated_at = now()
     where id = ${row.id}
       and is_on_market = true
       and nullif(listing_agent_name,'') is null;
  `);
}

async function main() {
  console.log("MXRE - Public address agent-name enrichment");
  console.log(JSON.stringify({ city: CITY, state: STATE, limit: LIMIT, row_id: ROW_ID ?? null, dry_run: DRY_RUN }, null, 2));
  const rows = await pg(`
    select id, address, city, state_code, zip, listing_url, listing_source
    from listing_signals
    where is_on_market = true
      and state_code = ${sql(STATE)}
      and upper(coalesce(city,'')) = ${sql(CITY)}
      and nullif(listing_agent_name,'') is null
      and nullif(address,'') is not null
      ${ROW_ID ? `and id = ${sql(ROW_ID)}` : ""}
      and coalesce(raw, '{}'::jsonb)->'publicAgentNameAttempt' is null
    order by last_seen_at desc nulls last, updated_at desc nulls last
    limit ${LIMIT};
  `) as ListingRow[];

  let found = 0;
  let updated = 0;
  let attempted = 0;
  for (const row of rows) {
    attempted++;
    const hit = await searchAddress(row);
    if (!hit) {
      if (!DRY_RUN) await saveAttempt(row, "no exact-address public search result with verifiable agent attribution");
      if (DEBUG) console.log(`  no hit: ${row.id} ${row.address}`);
      continue;
    }
    found++;
    console.log(JSON.stringify({ id: row.id, address: row.address, agent: hit.agentName, brokerage: hit.brokerage, confidence: hit.confidence, sourceUrl: hit.sourceUrl }, null, 2));
    if (!DRY_RUN) updated += await saveHit(row, hit);
  }

  console.log(JSON.stringify({ scanned: rows.length, attempted, found, updated, dry_run: DRY_RUN }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
