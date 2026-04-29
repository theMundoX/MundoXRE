#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "250", 10));
const DELAY_MS = Math.max(250, parseInt(process.argv.find(a => a.startsWith("--delay-ms="))?.split("=")[1] ?? "1200", 10));
const DRY_RUN = process.argv.includes("--dry-run");

type ListingRow = {
  id: number;
  listing_url: string;
  listing_agent_name: string | null;
  listing_agent_phone: string | null;
  listing_brokerage: string | null;
  raw: Record<string, unknown> | null;
};

type Detail = {
  agentName: string | null;
  agentPhone: string | null;
  brokerPhone: string | null;
  brokerage: string | null;
  remarks: string | null;
};

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
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function decodeText(value: string): string {
  return value
    .replace(/\\u002F/g, "/")
    .replace(/\\u0026/g, "&")
    .replace(/&amp;/g, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanPhone(value: string | null): string | null {
  if (!value) return null;
  const match = value.match(/(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}/);
  if (!match) return null;
  return match[0].replace(/[^\d+]/g, "").replace(/^1(?=\d{10}$)/, "");
}

function splitName(name: string | null): { first: string | null; last: string | null } {
  const clean = name?.replace(/\s+/g, " ").trim();
  if (!clean) return { first: null, last: null };
  const parts = clean.split(" ");
  return { first: parts[0] ?? null, last: parts.length > 1 ? parts.slice(1).join(" ") : null };
}

function extractDetail(html: string): Detail {
  const agentName = html.match(/\\"listingAgentName\\"\s*:\s*\\"([^"\\]+)\\"/)?.[1]
    ?? html.match(/Listed by\s*<span>([^<]+)<\/span>/)?.[1]
    ?? null;
  const agentPhone = cleanPhone(html.match(/\\"listingAgentNumber\\"\s*:\s*\\"([^"\\]+)\\"/)?.[1] ?? null);
  const brokerPhone = cleanPhone(html.match(/\\"listingBrokerNumber\\"\s*:\s*\\"([^"\\]+)\\"/)?.[1] ?? null);
  const brokerage = html.match(/agent-basic-details--broker[\s\S]{0,180}?<!-- -->([^<]+)<!-- -->/)?.[1]?.trim()
    ?? null;
  const remarksHtml = html.match(/data-rf-test-id="listingRemarks"[\s\S]{0,2500}?<div class="remarks"[\s\S]*?<p[^>]*>([\s\S]*?)<\/p>/)?.[1]
    ?? html.match(/<meta name="description" content="([^"]+)"/)?.[1]
    ?? null;
  const remarks = remarksHtml ? decodeText(remarksHtml) : null;
  return {
    agentName: agentName ? decodeText(agentName) : null,
    agentPhone,
    brokerPhone,
    brokerage: brokerage ? decodeText(brokerage) : null,
    remarks,
  };
}

async function fetchHtml(url: string): Promise<string | null> {
  const response = await fetch(url, {
    redirect: "follow",
    headers: {
      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
      "accept": "text/html,application/xhtml+xml",
      "accept-language": "en-US,en;q=0.9",
    },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    console.log(`  HTTP ${response.status}: ${url}`);
    return null;
  }
  return response.text();
}

async function main() {
  console.log("MXRE - Redfin detail page enrichment");
  console.log(`Limit: ${LIMIT}; delay ${DELAY_MS}ms; dry run ${DRY_RUN}`);

  const rows = await pg(`
    select id, listing_url, listing_agent_name, listing_agent_phone, listing_brokerage, raw
    from listing_signals
    where is_on_market = true
      and listing_source = 'redfin'
      and listing_url is not null
      and state_code = 'IN'
      and upper(city) = 'INDIANAPOLIS'
      and (
        listing_agent_phone is null
        or listing_agent_name is null
        or listing_brokerage is null
        or not (raw ? 'redfinDetail')
      )
    order by last_seen_at desc nulls last
    limit ${LIMIT};
  `) as ListingRow[];

  let fetched = 0;
  let detailFound = 0;
  let phones = 0;
  let names = 0;
  let brokerages = 0;
  let remarks = 0;

  for (const row of rows) {
    await sleep(DELAY_MS);
    const html = await fetchHtml(row.listing_url);
    fetched++;
    if (!html) continue;
    const detail = extractDetail(html);
    if (detail.agentName || detail.agentPhone || detail.brokerage || detail.remarks) detailFound++;
    if (detail.agentPhone) phones++;
    if (detail.agentName) names++;
    if (detail.brokerage) brokerages++;
    if (detail.remarks) remarks++;

    const name = splitName(detail.agentName);
    const rawPatch = {
      redfinDetail: {
        listingAgentName: detail.agentName,
        listingAgentNumber: detail.agentPhone,
        listingBrokerNumber: detail.brokerPhone,
        listingBrokerage: detail.brokerage,
        publicRemarks: detail.remarks,
        sourceUrl: row.listing_url,
        observedAt: new Date().toISOString(),
      },
    };

    if (!DRY_RUN && (detail.agentName || detail.agentPhone || detail.brokerage || detail.remarks)) {
      await pg(`
        update listing_signals
           set listing_agent_name = coalesce(listing_agent_name, ${sql(detail.agentName)}),
               listing_agent_first_name = coalesce(listing_agent_first_name, ${sql(name.first)}),
               listing_agent_last_name = coalesce(listing_agent_last_name, ${sql(name.last)}),
               listing_agent_phone = coalesce(listing_agent_phone, ${sql(detail.agentPhone)}),
               listing_brokerage = coalesce(listing_brokerage, ${sql(detail.brokerage)}),
               agent_contact_source = case
                 when ${sql(detail.agentPhone)} is not null then coalesce(agent_contact_source, 'redfin_detail_page')
                 else agent_contact_source
               end,
               agent_contact_confidence = case
                 when ${sql(detail.agentPhone)} is not null then coalesce(agent_contact_confidence, 'public_detail_page')
                 else agent_contact_confidence
               end,
               raw = coalesce(raw, '{}'::jsonb) || ${sql(JSON.stringify(rawPatch))}::jsonb,
               updated_at = now()
         where id = ${row.id};
      `);
    }

    if (fetched % 25 === 0) {
      console.log(`  ${fetched}/${rows.length}: details ${detailFound}, phones ${phones}, remarks ${remarks}`);
    }
  }

  console.log(JSON.stringify({ scanned: rows.length, fetched, detail_found: detailFound, names, phones, brokerages, remarks, dry_run: DRY_RUN }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
