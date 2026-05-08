#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

type ReapiResponse = Record<string, unknown>;

type Candidate = {
  property_id: number;
  address: string;
  city: string;
  state_code: string;
  zip: string | null;
  listing_id?: number | null;
  mls_list_price?: number | null;
  search_address?: string | null;
  search_city?: string | null;
  search_zip?: string | null;
  reasons: string[];
};

const args = process.argv.slice(2);
hydrateWindowsUserEnv();
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const force = args.includes("--force");
const cacheOnly = args.includes("--cache-only");
const skipEnsureQueue = args.includes("--skip-ensure-queue");
const onlyMissingEquity = args.includes("--only-missing-equity") || args.includes("--missing-equity-only");
const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const limit = Math.min(Math.max(Number(valueArg("limit") ?? "25"), 1), 2500);
const maxCalls = Math.min(Math.max(Number(valueArg("max-calls") ?? String(limit)), 0), limit);
const staleDays = Math.max(Number(valueArg("stale-days") ?? "30"), 1);
const progressHtml = valueArg("progress-html")
  ?? `logs/market-refresh/realestateapi-${city.toLowerCase().replace(/[^a-z0-9]+/g, "-")}-${state.toLowerCase()}-progress.html`;
const reapiKey = firstEnv("REALESTATEAPI_KEY", "REALESTATE_API_KEY", "REALESTATEAPI_API_KEY");
const databaseUrl = firstEnv("MXRE_DIRECT_PG_URL", "MXRE_PG_URL", "DATABASE_URL", "POSTGRES_URL");

const hasOpenMortgageBalanceSql = `
  exists (
    select 1 from mortgage_records mr
    where mr.property_id = p.id
      and coalesce(mr.open, true) = true
      and coalesce(nullif(mr.loan_amount,0), nullif(mr.original_amount,0), nullif(mr.estimated_current_balance,0)) is not null
  )
`;

const hasReapiFreeClearSql = `
  exists (
    select 1 from realestateapi_property_details rfc
    where rfc.property_id = p.id
      and jsonb_typeof(rfc.response_body) = 'object'
      and rfc.status = 'ok'
      and rfc.response_body <> '{}'::jsonb
      and coalesce(rfc.response_body->>'id', rfc.response_body->>'propertyId', rfc.response_body->>'apn', rfc.response_body->>'address', rfc.response_body->>'formattedAddress', rfc.response_body->>'owner1FullName') is not null
      and coalesce(jsonb_array_length(case when jsonb_typeof(rfc.response_body->'currentMortgages') = 'array' then rfc.response_body->'currentMortgages' else '[]'::jsonb end), 0) = 0
      and coalesce(nullif(regexp_replace(coalesce(rfc.response_body->>'estimatedMortgageBalance', rfc.response_body->>'openMortgageBalance', '0'), '[^0-9.-]', '', 'g'), '')::numeric, 0) = 0
  )
`;

const hasDebtCoverageSql = `(${hasOpenMortgageBalanceSql} or ${hasReapiFreeClearSql})`;
const paidSearchAddressSql = `coalesce(nullif(l.address,''), p.address)`;
const hasUsablePaidSearchAddressSql = `
  nullif(${paidSearchAddressSql}, '') is not null
  and ${paidSearchAddressSql} !~* '^\\s*(lot|0\\b)'
`;

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, MXRE_PG_URL, DATABASE_URL, or POSTGRES_URL.");
if (!dryRun && maxCalls > 0 && !reapiKey) {
  throw new Error("Set REALESTATEAPI_KEY before making paid RealEstateAPI calls. Use --dry-run to preview queue.");
}
if (!dryRun && maxCalls > 0 && reapiKey && !isValidHeaderValue(reapiKey)) {
  throw new Error("REALESTATEAPI_KEY is present but is not a valid HTTP header value. Reset the environment variable; do not run paid calls with this value.");
}

type Queryable = {
  query<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<{ rows: T[] }>;
  end(): Promise<void>;
};

function sqlLiteral(value: unknown): string {
  if (value == null) return "null";
  if (Array.isArray(value)) return `array[${value.map(sqlLiteral).join(",")}]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function bindSql(query: string, params: unknown[] = []): string {
  return params.reduceRight((sql, value, index) => {
    const token = new RegExp(`\\$${index + 1}(?!\\d)`, "g");
    return sql.replace(token, sqlLiteral(value));
  }, query);
}

function makeClient(): Queryable {
  if (/^https?:\/\//i.test(databaseUrl ?? "")) {
    const endpoint = databaseUrl.replace(/\/$/, "");
    const key = process.env.SUPABASE_SERVICE_KEY ?? "";
    return {
      async query<T = Record<string, unknown>>(query: string, params: unknown[] = []) {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
          body: JSON.stringify({ query: bindSql(query, params) }),
          signal: AbortSignal.timeout(120_000),
        });
        if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
        const body = await response.json();
        return { rows: Array.isArray(body) ? body as T[] : [] };
      },
      async end() {},
    };
  }
  return new Client({ connectionString: databaseUrl }) as unknown as Queryable;
}

function isValidHeaderValue(value: string): boolean {
  return value.trim().length >= 10 && !/[\u0000-\u001f\u007f]/.test(value);
}

const client = makeClient();
if (!/^https?:\/\//i.test(databaseUrl ?? "")) {
  await (client as unknown as Client).connect();
}

const stats = {
  city,
  state,
  dryRun,
  force,
  cacheOnly,
  skipEnsureQueue,
  onlyMissingEquity,
  queued: 0,
  candidates: 0,
  apiCalls: 0,
  cached: 0,
  normalized: 0,
  failed: 0,
};
const progressRows: Array<{
  at: string;
  status: string;
  propertyId: number;
  address: string;
  reasons: string;
  source: string;
  listPrice: number | null;
  mortgageBalance: number | null;
  estimatedPayment: number | null;
  estimatedEquity: number | null;
  equityPercent: number | null;
  currentMortgageCount: number | null;
  mortgageHistoryCount: number | null;
  agentEmail: string | null;
  agentPhone: string | null;
  detail: string;
}> = [];

try {
  const previewCandidates = skipEnsureQueue ? [] : await ensureQueue();
  const candidates = dryRun ? previewCandidates : await loadCandidates();
  stats.candidates = candidates.length;
  await writeProgress("initialized");

  for (const candidate of candidates) {
    if (!cacheOnly && stats.apiCalls >= maxCalls && !dryRun) break;
    const requestBody = buildRequest(candidate);
      addProgress(candidate, "running", "pending", null, String(requestBody.address ?? candidate.address));
    await writeProgress("running");
    try {
      await markQueueRunning(candidate.property_id, candidate.reasons);
      const cached = cacheOnly ? await loadAnyCache(candidate.property_id) : (force ? null : await loadFreshCache(candidate.property_id));
      if (cached) {
        stats.cached++;
        if (!dryRun) await normalizeAndStore(candidate, cached.response_body as ReapiResponse, cached.request_body as Record<string, unknown>);
        await markQueueComplete(candidate.property_id, candidate.reasons);
        stats.normalized++;
        addProgress(candidate, "cached_normalized", "cache", cached.response_body as ReapiResponse, String(cached.request_body?.address ?? requestBody.address ?? candidate.address));
        await writeProgress("running");
        continue;
      }

      if (cacheOnly) continue;

      if (dryRun) {
        console.log(JSON.stringify({ wouldCall: requestBody, property_id: candidate.property_id, reasons: candidate.reasons }));
        addProgress(candidate, "dry_run", "preview", null, String(requestBody.address ?? candidate.address));
        await writeProgress("running");
        continue;
      }

      stats.apiCalls++;
      const response = await callRealEstateApi(requestBody);
      await cacheResponse(candidate, requestBody, response);
      await normalizeAndStore(candidate, response, requestBody);
      await markQueueComplete(candidate.property_id, candidate.reasons);
      stats.normalized++;
      addProgress(candidate, "api_normalized", "paid_api", response, summarizeProgressResponse(response, candidate.mls_list_price ?? null));
      await writeProgress("running");
      await sleep(100);
    } catch (error) {
      stats.failed++;
      if (!dryRun && isProviderNoData(error)) {
        await cacheNoData(candidate, requestBody, error instanceof Error ? error.message : String(error));
      }
      await markQueueFailed(candidate.property_id, candidate.reasons, error instanceof Error ? error.message : String(error));
      addProgress(candidate, "failed", "error", null, error instanceof Error ? error.message : String(error));
      await writeProgress("running");
      console.error(`Failed property ${candidate.property_id}:`, error instanceof Error ? error.message : error);
    }
  }
} finally {
  await writeProgress("finished");
  await client.end();
}

console.log(JSON.stringify(stats, null, 2));
console.log(JSON.stringify({ progressHtml }, null, 2));

function addProgress(candidate: Candidate, status: string, source: string, response: ReapiResponse | null, detail: string) {
  const summary = response ? summarizeResponse(response, candidate.mls_list_price ?? null) : null;
  const agent = response ? bestAgent(response) : null;
  progressRows.unshift({
    at: new Date().toISOString(),
    status,
    propertyId: candidate.property_id,
    address: [candidate.search_address ?? candidate.address, candidate.search_city ?? candidate.city, candidate.state_code, candidate.search_zip ?? candidate.zip].filter(Boolean).join(", "),
    reasons: candidate.reasons.join(", "),
    source,
    listPrice: candidate.mls_list_price ?? null,
    mortgageBalance: summary?.estimatedMortgageBalance ?? summary?.openMortgageBalance ?? null,
    estimatedPayment: response ? numberOrNull(response.estimatedMortgagePayment) : null,
    estimatedEquity: summary?.estimatedEquity ?? null,
    equityPercent: summary?.equityPercent ?? null,
    currentMortgageCount: summary?.currentMortgageCount ?? null,
    mortgageHistoryCount: summary?.mortgageHistoryCount ?? null,
    agentEmail: agent?.email ?? null,
    agentPhone: agent?.phone ?? null,
    detail: detail.slice(0, 500),
  });
  if (progressRows.length > 250) progressRows.pop();
}

function summarizeProgressResponse(response: ReapiResponse, fallbackMarketValue: number | null = null) {
  const summary = summarizeResponse(response, fallbackMarketValue);
  return [
    `currentMortgages=${summary.currentMortgageCount}`,
    `mortgageHistory=${summary.mortgageHistoryCount}`,
    `estimatedBalance=${summary.estimatedMortgageBalance ?? "null"}`,
    `openBalance=${summary.openMortgageBalance ?? "null"}`,
    `equity=${summary.estimatedEquity ?? "null"}`,
    `equityPct=${summary.equityPercent ?? "null"}`,
  ].join("; ");
}

async function writeProgress(runStatus: string) {
  await mkdir(join(process.cwd(), "logs", "market-refresh"), { recursive: true });
  await writeFile(join(process.cwd(), progressHtml), renderProgressHtml(runStatus), "utf8");
}

function renderProgressHtml(runStatus: string) {
  const processed = stats.cached + stats.normalized + stats.failed;
  const pct = stats.candidates > 0 ? Math.round((processed / stats.candidates) * 1000) / 10 : 0;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="5">
  <title>RealEstateAPI Progress - ${esc(city)}, ${esc(state)}</title>
  <style>
    :root { font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:#17212f; background:#f5f7f9; }
    body { margin:0; padding:24px; } header { margin-bottom:16px; } h1 { margin:0 0 4px; font-size:24px; }
    .sub { color:#596b7e; font-size:13px; } .grid { display:grid; grid-template-columns:repeat(6,minmax(120px,1fr)); gap:10px; margin:16px 0; }
    .card { background:white; border:1px solid #d8e0e8; border-radius:8px; padding:12px; } .label { color:#596b7e; font-size:12px; font-weight:700; text-transform:uppercase; } .value { font-size:24px; font-weight:800; margin-top:4px; }
    .track { height:12px; background:#e5ebf1; border-radius:999px; overflow:hidden; margin:10px 0 18px; } .fill { height:100%; background:#276f63; width:${pct}%; }
    table { min-width:1900px; width:max-content; border-collapse:collapse; background:white; border:1px solid #d8e0e8; border-radius:8px; overflow:hidden; } th,td { padding:9px 8px; border-bottom:1px solid #e6ebf1; text-align:left; font-size:13px; vertical-align:top; white-space:nowrap; }
    th { color:#596b7e; font-size:11px; text-transform:uppercase; position:sticky; top:0; z-index:2; background:white; } code { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size:12px; }
    .scroll-note { margin:8px 0 6px; color:#596b7e; font-size:12px; }
    .scroll-actions { display:flex; gap:8px; margin:8px 0; flex-wrap:wrap; }
    button { min-height:32px; border:1px solid #c8d4df; border-radius:6px; background:#fff; color:#17212f; font-weight:700; cursor:pointer; }
    .key-rows { display:grid; gap:10px; margin:10px 0 14px; }
    .key-row { background:white; border:1px solid #d8e0e8; border-radius:8px; padding:10px; }
    .key-title { display:flex; justify-content:space-between; gap:12px; font-weight:800; margin-bottom:8px; }
    .key-fields { display:grid; grid-template-columns:repeat(4,minmax(150px,1fr)); gap:8px 12px; font-size:13px; }
    .key-field span { display:block; color:#596b7e; font-size:11px; text-transform:uppercase; font-weight:700; margin-bottom:2px; }
    .key-field code { white-space:normal; overflow-wrap:anywhere; }
    .table-wrap { overflow:scroll; max-height:470px; border:1px solid #d8e0e8; border-radius:8px; background:white; padding-bottom:10px; scrollbar-gutter:stable both-edges; }
    .table-wrap::-webkit-scrollbar { width:16px; height:16px; }
    .table-wrap::-webkit-scrollbar-thumb { background:#8a98a8; border-radius:999px; border:3px solid #eef2f6; }
    .table-wrap::-webkit-scrollbar-track { background:#eef2f6; border-radius:999px; }
    .money { text-align:right; font-variant-numeric:tabular-nums; } .muted { color:#6b7785; }
    @media (max-width:900px){ .grid{grid-template-columns:repeat(2,1fr);} .key-fields{grid-template-columns:repeat(2,minmax(130px,1fr));} body{padding:14px;} }
  </style>
</head>
<body>
  <header>
    <h1>RealEstateAPI Progress - ${esc(city)}, ${esc(state)}</h1>
    <div class="sub">Auto-refreshes every 5 seconds. Last written ${esc(new Date().toLocaleString())}. Status: <code>${esc(runStatus)}</code>.</div>
  </header>
  <div class="grid">
    <div class="card"><div class="label">Candidates</div><div class="value">${stats.candidates.toLocaleString()}</div></div>
    <div class="card"><div class="label">Processed</div><div class="value">${processed.toLocaleString()}</div></div>
    <div class="card"><div class="label">API Calls</div><div class="value">${stats.apiCalls.toLocaleString()}</div></div>
    <div class="card"><div class="label">Cached</div><div class="value">${stats.cached.toLocaleString()}</div></div>
    <div class="card"><div class="label">Normalized</div><div class="value">${stats.normalized.toLocaleString()}</div></div>
    <div class="card"><div class="label">Failed</div><div class="value">${stats.failed.toLocaleString()}</div></div>
  </div>
  <div class="track"><div class="fill"></div></div>
  <div class="scroll-note">Scroll sideways inside this row table to see mortgage balance, payment, equity, agent email, phone, and detail.</div>
  <div class="key-rows">
    ${progressRows.slice(0, 25).filter((row) => row.status !== "running").map((row) => `<div class="key-row">
      <div class="key-title"><div>${esc(row.address)}</div><div>${esc(row.status)}</div></div>
      <div class="key-fields">
        <div class="key-field"><span>List price</span>${money(row.listPrice)}</div>
        <div class="key-field"><span>Debt status</span>${debtStatus(row)}</div>
        <div class="key-field"><span>Payment</span>${paymentStatus(row)}</div>
        <div class="key-field"><span>Equity</span>${money(row.estimatedEquity)} ${row.equityPercent == null ? "" : `(${esc(row.equityPercent)}%)`}</div>
        <div class="key-field"><span>Mortgages</span>${row.currentMortgageCount ?? "-"} current / ${row.mortgageHistoryCount ?? "-"} history</div>
        <div class="key-field"><span>Agent email</span><code>${esc(row.agentEmail ?? "-")}</code></div>
        <div class="key-field"><span>Agent phone</span><code>${esc(row.agentPhone ?? "-")}</code></div>
        <div class="key-field"><span>Detail</span><code>${esc(row.detail || "-")}</code></div>
      </div>
    </div>`).join("")}
  </div>
  <div class="scroll-actions"><button type="button" onclick="document.querySelector('.table-wrap').scrollLeft-=500">Scroll left</button><button type="button" onclick="document.querySelector('.table-wrap').scrollLeft+=500">Scroll right</button></div>
  <div class="table-wrap"><table>
    <tr><th>Time</th><th>Status</th><th>Source</th><th>Property</th><th>Address</th><th>List Price</th><th>Mortgage Balance</th><th>Payment</th><th>Equity</th><th>Equity %</th><th>Mortgages</th><th>Agent Email</th><th>Agent Phone</th><th>Reasons</th><th>Detail</th></tr>
    ${progressRows.map((row) => `<tr><td>${esc(row.at)}</td><td><code>${esc(row.status)}</code></td><td>${esc(row.source)}</td><td>${row.propertyId}</td><td>${esc(row.address)}</td><td class="money">${money(row.listPrice)}</td><td class="money">${debtStatus(row)}</td><td class="money">${paymentStatus(row)}</td><td class="money">${money(row.estimatedEquity)}</td><td class="money">${row.equityPercent == null ? `<span class="muted">-</span>` : `${esc(row.equityPercent)}%`}</td><td>${row.currentMortgageCount ?? "-"} current / ${row.mortgageHistoryCount ?? "-"} history</td><td>${esc(row.agentEmail ?? "-")}</td><td>${esc(row.agentPhone ?? "-")}</td><td>${esc(displayReasons(row))}</td><td>${esc(row.detail)}</td></tr>`).join("")}
  </table></div>
</body>
</html>`;
}

function esc(value: unknown) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function money(value: unknown) {
  if (value == null || value === "") return `<span class="muted">-</span>`;
  const num = Number(value);
  return Number.isFinite(num) && num >= 0 ? `$${Math.round(num).toLocaleString()}` : `<span class="muted">-</span>`;
}

function isFreeClear(row: ProgressRow) {
  return row.currentMortgageCount === 0
    && (row.mortgageBalance == null || row.mortgageBalance === 0)
    && /(?:estimatedBalance=0|openBalance=0|currentMortgages=0)/.test(row.detail);
}

function debtStatus(row: ProgressRow) {
  if (isFreeClear(row)) return `$0 <span class="muted">free & clear</span>`;
  return money(row.mortgageBalance);
}

function paymentStatus(row: ProgressRow) {
  if (isFreeClear(row)) return `$0 <span class="muted">no current debt payment</span>`;
  return money(row.estimatedPayment);
}

function displayReasons(row: ProgressRow) {
  if (isFreeClear(row)) return row.reasons.replace(/missing_mortgage_balance/g, "free_and_clear_debt_covered");
  return row.reasons;
}

async function ensureQueue(): Promise<Candidate[]> {
  const { rows } = await client.query<Candidate>(`
    with active as (
      select distinct on (p.id)
        p.id as property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        l.id as listing_id,
        l.mls_list_price,
        coalesce(nullif(l.address,''), p.address) as search_address,
        coalesce(nullif(l.city,''), p.city) as search_city,
        coalesce(nullif(l.zip,''), p.zip) as search_zip,
        array_remove(array[
          case when not ${hasDebtCoverageSql} then 'missing_mortgage_balance' end,
          case when nullif(l.listing_agent_email,'') is null then 'missing_agent_email' end,
          case when nullif(l.listing_agent_phone,'') is null then 'missing_agent_phone' end,
          case when not exists (select 1 from mls_history mh where mh.property_id = p.id) then 'missing_mls_history' end
        ], null) as reasons
      from listing_signals l
      join properties p on p.id = l.property_id
      left join realestateapi_property_details cache on cache.property_id = p.id
      where l.is_on_market = true
        and p.state_code = $1
        and l.state_code = $1
        and upper(coalesce(l.city,'')) = $2
        and ${hasUsablePaidSearchAddressSql}
        and (
          $3::boolean = true
          or $5::boolean = true
          or cache.id is null
        )
        and (
          (
            $5::boolean = true
            and not ${hasDebtCoverageSql}
          )
          or (
            $5::boolean = false
            and (
              $3::boolean = true
              or nullif(l.listing_agent_email,'') is null
              or nullif(l.listing_agent_phone,'') is null
              or not exists (select 1 from mls_history mh where mh.property_id = p.id)
              or not ${hasDebtCoverageSql}
            )
          )
        )
      order by p.id, l.last_seen_at desc nulls last, l.updated_at desc nulls last
    )
    select * from active
    where array_length(reasons, 1) > 0
      and (
        $5::boolean = false
        or 'missing_mortgage_balance' = any(reasons)
      )
    order by array_length(reasons, 1) desc, property_id
    limit $4;
  `, [state, city, force, limit, onlyMissingEquity]);

  const queueRows: Array<{ propertyId: number; reason: string; priority: number }> = [];
  for (const candidate of rows) {
    const reasons = onlyMissingEquity ? candidate.reasons.filter((reason) => reason === "missing_mortgage_balance") : candidate.reasons;
    candidate.reasons = reasons.length > 0 ? reasons : ["missing_mortgage_balance"];
    for (const reason of candidate.reasons) {
      if (dryRun) {
        stats.queued++;
        continue;
      }
      queueRows.push({ propertyId: candidate.property_id, reason, priority: priorityForReason(reason) });
    }
  }

  if (queueRows.length > 0) {
    await client.query(`
      insert into property_enrichment_queue(property_id, provider, reason, status, priority, next_run_at)
      select property_id, 'realestateapi', reason, 'queued', priority, now()
      from unnest($1::int[], $2::text[], $3::int[]) as q(property_id, reason, priority)
      on conflict(property_id, provider, reason)
      do update set
        status = case when property_enrichment_queue.status = 'completed' then 'queued' else property_enrichment_queue.status end,
        priority = least(property_enrichment_queue.priority, excluded.priority),
        next_run_at = least(property_enrichment_queue.next_run_at, excluded.next_run_at),
        updated_at = now()
    `, [
      queueRows.map((row) => row.propertyId),
      queueRows.map((row) => row.reason),
      queueRows.map((row) => row.priority),
    ]);
    stats.queued += queueRows.length;
  }

  return rows;
}

async function loadCandidates(): Promise<Candidate[]> {
  if (cacheOnly) {
    const { rows } = await client.query<Candidate>(`
      select
        p.id as property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        null::int as listing_id,
        null::numeric as mls_list_price,
        p.address as search_address,
        p.city as search_city,
        p.zip as search_zip,
        array[case when $4::boolean then 'missing_mortgage_balance' else 'cache_only_renormalize' end]::text[] as reasons
      from realestateapi_property_details r
      join properties p on p.id = r.property_id
      join listing_signals l on l.property_id = p.id and l.is_on_market = true
      where p.state_code = $1
        and upper(coalesce(p.city,'')) = $2
        and (
          $4::boolean = false
          or not ${hasDebtCoverageSql}
        )
      group by p.id, p.address, p.city, p.state_code, p.zip
      order by p.id
      limit $3
    `, [state, city, limit, onlyMissingEquity]);
    return rows;
  }

  if (dryRun) {
    const { rows } = await client.query<Candidate>(`
      select
        p.id as property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        l.id as listing_id,
        l.mls_list_price,
        coalesce(nullif(l.address,''), p.address) as search_address,
        coalesce(nullif(l.city,''), p.city) as search_city,
        coalesce(nullif(l.zip,''), p.zip) as search_zip,
        array_agg(distinct q.reason order by q.reason) as reasons
      from property_enrichment_queue q
      join properties p on p.id = q.property_id
      join lateral (
        select id, address, city, state_code, zip, mls_list_price
          from listing_signals
         where property_id = p.id
           and is_on_market = true
           and state_code = $1
           and upper(coalesce(city,'')) = $2
         order by last_seen_at desc nulls last, updated_at desc nulls last
         limit 1
      ) l on true
      where q.provider = 'realestateapi'
        and q.status in ('queued','failed')
        and p.state_code = $1
        and ${hasUsablePaidSearchAddressSql}
        and (
          $4::boolean = true
          or not exists (
            select 1 from realestateapi_property_details nd
            where nd.property_id = p.id and nd.status = 'no_data'
          )
        )
      group by p.id, p.address, p.city, p.state_code, p.zip, l.id, l.address, l.city, l.zip, l.mls_list_price
      order by min(q.priority), p.id
      limit $3;
    `, [state, city, limit, force]);
    return rows;
  }

  const { rows } = await client.query<Candidate>(`
    select
      p.id as property_id,
      p.address,
      p.city,
      p.state_code,
      p.zip,
      l.id as listing_id,
      l.mls_list_price,
      coalesce(nullif(l.address,''), p.address) as search_address,
      coalesce(nullif(l.city,''), p.city) as search_city,
      coalesce(nullif(l.zip,''), p.zip) as search_zip,
      array_agg(distinct q.reason order by q.reason) as reasons
    from property_enrichment_queue q
    join properties p on p.id = q.property_id
    join lateral (
      select id, address, city, state_code, zip, mls_list_price
        from listing_signals
       where property_id = p.id
         and is_on_market = true
         and state_code = $1
         and upper(coalesce(city,'')) = $2
       order by last_seen_at desc nulls last, updated_at desc nulls last
       limit 1
    ) l on true
    where q.provider = 'realestateapi'
      and q.status in ('queued','failed')
      and q.next_run_at <= now()
      and p.state_code = $1
      and ${hasUsablePaidSearchAddressSql}
      and (
        $4::boolean = true
        or not exists (
          select 1 from realestateapi_property_details nd
          where nd.property_id = p.id and nd.status = 'no_data'
        )
      )
    group by p.id, p.address, p.city, p.state_code, p.zip, l.id, l.address, l.city, l.zip, l.mls_list_price
    order by min(q.priority), p.id
    limit $3;
  `, [state, city, limit, force]);
  return rows;
}

async function loadFreshCache(propertyId: number) {
  const { rows } = await client.query<{
    request_body: Record<string, unknown>;
    response_body: ReapiResponse;
  }>(`
    select request_body, response_body
    from realestateapi_property_details
    where property_id = $1
      and status = 'ok'
      and expires_at > now()
      and response_body <> '{}'::jsonb
    limit 1
  `, [propertyId]);
  return rows[0] ?? null;
}

async function loadAnyCache(propertyId: number) {
  const { rows } = await client.query<{
    request_body: Record<string, unknown>;
    response_body: ReapiResponse;
  }>(`
    select request_body, response_body
    from realestateapi_property_details
    where property_id = $1
    order by fetched_at desc nulls last, updated_at desc nulls last
    limit 1
  `, [propertyId]);
  return rows[0] ?? null;
}

function buildRequest(candidate: Candidate) {
  const address = candidate.search_address ?? candidate.address;
  const requestCity = candidate.search_city ?? candidate.city;
  const requestZip = candidate.search_zip ?? candidate.zip;
  const label = [
    address,
    requestCity,
    candidate.state_code,
    requestZip,
  ].filter(Boolean).join(", ");
  return {
    address: label,
    exact_match: true,
    comps: false,
  };
}

async function callRealEstateApi(body: Record<string, unknown>): Promise<ReapiResponse> {
  const response = await fetch("https://api.realestateapi.com/v2/PropertyDetail", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
      "x-api-key": reapiKey ?? "",
      "x-user-id": "mxre-fallback-enrichment",
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  });

  const text = await response.text();
  if (!response.ok) throw new Error(`RealEstateAPI ${response.status}: ${text.slice(0, 500)}`);
  const parsed = JSON.parse(text) as ReapiResponse;
  return normalizeResponseEnvelope(parsed);
}

function normalizeResponseEnvelope(response: ReapiResponse): ReapiResponse {
  const nestedStatus = numberOrNull(response.statusCode);
  if (nestedStatus != null && nestedStatus >= 400) {
    throw new Error(`RealEstateAPI ${nestedStatus}: ${stringOrNull(response.message) ?? stringOrNull(response.statusMessage) ?? "provider returned no property detail"}`);
  }
  const data = response.data;
  if (data && typeof data === "object" && !Array.isArray(data)) {
    const normalized = data as ReapiResponse;
    if (Object.keys(normalized).length === 0) {
      throw new Error("RealEstateAPI no_data: provider returned empty property detail");
    }
    return normalized;
  }
  return response;
}

function isProviderNoData(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /RealEstateAPI (?:404|no_data)|provider returned empty property detail|provider returned no property detail|must contain at least one of \[zip, city\]/i.test(message);
}

async function cacheResponse(candidate: Candidate, requestBody: Record<string, unknown>, response: ReapiResponse) {
  await client.query(`
    insert into realestateapi_property_details(
      property_id, realestateapi_id, request_body, response_body, normalized_summary, status, fetched_at, expires_at, updated_at
    )
    values ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, 'ok', now(), now() + ($6::text || ' days')::interval, now())
    on conflict(property_id)
    do update set
      realestateapi_id = excluded.realestateapi_id,
      request_body = excluded.request_body,
      response_body = excluded.response_body,
      normalized_summary = excluded.normalized_summary,
      status = 'ok',
      fetched_at = now(),
      expires_at = excluded.expires_at,
      updated_at = now()
  `, [
    candidate.property_id,
    stringOrNull(response.id),
    JSON.stringify(requestBody),
    JSON.stringify(response),
    JSON.stringify(summarizeResponse(response, candidate.mls_list_price ?? null)),
    staleDays,
  ]);
}

async function cacheNoData(candidate: Candidate, requestBody: Record<string, unknown>, reason: string) {
  await client.query(`
    insert into realestateapi_property_details(
      property_id, request_body, response_body, normalized_summary, status, fetched_at, expires_at, updated_at
    )
    values ($1, $2::jsonb, '{}'::jsonb, $3::jsonb, 'no_data', now(), now() + ($4::text || ' days')::interval, now())
    on conflict(property_id)
    do update set
      request_body = excluded.request_body,
      response_body = excluded.response_body,
      normalized_summary = excluded.normalized_summary,
      status = 'no_data',
      fetched_at = now(),
      expires_at = excluded.expires_at,
      updated_at = now()
  `, [
    candidate.property_id,
    JSON.stringify(requestBody),
    JSON.stringify({ coverageState: "no_data", reason: reason.slice(0, 500) }),
    staleDays,
  ]);
}

async function normalizeAndStore(candidate: Candidate, response: ReapiResponse, requestBody: Record<string, unknown>) {
  await client.query("begin");
  try {
    await upsertMortgages(candidate.property_id, response);
    await upsertSales(candidate.property_id, response);
    await upsertMlsHistory(candidate.property_id, response);
    await updateListingAgent(candidate.property_id, response);
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  }
}

async function upsertMortgages(propertyId: number, response: ReapiResponse) {
  const mortgages = dedupeByKey([
    ...arrayOfObjects(response.currentMortgages),
    ...arrayOfObjects(response.mortgageHistory),
  ], (row) => [
    stringOrNull(row.documentNumber) ?? "",
    stringOrNull(row.recordingDate) ?? stringOrNull(row.documentDate) ?? "",
    numberOrNull(row.amount) ?? "",
    stringOrNull(row.position) ?? "",
  ].join("|"));

  await client.query("delete from mortgage_records where property_id = $1 and source_url = 'realestateapi'", [propertyId]);

  const currentRows = mortgages.filter((row) => boolOrNull(row.open) !== false);
  const currentAmountTotal = currentRows.reduce((sum, row) => sum + (numberOrNull(row.amount) ?? 0), 0);
  const aggregateBalance = numberOrNull(response.estimatedMortgageBalance) ?? numberOrNull(response.openMortgageBalance);
  const aggregatePayment = numberOrNull(response.estimatedMortgagePayment);

  for (const row of mortgages) {
    const allocated = allocateMortgageEstimate(row, currentAmountTotal, aggregateBalance, aggregatePayment);
    await client.query(`
      insert into mortgage_records(
        property_id, document_type, recording_date, loan_amount, original_amount, lender_name, borrower_name,
        document_number, source_url, loan_type, open, position, interest_rate, interest_rate_type,
        lender_type, lender_code, loan_type_code, grantee_name, assumable, seq_no, term_months, term_type,
        maturity_date, estimated_current_balance, estimated_monthly_payment, raw, created_at
      )
      values (
        $1, $2, $3, $4, $4, $5, $6,
        $7, 'realestateapi', $8, $9, $10, $11, $12,
        $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24::jsonb, now()
      )
    `, [
      propertyId,
      stringOrNull(row.deedType) || "mortgage",
      dateOrNull(row.recordingDate) ?? dateOrNull(row.documentDate),
      dbIntOrNull(row.amount),
      stringOrNull(row.lenderName),
      stringOrNull(row.granteeName),
      stringOrNull(row.documentNumber),
      stringOrNull(row.loanType),
      boolOrNull(row.open) ?? true,
      positionNumber(row.position) ?? numberOrNull(row.seqNo),
      boundedNumberOrNull(row.interestRate, 99.999),
      stringOrNull(row.interestRateType),
      stringOrNull(row.lenderType),
      stringOrNull(row.lenderCode),
      stringOrNull(row.loanTypeCode),
      stringOrNull(row.granteeName),
      boolOrNull(row.assumable),
      numberOrNull(row.seqNo),
      termToMonths(row.term, row.termType),
      stringOrNull(row.termType),
      dateOrNull(row.maturityDate),
      dbIntOrNull(allocated.estimatedCurrentBalance),
      dbIntOrNull(allocated.estimatedMonthlyPayment),
      JSON.stringify({ provider: "realestateapi", row }),
    ]);
  }
}

async function upsertSales(propertyId: number, response: ReapiResponse) {
  const sales = dedupeByKey(arrayOfObjects(response.saleHistory), (row) => [
    stringOrNull(row.documentNumber) ?? "",
    stringOrNull(row.recordingDate) ?? stringOrNull(row.saleDate) ?? "",
    numberOrNull(row.saleAmount) ?? "",
  ].join("|"));

  await client.query("delete from sale_history where property_id = $1 and source_url = 'realestateapi'", [propertyId]);

  for (const row of sales) {
    await client.query(`
      insert into sale_history(
        property_id, recording_date, sale_date, sale_amount, document_type, document_type_code, document_number,
        buyer_names, seller_names, arms_length, owner_individual, purchase_method, down_payment, ltv,
        transaction_type, seq_no, source_url, created_at
      )
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'realestateapi',now())
    `, [
      propertyId,
      dateOrNull(row.recordingDate) ?? dateOrNull(row.saleDate),
      dateOrNull(row.saleDate),
      numberOrNull(row.saleAmount),
      stringOrNull(row.documentType),
      stringOrNull(row.documentTypeCode),
      stringOrNull(row.documentNumber),
      stringOrNull(row.buyerNames),
      stringOrNull(row.sellerNames),
      boolOrNull(row.armsLength),
      boolOrNull(row.ownerIndividual),
      normalizePurchaseMethod(row.purchaseMethod),
      numberOrNull(row.downPayment),
      boundedNumberOrNull(row.ltv, 999.99),
      stringOrNull(row.transactionType),
      numberOrNull(row.seqNo),
    ]);
  }
}

async function upsertMlsHistory(propertyId: number, response: ReapiResponse) {
  const rows = dedupeByKey(arrayOfObjects(response.mlsHistory), (row) => [
    stringOrNull(row.statusDate) ?? stringOrNull(row.lastStatusDate) ?? "",
    stringOrNull(row.status) ?? "",
    numberOrNull(row.price) ?? "",
    stringOrNull(row.agentName) ?? "",
  ].join("|"));

  await client.query("delete from mls_history where property_id = $1 and listing_source = 'realestateapi'", [propertyId]);

  for (const row of rows) {
    await client.query(`
      insert into mls_history(
        property_id, status, status_date, last_status_date, price, listing_type, days_on_market,
        agent_name, agent_email, agent_phone, agent_office, beds, baths, listing_source, listing_url, raw, created_at
      )
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'realestateapi',$14,$15::jsonb,now())
    `, [
      propertyId,
      stringOrNull(row.status) ?? "unknown",
      dateOrNull(row.statusDate) ?? dateOrNull(row.lastStatusDate),
      dateOrNull(row.lastStatusDate),
      numberOrNull(row.price),
      stringOrNull(row.type),
      numberOrNull(row.daysOnMarket),
      stringOrNull(row.agentName),
      stringOrNull(row.agentEmail),
      stringOrNull(row.agentPhone),
      stringOrNull(row.agentOffice),
      numberOrNull(row.beds),
      boundedNumberOrNull(row.baths, 999.9),
      null,
      JSON.stringify({ provider: "realestateapi", row }),
    ]);
  }
}

async function updateListingAgent(propertyId: number, response: ReapiResponse) {
  const agent = bestAgent(response);
  if (!agent) return;
  const split = splitName(agent.name);
  await client.query(`
    update listing_signals
    set
      listing_agent_name = coalesce(nullif(listing_agent_name,''), $2),
      listing_agent_first_name = coalesce(nullif(listing_agent_first_name,''), $3),
      listing_agent_last_name = coalesce(nullif(listing_agent_last_name,''), $4),
      listing_agent_email = coalesce(nullif(listing_agent_email,''), $5),
      listing_agent_phone = coalesce(nullif(listing_agent_phone,''), $6),
      listing_brokerage = coalesce(nullif(listing_brokerage,''), $7),
      agent_contact_source = case
        when $5::text is not null or $6::text is not null then 'realestateapi'
        else agent_contact_source
      end,
      agent_contact_confidence = case
        when $5::text is not null or $6::text is not null then 'high'
        else agent_contact_confidence
      end,
      updated_at = now()
    where property_id = $1
      and is_on_market = true
  `, [
    propertyId,
    agent.name,
    split.firstName,
    split.lastName,
    agent.email,
    agent.phone,
    agent.brokerage,
  ]);
}

function bestAgent(response: ReapiResponse): { name: string | null; email: string | null; phone: string | null; brokerage: string | null } | null {
  const histories = arrayOfObjects(response.mlsHistory);
  for (const row of histories) {
    const email = stringOrNull(row.agentEmail);
    const phone = stringOrNull(row.agentPhone);
    const name = stringOrNull(row.agentName);
    if (email || phone || name) {
      return {
        name,
        email,
        phone,
        brokerage: stringOrNull(row.agentOffice),
      };
    }
  }
  return null;
}

async function markQueueRunning(propertyId: number, reasons: string[]) {
  if (dryRun) return;
  await client.query(`
    update property_enrichment_queue
    set status = 'running', locked_at = now(), attempts = attempts + 1, updated_at = now()
    where property_id = $1 and provider = 'realestateapi' and reason = any($2::text[])
  `, [propertyId, reasons]);
}

async function markQueueComplete(propertyId: number, reasons: string[]) {
  if (dryRun) return;
  await client.query(`
    update property_enrichment_queue
    set status = 'completed', completed_at = now(), last_error = null, updated_at = now()
    where property_id = $1 and provider = 'realestateapi' and reason = any($2::text[])
  `, [propertyId, reasons]);
}

async function markQueueFailed(propertyId: number, reasons: string[], error: string) {
  if (dryRun) return;
  await client.query(`
    update property_enrichment_queue
    set
      status = 'failed',
      last_error = $3,
      next_run_at = now() + interval '6 hours',
      locked_at = null,
      updated_at = now()
    where property_id = $1 and provider = 'realestateapi' and reason = any($2::text[])
  `, [propertyId, reasons, error.slice(0, 1000)]);
}

function summarizeResponse(response: ReapiResponse, fallbackMarketValue: number | null = null) {
  const currentMortgageCount = arrayOfObjects(response.currentMortgages).length;
  const hasPropertyIdentity = stringOrNull(response.id)
    ?? stringOrNull(response.propertyId)
    ?? stringOrNull(response.apn)
    ?? stringOrNull(response.address)
    ?? stringOrNull(response.formattedAddress)
    ?? stringOrNull(response.owner1FullName);
  if (!hasPropertyIdentity && Object.keys(response).length === 0) {
    throw new Error("Cannot summarize empty RealEstateAPI response as coverage.");
  }
  const estimatedMortgageBalance = numberOrNull(response.estimatedMortgageBalance);
  const openMortgageBalance = numberOrNull(response.openMortgageBalance);
  const isFreeClear = currentMortgageCount === 0
    && (estimatedMortgageBalance == null || estimatedMortgageBalance === 0)
    && (openMortgageBalance == null || openMortgageBalance === 0);
  const marketValue = numberOrNull(response.estimatedValue)
    ?? numberOrNull(response.value)
    ?? numberOrNull(response.price)
    ?? fallbackMarketValue;
  const effectiveOpenBalance = isFreeClear ? 0 : openMortgageBalance;
  const effectiveEstimatedBalance = isFreeClear ? 0 : estimatedMortgageBalance;
  const estimatedEquity = numberOrNull(response.estimatedEquity)
    ?? (isFreeClear && marketValue != null ? marketValue : null);
  const equityPercent = numberOrNull(response.equityPercent)
    ?? (isFreeClear && marketValue != null && marketValue > 0 ? 100 : null);
  return {
    id: stringOrNull(response.id),
    hasMortgageHistory: arrayOfObjects(response.mortgageHistory).length > 0,
    mortgageHistoryCount: arrayOfObjects(response.mortgageHistory).length,
    currentMortgageCount,
    saleHistoryCount: arrayOfObjects(response.saleHistory).length,
    mlsHistoryCount: arrayOfObjects(response.mlsHistory).length,
    estimatedMortgageBalance: effectiveEstimatedBalance,
    openMortgageBalance: effectiveOpenBalance,
    estimatedEquity,
    equityPercent,
    freeClear: isFreeClear,
    lastUpdateDate: stringOrNull(response.lastUpdateDate),
  };
}

function arrayOfObjects(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
}

function dedupeByKey<T>(rows: T[], keyFn: (row: T) => string): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const row of rows) {
    const key = keyFn(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function stringOrNull(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function numberOrNull(value: unknown): number | null {
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? Math.round(num) : null;
}

function boundedNumberOrNull(value: unknown, maxAbs: number): number | null {
  if (value == null || value === "") return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.abs(num) <= maxAbs ? num : null;
}

function dbIntOrNull(value: unknown): number | null {
  const num = numberOrNull(value);
  if (num == null) return null;
  if (num > 2_147_483_647 || num < -2_147_483_648) return null;
  return num;
}

function boolOrNull(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function dateOrNull(value: unknown): string | null {
  const text = stringOrNull(value);
  if (!text) return null;
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function termToMonths(term: unknown, termType: unknown): number | null {
  const value = numberOrNull(term);
  if (value == null) return null;
  const type = String(termType ?? "").toLowerCase();
  if (type.includes("year")) return value * 12;
  return value;
}

function positionNumber(value: unknown): number | null {
  const text = stringOrNull(value)?.toLowerCase();
  if (!text) return null;
  if (text === "first" || text === "1st") return 1;
  if (text === "second" || text === "2nd") return 2;
  if (text === "third" || text === "3rd") return 3;
  return numberOrNull(value);
}

function normalizePurchaseMethod(value: unknown): string | null {
  const text = stringOrNull(value)?.toLowerCase();
  if (!text) return null;
  if (text.includes("cash")) return "cash";
  if (text.includes("financ")) return "financed";
  return text;
}

function allocateMortgageEstimate(
  row: Record<string, unknown>,
  currentAmountTotal: number,
  aggregateBalance: number | null,
  aggregatePayment: number | null,
) {
  const open = boolOrNull(row.open) !== false;
  const amount = numberOrNull(row.amount);
  if (!open || !aggregateBalance || !amount || currentAmountTotal <= 0) {
    return { estimatedCurrentBalance: null, estimatedMonthlyPayment: null };
  }

  const ratio = amount / currentAmountTotal;
  return {
    estimatedCurrentBalance: Math.round(aggregateBalance * ratio),
    estimatedMonthlyPayment: aggregatePayment ? Math.round(aggregatePayment * ratio) : null,
  };
}

function splitName(name: string | null) {
  if (!name) return { firstName: null, lastName: null };
  const parts = name.trim().split(/\s+/);
  if (parts.length === 1) return { firstName: null, lastName: parts[0] };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

function priorityForReason(reason: string): number {
  if (reason === "missing_mortgage_balance") return 10;
  if (reason === "missing_agent_email") return 20;
  if (reason === "missing_agent_phone") return 25;
  if (reason === "missing_mls_history") return 30;
  return 100;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
