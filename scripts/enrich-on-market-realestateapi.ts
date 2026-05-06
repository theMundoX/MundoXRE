#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";

type ReapiResponse = Record<string, unknown>;

type Candidate = {
  property_id: number;
  address: string;
  city: string;
  state_code: string;
  zip: string | null;
  listing_id?: number | null;
  search_address?: string | null;
  search_city?: string | null;
  search_zip?: string | null;
  reasons: string[];
};

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const force = args.includes("--force");
const cacheOnly = args.includes("--cache-only");
const skipEnsureQueue = args.includes("--skip-ensure-queue");
const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const limit = Math.min(Math.max(Number(valueArg("limit") ?? "25"), 1), 2500);
const maxCalls = Math.min(Math.max(Number(valueArg("max-calls") ?? String(limit)), 0), limit);
const staleDays = Math.max(Number(valueArg("stale-days") ?? "30"), 1);
const reapiKey = process.env.REALESTATEAPI_KEY
  ?? process.env.REALESTATE_API_KEY
  ?? process.env.REALESTATEAPI_API_KEY;
const databaseUrl = process.env.MXRE_DIRECT_PG_URL
  ?? process.env.MXRE_PG_URL
  ?? process.env.DATABASE_URL
  ?? process.env.POSTGRES_URL;

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
  queued: 0,
  candidates: 0,
  apiCalls: 0,
  cached: 0,
  normalized: 0,
  failed: 0,
};

try {
  const previewCandidates = skipEnsureQueue ? [] : await ensureQueue();
  const candidates = dryRun ? previewCandidates : await loadCandidates();
  stats.candidates = candidates.length;

  for (const candidate of candidates) {
    if (!cacheOnly && stats.apiCalls >= maxCalls && !dryRun) break;
    try {
      await markQueueRunning(candidate.property_id, candidate.reasons);
      const cached = cacheOnly ? await loadAnyCache(candidate.property_id) : (force ? null : await loadFreshCache(candidate.property_id));
      if (cached) {
        stats.cached++;
        if (!dryRun) await normalizeAndStore(candidate, cached.response_body as ReapiResponse, cached.request_body as Record<string, unknown>);
        await markQueueComplete(candidate.property_id, candidate.reasons);
        stats.normalized++;
        continue;
      }

      if (cacheOnly) continue;

      const requestBody = buildRequest(candidate);
      if (dryRun) {
        console.log(JSON.stringify({ wouldCall: requestBody, property_id: candidate.property_id, reasons: candidate.reasons }));
        continue;
      }

      const response = await callRealEstateApi(requestBody);
      stats.apiCalls++;
      await cacheResponse(candidate.property_id, requestBody, response);
      await normalizeAndStore(candidate, response, requestBody);
      await markQueueComplete(candidate.property_id, candidate.reasons);
      stats.normalized++;
      await sleep(350);
    } catch (error) {
      stats.failed++;
      await markQueueFailed(candidate.property_id, candidate.reasons, error instanceof Error ? error.message : String(error));
      console.error(`Failed property ${candidate.property_id}:`, error instanceof Error ? error.message : error);
    }
  }
} finally {
  await client.end();
}

console.log(JSON.stringify(stats, null, 2));

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
        coalesce(nullif(l.address,''), p.address) as search_address,
        coalesce(nullif(l.city,''), p.city) as search_city,
        coalesce(nullif(l.zip,''), p.zip) as search_zip,
        array_remove(array[
          case when not exists (
            select 1 from mortgage_records mr
            where mr.property_id = p.id
              and coalesce(mr.open, true) = true
              and coalesce(nullif(mr.loan_amount,0), nullif(mr.original_amount,0), nullif(mr.estimated_current_balance,0)) is not null
          ) then 'missing_mortgage_balance' end,
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
        and (
          $3::boolean = true
          or cache.id is null
        )
        and (
          $3::boolean = true
          or nullif(l.listing_agent_email,'') is null
          or nullif(l.listing_agent_phone,'') is null
          or not exists (select 1 from mls_history mh where mh.property_id = p.id)
          or not exists (
            select 1 from mortgage_records mr
            where mr.property_id = p.id
              and coalesce(mr.open, true) = true
              and coalesce(nullif(mr.loan_amount,0), nullif(mr.original_amount,0), nullif(mr.estimated_current_balance,0)) is not null
          )
        )
      order by p.id, l.last_seen_at desc nulls last, l.updated_at desc nulls last
    )
    select * from active
    where array_length(reasons, 1) > 0
    order by array_length(reasons, 1) desc, property_id
    limit $4;
  `, [state, city, force, limit]);

  for (const candidate of rows) {
    for (const reason of candidate.reasons) {
      if (dryRun) {
        stats.queued++;
        continue;
      }
      await client.query(`
        insert into property_enrichment_queue(property_id, provider, reason, status, priority, next_run_at)
        values ($1, 'realestateapi', $2, 'queued', $3, now())
        on conflict(property_id, provider, reason)
        do update set
          status = case when property_enrichment_queue.status = 'completed' then 'queued' else property_enrichment_queue.status end,
          priority = least(property_enrichment_queue.priority, excluded.priority),
          next_run_at = least(property_enrichment_queue.next_run_at, excluded.next_run_at),
          updated_at = now()
      `, [candidate.property_id, reason, priorityForReason(reason)]);
      stats.queued++;
    }
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
        p.address as search_address,
        p.city as search_city,
        p.zip as search_zip,
        array['cache_only_renormalize']::text[] as reasons
      from realestateapi_property_details r
      join properties p on p.id = r.property_id
      join listing_signals l on l.property_id = p.id and l.is_on_market = true
      where p.state_code = $1
        and upper(coalesce(p.city,'')) = $2
      group by p.id, p.address, p.city, p.state_code, p.zip
      order by p.id
      limit $3
    `, [state, city, limit]);
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
        coalesce(nullif(l.address,''), p.address) as search_address,
        coalesce(nullif(l.city,''), p.city) as search_city,
        coalesce(nullif(l.zip,''), p.zip) as search_zip,
        array_agg(distinct q.reason order by q.reason) as reasons
      from property_enrichment_queue q
      join properties p on p.id = q.property_id
      join lateral (
        select id, address, city, state_code, zip
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
      group by p.id, p.address, p.city, p.state_code, p.zip, l.id, l.address, l.city, l.zip
      order by min(q.priority), p.id
      limit $3;
    `, [state, city, limit]);
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
      coalesce(nullif(l.address,''), p.address) as search_address,
      coalesce(nullif(l.city,''), p.city) as search_city,
      coalesce(nullif(l.zip,''), p.zip) as search_zip,
      array_agg(distinct q.reason order by q.reason) as reasons
    from property_enrichment_queue q
    join properties p on p.id = q.property_id
    join lateral (
      select id, address, city, state_code, zip
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
    group by p.id, p.address, p.city, p.state_code, p.zip, l.id, l.address, l.city, l.zip
    order by min(q.priority), p.id
    limit $3;
  `, [state, city, limit]);
  return rows;
}

async function loadFreshCache(propertyId: number) {
  const { rows } = await client.query<{
    request_body: Record<string, unknown>;
    response_body: ReapiResponse;
  }>(`
    select request_body, response_body
    from realestateapi_property_details
    where property_id = $1 and expires_at > now()
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
  const label = [
    candidate.search_address ?? candidate.address,
    candidate.search_city ?? candidate.city,
    candidate.state_code,
    candidate.search_zip ?? candidate.zip,
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
  const data = response.data;
  if (data && typeof data === "object" && !Array.isArray(data)) return data as ReapiResponse;
  return response;
}

async function cacheResponse(propertyId: number, requestBody: Record<string, unknown>, response: ReapiResponse) {
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
    propertyId,
    stringOrNull(response.id),
    JSON.stringify(requestBody),
    JSON.stringify(response),
    JSON.stringify(summarizeResponse(response)),
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

function summarizeResponse(response: ReapiResponse) {
  return {
    id: stringOrNull(response.id),
    hasMortgageHistory: arrayOfObjects(response.mortgageHistory).length > 0,
    currentMortgageCount: arrayOfObjects(response.currentMortgages).length,
    saleHistoryCount: arrayOfObjects(response.saleHistory).length,
    mlsHistoryCount: arrayOfObjects(response.mlsHistory).length,
    estimatedMortgageBalance: numberOrNull(response.estimatedMortgageBalance),
    openMortgageBalance: numberOrNull(response.openMortgageBalance),
    estimatedEquity: numberOrNull(response.estimatedEquity),
    equityPercent: numberOrNull(response.equityPercent),
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
