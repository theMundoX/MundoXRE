export interface Env {
  MXRE_ORIGIN_URL?: string;
  MXRE_UPSTREAM_API_KEY?: string;
  MXRE_CLIENT_API_KEYS?: string;
  MXRE_BUY_BOX_CLUB_KEY?: string;
  SUPABASE_URL?: string;
  SUPABASE_SERVICE_KEY?: string;
  CACHE_SECONDS?: string;
}

type ApiClient = {
  id: string;
  key: string;
  environment?: string;
  monthlyQuota?: number;
};

function json(body: unknown, status = 200, headers: HeadersInit = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      ...headers,
    },
  });
}

function loadClients(env: Env): ApiClient[] {
  try {
    const parsed = JSON.parse(env.MXRE_CLIENT_API_KEYS || '[]') as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((client) => client as Partial<ApiClient>)
      .filter((client): client is ApiClient => Boolean(client.id && client.key));
  } catch {
    return [];
  }
}

function authenticate(request: Request, env: Env): ApiClient | null {
  const apiKey = request.headers.get('x-api-key');
  const clientId = request.headers.get('x-client-id');
  if (!apiKey) return null;

  if (env.MXRE_BUY_BOX_CLUB_KEY && apiKey === env.MXRE_BUY_BOX_CLUB_KEY) {
    if (clientId && clientId !== 'buy_box_club_prod') return null;
    return { id: 'buy_box_club_prod', key: apiKey, environment: 'production' };
  }

  const matches = loadClients(env).filter((client) => client.key === apiKey);
  if (matches.length === 0) return null;
  if (clientId) return matches.find((client) => client.id === clientId) ?? null;
  return matches[0] ?? null;
}

function shouldCache(request: Request, url: URL): boolean {
  if (request.method !== 'GET') return false;
  if (url.pathname === '/health') return false;
  if (!url.pathname.startsWith('/v1/')) return false;
  return true;
}

function positiveInt(value: string | null, fallback: number, max: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function dateParam(value: string | null): string | null {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  return value;
}

function sqlText(value: string): string {
  return value.replace(/'/g, "''");
}

async function queryPg<T>(env: Env, query: string): Promise<T[]> {
  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) {
    throw new Error('Supabase is not configured on the Worker.');
  }
  const response = await fetch(`${env.SUPABASE_URL.replace(/\/$/, '')}/pg/query`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      apikey: env.SUPABASE_SERVICE_KEY,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Database query failed: ${response.status} ${detail.slice(0, 300)}`);
  }
  const body = await response.json() as { data?: T[] } | T[];
  return Array.isArray(body) ? body : body.data ?? [];
}

async function creativeFinanceReport(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const market = url.pathname.split('/')[3]?.toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const asset = (url.searchParams.get('asset') ?? 'all').toLowerCase();
  if (!['all', 'single_family', 'multifamily'].includes(asset)) {
    return json({ error: 'Unsupported asset', supported_assets: ['all', 'single_family', 'multifamily'] }, 400);
  }

  const status = (url.searchParams.get('status') ?? 'positive').toLowerCase();
  if (!['positive', 'negative', 'all'].includes(status)) {
    return json({ error: 'Unsupported status', supported_statuses: ['positive', 'negative', 'all'] }, 400);
  }

  const zip = url.searchParams.get('zip')?.replace(/[^\d]/g, '').slice(0, 5) || null;
  const minPrice = url.searchParams.get('min_price')?.replace(/[^\d]/g, '') || null;
  const maxPrice = url.searchParams.get('max_price')?.replace(/[^\d]/g, '') || null;
  const minUnits = url.searchParams.get('min_units')?.replace(/[^\d]/g, '') || null;
  const maxUnits = url.searchParams.get('max_units')?.replace(/[^\d]/g, '') || null;
  const since = dateParam(url.searchParams.get('since'));
  const until = dateParam(url.searchParams.get('until'));
  const page = positiveInt(url.searchParams.get('page'), 1, 100000);
  const limit = positiveInt(url.searchParams.get('limit'), 50, 250);
  const offset = (page - 1) * limit;

  const assetWhere = asset === 'single_family'
    ? "and asset_group = 'single_family'"
    : asset === 'multifamily'
      ? "and asset_group in ('small_multifamily','commercial_multifamily')"
      : '';
  const statusWhere = status === 'all'
    ? "and l.creative_finance_status in ('positive','negative')"
    : `and l.creative_finance_status = '${sqlText(status)}'`;
  const listingWhere = [
    zip ? `and l.zip = '${sqlText(zip)}'` : '',
    minPrice ? `and l.mls_list_price >= ${minPrice}` : '',
    maxPrice ? `and l.mls_list_price <= ${maxPrice}` : '',
    minUnits ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '',
    maxUnits ? `and coalesce(p.total_units, 0) <= ${maxUnits}` : '',
    since ? `and coalesce(l.last_seen_at, l.first_seen_at) >= '${since}'::date` : '',
    until ? `and coalesce(l.last_seen_at, l.first_seen_at) < ('${until}'::date + interval '1 day')` : '',
  ].filter(Boolean).join('\n        ');

  const rows = await queryPg<Record<string, unknown>>(env, `
    with active as (
      select
        l.id as listing_id,
        l.property_id,
        coalesce(p.address, l.address) as address,
        coalesce(p.city, l.city) as city,
        coalesce(p.state_code, l.state_code) as state_code,
        coalesce(p.zip, l.zip) as zip,
        case
          when p.asset_type = 'commercial_multifamily' or coalesce(p.property_use, '') ilike '%APT%UNITS%' then 'commercial_multifamily'
          when p.asset_type = 'small_multifamily'
            or coalesce(p.property_use, '') ilike '%TWO FAMILY%'
            or coalesce(p.property_use, '') ilike '%THREE FAMILY%' then 'small_multifamily'
          when p.asset_subtype in ('sfr', 'condo')
            or coalesce(p.property_use, '') ilike '%ONE FAMILY%'
            or coalesce(p.property_use, '') ilike '%CONDO%'
            or coalesce(p.property_use, '') ilike 'RES VAC%' then 'single_family'
          when l.property_id is null then 'unlinked_listing'
          else coalesce(nullif(p.asset_type, ''), nullif(p.property_type, ''), 'unknown')
        end as asset_group,
        p.asset_type,
        p.asset_subtype,
        p.property_use,
        p.total_units,
        p.bedrooms,
        p.bathrooms,
        p.bathrooms_full,
        p.living_sqft,
        p.year_built,
        p.market_value,
        l.mls_list_price,
        l.days_on_market,
        l.listing_source,
        l.listing_url,
        l.listing_agent_name,
        l.listing_agent_first_name,
        l.listing_agent_last_name,
        l.listing_agent_email,
        l.listing_agent_phone,
        l.listing_brokerage,
        l.agent_contact_source,
        l.agent_contact_confidence,
        l.creative_finance_score,
        l.creative_finance_status,
        l.creative_finance_terms,
        l.creative_finance_negative_terms,
        l.creative_finance_rate_text,
        l.creative_finance_source,
        l.creative_finance_observed_at,
        l.first_seen_at,
        l.last_seen_at,
        l.raw
      from listing_signals l
      left join properties p on p.id = l.property_id
      where l.is_on_market = true
        and l.state_code = 'IN'
        and upper(trim(replace(coalesce(l.city, ''), ',', ''))) = 'INDIANAPOLIS'
        ${assetWhere}
        ${statusWhere}
        ${listingWhere}
    )
    select
      (select count(*)::int from active) as total,
      (select jsonb_build_object(
        'positive', count(*) filter (where creative_finance_status = 'positive'),
        'negative', count(*) filter (where creative_finance_status = 'negative'),
        'withRateText', count(*) filter (where creative_finance_rate_text is not null),
        'withAgentEmail', count(*) filter (where listing_agent_email is not null),
        'withAgentPhone', count(*) filter (where listing_agent_phone is not null),
        'withFullContact', count(*) filter (where listing_agent_email is not null and listing_agent_phone is not null),
        'medianListPrice', round(percentile_cont(0.5) within group (order by mls_list_price))::int,
        'medianCreativeScore', round(percentile_cont(0.5) within group (order by creative_finance_score))::int
      ) from active) as summary,
      (select coalesce(jsonb_agg(row_to_json(z)), '[]'::jsonb)
       from (
         select zip, count(*)::int as listings,
           count(*) filter (where creative_finance_status = 'positive')::int as "positive",
           count(*) filter (where creative_finance_status = 'negative')::int as "negative",
           count(*) filter (where listing_agent_email is not null)::int as "withAgentEmail",
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as "medianListPrice",
           round(percentile_cont(0.5) within group (order by creative_finance_score))::int as "medianCreativeScore"
         from active group by zip order by count(*) desc, zip
       ) z) as by_zip,
      (select coalesce(jsonb_agg(row_to_json(t)), '[]'::jsonb)
       from (
         select term, count(*)::int as listings
         from (
           select unnest(case when creative_finance_status = 'negative' then coalesce(creative_finance_negative_terms, array[]::text[]) else coalesce(creative_finance_terms, array[]::text[]) end) as term
           from active
         ) terms
         where term is not null and term <> ''
         group by term order by count(*) desc, term
       ) t) as by_term,
      (select coalesce(jsonb_agg(row_to_json(r)), '[]'::jsonb)
       from (
         select listing_id as "listingId", property_id as "propertyId", address, city, state_code as state, zip,
           asset_group as "assetGroup", asset_type as "assetType", asset_subtype as "assetSubtype",
           property_use as "propertyUse", total_units as "unitCount", bedrooms,
           coalesce(bathrooms, bathrooms_full) as bathrooms, living_sqft as "livingSqft",
           year_built as "yearBuilt", market_value as "marketValue", mls_list_price as "listPrice",
           days_on_market as "daysOnMarket", listing_source as "listingSource", listing_url as "listingUrl",
           listing_agent_name as "listingAgentName", listing_agent_first_name as "listingAgentFirstName",
           listing_agent_last_name as "listingAgentLastName", listing_agent_email as "listingAgentEmail",
           listing_agent_phone as "listingAgentPhone", listing_brokerage as "listingBrokerage",
           agent_contact_source as "agentContactSource", agent_contact_confidence as "agentContactConfidence",
           creative_finance_score as "creativeFinanceScore", creative_finance_status as "creativeFinanceStatus",
           creative_finance_terms as "creativeFinanceTerms", creative_finance_negative_terms as "creativeFinanceNegativeTerms",
           creative_finance_rate_text as "creativeFinanceRateText", creative_finance_source as "creativeFinanceSource",
           creative_finance_observed_at as "creativeFinanceObservedAt", first_seen_at as "firstSeenAt",
           last_seen_at as "lastSeenAt",
           left(coalesce(raw #>> '{redfinDetail,publicRemarks}', raw #>> '{redfinDetail,description}', raw #>> '{publicRemarks}', raw #>> '{remarks}', raw #>> '{description}', ''), 700) as "publicRemarksSnippet"
         from active
         order by coalesce(creative_finance_score, -1) desc, last_seen_at desc nulls last, address
         limit ${limit} offset ${offset}
       ) r) as results;
  `);

  const result = rows[0] ?? {};
  return json({
    market: 'indianapolis',
    report: 'creative_finance',
    geography: { scope: 'city', scope_label: 'Indianapolis City' },
    filters: { status, asset, zip, min_price: minPrice ? Number(minPrice) : null, max_price: maxPrice ? Number(maxPrice) : null, min_units: minUnits ? Number(minUnits) : null, max_units: maxUnits ? Number(maxUnits) : null, since, until },
    page,
    limit,
    total: result.total ?? 0,
    summary: result.summary ?? {},
    by_zip: result.by_zip ?? [],
    by_term: result.by_term ?? [],
    results: result.results ?? [],
    generated_at: new Date().toISOString(),
  });
}

async function proxyToOrigin(request: Request, env: Env, client: ApiClient | null): Promise<Response> {
  if (!env.MXRE_ORIGIN_URL || !env.MXRE_UPSTREAM_API_KEY) {
    return json({ error: 'Gateway origin not configured for this endpoint' }, 503);
  }
  const incomingUrl = new URL(request.url);
  const originUrl = new URL(incomingUrl.pathname + incomingUrl.search, env.MXRE_ORIGIN_URL);
  const requestId = request.headers.get('x-request-id') ?? crypto.randomUUID();

  const headers = new Headers(request.headers);
  headers.set('x-api-key', env.MXRE_UPSTREAM_API_KEY);
  if (client) headers.set('x-client-id', client.id);
  headers.set('x-request-id', requestId);
  headers.set('x-forwarded-host', incomingUrl.host);
  headers.delete('cf-connecting-ip');

  const upstreamResponse = await fetch(originUrl, {
    method: request.method,
    headers,
    body: request.method === 'GET' || request.method === 'HEAD' ? undefined : request.body,
    redirect: 'manual',
  });

  const responseHeaders = new Headers(upstreamResponse.headers);
  responseHeaders.set('x-request-id', requestId);
  if (client) responseHeaders.set('x-mxre-client-id', client.id);

  if (shouldCache(request, incomingUrl) && upstreamResponse.ok) {
    const seconds = Number(env.CACHE_SECONDS || '60');
    responseHeaders.set('cache-control', `private, max-age=${seconds}`);
    responseHeaders.set('cdn-cache-control', `max-age=${seconds}`);
  } else {
    responseHeaders.set('cache-control', 'no-store');
  }

  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers: responseHeaders,
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      return json({ status: 'ok', edge: 'cloudflare', timestamp: new Date().toISOString() });
    }

    if (!url.pathname.startsWith('/v1/')) {
      return json({ error: 'Not found' }, 404);
    }

    const client = authenticate(request, env);
    if (!client) {
      return json({ error: 'Unauthorized' }, 401);
    }

    if (/^\/v1\/markets\/[^/]+\/reports\/creative-finance$/.test(url.pathname)) {
      try {
        const response = await creativeFinanceReport(request, env);
        response.headers.set('x-mxre-client-id', client.id);
        response.headers.set('x-request-id', request.headers.get('x-request-id') ?? crypto.randomUUID());
        return response;
      } catch (error) {
        if (env.MXRE_ORIGIN_URL && env.MXRE_UPSTREAM_API_KEY) {
          return proxyToOrigin(request, env, client);
        }
        return json({ error: 'Failed to build creative finance report', detail: error instanceof Error ? error.message : String(error) }, 500);
      }
    }

    if (!env.MXRE_ORIGIN_URL || !env.MXRE_UPSTREAM_API_KEY) {
      return json({ error: 'Gateway origin not configured for this endpoint' }, 503);
    }

    return proxyToOrigin(request, env, client);
  },
};
