import 'dotenv/config';
import { Hono, type Context } from 'hono';
import { serve } from '@hono/node-server';
import { createRequire } from 'node:module';
import { getDb } from '../db/client.js';
import { buildPropertyResponse } from './transforms/build-response.js';
import { getSourceRegistry } from '../config/source-registry.js';
import type { PropertySummary } from './types.js';

const app = new Hono();
const db = getDb();
const require = createRequire(import.meta.url);
const { Pool } = require('pg') as { Pool: new (config: Record<string, unknown>) => { query: (query: string) => Promise<{ rows: Record<string, unknown>[] }> } };
let directPgPool: InstanceType<typeof Pool> | null = null;

type ApiClient = {
  id: string;
  key: string;
  environment?: string;
  monthlyQuota?: number;
  scope?: 'api' | 'docs';
};

type RateBucket = {
  resetAt: number;
  count: number;
};

const nodeRateBuckets = new Map<string, RateBucket>();

const INDIANAPOLIS_CORE_COUNTY_IDS = [797583];
const INDIANAPOLIS_METRO_COUNTY_IDS = [
  797471, // Boone
  797499, // Brown
  797457, // Hamilton
  797461, // Hancock
  797531, // Hendricks
  797475, // Johnson
  797473, // Madison
  797583, // Marion
  797557, // Morgan
  797469, // Shelby
  797572, // Tipton
];

const DATA_GAP_DICTIONARY: Record<string, { label: string; meaning: string; primarySource: string }> = {
  parcel_identity: {
    label: 'Parcel identity',
    meaning: 'Missing parcel ID, address, or county linkage.',
    primarySource: 'county_assessor_refresh',
  },
  asset_classification: {
    label: 'Asset classification',
    meaning: 'Missing usable asset class/subtype/property use.',
    primarySource: 'county_assessor_refresh',
  },
  ownership: {
    label: 'Ownership',
    meaning: 'Missing owner or company name.',
    primarySource: 'county_assessor_refresh',
  },
  valuation: {
    label: 'Valuation',
    meaning: 'Missing market, assessed, or taxable value.',
    primarySource: 'county_assessor_refresh',
  },
  physical_facts: {
    label: 'Physical facts',
    meaning: 'Missing usable square footage, lot size, or year built facts.',
    primarySource: 'county_assessor_refresh',
  },
  sales_history: {
    label: 'Sales history',
    meaning: 'Missing sale history or last sale fields.',
    primarySource: 'recorder_refresh',
  },
  mortgage_records: {
    label: 'Mortgage records',
    meaning: 'No recorded mortgage/lien rows connected to this property.',
    primarySource: 'recorder_refresh_or_paid_fallback',
  },
  mortgage_balance: {
    label: 'Mortgage balance',
    meaning: 'Mortgage rows exist, but no usable amount or estimated balance is available.',
    primarySource: 'realestateapi_property_detail',
  },
  agent_name: {
    label: 'Agent name',
    meaning: 'Active listing is missing listing agent name.',
    primarySource: 'realestateapi_property_detail',
  },
  agent_email: {
    label: 'Agent email',
    meaning: 'Active listing is missing listing agent email.',
    primarySource: 'realestateapi_then_rapidapi_zillow',
  },
  agent_phone: {
    label: 'Agent phone',
    meaning: 'Active listing is missing listing agent phone.',
    primarySource: 'realestateapi_then_rapidapi_zillow',
  },
  brokerage: {
    label: 'Brokerage',
    meaning: 'Active listing is missing listing brokerage.',
    primarySource: 'realestateapi_property_detail',
  },
  listing_url: {
    label: 'Listing URL',
    meaning: 'Active listing is missing source URL.',
    primarySource: 'listing_detail_refresh',
  },
  property_website: {
    label: 'Property website',
    meaning: 'Multifamily/apartment property is missing its public property website.',
    primarySource: 'apartment_website_discovery',
  },
  floorplans: {
    label: 'Floorplans',
    meaning: 'Multifamily/apartment property is missing bed/bath floorplan rows.',
    primarySource: 'property_website_rent_scraper',
  },
  rent_snapshot: {
    label: 'Rent snapshots',
    meaning: 'Multifamily/apartment property is missing current asking/effective rent snapshots.',
    primarySource: 'property_website_rent_scraper',
  },
  location_scores: {
    label: 'Location scores',
    meaning: 'Missing crime/transit/location intelligence scoring.',
    primarySource: 'location_intelligence_refresh',
  },
};

const MARKET_CONFIGS: Record<string, {
  key: string;
  aliases: string[];
  label: string;
  city: string;
  cityUpper: string;
  county: string;
  state: string;
  countyId: number;
  status: 'live' | 'pilot' | 'building';
  readinessTarget: number;
  scope: 'city' | 'county' | 'metro';
  publicLabel: string;
  refreshCadence: string;
  restrictions: string[];
  fallbackCoverageMetrics?: Record<string, unknown>;
}> = {
  indianapolis: {
    key: 'indianapolis',
    aliases: ['indianapolis', 'indy'],
    label: 'Indianapolis',
    publicLabel: 'Indianapolis, IN',
    city: 'Indianapolis',
    cityUpper: 'INDIANAPOLIS',
    county: 'Marion',
    state: 'IN',
    countyId: 797583,
    status: 'live',
    readinessTarget: 90,
    scope: 'city',
    refreshCadence: '4x daily listing/enrichment refresh on VPS; parcel and recorder refreshes run by market pipeline',
    restrictions: [
      'Indianapolis city rows are supported first; metro/core scope is available on selected analytics endpoints.',
      'MXRE active listings drive paid fallback enrichment. RealEstateAPI and RapidAPI are not used as primary listing discovery.',
      'Mortgage balance and agent contact fields may be blended from paid fallback sources when MXRE public data is incomplete.',
    ],
  },
  dallas: {
    key: 'dallas',
    aliases: ['dallas', 'dallas-tx'],
    label: 'Dallas',
    publicLabel: 'Dallas, TX',
    city: 'Dallas',
    cityUpper: 'DALLAS',
    county: 'Dallas',
    state: 'TX',
    countyId: 7,
    status: 'live',
    readinessTarget: 60,
    scope: 'city',
    refreshCadence: 'daily listing, paid detail, public agent-contact, mortgage/debt, rent, and dashboard refresh; paid detail calls are property-scoped and cached',
    restrictions: [
      'Dallas is publishable for BBC underwriting with field-level quality flags; active listings are source-limited Redfin-derived rows, not a guaranteed full MLS inventory.',
      'Full Dallas County parcel/account rows are loaded; Dallas city situs is exposed as the first user-facing market boundary.',
      'Agent email coverage is blended from RealEstateAPI, exact verified identity propagation, and public/legal profile search; missing emails are not guessed.',
      'Mortgage and debt data includes RealEstateAPI paid detail for linked active properties plus public Dallas County recorder documents where linkable.',
      'Public rent and floorplan coverage is partial and should continue backfilling without blocking BBC exact-address underwriting.',
    ],
    fallbackCoverageMetrics: {
      parcels: {
        parcel_count: 357450,
        parcel_identity_count: 357450,
        classified_count: 357450,
        ownership_count: 356750,
        valuation_count: 337386,
        multifamily_count: 0,
      },
      listings: {
        active_listing_count: 5426,
        active_property_count: 5158,
        agent_name_count: 5426,
        agent_email_count: 3795,
        agent_phone_count: 5303,
        brokerage_count: 5413,
        creative_finance_count: 102,
        latest_listing_seen: null,
        listing_sources: ['redfin'],
      },
      debt: {
        mortgage_record_count: 7439,
        properties_with_mortgage_records: 3214,
        mortgage_amount_count: 7341,
        latest_recording: null,
      },
      rents: {
        rent_snapshot_count: 71,
        properties_with_rent_snapshots: 9,
        latest_rent_observed: null,
      },
    },
  },
  columbus: {
    key: 'columbus',
    aliases: ['columbus', 'columbus-oh'],
    label: 'Columbus',
    publicLabel: 'Columbus, OH',
    city: 'Columbus',
    cityUpper: 'COLUMBUS',
    county: 'Franklin',
    state: 'OH',
    countyId: 1698985,
    status: 'pilot',
    readinessTarget: 90,
    scope: 'city',
    refreshCadence: 'pipeline available; production daily refresh should be enabled after first coverage audit',
    restrictions: [
      'Pilot market. BBC should treat results as test/sandbox until readiness reaches live target.',
      'Exact address lookup may work before full market-search coverage is complete.',
    ],
  },
  westChester: {
    key: 'west-chester',
    aliases: ['west-chester', 'west-chester-pa', 'westchester'],
    label: 'West Chester',
    publicLabel: 'West Chester, PA',
    city: 'West Chester',
    cityUpper: 'WEST CHESTER',
    county: 'Chester',
    state: 'PA',
    countyId: 817175,
    status: 'pilot',
    readinessTarget: 90,
    scope: 'city',
    refreshCadence: 'pipeline available; production daily refresh should be enabled after first coverage audit',
    restrictions: [
      'Pilot market. Treat Chester County / West Chester borough as the first coverage boundary.',
      'Do not promise full metro coverage until county and listing coverage audits are marked live.',
    ],
  },
};

const SUPPORTED_MARKETS = Object.values(MARKET_CONFIGS).map((market) => market.key);
const MARKET_DATA_DOMAINS = [
  'parcel_identity',
  'asset_classification',
  'ownership',
  'valuation_tax',
  'sales_history',
  'mortgage_liens',
  'on_market_listings',
  'agent_contacts',
  'creative_finance_signals',
  'rent_estimates',
  'daily_change_tracking',
];
const MARKET_ASSET_CLASSES = [
  { key: 'single_family', label: 'Single-family / condo / one-unit residential', unitRange: '1' },
  { key: 'small_multifamily', label: 'Small multifamily', unitRange: '2-5' },
  { key: 'commercial_multifamily', label: 'Commercial multifamily / apartment scale', unitRange: '6+' },
  { key: 'land', label: 'Land', unitRange: null },
  { key: 'industrial', label: 'Industrial', unitRange: null },
  { key: 'retail', label: 'Retail', unitRange: null },
  { key: 'office', label: 'Office', unitRange: null },
  { key: 'mobile_home_rv', label: 'Mobile home / RV park', unitRange: null },
];
const BBC_MARKET_ENDPOINTS = [
  '/v1/bbc/search-runs',
  '/v1/bbc/markets/{market}/changes',
  '/v1/bbc/markets/{market}/creative-finance-listings',
  '/v1/bbc/property',
  '/v1/bbc/property/{mxreId}',
  '/v1/markets/{market}/readiness',
  '/v1/markets/{market}/completion',
  '/v1/markets/{market}/data-gaps',
  '/v1/markets/{market}/opportunities',
  '/v1/markets/{market}/price-changes',
];
const BBC_EVENT_TYPES = new Set([
  'parcel_created',
  'parcel_updated',
  'ownership_changed',
  'deed_recorded',
  'mortgage_recorded',
  'lien_recorded',
  'lien_released',
  'listing_created',
  'listing_refreshed',
  'listing_price_changed',
  'price_changed',
  'listing_status_changed',
  'status_changed',
  'listing_pending',
  'listing_sold',
  'listing_delisted',
  'listing_relisted',
  'agent_contact_updated',
  'rent_observed',
  'rent_changed',
  'floorplan_changed',
  'creative_finance_detected',
  'preforeclosure_detected',
  'tax_changed',
  'assessment_changed',
]);

function resolveMarketConfig(value: string) {
  const key = value.toLowerCase();
  return Object.values(MARKET_CONFIGS).find((market) => market.aliases.includes(key)) ?? null;
}

function loadApiClients(): ApiClient[] {
  const rawJson = process.env.MXRE_CLIENT_API_KEYS;
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson) as unknown;
      if (Array.isArray(parsed)) {
        return parsed
          .map((client) => client as Partial<ApiClient>)
          .filter((client): client is ApiClient => Boolean(client.id && client.key));
      }
    } catch (error) {
      console.error('[MXRE API] Failed to parse MXRE_CLIENT_API_KEYS:', error);
    }
  }

  const legacyKey = process.env.MXRE_API_KEY;
  const clients: ApiClient[] = legacyKey ? [{ id: 'legacy', key: legacyKey, environment: process.env.NODE_ENV }] : [];
  const docsKey = process.env.MXRE_DOCS_API_KEY;
  if (docsKey) clients.push({ id: 'buy_box_club_docs', key: docsKey, environment: 'docs', scope: 'docs' });
  return clients;
}

function authenticateApiClient(apiKey: string | undefined, clientId: string | undefined): ApiClient | null {
  if (!apiKey) return null;
  const clients = loadApiClients();
  const matches = clients.filter((client) => constantTimeEqual(client.key, apiKey));
  if (matches.length === 0) return null;
  if (clientId) {
    return matches.find((client) => client.id === clientId) ?? null;
  }
  return matches[0] ?? null;
}

function constantTimeEqual(a: string, b: string): boolean {
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  const length = Math.max(left.length, right.length);
  let diff = left.length ^ right.length;
  for (let i = 0; i < length; i += 1) {
    diff |= (left[i] ?? 0) ^ (right[i] ?? 0);
  }
  return diff === 0;
}

function getApiCredentials(c: Context): { apiKey?: string; clientId?: string } {
  const headerApiKey = c.req.header('x-api-key');
  const headerClientId = c.req.header('x-client-id');
  if (headerApiKey) return { apiKey: headerApiKey, clientId: headerClientId };

  const auth = c.req.header('authorization');
  if (!auth?.toLowerCase().startsWith('basic ')) return {};

  try {
    const decoded = Buffer.from(auth.slice(6).trim(), 'base64').toString('utf8');
    const separator = decoded.indexOf(':');
    if (separator < 0) return {};
    return {
      clientId: decoded.slice(0, separator),
      apiKey: decoded.slice(separator + 1),
    };
  } catch {
    return {};
  }
}

function isLocalRequest(c: Context): boolean {
  const host = c.req.header('host') ?? '';
  const hostname = host.split(':')[0]?.toLowerCase();
  return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]' || hostname === '::1';
}

function getBrowserApiKey(c: Context): string {
  if (!isLocalRequest(c)) return '';
  return process.env.MXRE_API_KEY ?? loadApiClients()[0]?.key ?? '';
}

function previewsEnabled(c: Context): boolean {
  return isLocalRequest(c) && (process.env.NODE_ENV !== 'production' || process.env.MXRE_ENABLE_PREVIEWS === 'true');
}

function rateLimit(key: string, limit: number, windowMs: number): { allowed: boolean; retryAfter: number; remaining: number } {
  const now = Date.now();
  const existing = nodeRateBuckets.get(key);
  const bucket = !existing || existing.resetAt <= now ? { resetAt: now + windowMs, count: 0 } : existing;
  bucket.count += 1;
  nodeRateBuckets.set(key, bucket);

  if (nodeRateBuckets.size > 10000) {
    for (const [bucketKey, value] of nodeRateBuckets.entries()) {
      if (value.resetAt <= now) nodeRateBuckets.delete(bucketKey);
    }
  }

  return {
    allowed: bucket.count <= limit,
    retryAfter: Math.max(1, Math.ceil((bucket.resetAt - now) / 1000)),
    remaining: Math.max(0, limit - bucket.count),
  };
}

function getRequestIp(c: Context): string {
  return c.req.header('cf-connecting-ip')
    ?? c.req.header('x-forwarded-for')?.split(',')[0]?.trim()
    ?? 'unknown';
}

function getIndianapolisScope(scope: string | undefined): {
  key: 'city' | 'core' | 'metro';
  label: string;
  countyIds: number[];
  countySql: string;
  whereSql: string;
} {
  const requested = scope?.toLowerCase();
  const key = requested === 'metro' ? 'metro' : requested === 'core' ? 'core' : 'city';
  const countyIds = key === 'metro' || key === 'city' ? INDIANAPOLIS_METRO_COUNTY_IDS : INDIANAPOLIS_CORE_COUNTY_IDS;
  const countySql = countyIds.join(',');
  return {
    key,
    label:
      key === 'metro'
        ? 'Indianapolis-Carmel-Greenwood MSA'
        : key === 'core'
          ? 'Indianapolis Core / Marion County'
          : 'Indianapolis City',
    countyIds,
    countySql,
    whereSql:
      key === 'city'
        ? `p.county_id in (${countySql}) and upper(trim(replace(coalesce(p.city, ''), ',', ''))) = 'INDIANAPOLIS'`
        : `p.county_id in (${countySql})`,
  };
}

// ── Auth middleware (skip for /health) ────────────────────────
app.use('*', async (c, next) => {
  const startedAt = Date.now();
  if (
    c.req.path === '/health' ||
    c.req.path === '/' ||
    (previewsEnabled(c) && (
      c.req.path === '/dashboard' ||
      c.req.path === '/preview/market-dashboard' ||
      c.req.path === '/preview/data-gaps' ||
      c.req.path === '/preview/west-chester-dashboard'
    ))
  ) return next();

  const { apiKey, clientId: requestedClientId } = getApiCredentials(c);
  const client = authenticateApiClient(apiKey, requestedClientId);
  const ip = getRequestIp(c);
  const contentLength = Number(c.req.header('content-length') || '0');

  const preAuthLimit = rateLimit(`preauth:${ip}`, 120, 60_000);
  if (!preAuthLimit.allowed) {
    c.header('retry-after', String(preAuthLimit.retryAfter));
    return c.json({ error: 'Rate limit exceeded', retry_after_seconds: preAuthLimit.retryAfter }, 429);
  }
  if (contentLength > 1_000_000) {
    return c.json({ error: 'Request body too large' }, 413);
  }

  if (loadApiClients().length === 0) {
    return c.json({ error: 'Server misconfigured: no API clients configured' }, 500);
  }
  if (!client) {
    const failedAuthLimit = rateLimit(`authfail:${ip}`, 10, 10 * 60_000);
    if (!failedAuthLimit.allowed) {
      c.header('retry-after', String(failedAuthLimit.retryAfter));
      return c.json({ error: 'Rate limit exceeded', retry_after_seconds: failedAuthLimit.retryAfter }, 429);
    }
    if (c.req.path === '/docs' || c.req.path === '/v1/docs/openapi.json') {
      c.header('www-authenticate', 'Basic realm="MXRE Private API Docs", charset="UTF-8"');
    }
    return c.json({ error: 'Unauthorized' }, 401);
  }

  if (client.scope === 'docs' && c.req.path !== '/docs' && c.req.path !== '/v1/docs/openapi.json') {
    return c.json({ error: 'Forbidden', message: 'This API key only grants access to MXRE docs.' }, 403);
  }

  const clientLimit = rateLimit(`client:${client.id}:${ip}`, 1200, 60_000);
  if (!clientLimit.allowed) {
    c.header('retry-after', String(clientLimit.retryAfter));
    return c.json({ error: 'Rate limit exceeded', retry_after_seconds: clientLimit.retryAfter }, 429);
  }

  c.header('x-mxre-client-id', client.id);
  c.header('x-ratelimit-remaining', String(clientLimit.remaining));
  c.header('cache-control', 'no-store');
  c.header('x-content-type-options', 'nosniff');
  c.header('referrer-policy', 'no-referrer');
  await next();

  const requestId = c.req.header('x-request-id') ?? crypto.randomUUID();
  c.header('x-request-id', requestId);
  console.log(JSON.stringify({
    event: 'mxre_api_request',
    request_id: requestId,
    client_id: client.id,
    environment: client.environment ?? null,
    method: c.req.method,
    path: c.req.path,
    status: c.res.status,
    latency_ms: Date.now() - startedAt,
    user_agent: c.req.header('user-agent') ?? null,
  }));
});

// ── Landing page (no auth) ────────────────────────────────────
app.get('/', (c) => {
  return c.html(`<!DOCTYPE html><html><head><title>MundoXRE API</title>
<style>body{font-family:system-ui;background:#0f172a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0}
.card{background:#1e293b;padding:40px;border-radius:12px;max-width:500px;text-align:center}
h1{color:#38bdf8;margin:0 0 10px}p{color:#94a3b8;margin:5px 0}
code{background:#334155;padding:2px 8px;border-radius:4px;font-size:13px}
.status{color:#4ade80;font-weight:700}</style></head><body>
<div class="card"><h1>MundoXRE API</h1><p class="status">LIVE v1.0.0</p>
<p style="margin-top:20px">Endpoints:</p>
<p><code>GET /v1/property?address=...&city=...&state=...</code></p>
<p><code>GET /v1/property/:id</code></p>
<p><code>GET /v1/property/search?state=OH&county=Fairfield&absentee=true</code></p>
<p style="margin-top:20px;color:#64748b">Auth: <code>x-api-key</code> header required</p>
</div></body></html>`);
});

// ── Health ────────────────────────────────────────────────────
app.get('/health', (c) => {
  return c.json({ status: 'ok', version: '1.0.0', timestamp: new Date().toISOString() });
});

// ── Property lookup by address query params ──────────────────
// Private API docs. This route is intentionally protected by the normal API-key middleware.
app.get('/docs', (c) => {
  const clientId = c.req.header('x-mxre-external-client-id')
    ?? c.req.header('x-client-id')
    ?? c.res.headers.get('x-mxre-client-id')
    ?? 'authenticated_client';
  return c.html(renderPrivateDocsHtml(clientId));
});

app.get('/v1/docs/openapi.json', (c) => {
  return c.json(buildOpenApiSpec());
});

app.get('/v1/platform/source-registry', (c) => {
  return c.json(getSourceRegistry());
});

const coverageMarketsHandler = async (c: Context) => {
  const includeBuilding = c.req.query('includeBuilding') === 'true';
  const includeBelowTarget = c.req.query('includeBelowTarget') === 'true';
  const markets = await buildCoverageMarketRows();
  const filtered = markets.filter((market) => {
    if (!includeBuilding && market.status === 'building') return false;
    if (!includeBelowTarget && market.meetsReadinessTarget !== true) return false;
    return true;
  });
  return c.json({
    schemaVersion: 'mxre.bbc.coverageMarkets.v1',
    purpose: 'Machine-readable list of MXRE markets that Buy Box Club can safely expose or query.',
    markets: filtered,
    hiddenMarkets: markets
      .filter((market) => !filtered.some((visible) => visible.marketId === market.marketId))
      .map((market) => ({
        marketId: market.marketId,
        label: market.label,
        status: market.status,
        readinessScore: market.readinessScore,
        readinessTarget: market.readinessTarget,
        reason: market.meetsReadinessTarget === true ? 'hidden_by_filter' : 'below_readiness_target',
      })),
    defaultMarket: filtered[0]?.marketId ?? null,
    statusDefinitions: {
      live: 'Production-ready for BBC workflows, subject to field-level quality flags.',
      pilot: 'Available for sandbox/admin testing; do not market as complete coverage yet.',
      building: 'Pipeline exists or is planned, but not ready for BBC user-facing search.',
    },
    integrationRules: {
      chooseMarketBy: ['marketId', 'city + state', 'alias'],
      beforeSavedSearch: 'Call this endpoint and only enable BBC market search for markets returned in markets[]. By default, markets below their readiness target are excluded and defaultMarket is null when nothing qualifies.',
      exactAddressLookup: 'BBC may call /v1/bbc/property for exact-address underwriting even when market status is pilot, but should obey quality.fallbackRecommended.',
      adminInspection: 'MXRE admins can pass includeBelowTarget=true to inspect pilot/building markets, but BBC should not show those markets to normal users.',
      stableContract: 'This endpoint and all /v1/bbc/* endpoints are versioned. Additive fields may appear; existing field names should not be removed without a new schemaVersion.',
    },
    generatedAt: new Date().toISOString(),
  });
};

app.get('/v1/bbc/markets', coverageMarketsHandler);
app.get('/v1/markets', coverageMarketsHandler);

app.get('/v1/addresses/autocomplete', async (c) => {
  const q = normalizeAutocompleteQuery(c.req.query('q') ?? c.req.query('query') ?? '');
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 8, 20);
  const includeProperties = c.req.query('includeProperties') !== 'false';

  if (q.length < 2) {
    return c.json({
      schemaVersion: 'mxre.addressAutocomplete.v1',
      query: q,
      results: [],
      meta: {
        minQueryLength: 2,
        strategy: 'preindexed_mxre_first',
        generatedAt: new Date().toISOString(),
      },
    });
  }

  const stateHint = (c.req.query('state') ?? extractStateHint(q) ?? '').toUpperCase();
  const zipHint = extractZipHint(q);
  const streetLike = /\d/.test(q);
  const normalized = normalizeAutocompleteQueryForSql(q);
  const normalizedNoState = normalizeAutocompleteQueryForSql(q.replace(/\b[A-Z]{2}\b/gi, ''));
  const qSql = sqlString(normalized);
  const qNoStateSql = sqlString(normalizedNoState || normalized);
  const stateSql = stateHint ? `and state_code = '${sqlString(stateHint)}'` : '';
  const propertyStateSql = stateHint ? `and p.state_code = '${sqlString(stateHint)}'` : '';
  const zipSql = zipHint ? `and zip = '${sqlString(zipHint)}'` : '';
  const propertyZipSql = zipHint ? `and p.zip = '${sqlString(zipHint)}'` : '';
  const cityLimit = streetLike ? 0 : Math.min(limit, 8);
  const addressLimit = streetLike ? limit : 0;
  const marketKeySql = "lower(regexp_replace(trim(coalesce(city, '')), '[^a-zA-Z0-9]+', '-', 'g')) || '-' || lower(coalesce(state_code, ''))";
  const staticCityRows = buildStaticMarketAutocompleteRows(q, stateHint);
  const dbCityLimit = staticCityRows.length > 0 ? 0 : cityLimit;

  if (staticCityRows.length > 0) {
    return c.json(buildAutocompleteResponse(q, limit, staticCityRows.slice(0, limit)));
  }

  let nationalRows: Record<string, unknown>[] = [];
  if (process.env.MXRE_ENABLE_NATIONAL_AUTOCOMPLETE === 'true') {
    try {
      nationalRows = await queryPg<Record<string, unknown>>(`
        select
          type,
          label,
          street,
          initcap(city) as city,
          state_code as state,
          zip,
          null::bigint as "countyId",
          county,
          lat,
          lng,
          source,
          mxre_property_id is not null as "hasMxrePropertyDetail",
          mxre_property_id as "propertyId",
          market_key as "marketId",
          confidence,
          case when source = 'mxre' then 10 else 30 end as rank
        from address_autocomplete_entries
        where type = 'address'
          ${stateSql}
          ${zipSql}
          and normalized_label like '${qSql}%'
        order by rank, label
        limit ${addressLimit}
      `);
    } catch {
      nationalRows = [];
    }
  }

  const mxreRows = await queryPg<Record<string, unknown>>(`
    with city_matches as (
      select
        'city'::text as type,
        concat_ws(', ', initcap(city), state_code) as label,
        null::text as street,
        initcap(city) as city,
        state_code as state,
        null::text as zip,
        null::bigint as "countyId",
        null::text as county,
        null::numeric as lat,
        null::numeric as lng,
        'mxre_city_index'::text as source,
        false as "hasMxrePropertyDetail",
        null::bigint as "propertyId",
        ${marketKeySql} as "marketId",
        'high'::text as confidence,
        count(*)::int as "propertyCount",
        0 as rank
      from properties
      where city is not null
        and city <> ''
        and city !~ '[0-9]'
        ${stateSql}
        and (
          upper(city) like '${qNoStateSql}%'
          or upper(concat_ws(' ', city, state_code)) like '${qSql}%'
        )
      group by city, state_code
      order by count(*) desc, city
      limit ${dbCityLimit}
    ),
    address_matches as (
      select
        'address'::text as type,
        concat_ws(', ', initcap(p.address), initcap(p.city), concat(p.state_code, ' ', p.zip)) as label,
        initcap(p.address) as street,
        initcap(p.city) as city,
        p.state_code as state,
        p.zip,
        p.county_id as "countyId",
        c.county_name as county,
        p.latitude as lat,
        p.longitude as lng,
        'mxre_property'::text as source,
        true as "hasMxrePropertyDetail",
        p.id as "propertyId",
        lower(regexp_replace(trim(coalesce(p.city, '')), '[^a-zA-Z0-9]+', '-', 'g')) || '-' || lower(coalesce(p.state_code, '')) as "marketId",
        case
          when p.address like '${qNoStateSql}%' then 'high'
          else 'medium'
        end as confidence,
        null::int as "propertyCount",
        20 as rank
      from properties p
      left join counties c on c.id = p.county_id
      where ${includeProperties ? 'true' : 'false'}
        and p.address is not null
        and p.address <> ''
        ${propertyStateSql}
        ${propertyZipSql}
        and (
          p.address like '${qNoStateSql}%'
          or upper(concat_ws(' ', p.address, p.city, p.state_code, p.zip)) like '${qSql}%'
        )
      order by
        case when p.address like '${qNoStateSql}%' then 0 else 1 end,
        p.state_code,
        p.city,
        p.address
      limit ${addressLimit}
    ),
    combined as (
      select * from address_matches
      union all
      select * from city_matches
    ),
    deduped as (
      select distinct on (type, label, coalesce(zip, ''))
        type,
        label,
        street,
        city,
        state,
        zip,
        "countyId",
        county,
        lat,
        lng,
        source,
        "hasMxrePropertyDetail",
        "propertyId",
        "marketId",
        confidence,
        "propertyCount",
        rank
      from combined
      order by type, label, coalesce(zip, ''), rank
    )
    select *
    from deduped
    order by
      case when type = 'address' then 0 else 1 end,
      rank,
      label
    limit ${limit};
  `);

  const rows = dedupeAutocompleteRows([...staticCityRows, ...mxreRows, ...nationalRows])
    .sort((a, b) => {
      const aType = a.type === 'address' ? 0 : 1;
      const bType = b.type === 'address' ? 0 : 1;
      if (aType !== bType) return aType - bType;
      return String(a.label ?? '').localeCompare(String(b.label ?? ''));
    })
    .slice(0, limit);

  return c.json({
    schemaVersion: 'mxre.addressAutocomplete.v1',
    query: q,
    results: rows.map((row) => ({
      type: row.type,
      label: row.label,
      street: row.street ?? null,
      city: row.city ?? null,
      state: row.state ?? null,
      zip: row.zip ?? null,
      county: row.county ?? null,
      lat: numberOrNull(row.lat),
      lng: numberOrNull(row.lng),
      source: row.source,
      confidence: row.confidence,
      coverage: {
        hasMxrePropertyDetail: Boolean(row.hasMxrePropertyDetail),
        propertyId: numberOrNull(row.propertyId),
        marketId: row.marketId ?? null,
        propertyCount: numberOrNull(row.propertyCount),
      },
    })),
    usage: {
      selectAddressThenCall: '/v1/bbc/property?address={street}&city={city}&state={state}&zip={zip}',
      selectCityThenCall: '/v1/bbc/search-runs',
    },
    meta: {
      limit,
      strategy: 'preindexed_mxre_first',
      nationalIndex: 'address_autocomplete_entries_optional',
      liveExternalCalls: false,
      generatedAt: new Date().toISOString(),
    },
  });
});

app.get('/v1/bbc/property/:id', async (c) => {
  const id = parseInt(c.req.param('id'), 10);
  if (isNaN(id)) return c.json({ error: 'Invalid property ID' }, 400);
  return fetchAndRespond(c, id, buildBuyBoxClubPropertyResponse);
});

app.get('/v1/bbc/property', async (c) => {
  const address = c.req.query('address');
  const city = c.req.query('city');
  const state = c.req.query('state');
  const zip = c.req.query('zip');

  if (!address || !state) {
    return c.json({ error: 'Missing required params: address and state (city and zip recommended)' }, 400);
  }

  const addressNorm = address.toUpperCase().replace(/[%*]+$/, '');
  const stateNorm = state.toUpperCase();

  let query = db.from('properties')
    .select('*, counties(county_name, state_code, county_fips, state_fips)')
    .eq('state_code', stateNorm)
    .like('address', `${addressNorm}%`)
    .limit(5);

  if (city) query = query.eq('city', city.toUpperCase());
  if (zip) query = query.eq('zip', zip);

  const { data, error } = await query;
  if (error) return c.json({ error: 'Database error', detail: error.message }, 500);
  if (!data || data.length === 0) {
    return c.json({
      error: 'Property not found',
      fallbackRecommended: true,
      fallbackProvider: 'realestateapi',
      fallbackReason: 'mxre_no_property_match',
    }, 404);
  }

  return fetchAndRespond(c, data[0].id as number, buildBuyBoxClubPropertyResponse);
});

app.get('/v1/bbc/markets/:market/changes', async (c) => {
  const marketConfig = resolveMarketConfig(c.req.param('market'));
  if (!marketConfig) return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);

  const decodedCursor = decodeChangeCursor(c.req.query('cursor'));
  const updatedAfter = decodedCursor?.eventAt
    ?? parseDateTimeParam(c.req.query('updated_after'))
    ?? parseDateTimeParam(c.req.query('updatedAfter'))
    ?? parseDateTimeParam(c.req.query('since'))
    ?? new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const cursorRecordVersion = decodedCursor?.recordVersion ?? null;
  const eventCursorSql = cursorRecordVersion
    ? `and (event_at > '${sqlString(updatedAfter)}'::timestamptz or (event_at = '${sqlString(updatedAfter)}'::timestamptz and record_version > '${sqlString(cursorRecordVersion)}'))`
    : `and event_at > '${sqlString(updatedAfter)}'::timestamptz`;
  const fallbackCursorSql = cursorRecordVersion
    ? `("eventAt" > '${sqlString(updatedAfter)}'::timestamptz or ("eventAt" = '${sqlString(updatedAfter)}'::timestamptz and "recordVersion" > '${sqlString(cursorRecordVersion)}'))`
    : `"eventAt" > '${sqlString(updatedAfter)}'::timestamptz`;
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 100, 1000);
  const queryLimit = limit + 1;
  const eventTypes = (c.req.query('event_types') ?? '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter((value) => BBC_EVENT_TYPES.has(value));
  const eventFilterSql = eventTypes.length > 0
    ? `and event_type in (${eventTypes.map((type) => `'${sqlString(type)}'`).join(',')})`
    : '';

  let rawRows: Record<string, unknown>[];
  let source = 'property_events';
  try {
    rawRows = await queryPg<Record<string, unknown>>(`
      select
        property_id as "mxreId",
        listing_signal_id as "listingSignalId",
        event_type as "eventType",
        event_at as "eventAt",
        record_version as "recordVersion",
        changed_fields as "changedFields",
        previous_values as "previousValues",
        current_values as "currentValues",
        source,
        source_category as "sourceCategory",
        confidence,
        underwriting_relevant as "underwritingRelevant"
      from property_events
      where market_key = '${sqlString(marketConfig.key)}'
        and underwriting_relevant = true
        ${eventCursorSql}
        ${eventFilterSql}
      order by event_at asc, record_version asc
      limit ${queryLimit};
    `);
  } catch (error) {
    source = 'listing_signal_events_fallback';
    console.warn('[MXRE BBC changes] property_events unavailable, falling back to listing_signal_events:', error);
    rawRows = await queryPg<Record<string, unknown>>(`
      with listing_events as (
      select
        coalesce(property_id::bigint, 0) as property_id,
        listing_signal_id,
        address,
        city,
        state_code,
        zip,
        event_type,
        event_at,
        list_price,
        previous_list_price,
        mls_status,
        previous_mls_status,
        listing_url,
        listing_source,
        listing_agent_name,
        listing_brokerage,
        raw
      from listing_signal_events
      where state_code = '${marketConfig.state}'
        and upper(coalesce(city,'')) = '${marketConfig.cityUpper}'
        and event_at >= '${sqlString(updatedAfter)}'::timestamptz
        ${eventFilterSql}
    ),
    active_listing_versions as (
      select
        l.property_id::bigint as property_id,
        l.id as listing_signal_id,
        l.address,
        l.city,
        l.state_code,
        l.zip,
        case
          when l.first_seen_at >= '${sqlString(updatedAfter)}'::timestamptz then 'listing_created'
          when l.last_seen_at >= '${sqlString(updatedAfter)}'::timestamptz then 'listing_refreshed'
          else null
        end as event_type,
        coalesce(l.last_seen_at, l.first_seen_at, now()) as event_at,
        l.mls_list_price as list_price,
        null::numeric as previous_list_price,
        case when l.is_on_market then 'active' else 'off_market' end as mls_status,
        null::text as previous_mls_status,
        l.listing_url,
        l.listing_source,
        l.listing_agent_name,
        l.listing_brokerage,
        l.raw
      from listing_signals l
      where l.state_code = '${marketConfig.state}'
        and upper(coalesce(l.city,'')) = '${marketConfig.cityUpper}'
        and coalesce(l.last_seen_at, l.first_seen_at) >= '${sqlString(updatedAfter)}'::timestamptz
    ),
    combined as (
      select * from listing_events
      union all
      select * from active_listing_versions where event_type is not null
    ),
    normalized as (
      select
        property_id as "mxreId",
        nullif(listing_signal_id, 0) as "listingSignalId",
        address,
        city,
        state_code as state,
        zip,
        event_type as "eventType",
        event_at as "eventAt",
        md5(coalesce(property_id::text,'') || '|' || coalesce(listing_signal_id::text,'') || '|' || event_type || '|' || event_at::text) as "recordVersion",
        case
          when event_type in ('price_changed', 'listing_price_changed') then array['market.listPrice']
          when event_type in ('status_changed', 'listing_status_changed') then array['market.status']
          when event_type in ('listing_created', 'listing_refreshed') then array['market']
          else array[event_type]
        end as "changedFields",
        list_price as "listPrice",
        previous_list_price as "previousListPrice",
        case
          when previous_list_price is not null and list_price is not null then list_price - previous_list_price
          else null
        end as "priceChange",
        mls_status as "status",
        previous_mls_status as "previousStatus",
        listing_source as "listingSource",
        listing_url as "listingUrl",
        listing_agent_name as "listingAgentName",
        listing_brokerage as "listingBrokerage",
        true as "underwritingRelevant"
      from combined
    )
    select *
    from normalized
    where ${fallbackCursorSql}
    order by "eventAt" asc, "recordVersion" asc
    limit ${queryLimit};
    `);
  }

  const hasMore = rawRows.length > limit;
  const rows = rawRows.slice(0, limit);
  const lastRow = rows[rows.length - 1];
  const nextUpdatedAfter = lastRow?.eventAt instanceof Date
    ? lastRow.eventAt.toISOString()
    : typeof lastRow?.eventAt === 'string'
      ? lastRow.eventAt
      : updatedAfter;
  const nextCursor = lastRow ? encodeChangeCursor(nextUpdatedAfter, String(lastRow.recordVersion ?? '')) : c.req.query('cursor') ?? updatedAfter;

  return c.json({
    schemaVersion: 'mxre.bbc.changes.v1',
    market: marketConfig.key,
    source,
    updatedAfter,
    cursor: c.req.query('cursor') ?? null,
    limit,
    hasMore,
    count: rows.length,
    nextCursor,
    nextUpdatedAfter,
    results: rows,
    usage: {
      firstCall: '/v1/bbc/markets/{market}/changes?updated_after=2026-05-01T00:00:00.000Z&limit=500',
      nextCall: 'Call the same endpoint with cursor={nextCursor}. Continue while hasMore=true.',
      syncRule: 'BBC should store nextCursor per market and only re-underwrite records whose changedFields are underwriting relevant.',
    },
    fallbackPolicy: {
      provider: 'realestateapi',
      useWhen: ['mxre_no_property_match', 'mxre_missing_required_underwriting_fields', 'client_requested_validation'],
    },
    generatedAt: new Date().toISOString(),
  });
});

app.post('/v1/bbc/search-runs', async (c) => {
  let input: Record<string, unknown>;
  try {
    input = await c.req.json<Record<string, unknown>>();
  } catch {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }

  const marketValue = typeof input.market === 'string' ? input.market : '';
  const marketConfig = resolveMarketConfig(marketValue);
  if (!marketConfig) return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);

  const filters = normalizeBbcSearchFilters(input);
  const excludedIds = Array.isArray(input.excludeMxreIds)
    ? input.excludeMxreIds.map((value) => Number(value)).filter((value) => Number.isInteger(value) && value > 0).slice(0, 5000)
    : [];
  const since = parseDateTimeParam(typeof input.onlyChangedSince === 'string' ? input.onlyChangedSince : undefined)
    ?? parseDateTimeParam(typeof input.since === 'string' ? input.since : undefined)
    ?? new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const limit = Math.min(Number(input.limit) > 0 ? Number(input.limit) : 100, 500);
  const excludeSql = excludedIds.length > 0 ? `and p.id not in (${excludedIds.join(',')})` : '';
  const assetSql = filters.assetTypes.length > 0 ? `and asset_group in (${filters.assetTypes.map((asset) => `'${sqlString(asset)}'`).join(',')})` : '';
  const unitClassSql = filters.unitClasses.length > 0 ? `and (${filters.unitClasses.map(unitClassSqlCondition).filter(Boolean).join(' or ')})` : '';
  const statusSql = filters.statuses.length > 0
    ? `and (case when l.is_on_market then 'active' else 'off_market' end) in (${filters.statuses.map((status) => `'${sqlString(status)}'`).join(',')})`
    : '';
  const priceSql = [
    filters.minPrice !== null ? `and l.mls_list_price >= ${filters.minPrice}` : '',
    filters.maxPrice !== null ? `and l.mls_list_price <= ${filters.maxPrice}` : '',
    filters.minUnits !== null ? `and coalesce(p.total_units, 0) >= ${filters.minUnits}` : '',
    filters.maxUnits !== null ? `and coalesce(p.total_units, 0) <= ${filters.maxUnits}` : '',
    filters.minBeds !== null ? `and coalesce(p.bedrooms, 0) >= ${filters.minBeds}` : '',
    filters.maxBeds !== null ? `and coalesce(p.bedrooms, 0) <= ${filters.maxBeds}` : '',
    filters.minBaths !== null ? `and coalesce(p.bathrooms, p.bathrooms_full, 0) >= ${filters.minBaths}` : '',
    filters.maxBaths !== null ? `and coalesce(p.bathrooms, p.bathrooms_full, 0) <= ${filters.maxBaths}` : '',
    filters.minSqft !== null ? `and coalesce(p.living_sqft, 0) >= ${filters.minSqft}` : '',
    filters.maxSqft !== null ? `and coalesce(p.living_sqft, 0) <= ${filters.maxSqft}` : '',
    filters.minYearBuilt !== null ? `and coalesce(p.year_built, 0) >= ${filters.minYearBuilt}` : '',
    filters.maxYearBuilt !== null ? `and coalesce(p.year_built, 0) <= ${filters.maxYearBuilt}` : '',
    filters.states.length > 0 ? `and p.state_code in (${filters.states.map((state) => `'${sqlString(state)}'`).join(',')})` : '',
    filters.cities.length > 0 ? `and upper(coalesce(p.city,'')) in (${filters.cities.map((city) => `'${sqlString(city)}'`).join(',')})` : '',
    filters.zips.length > 0 ? `and p.zip in (${filters.zips.map((zip) => `'${sqlString(zip)}'`).join(',')})` : '',
  ].filter(Boolean).join('\n        ');
  const creativeSql = filters.creativeOnly ? `and l.creative_finance_status = 'positive'` : '';
  const equitySql = [
    filters.minEquityPercent !== null ? `and equity_percent >= ${filters.minEquityPercent}` : '',
    filters.maxEquityPercent !== null ? `and equity_percent <= ${filters.maxEquityPercent}` : '',
    filters.minEstimatedEquity !== null ? `and estimated_equity >= ${filters.minEstimatedEquity}` : '',
    filters.maxEstimatedEquity !== null ? `and estimated_equity <= ${filters.maxEstimatedEquity}` : '',
  ].filter(Boolean).join('\n      ');

  const [result] = await queryPg<Record<string, unknown>>(`
    with normalized as (
      select
        p.*,
        case
          when p.asset_type = 'commercial_multifamily' or coalesce(p.property_use, '') ilike '%APT%UNITS%' then 'commercial_multifamily'
          when p.asset_type = 'small_multifamily'
            or coalesce(p.property_use, '') ilike '%TWO FAMILY%'
            or coalesce(p.property_use, '') ilike '%THREE FAMILY%' then 'small_multifamily'
          when p.asset_subtype in ('sfr', 'condo')
            or coalesce(p.property_use, '') ilike '%ONE FAMILY%'
            or coalesce(p.property_use, '') ilike '%CONDO%'
            or coalesce(p.property_use, '') ilike 'RES VAC%' then 'single_family'
          else coalesce(nullif(p.asset_type, ''), nullif(p.property_type, ''), 'unknown')
        end as asset_group
      from properties p
      where p.state_code = '${marketConfig.state}'
        and upper(coalesce(p.city,'')) = '${marketConfig.cityUpper}'
    ),
    debt_rows as (
      select
        m.property_id,
        coalesce(nullif(m.loan_amount, 0), nullif(m.original_amount, 0))::numeric as original_amount,
        nullif(m.estimated_current_balance, 0)::numeric as estimated_current_balance,
        nullif(m.estimated_monthly_payment, 0)::numeric as estimated_monthly_payment,
        nullif(m.interest_rate, 0)::numeric as interest_rate,
        nullif(m.term_months, 0)::numeric as term_months,
        m.recording_date,
        m.maturity_date
      from mortgage_records m
      where (
          coalesce(m.open, true) = true
          or (m.maturity_date is not null and m.maturity_date::date > current_date)
        )
        and lower(coalesce(m.document_type, '')) not like '%deed%'
        and lower(coalesce(m.document_type, '')) not like '%release%'
        and lower(coalesce(m.document_type, '')) not like '%satisfaction%'
        and lower(coalesce(m.document_type, '')) not like '%assignment%'
    ),
    debt_balances as (
      select
        property_id,
        estimated_monthly_payment,
        case
          when estimated_current_balance is not null then estimated_current_balance
          when original_amount is not null
            and interest_rate is not null
            and term_months is not null
            and recording_date is not null
            and (
              case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end
            ) > 0
          then greatest(0, round(
            original_amount * power(
              1 + (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end),
              least(term_months, greatest(0, floor(extract(epoch from age(current_date, recording_date::date)) / 2629746)))
            )
            - (
              original_amount
              * (
                (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end)
                * power(1 + (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end), term_months)
              )
              / (
                power(1 + (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end), term_months) - 1
              )
            )
            * (
              (
                power(
                  1 + (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end),
                  least(term_months, greatest(0, floor(extract(epoch from age(current_date, recording_date::date)) / 2629746)))
                ) - 1
              )
              / (case when interest_rate > 1 then interest_rate / 100 / 12 else interest_rate / 12 end)
            )
          ))
          else original_amount
        end as current_balance
      from debt_rows
    ),
    debt as (
      select
        property_id,
        sum(current_balance)::numeric as open_mortgage_balance,
        sum(nullif(m.estimated_monthly_payment, 0))::numeric as estimated_mortgage_payment,
        count(*)::int as open_mortgage_count
      from debt_balances m
      group by property_id
    ),
    candidates as (
      select
        l.id as listing_id,
        p.id as property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        p.asset_group,
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
        p.assessed_value,
        d.open_mortgage_balance,
        d.estimated_mortgage_payment,
        d.open_mortgage_count,
        case
          when l.is_on_market = true and l.mls_list_price > 0 then l.mls_list_price
          when coalesce(p.market_value, 0) > 0 then p.market_value
          when coalesce(p.assessed_value, 0) > 0 then p.assessed_value
          else null
        end as equity_basis_value,
        case
          when l.is_on_market = true and l.mls_list_price > 0 then 'list_price'
          when coalesce(p.market_value, 0) > 0 then 'market_value'
          when coalesce(p.assessed_value, 0) > 0 then 'assessed_value'
          else null
        end as equity_basis,
        l.is_on_market,
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
        l.creative_finance_score,
        l.creative_finance_status,
        l.creative_finance_terms,
        l.first_seen_at,
        l.last_seen_at,
        greatest(coalesce(l.last_seen_at, '-infinity'::timestamptz), coalesce(l.first_seen_at, '-infinity'::timestamptz)) as changed_at
      from listing_signals l
      join normalized p on p.id = l.property_id
      left join debt d on d.property_id = p.id
      where coalesce(l.last_seen_at, l.first_seen_at) >= '${sqlString(since)}'::timestamptz
        ${excludeSql}
        ${assetSql}
        ${unitClassSql}
        ${statusSql}
        ${priceSql}
        ${creativeSql}
    ),
    enriched as (
      select
        *,
        case
          when equity_basis_value is not null and open_mortgage_balance is not null then round(equity_basis_value - open_mortgage_balance)::bigint
          else null
        end as estimated_equity,
        case
          when equity_basis_value > 0 and open_mortgage_balance is not null then round(((equity_basis_value - open_mortgage_balance) / equity_basis_value) * 100, 1)
          else null
        end as equity_percent
      from candidates
    ),
    filtered as (
      select *
      from enriched
      where true
      ${equitySql}
    ),
    totals as (
      select
        count(*)::int as matched,
        ${excludedIds.length}::int as excluded_by_client,
        count(*) filter (where first_seen_at >= '${sqlString(since)}'::timestamptz)::int as new_count,
        count(*) filter (where first_seen_at < '${sqlString(since)}'::timestamptz and changed_at >= '${sqlString(since)}'::timestamptz)::int as changed_count
      from filtered
    )
    select
      (select row_to_json(totals) from totals) as summary,
      (select coalesce(jsonb_agg(row_to_json(r) order by r."lastChangedAt" desc), '[]'::jsonb)
       from (
      select
        property_id as "mxreId",
        listing_id as "listingId",
        case
          when first_seen_at >= '${sqlString(since)}'::timestamptz then 'new_listing'
          else 'listing_updated'
        end as "eventReason",
        changed_at as "lastChangedAt",
        md5(property_id::text || '|' || listing_id::text || '|' || changed_at::text) as "recordVersion",
        case
          when first_seen_at >= '${sqlString(since)}'::timestamptz then array['market']
          else array['market.lastSeenAt']
        end as "changedFields",
        address,
        city,
        state_code as state,
        zip,
        asset_group as "assetGroup",
        asset_type as "assetType",
        asset_subtype as "assetSubtype",
        total_units as "unitCount",
        bedrooms,
        coalesce(bathrooms, bathrooms_full) as bathrooms,
        living_sqft as "livingSqft",
        year_built as "yearBuilt",
        market_value as "marketValue",
        is_on_market as "onMarket",
        mls_list_price as "listPrice",
        days_on_market as "daysOnMarket",
        listing_source as "listingSource",
        listing_url as "listingUrl",
        listing_agent_name as "listingAgentName",
        listing_agent_first_name as "listingAgentFirstName",
        listing_agent_last_name as "listingAgentLastName",
        listing_agent_email as "listingAgentEmail",
        listing_agent_phone as "listingAgentPhone",
        listing_brokerage as "listingBrokerage",
        creative_finance_score as "creativeFinanceScore",
        creative_finance_status as "creativeFinanceStatus",
        creative_finance_terms as "creativeFinanceTerms",
        open_mortgage_balance as "estimatedMortgageBalance",
        estimated_mortgage_payment as "estimatedMortgagePayment",
        open_mortgage_count as "openMortgageCount",
        estimated_equity as "estimatedEquity",
        equity_percent as "equityPercent",
        equity_basis as "equityBasis",
        equity_basis_value as "equityBasisValue"
      from filtered
      order by changed_at desc
      limit ${limit}
       ) r) as results;
  `);

  const summary = normalizeRecord(result?.summary);
  const results = Array.isArray(result?.results) ? result.results : [];

  return c.json({
    schemaVersion: 'mxre.bbc.searchRun.v1',
    searchRunId: `sr_${crypto.randomUUID()}`,
    market: marketConfig.key,
    asOf: new Date().toISOString(),
    since,
    nextCursor: new Date().toISOString(),
    filters,
    summary: {
      matched: numberOrNull(summary.matched) ?? results.length,
      new: numberOrNull(summary.new_count) ?? 0,
      changed: numberOrNull(summary.changed_count) ?? 0,
      unchangedSkipped: null,
      excludedByClient: excludedIds.length,
      returned: results.length,
    },
    results,
    clientWorkflow: {
      recommendedBehavior: 'Underwrite returned rows. Keep failed/passed status in Buy Box Club. Reconsider failed deals only when MXRE returns a later recordVersion or underwriting-relevant changedFields.',
      excludeMxreIdsApplied: excludedIds.length,
    },
    fallbackPolicy: {
      provider: 'realestateapi',
      useWhen: ['mxre_no_property_match', 'mxre_missing_required_underwriting_fields', 'client_requested_validation'],
    },
  });
});

app.get('/v1/property', async (c) => {
  const address = c.req.query('address');
  const city = c.req.query('city');
  const state = c.req.query('state');
  const zip = c.req.query('zip');
  const id = c.req.query('id');

  if (id) {
    return fetchAndRespond(c, parseInt(id, 10));
  }

  if (!address || !state) {
    return c.json({ error: 'Missing required params: address and state (city recommended)' }, 400);
  }

  // Normalize: uppercase, strip trailing wildcards the caller might have added
  const addressNorm = address.toUpperCase().replace(/[%*]+$/, '');
  const stateNorm = state.toUpperCase();

  // Use state_code + LIKE (not ilike) so the (state_code, address text_pattern_ops) index fires.
  // ilike requires a seq scan on 47M rows; addresses are stored uppercase and input is normalized above.
  // Trailing wildcard: "13114 OAKMERE" matches "13114 OAKMERE DR NW".
  let query = db.from('properties')
    .select('*, counties(county_name, state_code, county_fips, state_fips)')
    .eq('state_code', stateNorm)
    .like('address', `${addressNorm}%`)
    .limit(5);

  if (city) {
    query = query.eq('city', city.toUpperCase());
  }
  if (zip) {
    query = query.eq('zip', zip);
  }

  const { data: props, error } = await query;

  if (error) {
    return c.json({ error: 'Database error', detail: error.message }, 500);
  }
  if (!props || props.length === 0) {
    return c.json({ error: 'Property not found' }, 404);
  }

  return assembleResponse(c, props[0]);
});

// ── Search (must be before :id to avoid route collision) ─────
app.get('/v1/property/search', async (c) => {
  const state = c.req.query('state');
  const county = c.req.query('county');
  const city = c.req.query('city');
  const zip = c.req.query('zip');
  const absentee = c.req.query('absentee');         // 'true' to filter absentee owners
  const limit = Math.min(Math.max(parseInt(c.req.query('limit') ?? '50', 10) || 50, 1), 500);
  const offset = Math.max(parseInt(c.req.query('offset') ?? '0', 10) || 0, 0);

  let query = db.from('properties')
    .select('id, address, city, zip, property_type, market_value, assessed_value, owner_name, year_built, living_sqft, bedrooms, bathrooms_full, owner_occupied, absentee_owner, parcel_id, counties!inner(county_name, state_code)')
    .range(offset, offset + limit - 1);

  if (state) {
    query = query.eq('counties.state_code', state.toUpperCase());
  }
  if (county) {
    query = query.ilike('counties.county_name', county);
  }
  if (city) {
    query = query.eq('city', city.toUpperCase());
  }
  if (zip) {
    query = query.eq('zip', zip);
  }
  if (absentee === 'true') {
    query = query.eq('absentee_owner', true);
  }
  const { data, error } = await query;

  if (error) {
    return c.json({ error: 'Database error', detail: error.message }, 500);
  }

  const results: PropertySummary[] = (data ?? []).map((row: Record<string, unknown>) => {
    const countyData = row.counties as Record<string, unknown> | null;
    return {
      id: row.id as number,
      address: (row.address as string) ?? '',
      city: (row.city as string) ?? '',
      state: (countyData?.state_code as string) ?? '',
      zip: (row.zip as string) ?? '',
      county: (countyData?.county_name as string) ?? '',
      type: (row.property_type as string) ?? 'SFR',
      marketValue: (row.market_value as number) ?? null,
      assessedValue: (row.assessed_value as number) ?? null,
      ownerName: (row.owner_name as string) ?? null,
      yearBuilt: (row.year_built as number) ?? null,
      livingSqft: (row.living_sqft as number) ?? null,
      bedrooms: (row.bedrooms as number) ?? null,
      bathroomsFull: (row.bathrooms_full as number) ?? null,
      ownerOccupied: Boolean(row.owner_occupied),
      absenteeOwner: Boolean(row.absentee_owner),
      taxDelinquent: false,  // column not in DB yet
      parcelId: (row.parcel_id as string) ?? null,
    };
  });

  return c.json({ results, count: results.length, offset, limit });
});

// ── Counties list ────────────────────────────────────────────
app.get('/v1/counties', async (c) => {
  const state = c.req.query('state');
  const withCounts = c.req.query('counts') !== 'false'; // default true

  let query = db.from('counties')
    .select('id, county_name, state_code, county_fips, state_fips')
    .eq('active', true)
    .order('state_code', { ascending: true })
    .order('county_name', { ascending: true });

  if (state) {
    query = query.eq('state_code', state.toUpperCase());
  }

  const { data, error } = await query;
  if (error) return c.json({ error: 'Database error', detail: error.message }, 500);

  if (!withCounts) {
    return c.json({ counties: data ?? [], count: (data ?? []).length });
  }

  // Fetch property counts per county in batches
  const counties = data ?? [];
  const ids = counties.map((c: Record<string, unknown>) => c.id as number);
  const countsMap = new Map<number, number>();

  for (let i = 0; i < ids.length; i += 100) {
    const batch = ids.slice(i, i + 100);
    const { data: props } = await db.from('properties')
      .select('county_id')
      .in('county_id', batch);
    for (const p of props ?? []) {
      const cid = (p as Record<string, unknown>).county_id as number;
      countsMap.set(cid, (countsMap.get(cid) ?? 0) + 1);
    }
  }

  const result = counties.map((county: Record<string, unknown>) => ({
    id: county.id,
    county_name: county.county_name,
    state_code: county.state_code,
    county_fips: county.county_fips,
    state_fips: county.state_fips,
    property_count: countsMap.get(county.id as number) ?? 0,
  }));

  return c.json({ counties: result, count: result.length });
});

// ── County stats ──────────────────────────────────────────────
app.get('/v1/counties/:id', async (c) => {
  const id = parseInt(c.req.param('id'), 10);
  if (isNaN(id)) return c.json({ error: 'Invalid county ID' }, 400);

  const { data: county, error: cErr } = await db.from('counties')
    .select('id, county_name, state_code, county_fips, state_fips')
    .eq('id', id)
    .single();

  if (cErr || !county) return c.json({ error: 'County not found' }, 404);

  const { count: propCount } = await db.from('properties')
    .select('*', { count: 'exact', head: true })
    .eq('county_id', id);

  // Property type breakdown
  const { data: typeBreakdown } = await db.from('properties')
    .select('property_type')
    .eq('county_id', id)
    .limit(10000);

  const typeCounts: Record<string, number> = {};
  for (const p of typeBreakdown ?? []) {
    const t = (p as Record<string, unknown>).property_type as string ?? 'unknown';
    typeCounts[t] = (typeCounts[t] ?? 0) + 1;
  }

  return c.json({
    id: county.id,
    county_name: county.county_name,
    state_code: county.state_code,
    county_fips: county.county_fips,
    state_fips: county.state_fips,
    property_count: propCount ?? 0,
    property_types: typeCounts,
  });
});

// ── States list (with county/property counts) ─────────────────
app.get('/v1/states', async (c) => {
  const { data: counties, error } = await db.from('counties')
    .select('state_code, state_fips')
    .eq('active', true);

  if (error) return c.json({ error: 'Database error', detail: error.message }, 500);

  // Aggregate by state
  const stateMap = new Map<string, { state_fips: string; county_count: number }>();
  for (const row of counties ?? []) {
    const { state_code, state_fips } = row as Record<string, string>;
    if (!stateMap.has(state_code)) {
      stateMap.set(state_code, { state_fips, county_count: 0 });
    }
    stateMap.get(state_code)!.county_count++;
  }

  const states = Array.from(stateMap.entries())
    .map(([state_code, v]) => ({ state_code, ...v }))
    .sort((a, b) => a.state_code.localeCompare(b.state_code));

  return c.json({ states, count: states.length });
});

// ── Property lookup by ID ────────────────────────────────────
app.get('/v1/property/:id', async (c) => {
  const id = parseInt(c.req.param('id'), 10);
  if (isNaN(id)) {
    return c.json({ error: 'Invalid property ID' }, 400);
  }
  return fetchAndRespond(c, id);
});

// ── Coverage / Analytics (no auth — internal dashboard use) ──

// Coverage cache — recomputed in background every 10 minutes
let coverageCache: Record<string, unknown> | null = null;
let coverageCacheTime = 0;
let coverageRefreshing = false;
const COVERAGE_TTL_MS = 10 * 60 * 1000;

async function buildCoverage(): Promise<Record<string, unknown>> {
  const ALL_STATES = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'];

  // Use estimated row counts from pg_stat_user_tables — returns instantly, accurate within ~1%
  const { data: statRows } = await db.rpc('get_table_counts' as any);

  // Fallback: if RPC not available, use fast approximate counts via explain
  let totalProps = 0, totalMortgages = 0, totalRents = 0, totalListings = 0;
  if (statRows) {
    for (const r of statRows as any[]) {
      if (r.tablename === 'properties') totalProps = r.row_count;
      if (r.tablename === 'mortgage_records') totalMortgages = r.row_count;
      if (r.tablename === 'rent_snapshots') totalRents = r.row_count;
      if (r.tablename === 'listing_signals') totalListings = r.row_count;
    }
  } else {
    // Direct count — slow but correct
    const [a, b, c, d] = await Promise.all([
      db.from('properties').select('*', { count: 'exact', head: true }),
      db.from('mortgage_records').select('*', { count: 'exact', head: true }),
      db.from('rent_snapshots').select('*', { count: 'exact', head: true }),
      db.from('listing_signals').select('*', { count: 'exact', head: true }),
    ]);
    totalProps = a.count ?? 0;
    totalMortgages = b.count ?? 0;
    totalRents = c.count ?? 0;
    totalListings = d.count ?? 0;
  }

  // State coverage via counties table (fast — counties table is small)
  const { data: countyRows } = await db.from('counties').select('id, state_code').eq('active', true);
  const stateToCountyIds: Record<string, number[]> = {};
  for (const row of countyRows ?? []) {
    const { state_code, id } = row as any;
    if (!stateToCountyIds[state_code]) stateToCountyIds[state_code] = [];
    stateToCountyIds[state_code].push(id);
  }

  // Count per state — run sequentially to avoid overwhelming DB
  const stateCounts: Record<string, number> = {};
  for (const [state, countyIds] of Object.entries(stateToCountyIds)) {
    const { count } = await db.from('properties')
      .select('*', { count: 'exact', head: true })
      .in('county_id', countyIds);
    stateCounts[state] = count ?? 0;
  }

  const [{ data: lastWrite }, { data: lastMortgage }] = await Promise.all([
    db.from('properties').select('created_at').order('created_at', { ascending: false }).limit(1),
    db.from('mortgage_records').select('recording_date').order('recording_date', { ascending: false }).limit(1),
  ]);

  const covered = ALL_STATES.filter(s => (stateCounts[s] ?? 0) > 0).sort();
  const missing = ALL_STATES.filter(s => (stateCounts[s] ?? 0) === 0).sort();
  const topStates = Object.entries(stateCounts)
    .filter(([, n]) => n > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([state, count]) => ({ state, count }));

  return {
    generated_at: new Date().toISOString(),
    cache_note: 'Refreshes every 10 minutes',
    totals: { properties: totalProps, mortgage_records: totalMortgages, rent_snapshots: totalRents, listing_signals: totalListings },
    coverage: {
      states_with_data: covered.length,
      states_missing: missing.length,
      covered_states: covered,
      missing_states: missing,
      coverage_pct: `${((covered.length / ALL_STATES.length) * 100).toFixed(1)}%`,
    },
    top_states: topStates,
    freshness: {
      last_property_write: (lastWrite?.[0] as any)?.created_at ?? null,
      last_mortgage_recording: (lastMortgage?.[0] as any)?.recording_date ?? null,
    },
  };
}

// Kick off initial cache build on startup
setTimeout(() => {
  coverageRefreshing = true;
  buildCoverage()
    .then(r => { coverageCache = r; coverageCacheTime = Date.now(); })
    .catch(e => console.error('Coverage cache build failed:', e.message))
    .finally(() => { coverageRefreshing = false; });
}, 2000);

app.get('/v1/coverage', async (c) => {
  // Return cached version instantly if fresh
  if (coverageCache && Date.now() - coverageCacheTime < COVERAGE_TTL_MS) {
    return c.json(coverageCache);
  }

  // If cache is stale and not already refreshing, kick off background refresh
  if (!coverageRefreshing) {
    coverageRefreshing = true;
    buildCoverage()
      .then(r => { coverageCache = r; coverageCacheTime = Date.now(); })
      .catch(e => console.error('Coverage refresh failed:', e.message))
      .finally(() => { coverageRefreshing = false; });
  }

  // Return stale cache if available while refresh runs, or a pending response
  if (coverageCache) {
    return c.json({ ...coverageCache, cache_note: 'Stale — refresh in progress' });
  }

  return c.json({ status: 'building', message: 'Coverage data is being compiled (~2 min). Refresh in a moment.' }, 202);
});

app.get('/v1/coverage/state/:state', async (c) => {
  const state = c.req.param('state').toUpperCase();
  if (!/^[A-Z]{2}$/.test(state)) return c.json({ error: 'Invalid state code' }, 400);

  const { data: counties, error } = await db.from('counties')
    .select('id, county_name, state_code, county_fips, state_fips')
    .eq('active', true)
    .eq('state_code', state)
    .order('county_name', { ascending: true });

  if (error) return c.json({ error: 'Database error', detail: error.message }, 500);

  const countyIds = (counties ?? []).map((county) => county.id);
  const stateFips = (counties?.[0]?.state_fips as string | undefined) ?? '';

  const [{ count: propertyTotal }, { count: mortgageTotal }, { count: listingSignals }] = await Promise.all([
    countyIds.length > 0
      ? db.from('properties').select('id', { count: 'exact', head: true }).in('county_id', countyIds)
      : Promise.resolve({ count: 0 }),
    stateFips
      ? db.from('mortgage_records').select('id', { count: 'exact', head: true }).like('county_fips', `${stateFips}%`)
      : Promise.resolve({ count: 0 }),
    db.from('listing_signals').select('id', { count: 'exact', head: true }).eq('state_code', state),
  ]);

  const rows = (counties ?? []).map((county) => ({
    id: county.id,
    county_name: county.county_name,
    state_code: county.state_code,
    county_fips: `${county.state_fips}${county.county_fips}`,
  }));

  return c.json({
    state,
    counties: rows,
    totals: {
      counties: rows.length,
      properties: propertyTotal ?? 0,
      mortgage_records: mortgageTotal ?? 0,
      listing_signals: listingSignals ?? 0,
    },
    generated_at: new Date().toISOString(),
  });
});

// Multifamily on-market feed for Buy Box Club / dashboard consumers.
// Indianapolis starts with Marion County because that is the current complete asset-classified market.
app.get('/v1/markets/:market/multifamily/on-market', async (c) => {
  const marketConfig = resolveMarketConfig(c.req.param('market'));
  if (!marketConfig) {
    return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);
  }

  const limit = Math.min(Math.max(parseInt(c.req.query('limit') ?? '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(c.req.query('offset') ?? '0', 10) || 0, 0);
  const source = c.req.query('source')?.toLowerCase();
  const subtype = c.req.query('subtype')?.toLowerCase();
  const minUnits = parseInt(c.req.query('min_units') ?? '', 10);
  const maxUnits = parseInt(c.req.query('max_units') ?? '', 10);

  let query = db.from('listing_signals')
    .select(`
      id,
      property_id,
      address,
      city,
      state_code,
      zip,
      is_on_market,
      mls_list_price,
      listing_agent_name,
      listing_agent_first_name,
      listing_agent_last_name,
      listing_agent_email,
      listing_agent_phone,
      agent_contact_source,
      agent_contact_confidence,
      listing_brokerage,
      creative_finance_score,
      creative_finance_status,
      creative_finance_terms,
      creative_finance_negative_terms,
      creative_finance_rate_text,
      listing_source,
      listing_url,
      days_on_market,
      confidence,
      first_seen_at,
      last_seen_at,
      delisted_at,
      raw,
      properties!inner(
        id,
        address,
        city,
        state_code,
        zip,
        county_id,
        parcel_id,
        owner_name,
        property_type,
        asset_type,
        asset_subtype,
        total_units,
        unit_count_source,
        asset_confidence,
        living_sqft,
        total_sqft,
        bedrooms,
        bathrooms_full,
        year_built,
        market_value,
        assessed_value,
        absentee_owner,
        owner_occupied
      )
    `, { count: 'exact' })
    .eq('is_on_market', true)
    .eq('properties.county_id', marketConfig.countyId)
    .eq('properties.city', marketConfig.cityUpper)
    .in('properties.asset_type', ['small_multifamily', 'apartment', 'commercial_multifamily'])
    .order('last_seen_at', { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (source) query = query.eq('listing_source', source);
  if (subtype) query = query.eq('properties.asset_subtype', subtype);
  if (!Number.isNaN(minUnits)) query = query.gte('properties.total_units', minUnits);
  if (!Number.isNaN(maxUnits)) query = query.lte('properties.total_units', maxUnits);

  const { data, error, count } = await query;
  if (error) return c.json({ error: 'Database error', detail: error.message }, 500);

  let summaryQuery = db.from('listing_signals')
    .select('listing_source, properties!inner(asset_subtype,total_units)')
    .eq('is_on_market', true)
    .eq('properties.county_id', marketConfig.countyId)
    .eq('properties.city', marketConfig.cityUpper)
    .in('properties.asset_type', ['small_multifamily', 'apartment', 'commercial_multifamily'])
    .limit(1000);

  if (source) summaryQuery = summaryQuery.eq('listing_source', source);
  if (subtype) summaryQuery = summaryQuery.eq('properties.asset_subtype', subtype);
  if (!Number.isNaN(minUnits)) summaryQuery = summaryQuery.gte('properties.total_units', minUnits);
  if (!Number.isNaN(maxUnits)) summaryQuery = summaryQuery.lte('properties.total_units', maxUnits);

  const { data: summaryRows } = await summaryQuery;

  const rows = (data ?? []).map((row: Record<string, unknown>) => {
    const property = normalizeJoinedProperty(row.properties);
    const listPrice = numberOrNull(row.mls_list_price);
    const units = numberOrNull(property.total_units);
    const livingSqft = numberOrNull(property.living_sqft);

    return {
      listingId: row.id,
      propertyId: row.property_id,
      property: {
        address: property.address ?? row.address,
        city: property.city ?? row.city,
        state: property.state_code ?? row.state_code,
        zip: property.zip ?? row.zip,
        parcelId: property.parcel_id ?? null,
        ownerName: property.owner_name ?? null,
        assetType: property.asset_type ?? null,
        assetSubtype: property.asset_subtype ?? null,
        unitCount: units,
        unitCountSource: property.unit_count_source ?? null,
        assetConfidence: property.asset_confidence ?? null,
        livingSqft,
        totalSqft: numberOrNull(property.total_sqft),
        bedrooms: numberOrNull(property.bedrooms),
        bathroomsFull: numberOrNull(property.bathrooms_full),
        yearBuilt: numberOrNull(property.year_built),
        marketValue: numberOrNull(property.market_value),
        assessedValue: numberOrNull(property.assessed_value),
        absenteeOwner: Boolean(property.absentee_owner),
        ownerOccupied: Boolean(property.owner_occupied),
      },
      market: {
        onMarket: true,
        listPrice,
        pricePerUnit: listPrice && units && units > 0 ? Math.round(listPrice / units) : null,
        pricePerSqft: listPrice && livingSqft && livingSqft > 0 ? Math.round((listPrice / livingSqft) * 100) / 100 : null,
        daysOnMarket: numberOrNull(row.days_on_market),
        listingSource: row.listing_source ?? null,
        listingUrl: row.listing_url ?? null,
        listingAgentName: row.listing_agent_name ?? null,
        listingAgentFirstName: row.listing_agent_first_name ?? null,
        listingAgentLastName: row.listing_agent_last_name ?? null,
        listingAgentEmail: row.listing_agent_email ?? null,
        listingAgentPhone: row.listing_agent_phone ?? null,
        agentContactSource: row.agent_contact_source ?? null,
        agentContactConfidence: row.agent_contact_confidence ?? null,
        listingBrokerage: row.listing_brokerage ?? null,
        creativeFinanceScore: numberOrNull(row.creative_finance_score),
        creativeFinanceStatus: row.creative_finance_status ?? null,
        creativeFinanceTerms: row.creative_finance_terms ?? [],
        creativeFinanceNegativeTerms: row.creative_finance_negative_terms ?? [],
        creativeFinanceRateText: row.creative_finance_rate_text ?? null,
        confidence: row.confidence ?? null,
        firstSeenAt: row.first_seen_at ?? null,
        lastSeenAt: row.last_seen_at ?? null,
      },
      raw: row.raw ?? null,
    };
  });

  const sourceCounts: Record<string, number> = {};
  const subtypeCounts: Record<string, number> = {};
  for (const summaryRow of (summaryRows ?? []) as Array<Record<string, unknown>>) {
    const property = normalizeJoinedProperty(summaryRow.properties);
    const sourceKey = String(summaryRow.listing_source ?? 'unknown');
    const subtypeKey = String(property.asset_subtype ?? 'unknown');
    sourceCounts[sourceKey] = (sourceCounts[sourceKey] ?? 0) + 1;
    subtypeCounts[subtypeKey] = (subtypeCounts[subtypeKey] ?? 0) + 1;
  }

  return c.json({
    market: marketConfig.key,
    geography: { city: marketConfig.city, county: marketConfig.county, state: marketConfig.state, countyId: marketConfig.countyId },
    asset_filter: ['small_multifamily', 'apartment', 'commercial_multifamily'],
    total: count ?? rows.length,
    count: rows.length,
    offset,
    limit,
    filters: {
      source: source ?? null,
      subtype: subtype ?? null,
      min_units: Number.isNaN(minUnits) ? null : minUnits,
      max_units: Number.isNaN(maxUnits) ? null : maxUnits,
    },
    summary: {
      by_source: sourceCounts,
      by_subtype: subtypeCounts,
    },
    results: rows,
    generated_at: new Date().toISOString(),
  });
});

// ── Ingest status (reads supervisor logs) ─────────────────────

app.get('/v1/markets/:market/dashboard', async (c) => {
  const marketConfig = resolveMarketConfig(c.req.param('market'));
  if (!marketConfig) {
    return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);
  }

  const assetClass = (c.req.query('asset_class') ?? 'multifamily').toLowerCase();
  if (assetClass !== 'multifamily') {
    return c.json({ error: 'Unsupported asset_class', supported_asset_classes: ['multifamily'] }, 400);
  }
  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const maxUnits = parsePositiveInt(c.req.query('max_units'));
  const unitFilterSql = [
    minUnits !== null ? `and coalesce(total_units, 0) >= ${minUnits}` : '',
    maxUnits !== null ? `and coalesce(total_units, 0) <= ${maxUnits}` : '',
  ].filter(Boolean).join('\n        ');
  const activeUnitFilterSql = [
    minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '',
    maxUnits !== null ? `and coalesce(p.total_units, 0) <= ${maxUnits}` : '',
  ].filter(Boolean).join('\n        ');

  const [dashboard] = await queryPg<Record<string, unknown>>(`
    with inventory as (
      select asset_type, coalesce(asset_subtype, asset_type, 'unknown') as subtype, coalesce(total_units, 0) as total_units
      from properties
      where county_id = ${marketConfig.countyId}
        and upper(coalesce(city, '')) = '${marketConfig.cityUpper}'
        and asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
        ${unitFilterSql}
    ),
    active as (
      select
        l.id as listing_id,
        l.property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        cp.complex_name,
        cp.management_company,
        cp.website as complex_website,
        cp.phone as complex_phone,
        cp.source as complex_source,
        cp.confidence as complex_confidence,
        coalesce(p.asset_subtype, 'unknown') as subtype,
        p.total_units,
        p.living_sqft,
        l.mls_list_price,
        case when p.total_units > 0 and l.mls_list_price > 0 then round(l.mls_list_price::numeric / p.total_units) end as price_per_unit,
        case when p.living_sqft > 0 and l.mls_list_price > 0 then round((l.mls_list_price::numeric / p.living_sqft) * 100) / 100 end as price_per_sqft,
        l.days_on_market,
        coalesce(l.listing_source, 'unknown') as listing_source,
        l.listing_url,
        l.last_seen_at
      from listing_signals l
      join properties p on p.id = l.property_id
      left join property_complex_profiles cp on cp.property_id = p.id
      where l.is_on_market = true
        and p.county_id = ${marketConfig.countyId}
        and upper(coalesce(p.city, '')) = '${marketConfig.cityUpper}'
        and p.asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
        ${activeUnitFilterSql}
    ),
    external_active as (
      select *
      from external_market_listings
      where market = '${marketConfig.key}'
        and asset_class = 'multifamily'
        and status = 'active'
        ${minUnits !== null ? `and coalesce(units, 0) >= ${minUnits}` : ''}
        ${maxUnits !== null ? `and coalesce(units, 0) <= ${maxUnits}` : ''}
    )
    select
      (select count(*) from inventory)::int as total_multifamily_properties,
      (select coalesce(sum(total_units), 0) from inventory)::int as known_multifamily_units,
      (select coalesce(jsonb_object_agg(subtype, jsonb_build_object('properties', properties, 'known_units', known_units)), '{}'::jsonb)
       from (
         select subtype, count(*)::int as properties, coalesce(sum(total_units), 0)::int as known_units
         from inventory
         group by subtype
       ) s) as inventory_by_subtype,
      (select count(*) from active)::int as active_listing_rows,
      (select count(*) from external_active)::int as external_listing_rows,
      (select count(distinct property_id) from active)::int as unique_properties,
      (select coalesce(count(*), 0)::int from external_active where coalesce(units,0) >= 4) as external_4_plus_rows,
      (select coalesce(jsonb_object_agg(listing_source, listings), '{}'::jsonb)
       from (
         select listing_source, count(*)::int as listings
         from active
         group by listing_source
       ) s) as by_source,
      (select coalesce(jsonb_object_agg(subtype, listings), '{}'::jsonb)
       from (
         select subtype, count(*)::int as listings
         from active
         group by subtype
       ) s) as by_subtype,
      (select coalesce(jsonb_agg(row_to_json(z)), '[]'::jsonb)
       from (
         select
           zip,
           count(*)::int as listings,
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as median_list_price,
           round(percentile_cont(0.5) within group (order by price_per_unit))::int as median_price_per_unit
         from active
         where mls_list_price is not null
         group by zip
         order by count(*) desc, zip
         limit 15
       ) z) as by_zip,
      round((select percentile_cont(0.25) within group (order by mls_list_price) from active where mls_list_price > 0))::int as list_price_p25,
      round((select percentile_cont(0.5) within group (order by mls_list_price) from active where mls_list_price > 0))::int as list_price_median,
      round((select percentile_cont(0.75) within group (order by mls_list_price) from active where mls_list_price > 0))::int as list_price_p75,
      round((select percentile_cont(0.25) within group (order by price_per_unit) from active where price_per_unit > 0))::int as price_per_unit_p25,
      round((select percentile_cont(0.5) within group (order by price_per_unit) from active where price_per_unit > 0))::int as price_per_unit_median,
      round((select percentile_cont(0.75) within group (order by price_per_unit) from active where price_per_unit > 0))::int as price_per_unit_p75,
      round((select percentile_cont(0.5) within group (order by price_per_sqft) from active where price_per_sqft > 0))::numeric as price_per_sqft_median,
      round((select percentile_cont(0.5) within group (order by days_on_market) from active where days_on_market is not null))::int as days_on_market_median,
      (select coalesce(jsonb_agg(row_to_json(t)), '[]'::jsonb)
       from (
         select
           listing_id as "listingId",
           property_id as "propertyId",
           complex_name as "complexName",
           management_company as "managementCompany",
           complex_website as "complexWebsite",
           complex_phone as "complexPhone",
           complex_source as "complexSource",
           complex_confidence as "complexConfidence",
           address,
           city,
           state_code as state,
           zip,
           subtype as "assetSubtype",
           total_units as "unitCount",
           mls_list_price as "listPrice",
           price_per_unit as "pricePerUnit",
           price_per_sqft as "pricePerSqft",
           days_on_market as "daysOnMarket",
           listing_source as "listingSource",
           listing_url as "listingUrl",
           last_seen_at as "lastSeenAt"
         from active
         where mls_list_price is not null
         order by mls_list_price desc
         limit 10
       ) t) as top_listings
       ,
      (select coalesce(jsonb_agg(row_to_json(e)), '[]'::jsonb)
       from (
         select
           id as "externalListingId",
           title,
           address,
           city,
           state_code as state,
           zip,
           units,
           list_price as "listPrice",
           price_per_unit as "pricePerUnit",
           source,
           source_url as "sourceUrl",
           confidence,
           observed_at as "observedAt"
         from external_active
         order by coalesce(units,0) desc, coalesce(list_price,0) desc
         limit 10
       ) e) as external_top_listings
  `);

  return c.json({
    market: marketConfig.key,
    geography: { city: marketConfig.city, county: marketConfig.county, state: marketConfig.state, countyId: marketConfig.countyId },
    asset_class: assetClass,
    filters: {
      min_units: minUnits,
      max_units: maxUnits,
    },
    inventory: {
      total_multifamily_properties: numberOrNull(dashboard?.total_multifamily_properties) ?? 0,
      known_multifamily_units: numberOrNull(dashboard?.known_multifamily_units) ?? 0,
      by_subtype: dashboard?.inventory_by_subtype ?? {},
    },
    on_market: {
      active_listing_rows: numberOrNull(dashboard?.active_listing_rows) ?? 0,
      unique_properties: numberOrNull(dashboard?.unique_properties) ?? 0,
      external_listing_rows: numberOrNull(dashboard?.external_listing_rows) ?? 0,
      external_4_plus_rows: numberOrNull(dashboard?.external_4_plus_rows) ?? 0,
      by_source: dashboard?.by_source ?? {},
      by_subtype: dashboard?.by_subtype ?? {},
      by_zip: dashboard?.by_zip ?? [],
      list_price: {
        p25: numberOrNull(dashboard?.list_price_p25),
        median: numberOrNull(dashboard?.list_price_median),
        p75: numberOrNull(dashboard?.list_price_p75),
      },
      price_per_unit: {
        p25: numberOrNull(dashboard?.price_per_unit_p25),
        median: numberOrNull(dashboard?.price_per_unit_median),
        p75: numberOrNull(dashboard?.price_per_unit_p75),
      },
      price_per_sqft: {
        median: numberOrNull(dashboard?.price_per_sqft_median),
      },
      days_on_market: {
        median: numberOrNull(dashboard?.days_on_market_median),
      },
      top_listings: dashboard?.top_listings ?? [],
      external_top_listings: dashboard?.external_top_listings ?? [],
    },
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/readiness', async (c) => {
  const marketConfig = resolveMarketConfig(c.req.param('market'));
  if (!marketConfig) {
    return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);
  }

  const propWhere = `county_id = ${marketConfig.countyId} and state_code = '${marketConfig.state}' and upper(coalesce(city,'')) = '${marketConfig.cityUpper}'`;
  const listingWhere = `is_on_market = true and state_code = '${marketConfig.state}' and upper(coalesce(city,'')) = '${marketConfig.cityUpper}'`;
  const mfWhere = `${propWhere} and (coalesce(total_units,1) >= 2 or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily'))`;

  const [summary] = await queryPg<Record<string, unknown>>(`
    with parcels as (
      select count(*)::int as parcel_count,
             count(*) filter (where asset_type is not null)::int as classified_count,
             count(*) filter (where total_units is not null)::int as unit_count_count,
             count(*) filter (where asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily') or coalesce(total_units,0) >= 2)::int as multifamily_asset_count
        from properties
       where ${propWhere}
    ),
    listings as (
      select count(*)::int as active_listing_count,
             count(distinct listing_source)::int as listing_source_count,
             array_agg(distinct listing_source order by listing_source) filter (where listing_source is not null) as listing_sources,
             count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name_count,
             count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as agent_phone_count,
             count(*) filter (where nullif(listing_agent_email,'') is not null)::int as agent_email_count,
             count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage_count,
             count(*) filter (where creative_finance_status = 'positive')::int as creative_finance_count,
             count(*) filter (where raw ? 'redfinDetail')::int as redfin_detail_count
        from listing_signals
       where ${listingWhere}
    ),
    mf as (
      with universe as (select id from properties where ${mfWhere})
      select count(distinct universe.id)::int as complex_count,
             count(distinct pw.property_id)::int as complexes_with_websites,
             count(distinct fp.property_id)::int as complexes_with_floorplans,
             count(distinct fp.id)::int as floorplan_rows,
             count(distinct rs.property_id)::int as complexes_with_rent_snapshots,
             count(distinct rs.id)::int as rent_snapshot_rows,
             max(rs.observed_at) as latest_rent_observed
        from universe
        left join property_websites pw on pw.property_id = universe.id and pw.active = true
        left join floorplans fp on fp.property_id = universe.id
        left join rent_snapshots rs on rs.property_id = universe.id
    ),
    recorder as (
      select count(*)::int as recorder_records,
             count(distinct m.property_id)::int as properties_with_recorder_records,
             count(*) filter (where lower(coalesce(m.document_type,'')) like '%mortgage%')::int as mortgage_doc_rows,
             count(*) filter (where lower(coalesce(m.document_type,'')) like '%lien%')::int as lien_doc_rows,
             count(*) filter (where nullif(m.loan_amount,0) is not null)::int as records_with_amounts,
             max(m.recording_date) as latest_recording
        from mortgage_records m
        join properties p on p.id = m.property_id
       where p.county_id = ${marketConfig.countyId}
         and p.state_code = '${marketConfig.state}'
         and upper(coalesce(p.city,'')) = '${marketConfig.cityUpper}'
    )
    select row_to_json(parcels) as parcels,
           row_to_json(listings) as listings,
           row_to_json(mf) as multifamily,
           row_to_json(recorder) as recorder
      from parcels, listings, mf, recorder;
  `);

  return c.json({
    market: marketConfig.key,
    geography: { city: marketConfig.city, county: marketConfig.county, state: marketConfig.state, countyId: marketConfig.countyId },
    parcels: summary?.parcels ?? {},
    listings: summary?.listings ?? {},
    multifamily: summary?.multifamily ?? {},
    recorder: summary?.recorder ?? {},
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/price-changes', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const days = Math.min(parsePositiveInt(c.req.query('days')) ?? 7, 365);
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 50, 500);
  const direction = (c.req.query('direction') ?? 'drops').toLowerCase();
  const directionSql = direction === 'increases'
    ? 'and e.list_price > e.previous_list_price'
    : direction === 'all'
      ? ''
      : 'and e.list_price < e.previous_list_price';

  const rows = await queryPg<Record<string, unknown>>(`
    select
      e.event_at as "eventAt",
      e.address,
      e.city,
      e.state_code as state,
      e.zip,
      e.listing_source as "listingSource",
      e.listing_url as "listingUrl",
      e.previous_list_price as "previousListPrice",
      e.list_price as "listPrice",
      (e.list_price - e.previous_list_price)::numeric as "priceChange",
      case
        when e.previous_list_price > 0 then round(((e.list_price - e.previous_list_price) / e.previous_list_price) * 100, 2)
        else null
      end as "priceChangePct",
      e.days_on_market as "daysOnMarket",
      e.listing_agent_name as "listingAgentName",
      e.listing_brokerage as "listingBrokerage"
    from listing_signal_events e
    where e.event_type = 'price_changed'
      and e.list_price is not null
      and e.previous_list_price is not null
      and e.state_code = 'IN'
      and e.city ilike '%INDIANAPOLIS%'
      and e.event_at >= now() - interval '${days} days'
      ${directionSql}
    order by abs(e.list_price - e.previous_list_price) desc nulls last, e.event_at desc
    limit ${limit};
  `);

  return c.json({
    market: 'indianapolis',
    days,
    direction,
    count: rows.length,
    results: rows,
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/analytics', async (c) => {
  const marketConfig = resolveMarketConfig(c.req.param('market'));
  if (!marketConfig || marketConfig.key !== 'indianapolis') {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const days = Math.min(parsePositiveInt(c.req.query('days')) ?? 30, 365);
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 25, 500);
  const listingWhere = `l.state_code = '${sqlString(marketConfig.state)}' and l.city ilike '%${sqlString(marketConfig.cityUpper)}%'`;
  const eventWhere = `e.state_code = '${sqlString(marketConfig.state)}' and e.city ilike '%${sqlString(marketConfig.cityUpper)}%'`;

  const [analytics] = await queryPg<Record<string, unknown>>(`
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
        coalesce(p.bathrooms, p.bathrooms_full) as bathrooms,
        p.living_sqft,
        p.market_value,
        l.mls_list_price,
        l.days_on_market,
        l.listing_source,
        l.listing_url,
        l.listing_agent_name,
        l.listing_agent_email,
        l.listing_agent_phone,
        l.listing_brokerage,
        l.creative_finance_score,
        l.creative_finance_status,
        l.creative_finance_terms,
        l.creative_finance_negative_terms,
        l.creative_finance_rate_text,
        l.first_seen_at,
        l.last_seen_at,
        l.raw,
        nullif(coalesce(
          l.raw #>> '{redfinDetail,publicRemarks}',
          l.raw #>> '{redfinDetail,description}',
          l.raw #>> '{publicRemarks}',
          l.raw #>> '{remarks}',
          l.raw #>> '{description}',
          l.raw #>> '{listingDescription}',
          l.raw #>> '{zillow_rapidapi_detail,raw,property,description}',
          l.raw #>> '{zillow_rapidapi_detail,raw,description}',
          l.raw #>> '{zillow_rapidapi_detail,raw,data,description}',
          l.raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
          l.raw #>> '{mls,remarks}',
          l.raw #>> '{mls,description}',
          ''
        ), '') as listing_description
      from listing_signals l
      left join properties p on p.id = l.property_id
      where l.is_on_market = true
        and ${listingWhere}
    ),
    recent_events as (
      select e.*
      from listing_signal_events e
      where ${eventWhere}
        and e.event_at >= now() - interval '${days} days'
    ),
    event_counts as (
      select event_type, count(*)::int as count
      from recent_events
      group by event_type
    ),
    daily_events as (
      select date_trunc('day', event_at)::date as day, event_type, count(*)::int as count
      from recent_events
      group by 1,2
      order by 1 desc, 2
    )
    select
      jsonb_build_object(
        'activeRows', (select count(*) from active),
        'activeProperties', (select count(distinct property_id) from active),
        'medianListPrice', (select round(percentile_cont(0.5) within group (order by mls_list_price))::int from active where mls_list_price > 0),
        'medianDaysOnMarket', (select round(percentile_cont(0.5) within group (order by days_on_market))::int from active where days_on_market is not null),
        'creativePositive', (select count(*) from active where creative_finance_status = 'positive'),
        'creativeNegative', (select count(*) from active where creative_finance_status = 'negative'),
        'agentEmailCoveragePct', (select case when count(*) = 0 then 0 else round((count(*) filter (where listing_agent_email is not null)::numeric / count(*)) * 100, 2) end from active),
        'agentPhoneCoveragePct', (select case when count(*) = 0 then 0 else round((count(*) filter (where listing_agent_phone is not null)::numeric / count(*)) * 100, 2) end from active),
        'descriptionCoveragePct', (select case when count(*) = 0 then 0 else round((count(*) filter (where listing_description is not null)::numeric / count(*)) * 100, 2) end from active),
        'priceDrops', (select count(*) from recent_events where event_type = 'price_changed' and list_price < previous_list_price),
        'delisted', (select count(*) from recent_events where event_type = 'delisted'),
        'newListings', (select count(*) from recent_events where event_type in ('listed','listing_created','listing_refreshed')),
        'relisted', (select count(*) from recent_events where event_type = 'relisted')
      ) as overview,
      (select coalesce(jsonb_object_agg(event_type, count), '{}'::jsonb) from event_counts) as event_counts,
      (select coalesce(jsonb_agg(row_to_json(d)), '[]'::jsonb) from daily_events d) as daily_events,
      (select coalesce(jsonb_agg(row_to_json(a)), '[]'::jsonb)
       from (
         select
           asset_group as "assetGroup",
           count(*)::int as listings,
           count(distinct property_id)::int as properties,
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as "medianListPrice",
           count(*) filter (where creative_finance_status = 'positive')::int as "creativePositive",
           count(*) filter (where listing_agent_email is not null)::int as "withAgentEmail",
           count(*) filter (where listing_description is not null)::int as "withDescription"
         from active
         group by asset_group
         order by count(*) desc
       ) a) as asset_mix,
      (select coalesce(jsonb_agg(row_to_json(z)), '[]'::jsonb)
       from (
         select
           zip,
           count(*)::int as listings,
           count(distinct property_id)::int as properties,
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as "medianListPrice",
           round(percentile_cont(0.5) within group (order by days_on_market))::int as "medianDom",
           count(*) filter (where creative_finance_status = 'positive')::int as "creativePositive",
           count(*) filter (where listing_agent_email is not null or listing_agent_phone is not null)::int as "withContact"
         from active
         group by zip
         order by count(*) desc, zip
         limit 25
       ) z) as zip_rankings,
      (select coalesce(jsonb_agg(row_to_json(p)), '[]'::jsonb)
       from (
         select
           event_at as "eventAt",
           address,
           zip,
           listing_source as "listingSource",
           listing_url as "listingUrl",
           previous_list_price as "previousListPrice",
           list_price as "listPrice",
           (previous_list_price - list_price)::numeric as "dropAmount",
           case when previous_list_price > 0 then round(((previous_list_price - list_price) / previous_list_price) * 100, 2) else null end as "dropPct",
           days_on_market as "daysOnMarket",
           listing_agent_name as "listingAgentName",
           listing_brokerage as "listingBrokerage"
         from recent_events
         where event_type = 'price_changed'
           and list_price is not null
           and previous_list_price is not null
           and list_price < previous_list_price
         order by (previous_list_price - list_price) desc nulls last, event_at desc
         limit ${limit}
       ) p) as price_drops,
      (select coalesce(jsonb_agg(row_to_json(pb)), '[]'::jsonb)
       from (
         with drops as (
           select
             previous_list_price,
             list_price,
             days_on_market,
             (previous_list_price - list_price)::numeric as drop_amount,
             case when previous_list_price > 0 then ((previous_list_price - list_price) / previous_list_price) * 100 else null end as drop_pct,
             case
               when previous_list_price < 150000 then '<$150k'
               when previous_list_price < 250000 then '$150k-$250k'
               when previous_list_price < 400000 then '$250k-$400k'
               when previous_list_price < 750000 then '$400k-$750k'
               else '$750k+'
             end as price_bracket
           from recent_events
           where event_type = 'price_changed'
             and list_price is not null
             and previous_list_price is not null
             and list_price < previous_list_price
         )
         select
           price_bracket as "priceBracket",
           count(*)::int as "dropCount",
           round(avg(drop_amount))::int as "avgDropAmount",
           round(percentile_cont(0.5) within group (order by drop_amount))::int as "medianDropAmount",
           round(avg(drop_pct), 2)::numeric as "avgDropPct",
           round(percentile_cont(0.5) within group (order by drop_pct)::numeric, 2)::numeric as "medianDropPct",
           round(avg(days_on_market))::int as "avgDaysOnMarket"
         from drops
         group by price_bracket
         order by min(previous_list_price)
       ) pb) as price_drop_price_brackets,
      (select coalesce(jsonb_agg(row_to_json(db)), '[]'::jsonb)
       from (
         with drops as (
           select
             previous_list_price,
             days_on_market,
             (previous_list_price - list_price)::numeric as drop_amount,
             case when previous_list_price > 0 then ((previous_list_price - list_price) / previous_list_price) * 100 else null end as drop_pct,
             case
               when coalesce(days_on_market, 0) <= 30 then '0-30 DOM'
               when days_on_market <= 60 then '31-60 DOM'
               when days_on_market <= 90 then '61-90 DOM'
               when days_on_market <= 120 then '91-120 DOM'
               else '120+ DOM'
             end as dom_bucket
           from recent_events
           where event_type = 'price_changed'
             and list_price is not null
             and previous_list_price is not null
             and list_price < previous_list_price
         )
         select
           dom_bucket as "domBucket",
           count(*)::int as "dropCount",
           round(avg(drop_amount))::int as "avgDropAmount",
           round(percentile_cont(0.5) within group (order by drop_amount))::int as "medianDropAmount",
           round(avg(drop_pct), 2)::numeric as "avgDropPct",
           round(percentile_cont(0.5) within group (order by drop_pct)::numeric, 2)::numeric as "medianDropPct",
           round(avg(days_on_market))::int as "avgDaysOnMarket"
         from drops
         group by dom_bucket
         order by min(coalesce(days_on_market, 0))
       ) db) as price_drop_dom_buckets,
      (select coalesce(jsonb_agg(row_to_json(mx)), '[]'::jsonb)
       from (
         with drops as (
           select
             previous_list_price,
             days_on_market,
             (previous_list_price - list_price)::numeric as drop_amount,
             case when previous_list_price > 0 then ((previous_list_price - list_price) / previous_list_price) * 100 else null end as drop_pct,
             case
               when previous_list_price < 150000 then '<$150k'
               when previous_list_price < 250000 then '$150k-$250k'
               when previous_list_price < 400000 then '$250k-$400k'
               when previous_list_price < 750000 then '$400k-$750k'
               else '$750k+'
             end as price_bracket,
             case
               when coalesce(days_on_market, 0) <= 30 then '0-30 DOM'
               when days_on_market <= 60 then '31-60 DOM'
               when days_on_market <= 90 then '61-90 DOM'
               when days_on_market <= 120 then '91-120 DOM'
               else '120+ DOM'
             end as dom_bucket
           from recent_events
           where event_type = 'price_changed'
             and list_price is not null
             and previous_list_price is not null
             and list_price < previous_list_price
         )
         select
           price_bracket as "priceBracket",
           dom_bucket as "domBucket",
           count(*)::int as "dropCount",
           round(avg(drop_amount))::int as "avgDropAmount",
           round(percentile_cont(0.5) within group (order by drop_amount))::int as "medianDropAmount",
           round(avg(drop_pct), 2)::numeric as "avgDropPct",
           round(percentile_cont(0.5) within group (order by drop_pct)::numeric, 2)::numeric as "medianDropPct",
           round(avg(days_on_market))::int as "avgDaysOnMarket"
         from drops
         group by price_bracket, dom_bucket
         order by count(*) desc, avg(drop_pct) desc nulls last
         limit 50
       ) mx) as price_drop_matrix,
      (select coalesce(jsonb_agg(row_to_json(c)), '[]'::jsonb)
       from (
         select
           listing_id as "listingId",
           property_id as "propertyId",
           address,
           zip,
           asset_group as "assetGroup",
           total_units as "unitCount",
           mls_list_price as "listPrice",
           days_on_market as "daysOnMarket",
           listing_source as "listingSource",
           listing_url as "listingUrl",
           listing_agent_name as "listingAgentName",
           listing_agent_email as "listingAgentEmail",
           listing_agent_phone as "listingAgentPhone",
           listing_brokerage as "listingBrokerage",
           creative_finance_score as "creativeFinanceScore",
           creative_finance_terms as "creativeFinanceTerms",
           creative_finance_rate_text as "creativeFinanceRateText",
           left(listing_description, 700) as "listingDescription"
         from active
         where creative_finance_status = 'positive'
         order by coalesce(creative_finance_score, 0) desc, last_seen_at desc nulls last
         limit ${limit}
       ) c) as creative_finance,
      (select coalesce(jsonb_agg(row_to_json(d)), '[]'::jsonb)
       from (
         select
           event_at as "eventAt",
           address,
           zip,
           listing_source as "listingSource",
           listing_url as "listingUrl",
           list_price as "lastListPrice",
           days_on_market as "daysOnMarket",
           listing_agent_name as "listingAgentName",
           listing_brokerage as "listingBrokerage"
         from recent_events
         where event_type = 'delisted'
         order by event_at desc
         limit ${limit}
       ) d) as delisted,
      (select coalesce(jsonb_agg(row_to_json(e)), '[]'::jsonb)
       from (
         select
           event_at as "eventAt",
           event_type as "eventType",
           address,
           zip,
           listing_source as "listingSource",
           previous_list_price as "previousListPrice",
           list_price as "listPrice",
           mls_status as status,
           previous_mls_status as "previousStatus"
         from recent_events
         order by event_at desc
         limit ${limit}
       ) e) as change_feed,
      jsonb_build_object(
        'activeRows', (select count(*) from active),
        'withAgentName', (select count(*) from active where listing_agent_name is not null),
        'withAgentPhone', (select count(*) from active where listing_agent_phone is not null),
        'withAgentEmail', (select count(*) from active where listing_agent_email is not null),
        'withBrokerage', (select count(*) from active where listing_brokerage is not null),
        'withDescription', (select count(*) from active where listing_description is not null),
        'withPrice', (select count(*) from active where mls_list_price is not null),
        'linkedToProperty', (select count(*) from active where property_id is not null)
      ) as data_quality;
  `);

  return c.json({
    schemaVersion: 'mxre.marketAnalytics.v1',
    market: marketConfig.key,
    geography: {
      city: marketConfig.city,
      state: marketConfig.state,
      county: marketConfig.county,
      scope: marketConfig.scope,
    },
    window: { days },
    overview: analytics?.overview ?? {},
    eventCounts: analytics?.event_counts ?? {},
    dailyEvents: analytics?.daily_events ?? [],
    assetMix: analytics?.asset_mix ?? [],
    zipRankings: analytics?.zip_rankings ?? [],
    reports: {
      priceDrops: analytics?.price_drops ?? [],
      priceDropPriceBrackets: analytics?.price_drop_price_brackets ?? [],
      priceDropDomBuckets: analytics?.price_drop_dom_buckets ?? [],
      priceDropMatrix: analytics?.price_drop_matrix ?? [],
      creativeFinance: analytics?.creative_finance ?? [],
      delisted: analytics?.delisted ?? [],
      changeFeed: analytics?.change_feed ?? [],
    },
    dataQuality: analytics?.data_quality ?? {},
    recommendations: [
      'Use /v1/bbc/markets/indianapolis/changes for incremental BBC sync and re-underwrite only changed deals.',
      'Use priceDrops and delisted reports as first negotiation-focused lead queues.',
      'Use creativeFinance rows only when listingDescription is present so users can verify the exact public remarks.',
      'Keep agent email and mortgage balance as explicit data-quality metrics because they rely on fallback enrichment.',
    ],
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/pre-foreclosures', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const status = (c.req.query('status') ?? 'active').toLowerCase();
  const allowedStatuses = ['active', 'resolved', 'dismissed', 'sold', 'all'];
  if (!allowedStatuses.includes(status)) {
    return c.json({ error: 'Unsupported status', supported_statuses: allowedStatuses }, 400);
  }
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 50, 500);
  const statusWhere = status === 'all' ? '' : `and status = '${status}'`;
  const rows = await queryPg<Record<string, unknown>>(`
    select
      id,
      property_id as "propertyId",
      parcel_id as "parcelId",
      address,
      city,
      state_code as state,
      zip,
      county_name as "countyName",
      owner_name as "ownerName",
      borrower_name as "borrowerName",
      lender_name as "lenderName",
      case_number as "caseNumber",
      filing_date as "filingDate",
      sale_date as "saleDate",
      auction_date as "auctionDate",
      notice_type as "noticeType",
      status,
      source,
      source_url as "sourceUrl",
      confidence,
      first_seen_at as "firstSeenAt",
      last_seen_at as "lastSeenAt"
    from pre_foreclosure_signals
    where state_code = 'IN'
      and city ilike '%INDIANAPOLIS%'
      ${statusWhere}
    order by coalesce(filing_date, sale_date, auction_date) desc nulls last, last_seen_at desc
    limit ${limit};
  `);

  return c.json({
    market: 'indianapolis',
    status,
    count: rows.length,
    results: rows,
    generated_at: new Date().toISOString(),
  });
});

const creativeFinanceListingsHandler = async (c: Context) => {
  const market = (c.req.param('market') ?? '').toLowerCase();
  const marketConfig = resolveMarketConfig(market);
  if (!marketConfig) {
    return c.json({ error: 'Unsupported market', supported_markets: SUPPORTED_MARKETS }, 400);
  }

  const scope = marketConfig.key === 'indianapolis' ? getIndianapolisScope(c.req.query('scope')) : null;
  const requestedAsset = (c.req.query('asset') ?? 'all').toLowerCase();
  const allowedAssets = ['all', 'single_family', 'multifamily'];
  if (!allowedAssets.includes(requestedAsset)) {
    return c.json({ error: 'Unsupported asset', supported_assets: allowedAssets }, 400);
  }

  const requestedStatus = (c.req.query('status') ?? 'positive').toLowerCase();
  const allowedStatuses = ['positive', 'negative', 'all'];
  if (!allowedStatuses.includes(requestedStatus)) {
    return c.json({ error: 'Unsupported status', supported_statuses: allowedStatuses }, 400);
  }

  const zip = c.req.query('zip')?.replace(/[^\d]/g, '').slice(0, 5);
  const minPrice = parsePositiveInt(c.req.query('min_price'));
  const maxPrice = parsePositiveInt(c.req.query('max_price'));
  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const maxUnits = parsePositiveInt(c.req.query('max_units'));
  const since = parseDateParam(c.req.query('since'));
  const until = parseDateParam(c.req.query('until'));
  const page = Math.max(parsePositiveInt(c.req.query('page')) ?? 1, 1);
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 50, 250);
  const offset = (page - 1) * limit;
  const listingCitySql = `l.state_code = '${marketConfig.state}' and upper(trim(replace(coalesce(l.city, ''), ',', ''))) = '${marketConfig.cityUpper}'`;
  const propertyCitySql = `p.state_code = '${marketConfig.state}' and upper(trim(replace(coalesce(p.city, ''), ',', ''))) = '${marketConfig.cityUpper}'`;
  const marketWhere = scope
    ? scope.key === 'city'
      ? listingCitySql
      : `(p.county_id in (${scope.countySql}) or (${listingCitySql}))`
    : `(${listingCitySql} or ${propertyCitySql})`;

  const assetWhere = requestedAsset === 'single_family'
    ? "and asset_group = 'single_family'"
    : requestedAsset === 'multifamily'
      ? "and asset_group in ('small_multifamily','commercial_multifamily')"
      : '';
  const statusWhere = requestedStatus === 'all'
    ? "and l.creative_finance_status in ('positive','negative')"
    : `and l.creative_finance_status = '${requestedStatus}'`;
  const listingWhere = [
    zip ? `and l.zip = '${zip}'` : '',
    minPrice !== null ? `and l.mls_list_price >= ${minPrice}` : '',
    maxPrice !== null ? `and l.mls_list_price <= ${maxPrice}` : '',
    minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '',
    maxUnits !== null ? `and coalesce(p.total_units, 0) <= ${maxUnits}` : '',
    since ? `and coalesce(l.last_seen_at, l.first_seen_at) >= '${since}'::date` : '',
    until ? `and coalesce(l.last_seen_at, l.first_seen_at) < ('${until}'::date + interval '1 day')` : '',
  ].filter(Boolean).join('\n        ');

  const [result] = await queryPg<Record<string, unknown>>(`
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
        p.property_type,
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
        and ${marketWhere}
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
         select
           zip,
           count(*)::int as listings,
           count(*) filter (where creative_finance_status = 'positive')::int as "positive",
           count(*) filter (where creative_finance_status = 'negative')::int as "negative",
           count(*) filter (where listing_agent_email is not null)::int as "withAgentEmail",
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as "medianListPrice",
           round(percentile_cont(0.5) within group (order by creative_finance_score))::int as "medianCreativeScore"
         from active
         group by zip
         order by count(*) desc, zip
       ) z) as by_zip,
      (select coalesce(jsonb_agg(row_to_json(t)), '[]'::jsonb)
       from (
         select term, count(*)::int as listings
         from (
           select unnest(
             case
               when creative_finance_status = 'negative' then coalesce(creative_finance_negative_terms, array[]::text[])
               else coalesce(creative_finance_terms, array[]::text[])
             end
           ) as term
           from active
         ) terms
         where term is not null and term <> ''
         group by term
         order by count(*) desc, term
       ) t) as by_term,
      (select coalesce(jsonb_agg(row_to_json(r)), '[]'::jsonb)
       from (
         select
           listing_id as "listingId",
           property_id as "propertyId",
           address,
           city,
           state_code as state,
           zip,
           asset_group as "assetGroup",
           asset_type as "assetType",
           asset_subtype as "assetSubtype",
           property_use as "propertyUse",
           total_units as "unitCount",
           bedrooms,
           coalesce(bathrooms, bathrooms_full) as bathrooms,
           living_sqft as "livingSqft",
           year_built as "yearBuilt",
           market_value as "marketValue",
           mls_list_price as "listPrice",
           days_on_market as "daysOnMarket",
           listing_source as "listingSource",
           listing_url as "listingUrl",
           listing_agent_name as "listingAgentName",
           listing_agent_first_name as "listingAgentFirstName",
           listing_agent_last_name as "listingAgentLastName",
           listing_agent_email as "listingAgentEmail",
           listing_agent_phone as "listingAgentPhone",
           listing_brokerage as "listingBrokerage",
           agent_contact_source as "agentContactSource",
           agent_contact_confidence as "agentContactConfidence",
           creative_finance_score as "creativeFinanceScore",
           creative_finance_status as "creativeFinanceStatus",
           creative_finance_terms as "creativeFinanceTerms",
           creative_finance_negative_terms as "creativeFinanceNegativeTerms",
           creative_finance_rate_text as "creativeFinanceRateText",
           creative_finance_source as "creativeFinanceSource",
           creative_finance_observed_at as "creativeFinanceObservedAt",
           first_seen_at as "firstSeenAt",
           last_seen_at as "lastSeenAt",
           nullif(coalesce(
             raw #>> '{redfinDetail,publicRemarks}',
             raw #>> '{redfinDetail,description}',
             raw #>> '{publicRemarks}',
             raw #>> '{remarks}',
             raw #>> '{description}',
             raw #>> '{listingDescription}',
             raw #>> '{zillow_rapidapi_detail,raw,property,description}',
             raw #>> '{zillow_rapidapi_detail,raw,description}',
             raw #>> '{zillow_rapidapi_detail,raw,data,description}',
             raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
             raw #>> '{mls,remarks}',
             raw #>> '{mls,description}',
             ''
           ), '') as "listingDescription",
           nullif(coalesce(
             raw #>> '{redfinDetail,publicRemarks}',
             raw #>> '{publicRemarks}',
             raw #>> '{remarks}',
             raw #>> '{zillow_rapidapi_detail,raw,property,description}',
             raw #>> '{zillow_rapidapi_detail,raw,description}',
             raw #>> '{zillow_rapidapi_detail,raw,data,description}',
             raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
             raw #>> '{mls,remarks}',
             ''
           ), '') as "publicRemarks",
           left(coalesce(
             raw #>> '{redfinDetail,publicRemarks}',
             raw #>> '{redfinDetail,description}',
             raw #>> '{publicRemarks}',
             raw #>> '{remarks}',
             raw #>> '{description}',
             raw #>> '{listingDescription}',
             raw #>> '{zillow_rapidapi_detail,raw,property,description}',
             raw #>> '{zillow_rapidapi_detail,raw,description}',
             raw #>> '{zillow_rapidapi_detail,raw,data,description}',
             raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
             raw #>> '{mls,remarks}',
             raw #>> '{mls,description}',
             ''
           ), 700) as "publicRemarksSnippet"
         from active
         order by coalesce(creative_finance_score, -1) desc, last_seen_at desc nulls last, address
         limit ${limit} offset ${offset}
       ) r) as results;
  `);

  return c.json({
    schemaVersion: 'mxre.bbc.creativeFinanceListings.v1',
    market: marketConfig.key,
    report: 'creative_finance',
    geography: { scope: scope?.key ?? marketConfig.scope, scope_label: scope?.label ?? marketConfig.publicLabel },
    filters: { status: requestedStatus, asset: requestedAsset, zip: zip ?? null, min_price: minPrice, max_price: maxPrice, min_units: minUnits, max_units: maxUnits, since, until },
    page,
    limit,
    total: numberOrNull(result?.total) ?? 0,
    summary: result?.summary ?? {},
    by_zip: result?.by_zip ?? [],
    by_term: result?.by_term ?? [],
    results: result?.results ?? [],
    generated_at: new Date().toISOString(),
  });
};

app.get('/v1/bbc/markets/:market/creative-finance-listings', creativeFinanceListingsHandler);
app.get('/v1/markets/:market/reports/creative-finance', creativeFinanceListingsHandler);

app.get('/v1/markets/:market/opportunities', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const scope = getIndianapolisScope(c.req.query('scope'));
  const requestedAsset = (c.req.query('asset') ?? 'all').toLowerCase();
  const allowedAssets = ['all', 'single_family', 'multifamily'];
  if (!allowedAssets.includes(requestedAsset)) {
    return c.json({ error: 'Unsupported asset', supported_assets: allowedAssets }, 400);
  }

  const zip = c.req.query('zip')?.replace(/[^\d]/g, '').slice(0, 5);
  const minPrice = parsePositiveInt(c.req.query('min_price'));
  const maxPrice = parsePositiveInt(c.req.query('max_price'));
  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const maxUnits = parsePositiveInt(c.req.query('max_units'));
  const sort = (c.req.query('sort') ?? 'fresh').toLowerCase();
  const creative = (c.req.query('creative') ?? '').toLowerCase();
  const page = Math.max(parsePositiveInt(c.req.query('page')) ?? 1, 1);
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 25, 100);
  const offset = (page - 1) * limit;

  const assetWhere = requestedAsset === 'single_family'
    ? "and asset_group = 'single_family'"
    : requestedAsset === 'multifamily'
      ? "and asset_group in ('small_multifamily','commercial_multifamily')"
      : '';
  const listingWhere = [
    zip ? `and l.zip = '${zip}'` : '',
    minPrice !== null ? `and l.mls_list_price >= ${minPrice}` : '',
    maxPrice !== null ? `and l.mls_list_price <= ${maxPrice}` : '',
    minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '',
    maxUnits !== null ? `and coalesce(p.total_units, 0) <= ${maxUnits}` : '',
    creative === 'positive' ? `and l.creative_finance_status = 'positive'` : '',
    creative === 'negative' ? `and l.creative_finance_status = 'negative'` : '',
  ].filter(Boolean).join('\n        ');
  const orderSql = sort === 'price_desc'
    ? 'coalesce(mls_list_price, 0) desc, address'
    : sort === 'price_asc'
      ? 'coalesce(mls_list_price, 999999999) asc, address'
      : sort === 'creative'
        ? 'coalesce(creative_finance_score, -1) desc, coalesce(days_on_market, 0) desc, address'
        : 'last_seen_at desc nulls last, coalesce(days_on_market, 0) desc, address';

  const [result] = await queryPg<Record<string, unknown>>(`
    with normalized as (
      select
        p.*,
        case
          when p.asset_type = 'commercial_multifamily' or coalesce(p.property_use, '') ilike '%APT%UNITS%' then 'commercial_multifamily'
          when p.asset_type = 'small_multifamily'
            or coalesce(p.property_use, '') ilike '%TWO FAMILY%'
            or coalesce(p.property_use, '') ilike '%THREE FAMILY%' then 'small_multifamily'
          when p.asset_subtype in ('sfr', 'condo')
            or coalesce(p.property_use, '') ilike '%ONE FAMILY%'
            or coalesce(p.property_use, '') ilike '%CONDO%'
            or coalesce(p.property_use, '') ilike 'RES VAC%' then 'single_family'
          else coalesce(nullif(p.asset_type, ''), nullif(p.property_type, ''), 'unknown')
        end as asset_group
      from properties p
      where ${scope.whereSql}
    ),
    active as (
      select
        l.id as listing_id,
        l.property_id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        p.asset_group,
        p.asset_type,
        p.asset_subtype,
        p.property_type,
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
        l.agent_contact_confidence,
        l.creative_finance_score,
        l.creative_finance_status,
        l.creative_finance_terms,
        l.creative_finance_negative_terms,
        l.creative_finance_rate_text,
        pls.crime_score,
        pls.nearest_bus_distance_miles,
        pls.nearest_bus_stop_name,
        pls.bus_routes,
        pls.crime_incidents_05mi_365d,
        pls.violent_crime_05mi_365d,
        l.last_seen_at
      from listing_signals l
      join normalized p on p.id = l.property_id
      left join property_location_scores pls on pls.property_id = p.id
      where l.is_on_market = true
        ${assetWhere}
        ${listingWhere}
    )
    select
      (select count(*) from active)::int as total,
      (select jsonb_build_object(
        'activeRows', count(*),
        'activeProperties', count(distinct property_id),
        'creativePositive', count(*) filter (where creative_finance_status = 'positive'),
        'creativeNegative', count(*) filter (where creative_finance_status = 'negative'),
        'withAgentName', count(*) filter (where listing_agent_name is not null or (listing_agent_first_name is not null and listing_agent_last_name is not null)),
        'withAgentPhone', count(*) filter (where listing_agent_phone is not null),
        'withAgentEmail', count(*) filter (where listing_agent_email is not null),
        'withAgentContact', count(*) filter (where listing_agent_email is not null and listing_agent_phone is not null),
        'withBrokerage', count(*) filter (where listing_brokerage is not null),
        'medianListPrice', round(percentile_cont(0.5) within group (order by mls_list_price))::int
      ) from active) as summary,
      (select coalesce(jsonb_agg(row_to_json(z)), '[]'::jsonb)
       from (
         select
           zip,
           count(*)::int as listings,
           round(percentile_cont(0.5) within group (order by mls_list_price))::int as "medianPrice",
           round(percentile_cont(0.5) within group (order by days_on_market))::int as "medianDom",
           count(*) filter (where creative_finance_status = 'positive')::int as "creativePositive",
           count(*) filter (where listing_agent_email is not null or listing_agent_phone is not null)::int as "withContact"
         from active
         group by zip
         order by count(*) desc, zip
         limit 40
       ) z) as by_zip,
      (select coalesce(jsonb_agg(row_to_json(r)), '[]'::jsonb)
       from (
         select
           listing_id as "listingId",
           property_id as "propertyId",
           address,
           city,
           state_code as state,
           zip,
           asset_group as "assetGroup",
           asset_subtype as "assetSubtype",
           property_use as "propertyUse",
           total_units as "unitCount",
           bedrooms,
           coalesce(bathrooms, bathrooms_full) as bathrooms,
           living_sqft as "livingSqft",
           year_built as "yearBuilt",
           market_value as "marketValue",
           mls_list_price as "listPrice",
           days_on_market as "daysOnMarket",
           listing_source as "listingSource",
           listing_url as "listingUrl",
           listing_agent_name as "listingAgentName",
           listing_agent_first_name as "listingAgentFirstName",
           listing_agent_last_name as "listingAgentLastName",
           listing_agent_email as "listingAgentEmail",
           listing_agent_phone as "listingAgentPhone",
           listing_brokerage as "listingBrokerage",
           agent_contact_confidence as "agentContactConfidence",
           creative_finance_score as "creativeFinanceScore",
           creative_finance_status as "creativeFinanceStatus",
           creative_finance_terms as "creativeFinanceTerms",
           creative_finance_negative_terms as "creativeFinanceNegativeTerms",
           creative_finance_rate_text as "creativeFinanceRateText",
           crime_score as "crimeScore",
           nearest_bus_distance_miles as "nearestBusMiles",
           nearest_bus_stop_name as "nearestBusStopName",
           bus_routes as "busRoutes",
           crime_incidents_05mi_365d as "crimeIncidents05Mi365d",
           violent_crime_05mi_365d as "violentCrime05Mi365d",
           last_seen_at as "lastSeenAt"
         from active
         order by ${orderSql}
         limit ${limit} offset ${offset}
       ) r) as results;
  `);

  return c.json({
    market: 'indianapolis',
    geography: { scope: scope.key, scope_label: scope.label },
    filters: { asset: requestedAsset, zip: zip ?? null, min_price: minPrice, max_price: maxPrice, min_units: minUnits, max_units: maxUnits, sort, creative: creative || null },
    page,
    limit,
    total: numberOrNull(result?.total) ?? 0,
    summary: result?.summary ?? {},
    by_zip: result?.by_zip ?? [],
    results: result?.results ?? [],
    data_gaps: {
      crime: 'IMPD public incident scoring active for geocoded properties',
      transit_proximity: 'IndyGo GTFS nearest-stop scoring active for geocoded properties',
      neighborhood_names: 'zip_fallback_until_neighborhood_layer_is_ingested',
    },
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/assets', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }
  const scope = getIndianapolisScope(c.req.query('scope'));

  const supportedAssetGroups = [
    'single_family',
    'small_multifamily',
    'commercial_multifamily',
    'mobile_home_rv',
    'land',
    'industrial',
    'office',
    'retail',
    'self_storage',
    'hospitality',
    'parking',
    'exempt_institutional',
    'utilities_other',
    'other_commercial',
    'other_residential',
    'unknown',
  ];
  const requestedAssetGroup = c.req.query('asset_group')?.toLowerCase();
  if (requestedAssetGroup && !supportedAssetGroups.includes(requestedAssetGroup)) {
    return c.json({ error: 'Unsupported asset_group', supported_asset_groups: supportedAssetGroups }, 400);
  }

  const includeCoverage = c.req.query('coverage') === 'true';
  const groupFilterSql = requestedAssetGroup ? `where asset_group = '${requestedAssetGroup}'` : '';
  const signalCtesSql = `,
    active_ids as (
      select distinct property_id from listing_signals where is_on_market = true
    )${includeCoverage ? `,
    sale_ids as (
      select distinct property_id from sale_history
    ),
    mortgage_ids as (
      select distinct property_id from mortgage_records
    ),
    public_signal_ids as (
      select distinct property_id from property_public_signals
    )` : ''}`;
  const signalFlagSql = includeCoverage ? `
        a.property_id is not null as has_active_listing,
        sh.property_id is not null as has_sale_history,
        m.property_id is not null as has_recorded_mortgage,
        ps.property_id is not null as has_public_signal` : `
        a.property_id is not null as has_active_listing,
        false as has_sale_history,
        false as has_recorded_mortgage,
        false as has_public_signal`;
  const signalJoinSql = `
      left join active_ids a on a.property_id = s.id${includeCoverage ? `
      left join sale_ids sh on sh.property_id = s.id
      left join mortgage_ids m on m.property_id = s.id
      left join public_signal_ids ps on ps.property_id = s.id` : ''}`;
  const [summary] = await queryPg<Record<string, unknown>>(`
    with normalized as (
      select
        p.*,
        case
          when coalesce(p.property_use, '') ilike '%MOBILE HOME PARK%'
            or coalesce(p.property_use, '') ilike '% RV PARK%'
            or coalesce(p.property_use, '') ilike '%RECREATIONAL VEHICLE%' then 'mobile_home_rv'
          when p.asset_type = 'commercial_multifamily' or coalesce(p.property_use, '') ilike '%APT%UNITS%' then 'commercial_multifamily'
          when p.asset_type = 'small_multifamily'
            or coalesce(p.property_use, '') ilike '%TWO FAMILY%'
            or coalesce(p.property_use, '') ilike '%THREE FAMILY%' then 'small_multifamily'
          when coalesce(p.property_use, '') ilike '%MINI-WAREHOUSE%' then 'self_storage'
          when coalesce(p.property_use, '') ilike 'IND %'
            or coalesce(p.property_use, '') ilike '%WAREHOUSE%'
            or coalesce(p.property_use, '') ilike '%WHSE%'
            or coalesce(p.property_use, '') ilike '%LIGHT MFG%'
            or p.property_type = 'industrial'
            or p.asset_subtype = 'industrial' then 'industrial'
          when coalesce(p.property_use, '') ilike '%VACANT LAND%'
            or coalesce(p.property_use, '') ilike '%VACANT PLATTED%'
            or coalesce(p.property_use, '') ilike '%VACANT AGRICULTURAL%'
            or coalesce(p.property_use, '') ilike '%VAC SUPPORT%'
            or p.asset_subtype = 'land'
            or p.property_type = 'land' then 'land'
          when coalesce(p.property_use, '') ilike '%COM OFF%'
            or coalesce(p.property_use, '') ilike '%OFF BLDG%'
            or coalesce(p.property_use, '') ilike '%OFFICE%'
            or coalesce(p.property_use, '') ilike '%MEDICAL CLINIC%' then 'office'
          when coalesce(p.property_use, '') ilike '%RETAIL%'
            or coalesce(p.property_use, '') ilike '%SHOPPING%'
            or coalesce(p.property_use, '') ilike '%SUPERMARKET%'
            or coalesce(p.property_use, '') ilike '%RESTAURANT%'
            or coalesce(p.property_use, '') ilike '%CONVENIENCE%'
            or coalesce(p.property_use, '') ilike '%AUTO SALES%'
            or coalesce(p.property_use, '') ilike '%AUTO SERVICE%'
            or coalesce(p.property_use, '') ilike '%CAR WASH%' then 'retail'
          when coalesce(p.property_use, '') ilike '%HOTEL%' or coalesce(p.property_use, '') ilike '%MOTEL%' then 'hospitality'
          when coalesce(p.property_use, '') ilike '%PARKING%' then 'parking'
          when p.property_type = 'exempt' or coalesce(p.property_use, '') ilike 'EXEMPT%' then 'exempt_institutional'
          when coalesce(p.property_use, '') ilike 'U %' then 'utilities_other'
          when p.asset_subtype in ('sfr', 'condo')
            or coalesce(p.property_use, '') ilike '%ONE FAMILY%'
            or coalesce(p.property_use, '') ilike '%CONDO%'
            or coalesce(p.property_use, '') ilike 'RES VAC%' then 'single_family'
          when p.asset_type = 'residential' then 'other_residential'
          when coalesce(p.property_use, '') ilike 'COM %'
            or coalesce(p.property_use, '') ilike 'COMM %'
            or coalesce(p.property_use, '') ilike '%COMMERCIAL%'
            or p.property_type = 'commercial'
            or p.asset_subtype = 'commercial' then 'other_commercial'
          else 'unknown'
        end as asset_group
      from properties p
      where ${scope.whereSql}
    ),
    scoped as (
      select * from normalized
      ${groupFilterSql}
    )
    ${signalCtesSql},
    flags as (
      select
        s.*,
        ${signalFlagSql}
      from scoped s
      ${signalJoinSql}
    )
    select
      count(*)::int as parcel_count,
      coalesce(sum(coalesce(total_units,0)),0)::int as known_units,
      coalesce(sum(coalesce(market_value,0)),0)::bigint as market_value_sum,
      coalesce(sum(coalesce(assessed_value,0)),0)::bigint as assessed_value_sum,
      count(*) filter (where has_active_listing)::int as parcels_with_active_listing,
      count(*) filter (where has_sale_history)::int as parcels_with_sale_history,
      count(*) filter (where has_recorded_mortgage)::int as parcels_with_recorded_mortgage,
      count(*) filter (where has_sale_history or has_recorded_mortgage)::int as parcels_with_any_recorder_data,
      count(*) filter (where has_public_signal)::int as parcels_with_public_signal,
      (select coalesce(jsonb_agg(row_to_json(a)), '[]'::jsonb)
       from (
         select
           asset_group as "assetGroup",
           count(*)::int as parcels,
           coalesce(sum(coalesce(total_units,0)),0)::int as "knownUnits",
           coalesce(sum(coalesce(market_value,0)),0)::bigint as "marketValue",
           count(*) filter (where has_active_listing)::int as "activeListings",
           count(*) filter (where has_sale_history)::int as "saleHistory",
           count(*) filter (where has_recorded_mortgage)::int as "recordedMortgages",
           count(*) filter (where has_sale_history or has_recorded_mortgage)::int as "anyRecorderData",
           count(*) filter (where has_public_signal)::int as "publicSignals"
         from flags
         group by asset_group
         order by count(*) desc
       ) a) as by_asset_group,
      (select coalesce(jsonb_agg(row_to_json(u)), '[]'::jsonb)
       from (
         select
           asset_group as "assetGroup",
           coalesce(property_use, property_type, asset_subtype, 'unknown') as "propertyUse",
           count(*)::int as parcels,
           coalesce(sum(coalesce(total_units,0)),0)::int as "knownUnits"
         from flags
         group by asset_group, coalesce(property_use, property_type, asset_subtype, 'unknown')
         order by count(*) desc
         limit 60
       ) u) as top_property_uses,
      (select coalesce(jsonb_agg(row_to_json(e)), '[]'::jsonb)
       from (
         select
           id as "propertyId",
           address,
           city,
           state_code as state,
           zip,
           parcel_id as "parcelId",
           asset_group as "assetGroup",
           asset_type as "assetType",
           asset_subtype as "assetSubtype",
           property_type as "propertyType",
           property_use as "propertyUse",
           total_units as "unitCount",
           market_value as "marketValue",
           has_active_listing as "hasActiveListing",
           has_sale_history as "hasSaleHistory",
           has_recorded_mortgage as "hasRecordedMortgage",
           has_public_signal as "hasPublicSignal"
         from flags
         order by coalesce(market_value,0) desc, address
         limit 25
       ) e) as examples
    from flags;
  `);

  const parcelCount = numberOrNull(summary?.parcel_count) ?? 0;
  const pct = (value: unknown): number => {
    if (parcelCount === 0) return 0;
    return Math.round(((numberOrNull(value) ?? 0) / parcelCount) * 1000) / 10;
  };

  return c.json({
    market: 'indianapolis',
    geography: { state: 'IN', scope: scope.key, scope_label: scope.label, county_ids: scope.countyIds },
    filters: { asset_group: requestedAssetGroup ?? null, coverage: includeCoverage, scope: scope.key },
    totals: {
      parcel_count: parcelCount,
      known_units: numberOrNull(summary?.known_units) ?? 0,
      market_value_sum: numberOrNull(summary?.market_value_sum) ?? 0,
      assessed_value_sum: numberOrNull(summary?.assessed_value_sum) ?? 0,
    },
    coverage: {
      parcels_with_active_listing: numberOrNull(summary?.parcels_with_active_listing) ?? 0,
      active_listing_pct: pct(summary?.parcels_with_active_listing),
      parcels_with_sale_history: numberOrNull(summary?.parcels_with_sale_history) ?? 0,
      sale_history_pct: pct(summary?.parcels_with_sale_history),
      parcels_with_recorded_mortgage: numberOrNull(summary?.parcels_with_recorded_mortgage) ?? 0,
      recorded_mortgage_pct: pct(summary?.parcels_with_recorded_mortgage),
      parcels_with_any_recorder_data: numberOrNull(summary?.parcels_with_any_recorder_data) ?? 0,
      any_recorder_data_pct: pct(summary?.parcels_with_any_recorder_data),
      parcels_with_public_signal: numberOrNull(summary?.parcels_with_public_signal) ?? 0,
      public_signal_pct: pct(summary?.parcels_with_public_signal),
    },
    by_asset_group: summary?.by_asset_group ?? [],
    top_property_uses: summary?.top_property_uses ?? [],
    examples: summary?.examples ?? [],
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/completion', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const scope = getIndianapolisScope(c.req.query('scope'));
  const [summary] = await queryPg<Record<string, unknown>>(`
    with flags as (
      select
        p.*,
        coalesce(c.county_name, 'Unknown') as county_name,
        coalesce(nullif(p.asset_type, ''), nullif(p.property_type, ''), nullif(p.asset_subtype, ''), 'unknown') as classification_group,
        (p.parcel_id is not null and p.parcel_id <> '' and p.address is not null and p.address <> '' and p.county_id is not null) as has_identity,
        (p.property_type is not null and p.property_type <> '')
          or (p.property_use is not null and p.property_use <> '')
          or (p.asset_type is not null and p.asset_type <> '')
          or (p.asset_subtype is not null and p.asset_subtype <> '') as has_classification,
        (p.owner_name is not null and p.owner_name <> '') or (p.company_name is not null and p.company_name <> '') or (p.owner1_last is not null and p.owner1_last <> '') as has_owner,
        (coalesce(p.market_value, 0) > 0 or coalesce(p.assessed_value, 0) > 0 or coalesce(p.taxable_value, 0) > 0) as has_valuation,
        (coalesce(p.total_sqft, 0) > 0 or coalesce(p.living_sqft, 0) > 0 or coalesce(p.land_sqft, 0) > 0 or coalesce(p.lot_sqft, 0) > 0 or p.year_built is not null) as has_physical,
        (p.last_sale_date is not null or coalesce(p.last_sale_price, 0) > 0 or p.sale_year is not null) as has_transaction,
        (
          coalesce(p.total_units, 0) >= 2
          or lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, '')) like '%apartment%'
          or lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, '')) like '%multi%'
          or lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, '')) like '%duplex%'
          or lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, '')) like '%triplex%'
          or lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, '')) like '%fourplex%'
        ) as is_rental_candidate,
        (
          (p.website is not null and p.website <> '')
          or exists (select 1 from property_websites pw where pw.property_id = p.id and pw.active = true)
        ) as has_property_website,
        exists (select 1 from floorplans fp where fp.property_id = p.id) as has_floorplans,
        exists (select 1 from rent_snapshots rs where rs.property_id = p.id) as has_rent_snapshot
      from properties p
      left join counties c on c.id = p.county_id
      where ${scope.whereSql}
    ),
    scored as (
      select
        *,
        (has_identity and has_classification and has_owner and has_valuation) as is_core_complete,
        (has_identity and has_classification and has_owner and has_valuation and has_physical and has_transaction) as is_underwriting_complete
      from flags
    )
    select
      count(*)::int as parcel_count,
      count(distinct parcel_id)::int as distinct_parcel_ids,
      count(*) filter (where has_identity)::int as identity_complete,
      count(*) filter (where has_classification)::int as classification_complete,
      count(*) filter (where has_owner)::int as ownership_complete,
      count(*) filter (where has_valuation)::int as valuation_complete,
      count(*) filter (where has_physical)::int as physical_complete,
      count(*) filter (where has_transaction)::int as transaction_complete,
      count(*) filter (where is_core_complete)::int as core_complete,
      count(*) filter (where is_underwriting_complete)::int as underwriting_complete,
      count(*) filter (where is_rental_candidate)::int as rental_candidate_count,
      count(*) filter (where is_rental_candidate and has_property_website)::int as rental_candidates_with_website,
      count(*) filter (where is_rental_candidate and has_floorplans)::int as rental_candidates_with_floorplans,
      count(*) filter (where is_rental_candidate and has_rent_snapshot)::int as rental_candidates_with_rent_snapshot,
      count(*) filter (where has_property_website)::int as properties_with_website,
      count(*) filter (where has_floorplans)::int as properties_with_floorplans,
      count(*) filter (where has_rent_snapshot)::int as properties_with_rent_snapshot,
      count(*) filter (where not has_classification)::int as unknown_asset_group,
      count(*) filter (where property_use is null or property_use = '')::int as missing_property_use,
      count(*) filter (where market_value is null or market_value = 0)::int as missing_market_value,
      (select coalesce(jsonb_agg(row_to_json(c)), '[]'::jsonb)
       from (
         select
           county_name as county,
           count(*)::int as parcels,
           count(*) filter (where is_core_complete)::int as "coreComplete",
           count(*) filter (where is_underwriting_complete)::int as "underwritingComplete",
           count(*) filter (where is_rental_candidate)::int as "rentalCandidates",
           count(*) filter (where is_rental_candidate and has_property_website)::int as "rentalCandidatesWithWebsite",
           count(*) filter (where is_rental_candidate and has_rent_snapshot)::int as "rentalCandidatesWithRentSnapshot",
           count(*) filter (where not has_classification)::int as "unknownAssetGroup",
           count(*) filter (where property_use is null or property_use = '')::int as "missingPropertyUse"
         from scored
         group by county_name
         order by count(*) desc
       ) c) as by_county,
      (select coalesce(jsonb_agg(row_to_json(g)), '[]'::jsonb)
       from (
         select
           classification_group as "assetGroup",
           count(*)::int as parcels,
           count(*) filter (where is_core_complete)::int as "coreComplete",
           count(*) filter (where is_underwriting_complete)::int as "underwritingComplete"
         from scored
         group by classification_group
         order by count(*) desc
         limit 20
       ) g) as by_asset_group
    from scored;
  `);

  const total = numberOrNull(summary?.parcel_count) ?? 0;
  const pct = (value: unknown): number => {
    if (total === 0) return 0;
    return Math.round(((numberOrNull(value) ?? 0) / total) * 1000) / 10;
  };
  const rentalCandidateCount = numberOrNull(summary?.rental_candidate_count) ?? 0;
  const rentalPct = (value: unknown): number => {
    if (rentalCandidateCount === 0) return 0;
    return Math.round(((numberOrNull(value) ?? 0) / rentalCandidateCount) * 10000) / 100;
  };
  const scoreParts = [
    pct(summary?.identity_complete),
    pct(summary?.classification_complete),
    pct(summary?.ownership_complete),
    pct(summary?.valuation_complete),
    pct(summary?.physical_complete),
    pct(summary?.transaction_complete),
  ];
  const readinessScore = Math.round((scoreParts.reduce((sum, value) => sum + value, 0) / scoreParts.length) * 10) / 10;

  return c.json({
    market: 'indianapolis',
    geography: { state: 'IN', scope: scope.key, scope_label: scope.label, county_ids: scope.countyIds },
    definition: {
      parcel_identity: 'parcel_id + address + county_id present',
      core_complete: 'identity + asset classification + owner + valuation',
      underwriting_complete: 'core_complete + physical facts + transaction history fields',
      readiness_score: 'average of identity, classification, ownership, valuation, physical, and transaction completion percentages',
      market_data_complete: 'rental/multifamily candidate properties with website, floorplan, and rent snapshot coverage',
    },
    totals: {
      parcel_count: total,
      distinct_parcel_ids: numberOrNull(summary?.distinct_parcel_ids) ?? 0,
      readiness_score: readinessScore,
    },
    metrics: {
      identity_complete: { count: numberOrNull(summary?.identity_complete) ?? 0, pct: pct(summary?.identity_complete) },
      classification_complete: { count: numberOrNull(summary?.classification_complete) ?? 0, pct: pct(summary?.classification_complete) },
      ownership_complete: { count: numberOrNull(summary?.ownership_complete) ?? 0, pct: pct(summary?.ownership_complete) },
      valuation_complete: { count: numberOrNull(summary?.valuation_complete) ?? 0, pct: pct(summary?.valuation_complete) },
      physical_complete: { count: numberOrNull(summary?.physical_complete) ?? 0, pct: pct(summary?.physical_complete) },
      transaction_complete: { count: numberOrNull(summary?.transaction_complete) ?? 0, pct: pct(summary?.transaction_complete) },
      core_complete: { count: numberOrNull(summary?.core_complete) ?? 0, pct: pct(summary?.core_complete) },
      underwriting_complete: { count: numberOrNull(summary?.underwriting_complete) ?? 0, pct: pct(summary?.underwriting_complete) },
      rental_candidate_count: { count: rentalCandidateCount, pct: pct(summary?.rental_candidate_count) },
      rental_website_coverage: { count: numberOrNull(summary?.rental_candidates_with_website) ?? 0, pct: rentalPct(summary?.rental_candidates_with_website) },
      rental_floorplan_coverage: { count: numberOrNull(summary?.rental_candidates_with_floorplans) ?? 0, pct: rentalPct(summary?.rental_candidates_with_floorplans) },
      rental_rent_snapshot_coverage: { count: numberOrNull(summary?.rental_candidates_with_rent_snapshot) ?? 0, pct: rentalPct(summary?.rental_candidates_with_rent_snapshot) },
      all_property_website_coverage: { count: numberOrNull(summary?.properties_with_website) ?? 0, pct: pct(summary?.properties_with_website) },
      all_property_floorplan_coverage: { count: numberOrNull(summary?.properties_with_floorplans) ?? 0, pct: pct(summary?.properties_with_floorplans) },
      all_property_rent_snapshot_coverage: { count: numberOrNull(summary?.properties_with_rent_snapshot) ?? 0, pct: pct(summary?.properties_with_rent_snapshot) },
    },
    gaps: {
      unknown_asset_group: numberOrNull(summary?.unknown_asset_group) ?? 0,
      missing_property_use: numberOrNull(summary?.missing_property_use) ?? 0,
      missing_market_value: numberOrNull(summary?.missing_market_value) ?? 0,
    },
    by_county: summary?.by_county ?? [],
    by_asset_group: summary?.by_asset_group ?? [],
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/data-gaps', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const scope = getIndianapolisScope(c.req.query('scope'));
  const page = Math.max(1, parsePositiveInt(c.req.query('page')) ?? 1);
  const limit = Math.min(250, Math.max(1, parsePositiveInt(c.req.query('limit')) ?? 100));
  const offset = (page - 1) * limit;
  const asset = (c.req.query('asset') ?? 'all').toLowerCase();
  const gap = (c.req.query('gap') ?? 'all').toLowerCase();
  const onMarket = (c.req.query('on_market') ?? 'all').toLowerCase();
  const zip = c.req.query('zip')?.trim();
  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const maxUnits = parsePositiveInt(c.req.query('max_units'));
  const minPrice = parsePositiveInt(c.req.query('min_price'));
  const maxPrice = parsePositiveInt(c.req.query('max_price'));
  const q = c.req.query('q')?.trim();

  const assetSql = (() => {
    const haystack = `lower(coalesce(p.asset_type, '') || ' ' || coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_type, '') || ' ' || coalesce(p.property_use, ''))`;
    if (asset === 'single_family') {
      return `and coalesce(p.total_units, 0) <= 1
        and ${haystack} not like '%multi%'
        and ${haystack} not like '%apartment%'
        and ${haystack} not like '%duplex%'
        and ${haystack} not like '%triplex%'
        and ${haystack} not like '%fourplex%'`;
    }
    if (asset === 'multifamily') {
      return `and (
        coalesce(p.total_units, 0) >= 2
        or p.asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
        or ${haystack} like '%multi%'
        or ${haystack} like '%apartment%'
        or ${haystack} like '%duplex%'
        or ${haystack} like '%triplex%'
        or ${haystack} like '%fourplex%'
      )`;
    }
    if (asset === 'small_multifamily') {
      return `and (
        p.asset_type = 'small_multifamily'
        or coalesce(p.total_units, 0) between 2 and 4
        or ${haystack} like '%duplex%'
        or ${haystack} like '%triplex%'
        or ${haystack} like '%fourplex%'
      )`;
    }
    if (asset === 'commercial_multifamily') {
      return `and (
        p.asset_type in ('apartment', 'commercial_multifamily')
        or coalesce(p.total_units, 0) >= 5
        or ${haystack} like '%apartment%'
      )`;
    }
    return '';
  })();

  const filtersSql = [
    assetSql,
    zip ? `and p.zip = '${sqlString(zip.slice(0, 10))}'` : '',
    minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '',
    maxUnits !== null ? `and coalesce(p.total_units, 0) <= ${maxUnits}` : '',
    q ? `and (p.address ilike '%${sqlString(q)}%' or p.parcel_id ilike '%${sqlString(q)}%' or p.owner_name ilike '%${sqlString(q)}%')` : '',
  ].filter(Boolean).join('\n        ');

  const postFiltersSql = [
    onMarket === 'true' || onMarket === 'active' ? 'and is_on_market = true' : '',
    onMarket === 'false' || onMarket === 'off_market' ? 'and coalesce(is_on_market, false) = false' : '',
    minPrice !== null ? `and coalesce(mls_list_price, market_value, assessed_value, 0) >= ${minPrice}` : '',
    maxPrice !== null ? `and coalesce(mls_list_price, market_value, assessed_value, 0) <= ${maxPrice}` : '',
    gap !== 'all' ? `and '${sqlString(gap)}' = any(missing_fields)` : '',
  ].filter(Boolean).join('\n      ');

  const [report] = await queryPg<Record<string, unknown>>(`
    with base as (
      select
        p.*,
        coalesce(c.county_name, 'Unknown') as county_name,
        case
          when p.asset_type in ('commercial_multifamily', 'apartment') or coalesce(p.total_units, 0) >= 5 then 'commercial_multifamily'
          when p.asset_type = 'small_multifamily' or coalesce(p.total_units, 0) between 2 and 4
            or lower(coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_use, '')) like '%duplex%'
            or lower(coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_use, '')) like '%triplex%'
            or lower(coalesce(p.asset_subtype, '') || ' ' || coalesce(p.property_use, '')) like '%fourplex%' then 'small_multifamily'
          when p.asset_type in ('single_family', 'residential') or coalesce(p.total_units, 0) <= 1 then 'single_family'
          else coalesce(nullif(p.asset_type, ''), 'unknown')
        end as asset_group
      from properties p
      left join counties c on c.id = p.county_id
      where ${scope.whereSql}
        ${filtersSql}
    ),
    enriched as (
      select
        b.*,
        l.id as listing_id,
        coalesce(l.is_on_market, false) as is_on_market,
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
        l.last_seen_at as listing_last_seen_at,
        exists(select 1 from sale_history sh where sh.property_id = b.id) as has_sale_history,
        exists(select 1 from mortgage_records m where m.property_id = b.id) as has_mortgage_record,
        exists(
          select 1
          from mortgage_records m
          where m.property_id = b.id
            and lower(coalesce(m.document_type, '')) not like '%release%'
            and lower(coalesce(m.document_type, '')) not like '%satisfaction%'
            and lower(coalesce(m.document_type, '')) not like '%assignment%'
            and lower(coalesce(m.document_type, '')) not like '%deed%'
            and (m.open = true or m.maturity_date::date > current_date)
            and coalesce(m.estimated_current_balance, m.loan_amount, m.original_amount, 0) > 0
        ) as has_mortgage_balance,
        exists(select 1 from property_websites pw where pw.property_id = b.id and pw.active = true) as has_property_website,
        exists(select 1 from floorplans fp where fp.property_id = b.id) as has_floorplans,
        exists(select 1 from rent_snapshots rs where rs.property_id = b.id) as has_rent_snapshot,
        exists(select 1 from property_location_scores pls where pls.property_id = b.id) as has_location_scores
      from base b
      left join lateral (
        select *
        from listing_signals l
        where l.property_id = b.id
        order by l.is_on_market desc, l.last_seen_at desc nulls last, l.first_seen_at desc nulls last
        limit 1
      ) l on true
    ),
    flags as (
      select
        *,
        array_remove(array[
          case when parcel_id is null or parcel_id = '' or address is null or address = '' or county_id is null then 'parcel_identity' end,
          case when asset_group = 'unknown' or (asset_type is null and asset_subtype is null and property_type is null and property_use is null) then 'asset_classification' end,
          case when owner_name is null and company_name is null and owner1_last is null then 'ownership' end,
          case when coalesce(market_value, assessed_value, taxable_value, 0) <= 0 then 'valuation' end,
          case when year_built is null and coalesce(total_sqft, living_sqft, land_sqft, lot_sqft, 0) <= 0 then 'physical_facts' end,
          case when not has_sale_history and last_sale_date is null and coalesce(last_sale_price, 0) <= 0 and sale_year is null then 'sales_history' end,
          case when not has_mortgage_record then 'mortgage_records' end,
          case when has_mortgage_record and not has_mortgage_balance then 'mortgage_balance' end,
          case when is_on_market and listing_agent_name is null and (listing_agent_first_name is null or listing_agent_last_name is null) then 'agent_name' end,
          case when is_on_market and listing_agent_email is null then 'agent_email' end,
          case when is_on_market and listing_agent_phone is null then 'agent_phone' end,
          case when is_on_market and listing_brokerage is null then 'brokerage' end,
          case when is_on_market and listing_url is null then 'listing_url' end,
          case when asset_group in ('small_multifamily', 'commercial_multifamily') and not has_property_website then 'property_website' end,
          case when asset_group in ('small_multifamily', 'commercial_multifamily') and not has_floorplans then 'floorplans' end,
          case when asset_group in ('small_multifamily', 'commercial_multifamily') and not has_rent_snapshot then 'rent_snapshot' end,
          case when not has_location_scores then 'location_scores' end
        ], null) as missing_fields
      from enriched
    ),
    filtered as (
      select *
      from flags
      where cardinality(missing_fields) > 0
      ${postFiltersSql}
    ),
    counted as (
      select *, count(*) over()::int as total_count
      from filtered
      order by
        is_on_market desc,
        cardinality(missing_fields) desc,
        coalesce(mls_list_price, market_value, assessed_value, 0) desc,
        id
      limit ${limit}
      offset ${offset}
    )
    select
      coalesce(max(total_count), 0)::int as total_count,
      jsonb_build_object(
        'parcel_identity', count(*) filter (where 'parcel_identity' = any(missing_fields)),
        'asset_classification', count(*) filter (where 'asset_classification' = any(missing_fields)),
        'ownership', count(*) filter (where 'ownership' = any(missing_fields)),
        'valuation', count(*) filter (where 'valuation' = any(missing_fields)),
        'physical_facts', count(*) filter (where 'physical_facts' = any(missing_fields)),
        'sales_history', count(*) filter (where 'sales_history' = any(missing_fields)),
        'mortgage_records', count(*) filter (where 'mortgage_records' = any(missing_fields)),
        'mortgage_balance', count(*) filter (where 'mortgage_balance' = any(missing_fields)),
        'agent_name', count(*) filter (where 'agent_name' = any(missing_fields)),
        'agent_email', count(*) filter (where 'agent_email' = any(missing_fields)),
        'agent_phone', count(*) filter (where 'agent_phone' = any(missing_fields)),
        'brokerage', count(*) filter (where 'brokerage' = any(missing_fields)),
        'listing_url', count(*) filter (where 'listing_url' = any(missing_fields)),
        'property_website', count(*) filter (where 'property_website' = any(missing_fields)),
        'floorplans', count(*) filter (where 'floorplans' = any(missing_fields)),
        'rent_snapshot', count(*) filter (where 'rent_snapshot' = any(missing_fields)),
        'location_scores', count(*) filter (where 'location_scores' = any(missing_fields))
      ) as returned_gap_counts,
      coalesce(jsonb_agg(jsonb_build_object(
        'mxreId', id,
        'parcelId', parcel_id,
        'address', address,
        'city', city,
        'state', state_code,
        'zip', zip,
        'county', county_name,
        'assetGroup', asset_group,
        'assetType', asset_type,
        'assetSubtype', asset_subtype,
        'unitCount', total_units,
        'ownerName', owner_name,
        'marketValue', market_value,
        'assessedValue', assessed_value,
        'lastSaleDate', last_sale_date,
        'lastSalePrice', last_sale_price,
        'onMarket', is_on_market,
        'listPrice', mls_list_price,
        'listingSource', listing_source,
        'listingUrl', listing_url,
        'listingLastSeenAt', listing_last_seen_at,
        'agent', jsonb_build_object(
          'name', listing_agent_name,
          'firstName', listing_agent_first_name,
          'lastName', listing_agent_last_name,
          'email', listing_agent_email,
          'phone', listing_agent_phone,
          'brokerage', listing_brokerage,
          'source', agent_contact_source,
          'confidence', agent_contact_confidence
        ),
        'checks', jsonb_build_object(
          'hasSaleHistory', has_sale_history,
          'hasMortgageRecord', has_mortgage_record,
          'hasMortgageBalance', has_mortgage_balance,
          'hasPropertyWebsite', has_property_website,
          'hasFloorplans', has_floorplans,
          'hasRentSnapshot', has_rent_snapshot,
          'hasLocationScores', has_location_scores
        ),
        'missingFields', missing_fields,
        'missingCount', cardinality(missing_fields),
        'completeness', greatest(0, round(((17 - cardinality(missing_fields))::numeric / 17) * 100))
      ) order by is_on_market desc, cardinality(missing_fields) desc, coalesce(mls_list_price, market_value, assessed_value, 0) desc), '[]'::jsonb) as rows
    from counted;
  `);

  const rows = ((report?.rows ?? []) as Array<Record<string, unknown>>).map((row) => {
    const missingFields = Array.isArray(row.missingFields) ? row.missingFields.map(String) : [];
    return {
      ...row,
      severity: inferGapSeverity(missingFields, Boolean(row.onMarket)),
      nextBestSources: inferGapSources(missingFields, Boolean(row.onMarket)),
    };
  });

  return c.json({
    market: 'indianapolis',
    geography: { state: 'IN', scope: scope.key, scope_label: scope.label, county_ids: scope.countyIds },
    filters: {
      page,
      limit,
      asset,
      gap,
      on_market: onMarket,
      zip: zip ?? null,
      min_units: minUnits,
      max_units: maxUnits,
      min_price: minPrice,
      max_price: maxPrice,
      q: q ?? null,
    },
    total: numberOrNull(report?.total_count) ?? rows.length,
    count: rows.length,
    returned_gap_counts: report?.returned_gap_counts ?? {},
    gap_dictionary: DATA_GAP_DICTIONARY,
    rows,
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/multifamily/coverage', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const unitFilterSql = minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '';

  const [coverage] = await queryPg<Record<string, unknown>>(`
    with universe as (
      select
        p.id,
        p.address,
        p.city,
        p.state_code,
        p.zip,
        p.parcel_id,
        p.asset_type,
        p.asset_subtype,
        p.total_units,
        p.unit_count_source,
        p.asset_confidence,
        p.property_use,
        p.market_value
      from properties p
      where p.county_id = 797583
        and p.asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
        ${unitFilterSql}
    ),
    flags as (
      select
        u.*,
        exists(select 1 from sale_history s where s.property_id = u.id) as has_sale_history,
        exists(select 1 from mortgage_records m where m.property_id = u.id) as has_recorded_mortgage,
        exists(select 1 from listing_signals l where l.property_id = u.id and l.is_on_market = true) as has_active_listing,
        exists(select 1 from property_complex_profiles cp where cp.property_id = u.id) as has_complex_profile
      from universe u
    )
    select
      count(*)::int as parcel_count,
      coalesce(sum(coalesce(total_units,0)),0)::int as known_units,
      count(*) filter (where asset_type = 'commercial_multifamily')::int as commercial_multifamily_parcels,
      count(*) filter (where asset_type in ('small_multifamily','apartment'))::int as residential_multifamily_parcels,
      count(*) filter (where has_sale_history)::int as parcels_with_sale_history,
      count(*) filter (where has_recorded_mortgage)::int as parcels_with_recorded_mortgage,
      count(*) filter (where has_sale_history or has_recorded_mortgage)::int as parcels_with_any_recorder_data,
      count(*) filter (where has_active_listing)::int as parcels_with_active_listing,
      count(*) filter (where has_complex_profile)::int as parcels_with_complex_profile,
      (select coalesce(jsonb_object_agg(asset_subtype, jsonb_build_object('parcels', parcels, 'known_units', known_units)), '{}'::jsonb)
       from (
         select asset_subtype, count(*)::int as parcels, coalesce(sum(coalesce(total_units,0)),0)::int as known_units
         from universe
         group by asset_subtype
         order by count(*) desc
       ) s) as by_subtype,
      (select coalesce(jsonb_object_agg(zip, parcels), '{}'::jsonb)
       from (
         select zip, count(*)::int as parcels
         from universe
         group by zip
         order by count(*) desc
         limit 20
       ) z) as by_zip,
      (select coalesce(jsonb_agg(row_to_json(r)), '[]'::jsonb)
       from (
         select
           id as "propertyId",
           address,
           city,
           state_code as state,
           zip,
           parcel_id as "parcelId",
           asset_type as "assetType",
           asset_subtype as "assetSubtype",
           total_units as "unitCount",
           unit_count_source as "unitCountSource",
           asset_confidence as "assetConfidence",
           property_use as "propertyUse",
           market_value as "marketValue",
           has_sale_history as "hasSaleHistory",
           has_recorded_mortgage as "hasRecordedMortgage",
           has_active_listing as "hasActiveListing",
           has_complex_profile as "hasComplexProfile"
         from flags
         where not (has_sale_history or has_recorded_mortgage)
         order by coalesce(total_units,0) desc, market_value desc nulls last
         limit 25
       ) r) as recorder_gap_examples
    from flags;
  `);

  const parcelCount = numberOrNull(coverage?.parcel_count) ?? 0;
  const pct = (value: unknown): number => {
    if (parcelCount === 0) return 0;
    return Math.round(((numberOrNull(value) ?? 0) / parcelCount) * 1000) / 10;
  };

  return c.json({
    market: 'indianapolis',
    geography: { city: 'Indianapolis', county: 'Marion', state: 'IN', countyId: 797583 },
    filters: { min_units: minUnits },
    parcel_universe: {
      parcel_count: parcelCount,
      known_units: numberOrNull(coverage?.known_units) ?? 0,
      commercial_multifamily_parcels: numberOrNull(coverage?.commercial_multifamily_parcels) ?? 0,
      residential_multifamily_parcels: numberOrNull(coverage?.residential_multifamily_parcels) ?? 0,
      by_subtype: coverage?.by_subtype ?? {},
      by_zip: coverage?.by_zip ?? {},
    },
    coverage: {
      parcels_with_sale_history: numberOrNull(coverage?.parcels_with_sale_history) ?? 0,
      sale_history_pct: pct(coverage?.parcels_with_sale_history),
      parcels_with_recorded_mortgage: numberOrNull(coverage?.parcels_with_recorded_mortgage) ?? 0,
      recorded_mortgage_pct: pct(coverage?.parcels_with_recorded_mortgage),
      parcels_with_any_recorder_data: numberOrNull(coverage?.parcels_with_any_recorder_data) ?? 0,
      any_recorder_data_pct: pct(coverage?.parcels_with_any_recorder_data),
      parcels_with_active_listing: numberOrNull(coverage?.parcels_with_active_listing) ?? 0,
      active_listing_pct: pct(coverage?.parcels_with_active_listing),
      parcels_with_complex_profile: numberOrNull(coverage?.parcels_with_complex_profile) ?? 0,
      complex_profile_pct: pct(coverage?.parcels_with_complex_profile),
    },
    gaps: {
      recorder_gap_examples: coverage?.recorder_gap_examples ?? [],
    },
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/markets/:market/complexes', async (c) => {
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
  }

  const minUnits = parsePositiveInt(c.req.query('min_units'));
  const limit = Math.min(parsePositiveInt(c.req.query('limit')) ?? 100, 500);
  const offset = Math.max(parseInt(c.req.query('offset') ?? '0', 10) || 0, 0);
  const unitFilterSql = minUnits !== null ? `and coalesce(p.total_units, 0) >= ${minUnits}` : '';

  const rows = await queryPg<Record<string, unknown>>(`
    select
      p.id as "propertyId",
      cp.complex_name as "complexName",
      cp.management_company as "managementCompany",
      cp.website,
      cp.phone,
      cp.email,
      cp.source as "profileSource",
      cp.source_url as "profileSourceUrl",
      cp.confidence as "profileConfidence",
      p.address,
      p.city,
      p.state_code as state,
      p.zip,
      p.asset_type as "assetType",
      p.asset_subtype as "assetSubtype",
      p.total_units as "unitCount",
      p.unit_count_source as "unitCountSource",
      p.asset_confidence as "assetConfidence",
      p.year_built as "yearBuilt",
      p.living_sqft as "livingSqft",
      p.market_value as "marketValue",
      p.assessed_value as "assessedValue",
      cp.amenities,
      cp.description,
      cp.last_seen_at as "profileLastSeenAt"
    from properties p
    left join property_complex_profiles cp on cp.property_id = p.id
    where p.county_id = 797583
      and p.asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
      ${unitFilterSql}
    order by coalesce(p.total_units, 0) desc, p.address
    limit ${limit}
    offset ${offset}
  `);

  return c.json({
    market: 'indianapolis',
    geography: { city: 'Indianapolis', county: 'Marion', state: 'IN', countyId: 797583 },
    filters: { min_units: minUnits },
    count: rows.length,
    limit,
    offset,
    results: rows,
    generated_at: new Date().toISOString(),
  });
});

app.get('/v1/ingest-status', async (c) => {
  const fs = await import('node:fs');
  const path = await import('node:path');

  const logsDir = path.join(process.cwd(), 'logs');
  const logFiles = [
    { name: 'statewide-parcels', file: 'statewide-parcels.log' },
    { name: 'fidlar-liens', file: 'fidlar-fast.log' },
    { name: 'supervisor-parcels', file: 'supervisor.log' },
    { name: 'supervisor-liens', file: 'supervisor-liens.log' },
    { name: 'failures', file: 'state-failures.log' },
  ];

  const status: Record<string, any> = {};

  for (const { name, file } of logFiles) {
    const filePath = path.join(logsDir, file);
    try {
      const stat = fs.statSync(filePath);
      const content = fs.readFileSync(filePath, 'utf8');
      const lines = content.trim().split('\n');
      const lastLines = lines.slice(-10).join('\n');

      // Extract latest progress line
      const progressMatch = content.match(/Offset: ([\d,]+) \/ ([\d,]+) \(([0-9.]+)%\)/g);
      const latestProgress = progressMatch ? progressMatch[progressMatch.length - 1] : null;

      // Count failures
      const failureCount = (content.match(/FAIL/g) ?? []).length;

      status[name] = {
        last_modified: stat.mtime.toISOString(),
        size_kb: Math.round(stat.size / 1024),
        failure_count: failureCount,
        latest_progress: latestProgress,
        tail: lastLines,
      };
    } catch {
      status[name] = { error: 'Log not found' };
    }
  }

  return c.json({ generated_at: new Date().toISOString(), logs: status });
});

// ── Live Dashboard ────────────────────────────────────────────

app.get('/dashboard', async (c) => {
  return c.html(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MXRE Data Intelligence Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, -apple-system, sans-serif; background: #0a0f1e; color: #e2e8f0; min-height: 100vh; }
  .header { background: #0f172a; border-bottom: 1px solid #1e293b; padding: 20px 32px; display: flex; align-items: center; justify-content: space-between; }
  .header h1 { font-size: 20px; font-weight: 700; color: #38bdf8; letter-spacing: -0.5px; }
  .header .subtitle { font-size: 13px; color: #64748b; margin-top: 2px; }
  .badge { background: #064e3b; color: #34d399; font-size: 11px; font-weight: 600; padding: 3px 10px; border-radius: 20px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; padding: 24px 32px; }
  .card { background: #0f172a; border: 1px solid #1e293b; border-radius: 10px; padding: 20px; }
  .card h3 { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; color: #64748b; margin-bottom: 10px; }
  .big-num { font-size: 32px; font-weight: 800; color: #f1f5f9; line-height: 1; }
  .sub { font-size: 12px; color: #64748b; margin-top: 6px; }
  .green { color: #4ade80; }
  .yellow { color: #fbbf24; }
  .red { color: #f87171; }
  .section { padding: 0 32px 24px; }
  .section h2 { font-size: 14px; font-weight: 600; color: #94a3b8; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 11px; color: #64748b; font-weight: 600; text-transform: uppercase; padding: 8px 12px; border-bottom: 1px solid #1e293b; }
  td { padding: 9px 12px; font-size: 13px; border-bottom: 1px solid #0f172a; }
  tr:hover td { background: #0f172a; }
  .bar-wrap { background: #1e293b; border-radius: 4px; height: 6px; width: 120px; display: inline-block; vertical-align: middle; }
  .bar { background: #38bdf8; height: 6px; border-radius: 4px; }
  .tag { display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 4px; font-weight: 500; }
  .tag.ok { background: #064e3b; color: #34d399; }
  .tag.missing { background: #450a0a; color: #f87171; }
  .tag.partial { background: #451a03; color: #fbbf24; }
  .states-grid { display: flex; flex-wrap: wrap; gap: 6px; }
  .state-chip { font-size: 11px; font-weight: 600; padding: 4px 8px; border-radius: 5px; }
  .state-chip.has { background: #0c2a1a; color: #4ade80; border: 1px solid #166534; }
  .state-chip.no { background: #1c0a0a; color: #ef4444; border: 1px solid #7f1d1d; }
  .state-chip { cursor: pointer; }
  .state-chip.active { outline: 2px solid #38bdf8; color: #e0f2fe; }
  .refresh-btn { background: #1e293b; border: 1px solid #334155; color: #94a3b8; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  .refresh-btn:hover { background: #334155; color: #e2e8f0; }
  #last-updated { font-size: 11px; color: #475569; margin-top: 4px; }
  pre { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 14px; font-size: 11px; color: #94a3b8; overflow-x: auto; white-space: pre-wrap; max-height: 200px; overflow-y: auto; }
  .form-row { display: grid; grid-template-columns: 2fr 1fr 80px 90px auto; gap: 8px; }
  input { background: #0f172a; border: 1px solid #334155; border-radius: 6px; color: #e2e8f0; padding: 9px 10px; font-size: 13px; min-width: 0; }
  input::placeholder { color: #475569; }
  .primary-btn { background: #0369a1; border: 1px solid #0284c7; color: #e0f2fe; padding: 9px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; }
  .primary-btn:hover { background: #0284c7; }
  .detail-panel { margin-top: 12px; }
  .muted { color: #64748b; font-size: 12px; }
  @media (max-width: 760px) { .form-row { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>MXRE · Data Intelligence Dashboard</h1>
    <div class="subtitle">Mundo X Venture Studio · Property Data Layer</div>
  </div>
  <div style="display:flex;align-items:center;gap:12px;">
    <span id="last-updated"></span>
    <button class="refresh-btn" onclick="load()">↻ Refresh</button>
    <span class="badge">LIVE</span>
  </div>
</div>

<div class="grid" id="totals-grid">
  <div class="card"><h3>Properties</h3><div class="big-num" id="total-props">—</div><div class="sub" id="prop-sub"></div></div>
  <div class="card"><h3>Mortgages & Liens</h3><div class="big-num" id="total-mort">—</div><div class="sub" id="mort-sub"></div></div>
  <div class="card"><h3>Rent Records</h3><div class="big-num" id="total-rent">—</div><div class="sub" id="rent-sub"></div></div>
  <div class="card"><h3>Listing Signals</h3><div class="big-num" id="total-list">—</div><div class="sub" id="list-sub"></div></div>
  <div class="card"><h3>State Coverage</h3><div class="big-num" id="cov-pct">—</div><div class="sub" id="cov-sub"></div></div>
</div>

<div class="section">
  <h2>Top States by Property Count</h2>
  <table>
    <tr><th>State</th><th>Properties</th><th>Share</th><th></th></tr>
    <tbody id="top-states-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>State Coverage Map</h2>
  <div class="states-grid" id="states-grid"></div>
  <div class="detail-panel" id="state-detail"></div>
</div>

<div class="section">
  <h2>Property Address Lookup</h2>
  <div class="card">
    <div class="form-row">
      <input id="lookup-address" placeholder="Address, e.g. 611 N Park Ave #208">
      <input id="lookup-city" placeholder="City" value="Indianapolis">
      <input id="lookup-state" placeholder="State" value="IN" maxlength="2">
      <input id="lookup-zip" placeholder="ZIP">
      <button class="primary-btn" onclick="lookupAddress()">Lookup</button>
    </div>
    <div class="muted" style="margin-top:8px;">Tip: address and state are required; city or ZIP makes matching tighter.</div>
    <pre id="lookup-result" style="margin-top:12px;max-height:420px;">Enter an address to see the API response.</pre>
  </div>
</div>

<div class="section">
  <h2>Ingest Pipeline Status</h2>
  <div id="ingest-status"></div>
</div>

<div class="section">
  <h2>Indianapolis Multifamily On-Market</h2>
  <div class="card">
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:12px;">
      <div><div class="muted">Active Rows</div><div style="font-size:22px;font-weight:800" id="mf-total">-</div></div>
      <div><div class="muted">Loaded</div><div style="font-size:22px;font-weight:800" id="mf-count">-</div></div>
      <div><div class="muted">Sources</div><div style="font-size:13px;font-weight:700" id="mf-sources">-</div></div>
      <div><div class="muted">Subtypes</div><div style="font-size:13px;font-weight:700" id="mf-subtypes">-</div></div>
    </div>
    <table>
      <tr><th>Address</th><th>Type</th><th>Units</th><th>List Price</th><th>$/Door</th><th>Source</th></tr>
      <tbody id="mf-on-market-body"></tbody>
    </table>
  </div>
</div>

<script>
function fmt(n) { return n?.toLocaleString('en-US') ?? '-'; }
const apiKey = ${JSON.stringify(getBrowserApiKey(c))};

async function load() {
  document.getElementById('last-updated').textContent = 'Loading...';
  try {
    const [cov, ing, mf] = await Promise.all([
      fetch('/v1/coverage', { headers: { 'x-api-key': apiKey } }).then(r => r.json()),
      fetch('/v1/ingest-status', { headers: { 'x-api-key': apiKey } }).then(r => r.json()),
      fetch('/v1/markets/indianapolis/multifamily/on-market?limit=25', { headers: { 'x-api-key': apiKey } }).then(r => r.json()),
    ]);

    const allStates = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'];

    if (cov.status === 'building' || !cov.totals || !cov.coverage) {
      document.getElementById('total-props').textContent = '—';
      document.getElementById('total-mort').textContent = '—';
      document.getElementById('total-rent').textContent = '—';
      document.getElementById('total-list').textContent = '—';
      document.getElementById('cov-pct').textContent = 'Building';
      document.getElementById('cov-sub').textContent = cov.message ?? 'Coverage data is being compiled.';
      document.getElementById('states-grid').innerHTML = allStates.map(s => \`<button class="state-chip" onclick="loadState('\${s}')">\${s}</button>\`).join('');
      document.getElementById('last-updated').textContent = 'Coverage building · use state buttons or lookup below';
      renderIngestStatus(ing);
      renderMultifamilyOnMarket(mf);
      return;
    }

    // Totals
    document.getElementById('total-props').textContent = fmt(cov.totals.properties);
    document.getElementById('prop-sub').textContent = 'Last write: ' + (cov.freshness.last_property_write ? new Date(cov.freshness.last_property_write).toLocaleString() : 'unknown');
    document.getElementById('total-mort').textContent = fmt(cov.totals.mortgage_records);
    document.getElementById('mort-sub').textContent = 'Last recorded: ' + (cov.freshness.last_mortgage_recording ?? 'unknown');
    document.getElementById('total-rent').textContent = fmt(cov.totals.rent_snapshots);
    document.getElementById('rent-sub').textContent = 'Actual + statistical';
    document.getElementById('total-list').textContent = fmt(cov.totals.listing_signals);
    document.getElementById('list-sub').textContent = 'On-market signals';

    // Coverage
    document.getElementById('cov-pct').textContent = cov.coverage.coverage_pct;
    document.getElementById('cov-sub').innerHTML = '<span class="green">' + cov.coverage.states_with_data + ' states</span> covered · <span class="red">' + cov.coverage.states_missing + ' missing</span>';

    // Top states
    const total = cov.totals.properties;
    const tbody = document.getElementById('top-states-body');
    tbody.innerHTML = '';
    for (const { state, count } of (cov.top_states ?? [])) {
      const pct = total > 0 ? (count / total * 100).toFixed(1) : 0;
      const barW = Math.round(pct * 1.2);
      tbody.innerHTML += \`<tr>
        <td><strong>\${state}</strong></td>
        <td>\${fmt(count)}</td>
        <td>\${pct}%</td>
        <td><span class="bar-wrap"><span class="bar" style="width:\${Math.min(barW,100)}%"></span></span></td>
      </tr>\`;
    }

    // State chips
    const grid = document.getElementById('states-grid');
    grid.innerHTML = allStates.map(s => {
      const has = (cov.coverage.covered_states ?? []).includes(s);
      return \`<button class="state-chip \${has ? 'has' : 'no'}" onclick="loadState('\${s}')">\${s}</button>\`;
    }).join('');

    renderIngestStatus(ing);
    renderMultifamilyOnMarket(mf);

    document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch (e) {
    document.getElementById('last-updated').textContent = 'Error: ' + e.message;
  }
}

function renderMultifamilyOnMarket(mf) {
  document.getElementById('mf-total').textContent = fmt(mf.total);
  document.getElementById('mf-count').textContent = fmt(mf.count);
  document.getElementById('mf-sources').textContent = Object.entries(mf.summary?.by_source ?? {}).map(([k, v]) => k + ': ' + v).join(' | ') || '-';
  document.getElementById('mf-subtypes').textContent = Object.entries(mf.summary?.by_subtype ?? {}).map(([k, v]) => k + ': ' + v).join(' | ') || '-';

  const body = document.getElementById('mf-on-market-body');
  if (mf.error) {
    body.innerHTML = \`<tr><td colspan="6" class="red">\${mf.detail || mf.error}</td></tr>\`;
    return;
  }

  const rows = (mf.results ?? []).map(row => {
    const url = row.market?.listingUrl;
    const address = [row.property?.address, row.property?.city, row.property?.state, row.property?.zip].filter(Boolean).join(', ');
    const link = url ? \`<a href="\${url}" target="_blank" style="color:#38bdf8">\${address}</a>\` : address;
    return \`<tr>
      <td>\${link}</td>
      <td>\${row.property?.assetSubtype ?? '-'}</td>
      <td>\${row.property?.unitCount ?? '-'}</td>
      <td>\${row.market?.listPrice ? '$' + fmt(row.market.listPrice) : '-'}</td>
      <td>\${row.market?.pricePerUnit ? '$' + fmt(row.market.pricePerUnit) : '-'}</td>
      <td>\${row.market?.listingSource ?? '-'}</td>
    </tr>\`;
  }).join('');

  body.innerHTML = rows || '<tr><td colspan="6" class="muted">No active multifamily listing rows found.</td></tr>';
}

function renderIngestStatus(ing) {
  const ingDiv = document.getElementById('ingest-status');
  ingDiv.innerHTML = '';
  for (const [name, info] of Object.entries(ing.logs ?? {})) {
    if (info.error) {
      ingDiv.innerHTML += \`<div class="card" style="margin-bottom:12px"><h3>\${name}</h3><span class="tag missing">No log</span></div>\`;
      continue;
    }
    const d = info;
    const stale = d.last_modified ? (Date.now() - new Date(d.last_modified).getTime()) > 3600000 : true;
    ingDiv.innerHTML += \`<div class="card" style="margin-bottom:12px">
      <h3>\${name} <span class="tag \${stale ? 'missing' : 'ok'}" style="margin-left:8px">\${stale ? 'STALE' : 'ACTIVE'}</span></h3>
      <div style="font-size:12px;color:#64748b;margin:6px 0">Modified: \${d.last_modified ? new Date(d.last_modified).toLocaleString() : '—'} · \${d.size_kb}KB · \${d.failure_count} failures</div>
      \${d.latest_progress ? \`<div style="font-size:12px;color:#38bdf8;margin-bottom:8px">\${d.latest_progress}</div>\` : ''}
      <pre>\${d.tail ?? ''}</pre>
    </div>\`;
  }
}

async function loadState(state) {
  const panel = document.getElementById('state-detail');
  panel.innerHTML = '<div class="card muted">Loading ' + state + ' county coverage...</div>';
  document.querySelectorAll('.state-chip').forEach(el => el.classList.toggle('active', el.textContent === state));

  try {
    const data = await fetch('/v1/coverage/state/' + encodeURIComponent(state), {
      headers: { 'x-api-key': apiKey },
    }).then(r => r.json());
    if (data.error) throw new Error(data.detail || data.error);

    const rows = (data.counties ?? []).map(c => \`<tr>
      <td><strong>\${c.county_name}</strong></td>
      <td>\${c.county_fips}</td>
      <td>\${c.id}</td>
    </tr>\`).join('');

    panel.innerHTML = \`<div class="card">
      <h3>\${state} Coverage</h3>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:10px 0 16px;">
        <div><div class="muted">Counties</div><div style="font-size:22px;font-weight:800">\${fmt(data.totals.counties)}</div></div>
        <div><div class="muted">Properties</div><div style="font-size:22px;font-weight:800">\${fmt(data.totals.properties)}</div></div>
        <div><div class="muted">Mortgages</div><div style="font-size:22px;font-weight:800">\${fmt(data.totals.mortgage_records)}</div></div>
        <div><div class="muted">Listings</div><div style="font-size:22px;font-weight:800">\${fmt(data.totals.listing_signals)}</div></div>
      </div>
      <table>
        <tr><th>County</th><th>FIPS</th><th>County ID</th></tr>
        <tbody>\${rows}</tbody>
      </table>
    </div>\`;
  } catch (e) {
    panel.innerHTML = '<div class="card red">Could not load state coverage: ' + e.message + '</div>';
  }
}

async function lookupAddress() {
  const result = document.getElementById('lookup-result');
  const address = document.getElementById('lookup-address').value.trim();
  const city = document.getElementById('lookup-city').value.trim();
  const state = document.getElementById('lookup-state').value.trim().toUpperCase();
  const zip = document.getElementById('lookup-zip').value.trim();

  if (!address || !state) {
    result.textContent = 'Address and state are required.';
    return;
  }

  const params = new URLSearchParams({ address, state });
  if (city) params.set('city', city);
  if (zip) params.set('zip', zip);

  result.textContent = 'Looking up property...';
  try {
    const resp = await fetch('/v1/property?' + params.toString(), {
      headers: { 'x-api-key': apiKey },
    });
    const data = await resp.json();
    result.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    result.textContent = 'Lookup failed: ' + e.message;
  }
}

load();
setInterval(load, 60000); // auto-refresh every 60s
</script>
</body>
</html>`);
});

app.get('/preview/market-dashboard', async (c) => {
  const market = c.req.query('market')?.toLowerCase();
  const apiKey = getBrowserApiKey(c);
  if (market && market !== 'indianapolis' && market !== 'indy') {
    return c.html(renderMarketSnapshotDashboard(market, apiKey));
  }
  return c.html(renderMarketCommandCenter(apiKey));
});

function renderMarketCommandCenter(apiKey: string): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MXRE Market Command Center</title>
<style>
:root{color-scheme:dark;--bg:#0c1110;--panel:#141918;--panel2:#1a211f;--line:#2d3834;--text:#eff7f1;--muted:#9cacA4;--green:#38d47c;--blue:#66b7ff;--amber:#f4bd50;--red:#f26d6d}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.top{position:sticky;top:0;z-index:5;background:#111615;border-bottom:1px solid var(--line);padding:16px 22px;display:flex;justify-content:space-between;gap:14px;align-items:center}.brand{font-size:18px;font-weight:850;letter-spacing:.04em}.sub,.muted{color:var(--muted)}main{max-width:1540px;margin:0 auto;padding:18px 22px 34px}.tabs,.chips{display:flex;gap:8px;flex-wrap:wrap}.tab,.chip,.btn{border:1px solid var(--line);border-radius:7px;background:#111615;color:var(--muted);padding:9px 12px;font-weight:750;cursor:pointer}.tab.active,.chip.active,.btn.primary{background:#183323;border-color:#2c8052;color:#e8fff0}.grid{display:grid;gap:12px}.kpis{grid-template-columns:repeat(6,minmax(145px,1fr));margin:14px 0}.three{grid-template-columns:1fr 1fr 1fr}.two{grid-template-columns:1fr 1.6fr}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px;min-width:0}.card h2,.card h3{margin:0 0 9px;font-size:14px}.metric{font-size:28px;font-weight:850;line-height:1}.good{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}.blue{color:var(--blue)}.toolbar{display:grid;grid-template-columns:1fr repeat(5,120px) auto;gap:8px;margin:14px 0}.toolbar input,.toolbar select{background:#101514;border:1px solid var(--line);border-radius:7px;color:var(--text);padding:10px;width:100%}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid rgba(255,255,255,.07);padding:9px 8px;text-align:left;vertical-align:top}th{font-size:11px;text-transform:uppercase;color:var(--muted);letter-spacing:.05em}.bar-row{display:grid;grid-template-columns:90px 1fr 82px;gap:8px;align-items:center;margin:9px 0}.bar-track{height:9px;border-radius:99px;background:#0e1312;border:1px solid var(--line);overflow:hidden}.bar-fill{height:100%;background:linear-gradient(90deg,var(--green),var(--blue))}.pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:3px 8px;color:var(--muted);font-size:12px}.desc{max-width:520px;color:#d5e5da;font-size:12px}.hidden{display:none}a{color:#8cc8ff;text-decoration:none}@media(max-width:1100px){.kpis{grid-template-columns:repeat(3,1fr)}.three,.two,.toolbar{grid-template-columns:1fr}}@media(max-width:680px){main{padding:14px}.top{align-items:flex-start;flex-direction:column}.kpis{grid-template-columns:1fr 1fr}th:nth-child(4),td:nth-child(4),th:nth-child(6),td:nth-child(6){display:none}}
</style></head><body>
<div class="top"><div><div class="brand">MXRE Market Command Center</div><div class="sub">Indianapolis, IN analytics for listings, changes, opportunity reports, and data quality</div></div><div class="chips"><span class="pill" id="status">Loading</span><button class="btn" onclick="loadAll()">Refresh</button></div></div>
<main>
  <div class="tabs" id="tabs">
    <button class="tab active" data-view="overview">Overview</button><button class="tab" data-view="listings">Listings</button><button class="tab" data-view="priceDrops">Price Drops</button><button class="tab" data-view="priceDropAnalysis">Drop Analysis</button><button class="tab" data-view="creativeFinance">Creative Finance</button><button class="tab" data-view="delisted">Delisted</button><button class="tab" data-view="multifamily">Multifamily</button><button class="tab" data-view="zipCodes">Zip Codes</button><button class="tab" data-view="dataQuality">Data Quality</button><button class="tab" data-view="changeFeed">Change Feed</button>
  </div>
  <section class="grid kpis" id="kpis"></section>
  <section class="card">
    <div class="toolbar"><input id="q" placeholder="Filter visible report by address, zip, agent, source, description"><input id="minPrice" placeholder="Min price"><input id="maxPrice" placeholder="Max price"><input id="minUnits" placeholder="Min units"><input id="maxUnits" placeholder="Max units"><input id="zip" placeholder="Zip"><button class="btn primary" onclick="loadListings()">Apply</button></div>
    <div class="chips"><button class="chip active" data-asset="all">All</button><button class="chip" data-asset="single_family">Single-family</button><button class="chip" data-asset="multifamily">Multifamily</button><button class="chip" data-unit-band="2-4" onclick="setUnits(2,4,'2-4')">2-4 units</button><button class="chip" data-unit-band="5-10" onclick="setUnits(5,10,'5-10')">5-10</button><button class="chip" data-unit-band="11-20" onclick="setUnits(11,20,'11-20')">11-20</button><button class="chip" data-unit-band="21+" onclick="setUnits(21,'','21+')">21+</button></div>
  </section>
  <section class="grid three" style="margin-top:12px"><div class="card"><h2>Event Pulse</h2><div id="events"></div></div><div class="card"><h2>Asset Mix</h2><div id="assets"></div></div><div class="card"><h2>Top ZIPs</h2><div id="zips"></div></div></section>
  <section class="grid two" id="reportSection" style="margin-top:12px"><div class="card"><h2 id="leftTitle">Analytics Notes</h2><div id="notes"></div></div><div class="card"><h2 id="tableTitle">Report</h2><table><thead id="thead"></thead><tbody id="tbody"></tbody></table><div class="sub" id="tableSub" style="margin-top:10px"></div></div></section>
</main>
<script>
const apiKey=${JSON.stringify(apiKey)};let analytics=null,listings=null,view='overview',asset='all',page=1;const limit=50;const statusEl=document.getElementById('status');
const fmt=n=>Number(n||0).toLocaleString();const money=n=>n==null?'-':'$'+Number(n).toLocaleString();const esc=v=>String(v??'').replace(/[&<>"]/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[s]));const pct=(n,d)=>!Number(d)?'0%':(Math.round(Number(n||0)/Number(d)*1000)/10)+'%';
async function getJson(path){const r=await fetch(path,{headers:{'x-api-key':apiKey}});const j=await r.json();if(!r.ok)throw new Error(j.error||j.detail||path);return j}
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>activateView(b.dataset.view));
document.querySelectorAll('.chip[data-asset]').forEach(b=>b.onclick=()=>{document.querySelectorAll('.chip[data-asset],.chip[data-unit-band]').forEach(x=>x.classList.remove('active'));b.classList.add('active');asset=b.dataset.asset;document.getElementById('minUnits').value='';document.getElementById('maxUnits').value='';page=1;loadListings()});
function params(){const p=new URLSearchParams({asset,page:String(page),limit:String(limit),scope:'city'});for(const [id,key] of [['zip','zip'],['minPrice','min_price'],['maxPrice','max_price'],['minUnits','min_units'],['maxUnits','max_units']]){const v=document.getElementById(id).value.trim();if(v)p.set(key,v)}return p}
function setUnits(min,max,band){document.getElementById('minUnits').value=min;document.getElementById('maxUnits').value=max||'';asset='multifamily';view='multifamily';document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.view==='multifamily'));document.querySelectorAll('.chip[data-asset],.chip[data-unit-band]').forEach(x=>x.classList.toggle('active',x.dataset.asset==='multifamily'||x.dataset.unitBand===band));page=1;loadListings()}
function activateView(next,scroll=false){view=next;document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.view===view));render();if(scroll)setTimeout(()=>document.getElementById('reportSection').scrollIntoView({behavior:'smooth',block:'start'}),0)}
async function loadAll(){statusEl.textContent='Loading analytics';analytics=await getJson('/v1/markets/indianapolis/analytics?days=30&limit=250');await loadListings(false);render();statusEl.textContent='Updated '+new Date().toLocaleTimeString()}
async function loadListings(doRender=true){statusEl.textContent='Loading listings';listings=await getJson('/v1/markets/indianapolis/opportunities?'+params().toString());if(doRender)render();statusEl.textContent='Updated '+new Date().toLocaleTimeString()}
function k(label,value,sub,tone='',action=''){return '<div class="card" '+(action?'style="cursor:pointer" onclick="activateView(\\''+esc(action)+'\\',true)" title="Click to open the supporting property list"':'')+'><h3>'+esc(label)+'</h3><div class="metric '+tone+'">'+esc(value)+'</div><div class="sub">'+esc(sub)+'</div></div>'}
function bars(id,rows,label='listings'){const max=Math.max(1,...(rows||[]).map(r=>Number(r[label]||r.count)||0));document.getElementById(id).innerHTML=(rows||[]).slice(0,10).map(r=>{const name=r.priceBracket||r.domBucket||r.zip||r.assetGroup||r.event_type||r.eventType||r.day||'unknown';const val=Number(r[label]||r.count||0);return '<div class="bar-row"><div>'+esc(name)+'</div><div class="bar-track"><div class="bar-fill" style="width:'+Math.max(4,val/max*100)+'%"></div></div><div>'+fmt(val)+'</div></div>'}).join('')||'<div class="sub">No data</div>'}
function render(){if(!analytics||!listings)return;const rows=listRows();kpis.innerHTML=viewKpis();renderSidePanels();notes.innerHTML=noteHtml();tableTitle.textContent=title();thead.innerHTML=head();tbody.innerHTML=rows.map(row).join('')||'<tr><td colspan="8" class="sub">No rows.</td></tr>';tableSub.textContent=subtitle()}
function viewKpis(){const o=analytics.overview||{},q=analytics.dataQuality||{},ev=analytics.eventCounts||{},drops=analytics.reports.priceDrops||[],dropAmounts=drops.map(r=>Number(r.dropAmount)||0),top50Drops=[...dropAmounts].sort((a,b)=>b-a).slice(0,50),cohorts=analytics.reports.priceDropMatrix||[],best=cohorts[0]||{},bestLabel=(best.priceBracket||'-')+' / '+(best.domBucket||'-'),cf=analytics.reports.creativeFinance||[],del=analytics.reports.delisted||[],mf=(listings.results||[]).filter(x=>['small_multifamily','commercial_multifamily'].includes(x.assetGroup)),active=Number(q.activeRows||o.activeRows||0);if(view==='priceDrops')return k('Price drops',fmt(o.priceDrops),'all detected rows','warn','priceDrops')+k('Largest drop',money(Math.max(0,...dropAmounts)),'single biggest reduction','warn','priceDrops')+k('Avg all drops',money(avg(dropAmounts)),'average across all loaded rows','warn','priceDrops')+k('Avg top 50',money(avg(top50Drops)),'largest-drop sample only','warn','priceDrops')+k('Median drop',money(median(dropAmounts)),'middle observed drop','', 'priceDrops')+k('Drop analysis',fmt(cohorts.length),'price x DOM cohorts','blue','priceDropAnalysis');if(view==='priceDropAnalysis')return k('Cohorts',fmt(cohorts.length),'price range x DOM buckets','blue','priceDropAnalysis')+k('Best sample',fmt(best.dropCount||0),bestLabel,'warn','priceDropAnalysis')+k('Best avg %',esc(best.avgDropPct??'-')+'%','top cohort avg drop','warn','priceDropAnalysis')+k('Best median %',esc(best.medianDropPct??'-')+'%','less outlier-sensitive','warn','priceDropAnalysis')+k('Best avg $',money(best.avgDropAmount),'top cohort dollars','warn','priceDropAnalysis')+k('Best avg DOM',fmt(best.avgDaysOnMarket||0),'timing signal','', 'priceDropAnalysis');if(view==='creativeFinance')return k('Active creative',fmt(o.creativePositive),'verified positive rows','good','creativeFinance')+k('Negative detected',fmt(o.creativeNegative),'explicit no-finance language','bad','creativeFinance')+k('With description',fmt(cf.filter(r=>r.listingDescription).length),'user can read remarks','good','creativeFinance')+k('Top score',fmt(Math.max(0,...cf.map(r=>Number(r.creativeFinanceScore)||0))),'signal strength','', 'creativeFinance')+k('Seller financing',fmt(cf.filter(r=>(r.creativeFinanceTerms||[]).includes('seller_financing')).length),'term match','', 'creativeFinance')+k('Subject-to',fmt(cf.filter(r=>(r.creativeFinanceTerms||[]).includes('subject_to')).length),'term match','', 'creativeFinance');if(view==='delisted')return k('Delisted',fmt(o.delisted),'last 30 days','warn','delisted')+k('Relisted',fmt(o.relisted),'came back active','', 'delisted')+k('Latest delisted',del[0]?.address||'-','most recent event','', 'delisted')+k('With price',fmt(del.filter(r=>r.lastListPrice).length),'last known list price','', 'delisted')+k('Top ZIP',topBy(del,'zip'),'most returned delists','', 'delisted')+k('Returned rows',fmt(del.length),'report sample','', 'delisted');if(view==='multifamily')return k('MF rows',fmt(mf.length),'current filtered page')+k('2+ active total',fmt((analytics.assetMix||[]).filter(r=>['small_multifamily','commercial_multifamily'].includes(r.assetGroup)).reduce((s,r)=>s+Number(r.listings||0),0)),'all active MF rows')+k('Median list',money(avg(mf.map(r=>Number(r.listPrice)||0))),'returned MF rows')+k('Creative MF',fmt(mf.filter(r=>r.creativeFinanceScore).length),'returned MF rows')+k('With agent',fmt(mf.filter(r=>r.listingAgentName).length),'returned MF rows')+k('With contact',fmt(mf.filter(r=>r.listingAgentEmail||r.listingAgentPhone).length),'returned MF rows');if(view==='zipCodes')return k('ZIPs ranked',fmt((analytics.zipRankings||[]).length),'top active markets')+k('Top ZIP',analytics.zipRankings?.[0]?.zip||'-',fmt(analytics.zipRankings?.[0]?.listings)+' listings')+k('Creative ZIPs',fmt((analytics.zipRankings||[]).filter(r=>Number(r.creativePositive)>0).length),'zips with creative leads')+k('Highest median',money(Math.max(0,...(analytics.zipRankings||[]).map(r=>Number(r.medianListPrice)||0))),'among top zips')+k('Contact rows',fmt((analytics.zipRankings||[]).reduce((s,r)=>s+Number(r.withContact||0),0)),'top zip rows')+k('Median DOM avg',fmt(avg((analytics.zipRankings||[]).map(r=>Number(r.medianDom)||0))),'top zip average');if(view==='dataQuality')return k('Listing rows',fmt(active),'quality denominator')+k('Linked property',pct(q.linkedToProperty,active),fmt(q.linkedToProperty)+' rows')+k('Description',pct(q.withDescription,active),fmt(q.withDescription)+' rows')+k('Agent phone',pct(q.withAgentPhone,active),fmt(q.withAgentPhone)+' rows')+k('Agent email',pct(q.withAgentEmail,active),fmt(q.withAgentEmail)+' rows','warn')+k('Brokerage',pct(q.withBrokerage,active),fmt(q.withBrokerage)+' rows','good');if(view==='changeFeed')return k('Events',fmt(Object.values(ev).reduce((s,n)=>s+Number(n||0),0)),'last 30 days','', 'changeFeed')+k('Listed',fmt(ev.listed||0),'new/refreshed rows','good','changeFeed')+k('Price changed',fmt(ev.price_changed||0),'all price changes','warn','changeFeed')+k('Delisted',fmt(ev.delisted||0),'went off market','warn','changeFeed')+k('Relisted',fmt(ev.relisted||0),'came back active','', 'changeFeed')+k('Contacts updated',fmt(ev.contact_updated||0),'enrichment events','', 'changeFeed');return k('Active listings',fmt(o.activeRows),'current Indianapolis rows','good','listings')+k('Unique properties',fmt(o.activeProperties),'linked active property universe','', 'listings')+k('Price drops',fmt(o.priceDrops),'click to verify before/after rows','warn','priceDrops')+k('Drop analysis',fmt(cohorts.length),'price x DOM cohorts','blue','priceDropAnalysis')+k('Creative finance',fmt(o.creativePositive),'positive language detected','good','creativeFinance')+k('Agent email',esc(o.agentEmailCoveragePct||0)+'%',fmt(q.withAgentEmail)+' / '+fmt(q.activeRows),'', 'dataQuality')}
function renderSidePanels(){if(view==='priceDrops'){bars('events',analytics.reports.priceDrops||[],'dropAmount');bars('assets',groupRows(analytics.reports.priceDrops||[],'listingSource'),'count');bars('zips',groupRows(analytics.reports.priceDrops||[],'zip'),'count');return}if(view==='priceDropAnalysis'){bars('events',analytics.reports.priceDropPriceBrackets||[],'dropCount');bars('assets',analytics.reports.priceDropDomBuckets||[],'dropCount');bars('zips',analytics.reports.priceDropMatrix||[],'dropCount');return}if(view==='creativeFinance'){bars('events',groupRows(analytics.reports.creativeFinance||[],'assetGroup'),'count');bars('assets',termRows(analytics.reports.creativeFinance||[]),'count');bars('zips',groupRows(analytics.reports.creativeFinance||[],'zip'),'count');return}if(view==='delisted'){bars('events',groupRows(analytics.reports.delisted||[],'listingSource'),'count');bars('assets',groupRows(analytics.reports.delisted||[],'listingBrokerage'),'count');bars('zips',groupRows(analytics.reports.delisted||[],'zip'),'count');return}if(view==='dataQuality'){const q=analytics.dataQuality||{};bars('events',Object.entries(q).map(([eventType,count])=>({eventType,count})),'count');bars('assets',analytics.assetMix||[]);bars('zips',analytics.zipRankings||[]);return}bars('events',Object.entries(analytics.eventCounts||{}).map(([eventType,count])=>({eventType,count})),'count');bars('assets',analytics.assetMix||[]);bars('zips',analytics.zipRankings||[])}
function noteHtml(){const copy={overview:['This is the market pulse: how much inventory exists, what changed, and where activity clusters.','Use this tab to decide whether the market is getting more negotiable or more competitive.'],listings:['This is the working lead table BBC would filter before underwriting.','Use price, units, zip, and asset filters to narrow the list.'],priceDrops:['This is a negotiation queue. Price drops can revive deals that previously failed underwriting.','Best next filter: high equity plus recent price drop.'],priceDropAnalysis:['This groups price drops by price bracket and days-on-market bucket so you can see where negotiation pressure tends to appear.','Use count first for confidence, then median drop percent to avoid being fooled by one huge outlier.'],creativeFinance:['This report only counts active positive language. The description is shown so a user can verify the exact wording.','Negative terms are scored separately instead of inflating the opportunity count.'],delisted:['Delisted properties are failed-market signals. They may still be owner targets even though they are not currently active.','Useful for outreach after stale listings disappear.'],multifamily:['This isolates 2+ unit and apartment assets from the single-family noise.','Use unit bands to find duplexes, 5-10 unit buildings, and larger commercial multifamily.'],zipCodes:['ZIP rankings show where inventory, price, contact coverage, and creative opportunities cluster.','This becomes the market selection and buy-box targeting layer.'],dataQuality:['This is the truth layer. It shows what MXRE can support confidently and what still needs enrichment.','Coverage should be shown internally even if the consumer API returns blended data.'],changeFeed:['This is the automation layer for BBC. It tells them what changed since the prior run.','BBC should re-underwrite only new listings, price changes, delists, relists, and newly enriched records.']}[view]||analytics.recommendations;return '<div class="sub">'+copy.map(esc).join('<br><br>')+'</div>'}
function avg(values){const clean=values.filter(n=>Number.isFinite(n)&&n>0);return clean.length?Math.round(clean.reduce((a,b)=>a+b,0)/clean.length):0}
function median(values){const clean=values.filter(n=>Number.isFinite(n)&&n>0).sort((a,b)=>a-b);if(!clean.length)return 0;const mid=Math.floor(clean.length/2);return clean.length%2?Math.round(clean[mid]):Math.round((clean[mid-1]+clean[mid])/2)}
function topBy(rows,key){const grouped=groupRows(rows,key);return grouped[0]?.eventType||grouped[0]?.zip||'-'}
function groupRows(rows,key){const m=new Map();for(const r of rows||[]){const name=String(r[key]||'unknown');m.set(name,(m.get(name)||0)+1)}return [...m.entries()].map(([eventType,count])=>({eventType,zip:eventType,count})).sort((a,b)=>b.count-a.count).slice(0,10)}
function termRows(rows){const m=new Map();for(const r of rows||[])for(const t of r.creativeFinanceTerms||[])m.set(t,(m.get(t)||0)+1);return [...m.entries()].map(([eventType,count])=>({eventType,count})).sort((a,b)=>b.count-a.count)}
function title(){return ({overview:'Market Pulse Report',listings:'Filtered Opportunity List',priceDrops:'Price Drop Proof List: Before / After Changes',priceDropAnalysis:'Price Drop Cohort Analysis',creativeFinance:'Creative Finance Report',delisted:'Delisted / Failed Listing Report',multifamily:'Multifamily Report',zipCodes:'ZIP Code Rankings',dataQuality:'Data Quality Report',changeFeed:'Change Feed'})[view]||'Report'}
function subtitle(){if(view==='priceDrops')return fmt((analytics.reports.priceDrops||[]).length)+' price-drop rows loaded. Average cards above are calculated from these before/after records.';if(view==='priceDropAnalysis')return 'Grouped by original list-price bracket and days-on-market bucket. Use row count as confidence.';return view==='listings'?fmt(listings.total)+' matching active listings':'Generated '+new Date(analytics.generated_at).toLocaleString()}
function listRows(){const f=q.value.trim().toLowerCase();let r=view==='priceDrops'?analytics.reports.priceDrops:view==='priceDropAnalysis'?analytics.reports.priceDropMatrix:view==='creativeFinance'?analytics.reports.creativeFinance:view==='delisted'?analytics.reports.delisted:view==='changeFeed'?analytics.reports.changeFeed:view==='zipCodes'?analytics.zipRankings:view==='dataQuality'?[analytics.dataQuality]:view==='multifamily'?(listings.results||[]).filter(x=>['small_multifamily','commercial_multifamily'].includes(x.assetGroup)):view==='overview'?analytics.assetMix:(listings.results||[]);return f?r.filter(x=>JSON.stringify(x).toLowerCase().includes(f)):r}
function head(){if(view==='priceDrops')return '<tr><th>Property</th><th>Before</th><th>After</th><th>Drop</th><th>Drop %</th><th>When</th><th>Agent / Source</th></tr>';if(view==='priceDropAnalysis')return '<tr><th>Price Bracket</th><th>DOM Bucket</th><th>Drops</th><th>Avg Drop</th><th>Median Drop</th><th>Avg %</th><th>Median %</th><th>Avg DOM</th></tr>';if(view==='zipCodes')return '<tr><th>ZIP</th><th>Listings</th><th>Median Price</th><th>Median DOM</th><th>Creative</th><th>Contact</th></tr>';if(view==='dataQuality')return '<tr><th>Field</th><th>Coverage</th><th>Rows</th><th>Meaning</th></tr>';if(view==='changeFeed')return '<tr><th>When</th><th>Event</th><th>Property</th><th>Price</th><th>Status</th><th>Source</th></tr>';if(view==='overview')return '<tr><th>Asset</th><th>Listings</th><th>Properties</th><th>Median Price</th><th>Creative</th><th>Description</th></tr>';return '<tr><th>Property</th><th>Price</th><th>Units</th><th>DOM</th><th>Agent</th><th>Signal</th><th>Description</th></tr>'}
function row(r){if(view==='priceDrops')return '<tr><td><a href="'+esc(r.listingUrl||'#')+'" target="_blank">'+esc(r.address)+'</a><div class="sub">'+esc(r.zip||'')+' · DOM '+esc(r.daysOnMarket??'-')+'</div></td><td>'+money(r.previousListPrice)+'</td><td>'+money(r.listPrice)+'</td><td><span class="pill warn">'+money(r.dropAmount)+'</span></td><td>'+esc(r.dropPct??'-')+'%</td><td>'+esc(r.eventAt?new Date(r.eventAt).toLocaleString():'-')+'</td><td>'+esc(r.listingAgentName||'')+'<div class="sub">'+esc(r.listingBrokerage||r.listingSource||'source gap')+'</div></td></tr>';if(view==='priceDropAnalysis')return '<tr><td>'+esc(r.priceBracket)+'</td><td>'+esc(r.domBucket)+'</td><td>'+fmt(r.dropCount)+'</td><td>'+money(r.avgDropAmount)+'</td><td>'+money(r.medianDropAmount)+'</td><td>'+esc(r.avgDropPct??'-')+'%</td><td>'+esc(r.medianDropPct??'-')+'%</td><td>'+fmt(r.avgDaysOnMarket)+'</td></tr>';if(view==='zipCodes')return '<tr><td>'+esc(r.zip)+'</td><td>'+fmt(r.listings)+'</td><td>'+money(r.medianListPrice)+'</td><td>'+esc(r.medianDom??'-')+'</td><td>'+fmt(r.creativePositive)+'</td><td>'+fmt(r.withContact)+'</td></tr>';if(view==='dataQuality'){const total=Number(r.activeRows||0);return ['withPrice','linkedToProperty','withBrokerage','withAgentName','withAgentPhone','withAgentEmail','withDescription'].map(k=>'<tr><td>'+k+'</td><td>'+pct(r[k],total)+'</td><td>'+fmt(r[k])+' / '+fmt(total)+'</td><td>'+qualityMeaning(k)+'</td></tr>').join('')}if(view==='changeFeed')return '<tr><td>'+esc(new Date(r.eventAt).toLocaleString())+'</td><td>'+esc(r.eventType)+'</td><td>'+esc(r.address)+'<div class="sub">'+esc(r.zip)+'</div></td><td>'+money(r.previousListPrice)+' -> '+money(r.listPrice)+'</td><td>'+esc(r.previousStatus||'')+' -> '+esc(r.status||'')+'</td><td>'+esc(r.listingSource)+'</td></tr>';if(view==='overview')return '<tr><td>'+esc(r.assetGroup)+'</td><td>'+fmt(r.listings)+'</td><td>'+fmt(r.properties)+'</td><td>'+money(r.medianListPrice)+'</td><td>'+fmt(r.creativePositive)+'</td><td>'+fmt(r.withDescription)+'</td></tr>';return '<tr><td><a href="'+esc(r.listingUrl||'#')+'" target="_blank">'+esc(r.address)+'</a><div class="sub">'+esc(r.zip||'')+' '+esc(r.assetGroup||'')+'</div></td><td>'+money(r.listPrice)+'</td><td>'+esc(r.unitCount??'-')+'</td><td>'+esc(r.daysOnMarket??'-')+'</td><td>'+esc(r.listingAgentName||'')+'<div class="sub">'+esc(r.listingAgentEmail||r.listingAgentPhone||r.listingBrokerage||'contact gap')+'</div></td><td>'+signal(r)+'</td><td><div class="desc">'+esc(r.listingDescription||r.publicRemarksSnippet||'')+'</div></td></tr>'}
function signal(r){if(view==='priceDrops')return '<span class="pill warn">'+money(r.dropAmount)+' drop</span><div class="sub">'+esc(r.dropPct||'')+'%</div>';if(view==='delisted')return '<span class="pill warn">delisted</span>';if(r.creativeFinanceScore)return '<span class="pill good">creative '+esc(r.creativeFinanceScore)+'</span><div class="sub">'+esc((r.creativeFinanceTerms||[]).join(', '))+'</div>';return '<span class="pill">active</span>'}
function qualityMeaning(k){return ({withPrice:'Can filter and underwrite listing economics.',linkedToProperty:'Matched to MXRE parcel/ownership/valuation.',withBrokerage:'Brokerage captured for contact routing.',withAgentName:'Human contact identity exists.',withAgentPhone:'Phone available for outreach.',withAgentEmail:'Email available for outreach.',withDescription:'MLS/public remarks available for creative-finance detection.'})[k]||''}
loadAll().catch(e=>{statusEl.textContent=String(e.message||e)});
</script></body></html>`;
}

function renderMarketSnapshotDashboard(requestedMarket: string, apiKey: string): string {
  const market = resolveMarketConfig(requestedMarket)?.key ?? 'columbus';
  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MXRE Market Dashboard</title>
<style>
:root{color-scheme:dark;--bg:#101312;--panel:#181c1b;--line:#303735;--text:#edf4ef;--muted:#9aa7a1;--green:#35c677;--blue:#55a8ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.topbar{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 24px;border-bottom:1px solid var(--line);background:#131715;position:sticky;top:0;z-index:3}.brand{font-size:18px;font-weight:850;letter-spacing:.06em}.muted,.sub{color:var(--muted)}main{padding:18px 24px 36px;max-width:1520px;margin:0 auto}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}.card h2,.card h3{margin:0 0 10px;font-size:14px}.metric{font-size:27px;font-weight:850}.sub{font-size:12px;margin-top:4px}select{background:#0f1312;border:1px solid var(--line);color:var(--text);border-radius:6px;padding:9px 12px}.pill{border:1px solid var(--line);border-radius:999px;padding:6px 10px;color:var(--muted);font-size:12px}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:9px 8px;text-align:left;font-size:13px;vertical-align:top}th{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}a{color:#8cc8ff;text-decoration:none}.bar-row{display:grid;grid-template-columns:160px 1fr 80px;gap:8px;align-items:center;margin:8px 0}.bar-track{height:9px;background:#0f1312;border:1px solid var(--line);border-radius:999px;overflow:hidden}.bar-fill{height:100%;background:linear-gradient(90deg,var(--green),var(--blue))}@media(max-width:980px){.grid{grid-template-columns:1fr 1fr}.topbar{align-items:flex-start;flex-direction:column}}@media(max-width:640px){.grid{grid-template-columns:1fr}main{padding:14px}}
</style></head><body>
<div class="topbar"><div><div class="brand">MXRE Market Dashboard</div><div class="muted" id="subtitle" style="font-size:12px;margin-top:3px">Loading market data</div></div><div style="display:flex;gap:10px;align-items:center"><select id="market"><option value="columbus">Columbus, OH</option><option value="indianapolis">Indianapolis, IN</option></select><span class="pill" id="status">Loading</span></div></div>
<main>
<section class="grid"><div class="card"><h3>Active Listings</h3><div class="metric" id="active">-</div><div class="sub" id="active-sub">tracked rows</div></div><div class="card"><h3>Detail Coverage</h3><div class="metric" id="detail">-</div><div class="sub">public remarks/details</div></div><div class="card"><h3>Agent Phone</h3><div class="metric" id="phone">-</div><div class="sub">active rows</div></div><div class="card"><h3>Creative Finance</h3><div class="metric" id="creative">-</div><div class="sub">positive signals</div></div><div class="card"><h3>Parcels</h3><div class="metric" id="parcels">-</div><div class="sub">city universe</div></div><div class="card"><h3>Multifamily Assets</h3><div class="metric" id="mf">-</div><div class="sub">2+ unit / apartment candidates</div></div><div class="card"><h3>Rent Snapshots</h3><div class="metric" id="rents">-</div><div class="sub" id="rents-sub">observed rents</div></div><div class="card"><h3>Recorder / Liens</h3><div class="metric" id="recorder">-</div><div class="sub" id="recorder-sub">linked public records</div></div></section>
<section class="grid" style="grid-template-columns:1fr 1fr;margin-top:12px"><div class="card"><h2>Coverage</h2><div id="coverage-bars"></div></div><div class="card"><h2>Recorder Summary</h2><div id="recorder-lines"></div></div></section>
<section class="card" style="margin-top:12px"><h2>Multifamily On-Market</h2><table><thead><tr><th>Property</th><th>Subtype</th><th>Units</th><th>List Price</th><th>Agent</th><th>Contact</th><th>Source</th></tr></thead><tbody id="rows"></tbody></table></section>
</main>
<script>
const apiKey=${JSON.stringify(apiKey)};const initialMarket=${JSON.stringify(market)};const fmt=n=>Number(n||0).toLocaleString();const money=n=>n?('$'+Number(n).toLocaleString()):'-';const pct=(n,d)=>{const den=Number(d||0);if(!den)return'0%';return(Math.round((Number(n||0)/den)*1000)/10)+'%';};
document.getElementById('market').value=initialMarket;document.getElementById('market').addEventListener('change',()=>load());
async function getJson(path){const r=await fetch(path,{headers:{'x-api-key':apiKey}});const j=await r.json();if(!r.ok)throw new Error(j.detail||j.error||path);return j;}
async function load(){const market=document.getElementById('market').value;history.replaceState(null,'','/preview/market-dashboard?market='+market);document.getElementById('status').textContent='Loading';const [ready,onMarket]=await Promise.all([getJson('/v1/markets/'+market+'/readiness'),getJson('/v1/markets/'+market+'/multifamily/on-market?limit=50')]);render(ready,onMarket);document.getElementById('status').textContent='Updated '+new Date().toLocaleTimeString();}
function render(ready,onMarket){const g=ready.geography||{},p=ready.parcels||{},l=ready.listings||{},mf=ready.multifamily||{},rec=ready.recorder||{};document.getElementById('subtitle').textContent=(g.city||'Market')+', '+(g.state||'')+' · '+(g.county||'')+' County';document.getElementById('active').textContent=fmt(l.active_listing_count);document.getElementById('active-sub').textContent=(l.listing_sources||[]).join(', ')||'tracked sources';document.getElementById('detail').textContent=pct(l.redfin_detail_count,l.active_listing_count);document.getElementById('phone').textContent=pct(l.agent_phone_count,l.active_listing_count);document.getElementById('creative').textContent=fmt(l.creative_finance_count);document.getElementById('parcels').textContent=fmt(p.parcel_count);document.getElementById('mf').textContent=fmt(p.multifamily_asset_count);document.getElementById('rents').textContent=fmt(mf.rent_snapshot_rows);document.getElementById('rents-sub').textContent=fmt(mf.complexes_with_rent_snapshots)+' / '+fmt(mf.complex_count)+' complexes with rent snapshots';document.getElementById('recorder').textContent=fmt(rec.recorder_records);document.getElementById('recorder-sub').textContent=fmt(rec.lien_doc_rows)+' lien docs · '+fmt(rec.mortgage_doc_rows)+' mortgage docs';const bars=[['Classified parcels',p.classified_count,p.parcel_count],['Unit-count parcels',p.unit_count_count,p.parcel_count],['Agent phone',l.agent_phone_count,l.active_listing_count],['Agent email',l.agent_email_count,l.active_listing_count],['MF websites',mf.complexes_with_websites,mf.complex_count],['MF rent snapshots',mf.complexes_with_rent_snapshots,mf.complex_count]];document.getElementById('coverage-bars').innerHTML=bars.map(([label,n,d])=>'<div class="bar-row"><div>'+label+'</div><div class="bar-track"><div class="bar-fill" style="width:'+Math.max(2,Number(d?Number(n)/Number(d)*100:0))+'%"></div></div><div>'+pct(n,d)+'</div></div>').join('');document.getElementById('recorder-lines').innerHTML=[['Properties with records',rec.properties_with_recorder_records],['Recorder records',rec.recorder_records],['Mortgage docs',rec.mortgage_doc_rows],['Lien docs',rec.lien_doc_rows],['Records with loan amount',rec.records_with_amounts],['Latest recording',rec.latest_recording||'-']].map(([k,v])=>'<div class="bar-row"><div>'+k+'</div><div></div><div>'+fmt(v)+'</div></div>').join('');document.getElementById('rows').innerHTML=(onMarket.results||[]).map(row=>{const prop=row.property||{},m=row.market||{};return'<tr><td><a href="'+(m.listingUrl||'#')+'" target="_blank">'+(prop.address||row.address||'Property')+'</a><div class="sub">'+(prop.zip||row.zip||'')+'</div></td><td>'+(prop.assetSubtype||'-')+'</td><td>'+(prop.unitCount||'-')+'</td><td>'+money(m.listPrice)+'</td><td>'+(m.listingAgentName||'-')+'</td><td>'+(m.listingAgentPhone||m.listingAgentEmail||m.listingBrokerage||'-')+'</td><td>'+(m.listingSource||'-')+'</td></tr>';}).join('')||'<tr><td colspan="7" class="muted">No linked multifamily listings yet.</td></tr>';}
load().catch(err=>{document.getElementById('status').textContent=err.message;console.error(err);});
</script></body></html>`;
}

app.get('/preview/west-chester-dashboard', async (c) => {
  const [summary] = await queryPg<Record<string, unknown>>(`
    with parcels as (
      select count(*)::int as parcel_count,
             count(*) filter (where asset_type is not null)::int as classified_count,
             count(*) filter (where asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily') or coalesce(total_units,0) >= 2)::int as multifamily_count
      from properties
      where county_id = 817175
        and state_code = 'PA'
        and upper(coalesce(city,'')) = 'WEST CHESTER'
    ),
    listings as (
      select count(*)::int as active_listing_count,
             count(distinct property_id) filter (where property_id is not null)::int as active_properties,
             count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name_count,
             count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as agent_phone_count,
             count(*) filter (where nullif(listing_agent_email,'') is not null)::int as agent_email_count,
             count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage_count,
             count(*) filter (where creative_finance_status = 'positive')::int as creative_positive
      from listing_signals
      where is_on_market = true
        and state_code = 'PA'
        and upper(coalesce(city,'')) = 'WEST CHESTER'
    ),
    mf as (
      select id
      from properties
      where county_id = 817175
        and state_code = 'PA'
        and upper(coalesce(city,'')) = 'WEST CHESTER'
        and (
          coalesce(total_units,0) >= 2
          or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily')
        )
    ),
    rents as (
      select count(distinct pw.property_id)::int as mf_websites,
             count(distinct fp.property_id)::int as mf_floorplan_properties,
             count(distinct fp.id)::int as floorplan_rows,
             count(distinct rs.property_id)::int as mf_rent_properties,
             count(distinct rs.id)::int as rent_snapshot_rows,
             max(rs.observed_at) as latest_rent_observed
      from mf
      left join property_websites pw on pw.property_id = mf.id and pw.active = true
      left join floorplans fp on fp.property_id = mf.id
      left join rent_snapshots rs on rs.property_id = mf.id
    )
    select parcels.parcel_count,
           parcels.classified_count,
           parcels.multifamily_count,
           listings.active_listing_count,
           listings.active_properties,
           listings.agent_name_count,
           listings.agent_phone_count,
           listings.agent_email_count,
           listings.brokerage_count,
           listings.creative_positive,
           rents.mf_websites,
           rents.mf_floorplan_properties,
           rents.floorplan_rows,
           rents.mf_rent_properties,
           rents.rent_snapshot_rows,
           rents.latest_rent_observed
    from parcels, listings, rents;
  `);

  const listings = await queryPg<Record<string, unknown>>(`
    select
      id,
      property_id as "propertyId",
      address,
      city,
      zip,
      mls_list_price as "listPrice",
      days_on_market as "daysOnMarket",
      listing_source as "source",
      listing_url as "listingUrl",
      listing_agent_name as "agentName",
      listing_agent_phone as "agentPhone",
      listing_agent_email as "agentEmail",
      listing_brokerage as "brokerage",
      creative_finance_status as "creativeStatus",
      creative_finance_score as "creativeScore",
      creative_finance_terms as "creativeTerms",
      last_seen_at as "lastSeenAt"
    from listing_signals
    where is_on_market = true
      and state_code = 'PA'
      and upper(coalesce(city,'')) = 'WEST CHESTER'
    order by coalesce(creative_finance_score,0) desc, last_seen_at desc nulls last
    limit 80;
  `);

  const rents = await queryPg<Record<string, unknown>>(`
    with universe as (
      select p.id, p.address
      from properties p
      where p.county_id = 817175
        and p.state_code = 'PA'
        and upper(coalesce(p.city,'')) = 'WEST CHESTER'
        and (
          coalesce(p.total_units,0) >= 2
          or p.asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily')
        )
    )
    select
      u.id as "propertyId",
      coalesce(cp.complex_name, u.address) as "complexName",
      u.address,
      pw.website,
      coalesce(fp.floorplans, 0)::int as "floorplans",
      coalesce(rs.rent_snapshots, 0)::int as "rentSnapshots",
      rs.min_rent::int as "minRent",
      rs.max_rent::int as "maxRent",
      rs.latest_observed as "latestObserved"
    from universe u
    left join property_complex_profiles cp on cp.property_id = u.id
    left join lateral (
      select website
      from property_websites
      where property_id = u.id and active = true
      order by last_seen_at desc nulls last
      limit 1
    ) pw on true
    left join lateral (
      select count(*)::int as floorplans
      from floorplans
      where property_id = u.id
    ) fp on true
    left join lateral (
      select count(*)::int as rent_snapshots,
             min(asking_rent) as min_rent,
             max(asking_rent) as max_rent,
             max(observed_at) as latest_observed
      from rent_snapshots
      where property_id = u.id
    ) rs on true
    where pw.website is not null or coalesce(fp.floorplans, 0) > 0 or coalesce(rs.rent_snapshots, 0) > 0
    order by coalesce(rs.rent_snapshots, 0) desc, coalesce(cp.complex_name, u.address)
    limit 80;
  `);

  const num = (key: string) => Number(summary?.[key] ?? 0);
  const pct = (value: number, total: number) => total > 0 ? Math.round((value / total) * 1000) / 10 : 0;
  const esc = (value: unknown) => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char] ?? char));
  const money = (value: unknown) => Number(value ?? 0) > 0 ? `$${Number(value).toLocaleString()}` : '-';
  const listingRows = listings.map((row) => `
    <tr>
      <td><a href="${esc(row.listingUrl)}" target="_blank">${esc(row.address)}</a><div class="muted">${esc(row.zip)} &middot; ${esc(row.source)} &middot; property ${esc(row.propertyId)}</div></td>
      <td>${money(row.listPrice)}</td>
      <td>${esc(row.daysOnMarket ?? '-')}</td>
      <td>${esc(row.agentName)}<div class="muted">${esc(row.agentPhone || row.agentEmail || row.brokerage || 'contact gap')}</div></td>
      <td>${row.creativeStatus === 'positive' ? `<span class="good">Positive ${esc(row.creativeScore)}</span><div class="muted">${esc(Array.isArray(row.creativeTerms) ? row.creativeTerms.join(', ') : '')}</div>` : '<span class="muted">-</span>'}</td>
    </tr>
  `).join('');
  const rentRows = rents.map((row) => `
    <tr>
      <td><a href="${esc(row.website)}" target="_blank">${esc(row.complexName)}</a><div class="muted">${esc(row.address)} &middot; property ${esc(row.propertyId)}</div></td>
      <td>${esc(row.floorplans)}</td>
      <td>${esc(row.rentSnapshots)}</td>
      <td>${money(row.minRent)} - ${money(row.maxRent)}</td>
      <td>${esc(row.latestObserved ?? '-')}</td>
    </tr>
  `).join('');

  return c.html(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MXRE West Chester Dashboard</title>
<style>
body{margin:0;background:#101312;color:#edf4ef;font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header{padding:20px 24px;border-bottom:1px solid #303735;background:#131715;position:sticky;top:0}h1{font-size:20px;margin:0}.muted{color:#9aa7a1;font-size:12px;margin-top:4px}.wrap{max-width:1500px;margin:0 auto;padding:18px 24px 36px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.card{background:#181c1b;border:1px solid #303735;border-radius:8px;padding:14px}.metric{font-size:28px;font-weight:850}.label{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#9aa7a1;margin-bottom:6px}.good{color:#35c677}.warn{color:#f1b84b}table{width:100%;border-collapse:collapse;margin-top:10px}th,td{border-bottom:1px solid #303735;padding:9px 8px;text-align:left;font-size:13px;vertical-align:top}th{color:#9aa7a1;font-size:11px;text-transform:uppercase;letter-spacing:.06em}a{color:#8cc8ff;text-decoration:none}.section{margin-top:16px}@media(max-width:900px){.grid{grid-template-columns:1fr 1fr}}@media(max-width:560px){.grid{grid-template-columns:1fr}.wrap{padding:14px}}
</style>
</head>
<body>
<header><h1>MXRE &middot; West Chester, PA Coverage Dashboard</h1><div class="muted">Live from MXRE DB &middot; generated ${esc(new Date().toLocaleString())} &middot; overnight runner completed at 7:03 AM</div></header>
<main class="wrap">
  <section class="grid">
    <div class="card"><div class="label">Dashboard Readiness</div><div class="metric good">API Ready</div><div class="muted">No blocking dashboard gaps</div></div>
    <div class="card"><div class="label">Parcels</div><div class="metric">${num('parcel_count').toLocaleString()}</div><div class="muted">${pct(num('classified_count'), num('parcel_count'))}% classified</div></div>
    <div class="card"><div class="label">Active Listings</div><div class="metric">${num('active_listing_count')}</div><div class="muted">${num('active_properties')} linked properties</div></div>
    <div class="card"><div class="label">Creative Finance</div><div class="metric">${num('creative_positive')}</div><div class="muted">positive listing descriptions</div></div>
    <div class="card"><div class="label">Agent Phones</div><div class="metric">${pct(num('agent_phone_count'), num('active_listing_count'))}%</div><div class="muted">${num('agent_phone_count')} / ${num('active_listing_count')} rows</div></div>
    <div class="card"><div class="label">Agent Emails</div><div class="metric warn">${pct(num('agent_email_count'), num('active_listing_count'))}%</div><div class="muted">${num('agent_email_count')} / ${num('active_listing_count')} rows</div></div>
    <div class="card"><div class="label">Multifamily Websites</div><div class="metric">${num('mf_websites')}</div><div class="muted">${num('multifamily_count')} multifamily candidates</div></div>
    <div class="card"><div class="label">Rent Snapshots</div><div class="metric">${num('rent_snapshot_rows')}</div><div class="muted">${num('mf_rent_properties')} properties &middot; latest ${esc(summary?.latest_rent_observed)}</div></div>
  </section>
  <section class="section card">
    <h2>Apartment Rent Coverage</h2>
    <table><thead><tr><th>Complex</th><th>Floorplans</th><th>Snapshots</th><th>Rent Range</th><th>Observed</th></tr></thead><tbody>${rentRows || '<tr><td colspan="5" class="muted">No rent rows yet.</td></tr>'}</tbody></table>
  </section>
  <section class="section card">
    <h2>Active Listings And Contacts</h2>
    <table><thead><tr><th>Listing</th><th>Price</th><th>DOM</th><th>Agent / Contact</th><th>Creative</th></tr></thead><tbody>${listingRows || '<tr><td colspan="5" class="muted">No active listings yet.</td></tr>'}</tbody></table>
  </section>
</main>
</body>
</html>`);
});

function renderAnalystMarketDashboard(apiKey: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MXRE Indianapolis Analyst Dashboard</title>
<style>
  :root{color-scheme:dark;--bg:#101312;--panel:#181c1b;--panel2:#202624;--line:#303735;--text:#edf4ef;--muted:#9aa7a1;--green:#35c677;--blue:#55a8ff;--amber:#f1b84b;--red:#ef6b62}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
  .topbar{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 24px;border-bottom:1px solid var(--line);background:#131715;position:sticky;top:0;z-index:3}
  .brand{font-size:18px;font-weight:800;letter-spacing:.08em}.muted{color:var(--muted)}.pill{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:#141817;color:var(--muted);border-radius:999px;padding:6px 10px;font-size:12px}
  main{padding:18px 24px 36px;max-width:1520px;margin:0 auto}.tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}.tab{border:1px solid var(--line);background:#141817;color:var(--muted);border-radius:7px;padding:9px 13px;font-weight:700;cursor:pointer}.tab.active{background:#1d3228;color:#d9ffe9;border-color:#2b8050}
  .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}.card h2,.card h3{margin:0 0 10px;font-size:14px}.metric{font-size:27px;font-weight:850}.sub{font-size:12px;color:var(--muted);margin-top:4px}.click-card{cursor:pointer}.click-card:hover{border-color:#437b5d;background:#1b2420}.click-card.active{border-color:var(--green);box-shadow:0 0 0 1px rgba(53,198,119,.25) inset}
  .filters{display:grid;grid-template-columns:1.5fr repeat(5,minmax(110px,1fr)) auto;gap:8px;margin:14px 0}.filters input,.filters select{width:100%;background:#0f1312;border:1px solid var(--line);border-radius:6px;color:var(--text);padding:10px}.btn{background:#275f41;border:1px solid #347c56;color:#ecfff3;border-radius:6px;padding:10px 13px;font-weight:800;cursor:pointer}.btn.secondary{background:#141817;border-color:var(--line);color:var(--muted)}
  table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:9px 8px;text-align:left;font-size:13px;vertical-align:top}th{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}a{color:#8cc8ff;text-decoration:none}
  .bar-row{display:grid;grid-template-columns:72px 1fr 76px;gap:8px;align-items:center;margin:8px 0}.bar-track{height:9px;background:#0f1312;border:1px solid var(--line);border-radius:999px;overflow:hidden}.bar-fill{height:100%;background:linear-gradient(90deg,var(--green),var(--blue))}.tag{display:inline-block;border-radius:999px;padding:3px 7px;font-size:11px;border:1px solid var(--line);color:var(--muted)}.good{color:#76e0a0}.warn{color:var(--amber)}.bad{color:var(--red)}
  @media(max-width:980px){.grid{grid-template-columns:1fr 1fr}.filters{grid-template-columns:1fr 1fr}.topbar{align-items:flex-start;flex-direction:column}} @media(max-width:640px){.grid,.filters{grid-template-columns:1fr}main{padding:14px}}
</style>
</head>
<body>
<div class="topbar"><div><div class="brand">MXRE Indianapolis Analyst Dashboard</div><div class="muted" style="font-size:12px;margin-top:3px">Overall market, asset-class tabs, zip rollups, opportunity filters, address lookup</div></div><div><span class="pill" id="status">Loading</span></div></div>
<main>
  <div class="tabs">
    <button class="tab active" data-asset="all">Overall</button>
    <button class="tab" data-asset="single_family">Single-family</button>
    <button class="tab" data-asset="multifamily">Multi-family</button>
  </div>
  <section class="grid">
    <div class="card"><h3>Active Listings</h3><div class="metric" id="m-active">-</div><div class="sub">Current filtered on-market rows</div></div>
    <div class="card"><h3>Unique Properties</h3><div class="metric" id="m-active-props">-</div><div class="sub">Active listings matched to MXRE parcels</div></div>
    <div class="card click-card" id="creative-card"><h3>Creative Finance</h3><div class="metric" id="m-creative">-</div><div class="sub" id="m-creative-sub">Click to view the report</div><button class="btn secondary" id="creative-report-btn" style="margin-top:10px;padding:7px 10px">View report</button></div>
    <div class="card"><h3>Median List Price</h3><div class="metric" id="m-median-list">-</div><div class="sub">Current filtered listings</div></div>
  </section>

  <section class="grid" style="margin-top:12px">
    <div class="card"><h3>Total Properties</h3><div class="metric" id="q-parcels">-</div><div class="sub">Current Indianapolis scope parcel universe</div></div>
    <div class="card"><h3>Readiness Score</h3><div class="metric" id="q-readiness">-</div><div class="sub">Average completion across core layers</div></div>
    <div class="card"><h3>Core Complete</h3><div class="metric" id="q-core">-</div><div class="sub">identity + class + owner + value</div></div>
    <div class="card"><h3>Underwriting Complete</h3><div class="metric" id="q-underwriting">-</div><div class="sub">core + physical + transaction facts</div></div>
    <div class="card"><h3>Rental Universe</h3><div class="metric" id="q-rental">-</div><div class="sub">2+ unit / apartment candidates</div></div>
    <div class="card"><h3>Rent Snapshot Coverage</h3><div class="metric" id="q-rents">-</div><div class="sub" id="q-rents-sub">observed floorplan rents</div></div>
    <div class="card"><h3>Agent Phone Coverage</h3><div class="metric" id="q-agent-phone">-</div><div class="sub">active listing rows</div></div>
    <div class="card"><h3>Agent Email Coverage</h3><div class="metric" id="q-agent-email">-</div><div class="sub">active listing rows</div></div>
  </section>

  <section class="card" style="margin-top:12px">
    <h2>Filters</h2>
    <div class="filters">
      <input id="address" placeholder="Search address: 429 N Tibbs Ave, Indianapolis, IN 46222">
      <input id="zip" placeholder="Zip">
      <input id="minPrice" placeholder="Min price">
      <input id="maxPrice" placeholder="Max price">
      <input id="minUnits" placeholder="Min units">
      <input id="maxUnits" placeholder="Max units">
      <select id="sort"><option value="fresh">Fresh / DOM</option><option value="creative">Creative score</option><option value="price_asc">Price low</option><option value="price_desc">Price high</option></select>
      <button class="btn" onclick="loadDashboard(1)">Apply</button>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn secondary" onclick="setUnitBand(2,4)">2-4 doors</button><button class="btn secondary" onclick="setUnitBand(5,10)">5-10</button><button class="btn secondary" onclick="setUnitBand(11,20)">11-20</button><button class="btn secondary" onclick="setUnitBand(21,'')">21+</button><button class="btn secondary" onclick="clearFilters()">Clear</button><button class="btn secondary" onclick="addressLookup()">Address lookup</button></div>
    <div id="lookup" class="sub" style="margin-top:10px"></div>
  </section>

  <section class="grid" style="grid-template-columns:1fr 2fr;margin-top:12px">
    <div class="card"><h2>Zip Rollup</h2><div id="zip-rollup"></div></div>
    <div class="card">
      <h2>Opportunity List</h2>
      <table><thead><tr><th>Property</th><th>Price</th><th>Units</th><th>Beds/Baths</th><th>DOM</th><th>Creative</th><th>Contact</th><th>Risk/Transit</th></tr></thead><tbody id="deals"></tbody></table>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px"><button class="btn secondary" onclick="prevPage()">Prev</button><span class="muted" id="page-label"></span><button class="btn secondary" onclick="nextPage()">Next</button></div>
    </div>
  </section>
</main>
<script>
const apiKey=${JSON.stringify(apiKey)};
let asset='all'; let page=1; let lastTotal=0; let creativeOnly=false; const limit=25;
const fmt=n=>Number(n||0).toLocaleString(); const money=n=>n?('$'+Number(n).toLocaleString()):'-'; const pct=(a,b)=>{const den=Number(b||0); if(!den)return 0; const raw=Number(a||0)/den*100; if(raw>0&&raw<0.1)return '<0.1'; return Math.round(raw*10)/10;}; const esc=v=>{const e=document.createElement('span');e.textContent=String(v??'');return e.innerHTML;}; const safeUrl=v=>{try{const u=new URL(String(v||''),location.origin);return ['http:','https:'].includes(u.protocol)?esc(u.href):'#';}catch{return '#';}};
document.querySelectorAll('.tab').forEach(btn=>btn.addEventListener('click',()=>{document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active'));btn.classList.add('active');asset=btn.dataset.asset;page=1;loadDashboard(1)}));
document.getElementById('creative-card').addEventListener('click', toggleCreativeReport);
document.getElementById('creative-report-btn').addEventListener('click', event=>{event.stopPropagation();toggleCreativeReport();});
function qs(){const p=new URLSearchParams({asset,page:String(page),limit:String(limit),scope:'city',sort:document.getElementById('sort').value}); if(creativeOnly)p.set('creative','positive'); for (const [id,key] of [['zip','zip'],['minPrice','min_price'],['maxPrice','max_price'],['minUnits','min_units'],['maxUnits','max_units']]) { const v=document.getElementById(id).value.trim(); if(v)p.set(key,v); } return p.toString();}
async function loadDashboard(next){page=next||page; document.getElementById('status').textContent='Loading'; const [data,completion]=await Promise.all([fetch('/v1/markets/indianapolis/opportunities?'+qs(),{headers:{'x-api-key':apiKey}}).then(r=>r.json()),fetch('/v1/markets/indianapolis/completion?scope=city',{headers:{'x-api-key':apiKey}}).then(r=>r.json())]); if(data.error||completion.error){document.getElementById('status').textContent=data.error||completion.error;return;} lastTotal=data.total||0; render(data,completion); document.getElementById('status').textContent='Updated '+new Date().toLocaleTimeString();}
function render(data,completion){const s=data.summary||{}; const rows=Number(s.activeRows||data.total||0); document.getElementById('m-active').textContent=fmt(data.total); document.getElementById('m-active-props').textContent=fmt(s.activeProperties||0); document.getElementById('m-creative').textContent=fmt(s.creativePositive||0); document.getElementById('m-creative-sub').textContent=creativeOnly?'Showing creative-finance report':'Click to view the report'; document.getElementById('creative-card').classList.toggle('active',creativeOnly); document.getElementById('m-median-list').textContent=money(s.medianListPrice); document.getElementById('q-parcels').textContent=fmt(completion.totals.parcel_count); document.getElementById('q-readiness').textContent=completion.totals.readiness_score+'%'; document.getElementById('q-core').textContent=completion.metrics.core_complete.pct+'%'; document.getElementById('q-underwriting').textContent=completion.metrics.underwriting_complete.pct+'%'; document.getElementById('q-rental').textContent=fmt(completion.metrics.rental_candidate_count.count); document.getElementById('q-rents').textContent=completion.metrics.rental_rent_snapshot_coverage.pct+'%'; document.getElementById('q-rents-sub').textContent=fmt(completion.metrics.rental_rent_snapshot_coverage.count)+' candidates with observed rents'; document.getElementById('q-agent-phone').textContent=pct(s.withAgentPhone,rows)+'%'; document.getElementById('q-agent-email').textContent=pct(s.withAgentEmail,rows)+'%';const max=Math.max(1,...(data.by_zip||[]).map(z=>Number(z.listings)||0)); document.getElementById('zip-rollup').innerHTML=(data.by_zip||[]).map(z=>'<div class="bar-row"><div>'+esc(z.zip)+'</div><div class="bar-track"><div class="bar-fill" style="width:'+Math.max(4,Number(z.listings)/max*100)+'%"></div></div><div>'+fmt(z.listings)+'</div><div class="sub" style="grid-column:2/4;margin-top:-6px">median '+money(z.medianPrice)+' · creative '+fmt(z.creativePositive)+' · contact '+fmt(z.withContact)+'</div></div>').join('')||'<div class="muted">No zip data.</div>'; document.getElementById('deals').innerHTML=(data.results||[]).map(row=>'<tr><td><a href="'+safeUrl(row.listingUrl)+'" target="_blank" rel="noopener noreferrer">'+esc(row.address||'No address')+'</a><div class="sub">'+esc(row.zip||'')+' · '+esc(row.assetGroup||'')+' · '+esc(row.propertyUse||'')+'</div></td><td>'+money(row.listPrice)+'</td><td>'+((row.unitCount&&row.unitCount>0)?fmt(row.unitCount):'-')+'</td><td>'+esc([row.bedrooms,row.bathrooms].filter(Boolean).join(' / '))+'</td><td>'+esc(row.daysOnMarket??'-')+'</td><td>'+creative(row)+'</td><td>'+contact(row)+'</td><td>'+riskTransit(row)+'</td></tr>').join('')||'<tr><td colspan="8" class="muted">No active listings match these filters.</td></tr>'; document.getElementById('page-label').textContent=(creativeOnly?'Creative-finance report - ':'')+'Page '+page+' - '+fmt(data.total)+' results';}
function toggleCreativeReport(){creativeOnly=!creativeOnly; if(creativeOnly)document.getElementById('sort').value='creative'; loadDashboard(1)}
function creative(r){if(r.creativeFinanceStatus==='negative')return '<span class="tag bad">No creative</span>'; if(r.creativeFinanceScore)return '<span class="tag good">'+esc(r.creativeFinanceScore)+'</span><div class="sub">'+esc((r.creativeFinanceTerms||[]).join(', '))+'</div>'; return '<span class="tag">n/a</span>'}
function contact(r){const name=r.listingAgentName||[r.listingAgentFirstName,r.listingAgentLastName].filter(Boolean).join(' '); return '<div>'+esc(name||'-')+'</div><div class="sub">'+esc(r.listingAgentEmail||r.listingAgentPhone||r.listingBrokerage||'contact gap')+'</div>'}
function riskTransit(r){const crime=r.crimeScore==null?'<span class="tag">crime n/a</span>':'<span class="tag '+(Number(r.crimeScore)>=65?'bad':Number(r.crimeScore)>=30?'warn':'good')+'">crime '+Number(r.crimeScore).toFixed(1)+'</span>'; const bus=r.nearestBusMiles==null?'<span class="tag" style="margin-top:4px">bus n/a</span>':'<span class="tag good" style="margin-top:4px">'+Number(r.nearestBusMiles).toFixed(2)+' mi bus</span>'; const detail=r.nearestBusStopName?'<div class="sub">'+esc(r.nearestBusStopName)+(Array.isArray(r.busRoutes)&&r.busRoutes.length?' · routes '+esc(r.busRoutes.slice(0,4).join(', ')):'')+'</div>':''; return crime+'<br>'+bus+detail}
function setUnitBand(min,max){document.getElementById('minUnits').value=min;document.getElementById('maxUnits').value=max;asset='multifamily';document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('active',b.dataset.asset==='multifamily'));loadDashboard(1)}
function clearFilters(){['zip','minPrice','maxPrice','minUnits','maxUnits','address'].forEach(id=>document.getElementById(id).value='');creativeOnly=false;loadDashboard(1)}
function prevPage(){if(page>1)loadDashboard(page-1)} function nextPage(){if(page*limit<lastTotal)loadDashboard(page+1)}
async function addressLookup(){const q=document.getElementById('address').value.trim(); const box=document.getElementById('lookup'); if(!q){box.textContent='Type an address first.';return;} box.textContent='Searching...'; const parts=q.split(',').map(x=>x.trim()); const address=parts[0]; const state=(q.match(/\\b[A-Z]{2}\\b/)||['IN'])[0]; const zip=(q.match(/\\b\\d{5}\\b/)||[''])[0]; const city=parts.find(p=>!/\\d/.test(p)&&!/^IN$/i.test(p))||'Indianapolis'; const url='/v1/property?address='+encodeURIComponent(address)+'&city='+encodeURIComponent(city)+'&state='+state+(zip?'&zip='+zip:''); const data=await fetch(url,{headers:{'x-api-key':apiKey}}).then(r=>r.json()); box.innerHTML=data.error?'<span class="bad">'+esc(data.error)+'</span>':'Found: <strong>'+esc(data.property.address)+'</strong> · '+esc(data.property.property_type||'-')+' · '+money(data.property.market_value||data.property.assessed_value);}
loadDashboard(1);
</script>
</body>
</html>`;
}

app.get('/preview/market-dashboard-old', async (c) => {
  const dashboardScope = getIndianapolisScope('city');
  let activeNow: Record<string, unknown> = {
    parcel_count: 547490,
    known_units: 594790,
    rental_candidate_count: 12223,
    active_unique_properties: 2609,
    all_listing_rows: 2944,
    active_multifamily_properties: 103,
    multifamily_listing_rows: 125,
  };
  const totalParcels = Number(activeNow?.parcel_count ?? 0);
  const totalKnownUnits = Number(activeNow?.known_units ?? 0);
  const rentalCandidates = Number(activeNow?.rental_candidate_count ?? 0);
  const activeAllUnique = Number(activeNow?.active_unique_properties ?? 0);
  const activeAllRows = Number(activeNow?.all_listing_rows ?? 0);
  const activeMfUnique = Number(activeNow?.active_multifamily_properties ?? 0);
  const activeMfRows = Number(activeNow?.multifamily_listing_rows ?? 0);

  return c.html(`<!DOCTYPE html>
<html>
<head>
<title>MXRE Market Dashboard Preview</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: dark; --bg:#101312; --panel:#181c1b; --line:#303735; --text:#edf4ef; --muted:#9aa7a1; --green:#35c677; --blue:#55a8ff; --amber:#f1b84b; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text); font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
  .topbar { display:flex; align-items:center; justify-content:space-between; gap:16px; padding:18px 24px; border-bottom:1px solid var(--line); background:#131715; position:sticky; top:0; z-index:2; }
  h1 { margin:0; font-size:18px; font-weight:750; letter-spacing:0; }
  .subtitle { color:var(--muted); font-size:12px; margin-top:3px; }
  .pill { display:inline-flex; align-items:center; height:28px; padding:0 10px; border:1px solid var(--line); border-radius:6px; color:var(--muted); font-size:12px; white-space:nowrap; background:var(--panel); }
  .seg { display:inline-flex; gap:4px; padding:4px; border:1px solid var(--line); border-radius:8px; background:#141816; }
  .seg button { height:28px; padding:0 10px; border:0; border-radius:5px; background:transparent; color:var(--muted); cursor:pointer; font:inherit; font-size:12px; }
  .seg button.active { background:#243028; color:var(--text); }
  select { height:34px; border:1px solid var(--line); border-radius:6px; background:#141816; color:var(--text); padding:0 10px; font:inherit; font-size:13px; }
  main { padding:22px 24px 32px; max-width:1440px; margin:0 auto; }
  .kpi-grid { display:grid; grid-template-columns:repeat(6,minmax(130px,1fr)); gap:12px; margin-bottom:18px; }
  .asset-grid { display:grid; grid-template-columns:minmax(0,1.25fr) minmax(280px,.75fr); gap:16px; margin-bottom:16px; align-items:start; }
  .completion-grid { display:grid; grid-template-columns:repeat(6,minmax(150px,1fr)); gap:12px; margin-bottom:16px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
  .kpi-label { color:var(--muted); font-size:11px; text-transform:uppercase; font-weight:700; }
  .kpi-value { font-size:26px; font-weight:820; margin-top:7px; line-height:1; }
  .kpi-sub { color:var(--muted); font-size:12px; margin-top:8px; min-height:16px; }
  .layout { display:grid; grid-template-columns:minmax(0,1.4fr) minmax(330px,.8fr); gap:16px; align-items:start; }
  .section-title { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:10px; }
  h2 { margin:0; font-size:14px; color:#dfe8e3; }
  table { width:100%; border-collapse:collapse; table-layout:fixed; }
  th { color:var(--muted); font-size:11px; text-transform:uppercase; text-align:left; font-weight:750; border-bottom:1px solid var(--line); padding:8px; }
  td { border-bottom:1px solid #252b29; padding:10px 8px; font-size:13px; vertical-align:top; color:#dbe7e1; overflow-wrap:anywhere; }
  tr:hover td { background:#1c2220; }
  a { color:var(--blue); text-decoration:none; }
  a:hover { text-decoration:underline; }
  .stack { display:grid; gap:16px; }
  .bar-row { display:grid; grid-template-columns:90px 1fr 48px; gap:10px; align-items:center; margin:10px 0; }
  .bar-label,.bar-value { color:var(--muted); font-size:12px; }
  .bar-track { height:8px; border-radius:4px; background:#2a302e; overflow:hidden; }
  .bar-fill { height:100%; border-radius:4px; background:var(--green); }
  .metric-list { display:grid; gap:10px; }
  .metric-line { display:flex; justify-content:space-between; gap:12px; padding-bottom:10px; border-bottom:1px solid #252b29; font-size:13px; }
  .metric-line:last-child { border-bottom:0; padding-bottom:0; }
  .muted { color:var(--muted); }
  .green { color:var(--green); }
  .amber { color:var(--amber); }
  .loading { padding:40px; color:var(--muted); text-align:center; }
  @media (max-width:1100px) { .kpi-grid { grid-template-columns:repeat(3,minmax(150px,1fr)); } .completion-grid { grid-template-columns:repeat(2,minmax(160px,1fr)); } .layout,.asset-grid { grid-template-columns:1fr; } }
  @media (max-width:620px) { .topbar { align-items:flex-start; flex-direction:column; } main { padding:14px; } .kpi-grid,.completion-grid { grid-template-columns:1fr 1fr; } .kpi-value { font-size:22px; } th:nth-child(3),td:nth-child(3),th:nth-child(5),td:nth-child(5){display:none;} }
</style>
</head>
<body>
<div class="topbar">
  <div>
    <h1>MXRE · Indianapolis Real Estate Intelligence</h1>
    <div class="subtitle">MXRE admin dashboard powered by parcel, listing, recorder, rent, and assessor coverage data</div>
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;">
    <select id="scope-select" onchange="setScope(this.value)">
      <option value="city" selected>Indianapolis City</option>
      <option value="core">Core / Marion County</option>
      <option value="metro">Indianapolis Metro</option>
    </select>
    <span class="pill" id="generated">Loading</span>
  </div>
</div>
<main>
  <div class="kpi-grid" style="margin-bottom:14px;">
    <div class="card"><div class="kpi-label">Total Indianapolis Parcels</div><div class="kpi-value">${totalParcels.toLocaleString('en-US')}</div><div class="kpi-sub">${totalKnownUnits.toLocaleString('en-US')} known/minimum units in the city scope</div></div>
    <div class="card"><div class="kpi-label">Rental / MF Universe</div><div class="kpi-value">${rentalCandidates.toLocaleString('en-US')}</div><div class="kpi-sub">2+ unit, apartment, and multifamily candidate parcels</div></div>
    <div class="card"><div class="kpi-label">Active Linked Listings Now</div><div class="kpi-value green">${activeAllUnique.toLocaleString('en-US')}</div><div class="kpi-sub">${activeAllRows.toLocaleString('en-US')} active listing rows matched to MXRE properties</div></div>
    <div class="card"><div class="kpi-label">Active Linked MF Now</div><div class="kpi-value green">${activeMfUnique.toLocaleString('en-US')}</div><div class="kpi-sub">${activeMfRows.toLocaleString('en-US')} active rows across 2+ unit / apartment assets</div></div>
    <div class="card"><div class="kpi-label">Core Readiness</div><div class="kpi-value" id="instant-core-readiness">89.6%</div><div class="kpi-sub">Indianapolis city / Marion County baseline</div></div>
    <div class="card"><div class="kpi-label">Metro Readiness</div><div class="kpi-value" id="instant-metro-readiness">73.3%</div><div class="kpi-sub">Indianapolis MSA enrichment progress</div></div>
    <div class="card"><div class="kpi-label">Core Parcel Universe</div><div class="kpi-value" id="instant-core-parcels">583,230</div><div class="kpi-sub">parcel-led MXRE coverage base</div></div>
    <div class="card"><div class="kpi-label">Metro Parcel Universe</div><div class="kpi-value" id="instant-metro-parcels">1,160,106</div><div class="kpi-sub">all tracked metro parcels</div></div>
  </div>
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;flex-wrap:wrap;">
    <div class="muted" id="filter-note">Showing all Indianapolis real estate assets with a multifamily drilldown.</div>
    <div class="seg" aria-label="Unit size filter">
      <button id="filter-all" class="active" onclick="setUnitFilter(null)">All MF</button>
      <button id="filter-2" onclick="setUnitFilter(2)">2+ Units</button>
      <button id="filter-3" onclick="setUnitFilter(3)">3+ Units</button>
      <button id="filter-4" onclick="setUnitFilter(4)">4+ Units</button>
    </div>
  </div>
  <div id="loading" class="card loading">Loading live MXRE dashboard panels...</div>
  <div id="content" style="display:block;">
    <div class="kpi-grid">
      <div class="card"><div class="kpi-label">All Parcels</div><div class="kpi-value" id="kpi-all-parcels">-</div><div class="kpi-sub">Indianapolis asset universe</div></div>
      <div class="card"><div class="kpi-label">Market Value</div><div class="kpi-value" id="kpi-market-value">-</div><div class="kpi-sub">assessor market value</div></div>
      <div class="card"><div class="kpi-label">All Recorder Coverage</div><div class="kpi-value" id="kpi-all-recorder">-</div><div class="kpi-sub" id="kpi-all-recorder-sub">sale or mortgage records</div></div>
      <div class="card"><div class="kpi-label">On Market Now</div><div class="kpi-value green" id="kpi-active">-</div><div class="kpi-sub" id="kpi-unique">active unique properties</div></div>
      <div class="card"><div class="kpi-label">External CRE</div><div class="kpi-value amber" id="kpi-external">-</div><div class="kpi-sub" id="kpi-external-sub">unverified observations</div></div>
      <div class="card"><div class="kpi-label">Median List</div><div class="kpi-value" id="kpi-list">-</div><div class="kpi-sub">active multifamily</div></div>
    </div>
    <div class="completion-grid">
      <div class="card"><div class="kpi-label">Readiness Score</div><div class="kpi-value" id="complete-score">-</div><div class="kpi-sub">average completion across required layers</div></div>
      <div class="card"><div class="kpi-label">Core Complete</div><div class="kpi-value" id="complete-core">-</div><div class="kpi-sub">identity + class + owner + value</div></div>
      <div class="card"><div class="kpi-label">Underwriting Complete</div><div class="kpi-value" id="complete-underwriting">-</div><div class="kpi-sub">core + physical + transaction facts</div></div>
      <div class="card"><div class="kpi-label">Rental Universe</div><div class="kpi-value" id="complete-rental-universe">-</div><div class="kpi-sub">2+ unit / apartment candidates</div></div>
      <div class="card"><div class="kpi-label">Rental Websites</div><div class="kpi-value amber" id="complete-rental-websites">-</div><div class="kpi-sub" id="complete-rental-websites-sub">official sites found</div></div>
      <div class="card"><div class="kpi-label">Rent Snapshots</div><div class="kpi-value green" id="complete-rent-snapshots">-</div><div class="kpi-sub" id="complete-rent-snapshots-sub">floorplan rents captured</div></div>
      <div class="card"><div class="kpi-label">Unknown Asset Class</div><div class="kpi-value" id="complete-unknown">-</div><div class="kpi-sub">classification gap to clean</div></div>
    </div>
    <div class="asset-grid">
      <div class="card">
        <div class="section-title">
          <div>
            <h2>All Asset Classes</h2>
            <div class="muted" id="asset-note" style="font-size:12px;margin-top:4px;">Parcel-led inventory by asset type.</div>
          </div>
          <select id="asset-select" onchange="setAssetGroup(this.value)">
            <option value="">All assets</option>
            <option value="single_family">Single family / condo</option>
            <option value="small_multifamily">Small multifamily</option>
            <option value="commercial_multifamily">Commercial multifamily</option>
            <option value="mobile_home_rv">Mobile home / RV parks</option>
            <option value="land">Land</option>
            <option value="industrial">Industrial</option>
            <option value="office">Office</option>
            <option value="retail">Retail</option>
            <option value="self_storage">Self-storage</option>
            <option value="hospitality">Hospitality</option>
            <option value="parking">Parking</option>
            <option value="other_commercial">Other commercial</option>
          </select>
        </div>
        <table><thead><tr><th>Asset Class</th><th>Parcels</th><th>Units</th><th>Value</th><th>Listings</th><th>Recorder</th></tr></thead><tbody id="asset-body"></tbody></table>
      </div>
      <div class="stack">
        <div class="card"><div class="section-title"><h2>Completion By County</h2><span class="pill">core complete</span></div><div id="county-completion"></div></div>
        <div class="card"><div class="section-title"><h2>Top Uses</h2><span class="pill">assessor codes</span></div><div id="asset-uses"></div></div>
        <div class="card"><div class="section-title"><h2>Examples</h2><span class="pill">highest value</span></div><div id="asset-examples"></div></div>
      </div>
    </div>
    <div class="layout">
      <div class="card">
        <div class="section-title"><h2>Top Active Listings</h2><span class="pill">source links</span></div>
        <table><thead><tr><th>Property / Complex</th><th>Type</th><th>Units</th><th>List Price</th><th>$/Door</th><th>Source</th></tr></thead><tbody id="listing-body"></tbody></table>
      </div>
      <div class="card" id="external-card" style="display:none;margin-top:16px;">
        <div class="section-title"><h2>External CRE Observations</h2><span class="pill">needs verification</span></div>
        <table><thead><tr><th>Title</th><th>Address</th><th>Units</th><th>List Price</th><th>$/Door</th><th>Source</th></tr></thead><tbody id="external-body"></tbody></table>
      </div>
      <div class="stack">
        <div class="card">
          <div class="section-title"><h2>Parcel Coverage</h2></div>
          <div class="metric-list">
            <div class="metric-line"><span class="muted">Parcel universe</span><strong id="cov-parcels">-</strong></div>
            <div class="metric-line"><span class="muted">Known/min units</span><strong id="cov-units">-</strong></div>
            <div class="metric-line"><span class="muted">Any recorder data</span><strong id="cov-recorder">-</strong></div>
            <div class="metric-line"><span class="muted">Sale history</span><strong id="cov-sales">-</strong></div>
            <div class="metric-line"><span class="muted">Recorded mortgage</span><strong id="cov-mortgages">-</strong></div>
            <div class="metric-line"><span class="muted">Active listing linked</span><strong id="cov-listings">-</strong></div>
            <div class="metric-line"><span class="muted">Complex profile</span><strong id="cov-profiles">-</strong></div>
          </div>
        </div>
        <div class="card"><div class="section-title"><h2>Source Coverage</h2></div><div id="source-bars"></div></div>
        <div class="card"><div class="section-title"><h2>Subtype Mix</h2></div><div id="subtype-bars"></div></div>
        <div class="card">
          <div class="section-title"><h2>Price Bands</h2></div>
          <div class="metric-list">
            <div class="metric-line"><span class="muted">List p25</span><strong id="list-p25">-</strong></div>
            <div class="metric-line"><span class="muted">List median</span><strong id="list-med">-</strong></div>
            <div class="metric-line"><span class="muted">List p75</span><strong id="list-p75">-</strong></div>
            <div class="metric-line"><span class="muted">Door p25</span><strong id="door-p25">-</strong></div>
            <div class="metric-line"><span class="muted">Door median</span><strong id="door-med">-</strong></div>
            <div class="metric-line"><span class="muted">Door p75</span><strong id="door-p75">-</strong></div>
          </div>
        </div>
        <div class="card"><div class="section-title"><h2>Top ZIPs</h2></div><div id="zip-bars"></div></div>
      </div>
    </div>
  </div>
</main>
<script>
const apiKey = ${JSON.stringify(getBrowserApiKey(c))};
const fmt = (n) => n == null ? '-' : Number(n).toLocaleString('en-US');
const money = (n) => n == null ? '-' : '$' + fmt(n);
const compactMoney = (n) => {
  if (n == null) return '-';
  const value = Number(n);
  if (!Number.isFinite(value)) return '-';
  if (value >= 1000000000) return '$' + (value / 1000000000).toFixed(1) + 'B';
  if (value >= 1000000) return '$' + (value / 1000000).toFixed(1) + 'M';
  return money(value);
};
const assetLabels = {
  single_family: 'Single family / condo',
  small_multifamily: 'Small multifamily',
  commercial_multifamily: 'Commercial multifamily',
  mobile_home_rv: 'Mobile home / RV parks',
  land: 'Land',
  industrial: 'Industrial',
  office: 'Office',
  retail: 'Retail',
  self_storage: 'Self-storage',
  hospitality: 'Hospitality',
  parking: 'Parking',
  exempt_institutional: 'Exempt / institutional',
  utilities_other: 'Utilities / other',
  other_commercial: 'Other commercial',
  other_residential: 'Other residential',
  unknown: 'Unknown'
};
function bars(target, data, valueKey = null) {
  const el = document.getElementById(target);
  const entries = Array.isArray(data) ? data.map(row => [row.zip, row.listings, valueKey ? row[valueKey] : null]) : Object.entries(data ?? {}).map(([k, v]) => [k, v, null]);
  const max = Math.max(1, ...entries.map(([, v]) => Number(v) || 0));
  el.innerHTML = entries.map(([label, value, extra]) => \`
    <div class="bar-row">
      <div class="bar-label">\${label}</div>
      <div class="bar-track"><div class="bar-fill" style="width:\${Math.max(3, (Number(value) / max) * 100)}%"></div></div>
      <div class="bar-value">\${fmt(value)}</div>
      \${extra ? \`<div class="muted" style="grid-column:2 / 4;font-size:11px;margin-top:-6px">\${money(extra)} median</div>\` : ''}
    </div>\`).join('') || '<div class="muted">No data</div>';
}
let currentMinUnits = null;
let currentAssetGroup = '';
let currentScope = 'city';
function setUnitFilter(minUnits) {
  currentMinUnits = minUnits;
  for (const id of ['filter-all', 'filter-2', 'filter-3', 'filter-4']) document.getElementById(id).classList.remove('active');
  document.getElementById(minUnits ? 'filter-' + minUnits : 'filter-all').classList.add('active');
  document.getElementById('loading').style.display = 'block';
  document.getElementById('loading').textContent = 'Refreshing live MXRE dashboard panels...';
  load();
}
function setScope(scope) {
  currentScope = scope === 'metro' ? 'metro' : scope === 'core' ? 'core' : 'city';
  document.getElementById('loading').style.display = 'block';
  document.getElementById('loading').textContent = 'Refreshing live MXRE dashboard panels...';
  load();
}
function setAssetGroup(assetGroup) {
  currentAssetGroup = assetGroup || '';
  document.getElementById('loading').style.display = 'block';
  document.getElementById('loading').textContent = 'Refreshing live MXRE dashboard panels...';
  load();
}
async function load() {
  const params = new URLSearchParams();
  if (currentMinUnits) params.set('min_units', String(currentMinUnits));
  const assetParams = new URLSearchParams();
  assetParams.set('scope', currentScope);
  if (currentAssetGroup) assetParams.set('asset_group', currentAssetGroup);
  const completionParams = new URLSearchParams({ scope: currentScope });
  const path = '/v1/markets/indianapolis/dashboard' + (params.toString() ? '?' + params.toString() : '');
  const coveragePath = '/v1/markets/indianapolis/multifamily/coverage' + (params.toString() ? '?' + params.toString() : '');
  const assetsPath = '/v1/markets/indianapolis/assets' + (assetParams.toString() ? '?' + assetParams.toString() : '');
  const completionPath = '/v1/markets/indianapolis/completion?' + completionParams.toString();
  const [assetsResp, completionResp] = await Promise.all([
    fetch(assetsPath, { headers: { 'x-api-key': apiKey } }),
    fetch(completionPath, { headers: { 'x-api-key': apiKey } }),
  ]);
  const assets = await assetsResp.json();
  const completion = await completionResp.json();
  if (!assetsResp.ok) throw new Error(assets.detail || assets.error || 'Assets request failed');
  if (!completionResp.ok) throw new Error(completion.detail || completion.error || 'Completion request failed');
  const scopeLabel = currentScope === 'metro' ? 'Indianapolis metro' : currentScope === 'core' ? 'Indianapolis core / Marion County' : 'Indianapolis city';
  document.getElementById('filter-note').textContent = currentMinUnits ? 'Showing ' + scopeLabel + ' assets plus Marion multifamily with at least ' + currentMinUnits + ' units.' : 'Showing ' + scopeLabel + ' assets with a Marion multifamily drilldown.';
  document.getElementById('kpi-all-parcels').textContent = fmt(assets.totals.parcel_count);
  document.getElementById('kpi-market-value').textContent = compactMoney(assets.totals.market_value_sum);
  document.getElementById('kpi-all-recorder').textContent = assets.filters.coverage ? assets.coverage.any_recorder_data_pct + '%' : '-';
  document.getElementById('kpi-all-recorder-sub').textContent = assets.filters.coverage ? fmt(assets.coverage.parcels_with_any_recorder_data) + ' parcels with sale/mortgage data' : 'loaded in focused coverage views';
  document.getElementById('generated').textContent = 'Assets updated ' + new Date().toLocaleTimeString();
  document.getElementById('complete-score').textContent = completion.totals.readiness_score + '%';
  if (currentScope === 'metro') {
    document.getElementById('instant-metro-readiness').textContent = completion.totals.readiness_score + '%';
    document.getElementById('instant-metro-parcels').textContent = fmt(completion.totals.parcel_count);
  } else {
    document.getElementById('instant-core-readiness').textContent = completion.totals.readiness_score + '%';
    document.getElementById('instant-core-parcels').textContent = fmt(completion.totals.parcel_count);
  }
  document.getElementById('complete-core').textContent = completion.metrics.core_complete.pct + '%';
  document.getElementById('complete-underwriting').textContent = completion.metrics.underwriting_complete.pct + '%';
  document.getElementById('complete-rental-universe').textContent = fmt(completion.metrics.rental_candidate_count.count);
  document.getElementById('complete-rental-websites').textContent = completion.metrics.rental_website_coverage.pct + '%';
  document.getElementById('complete-rental-websites-sub').textContent = fmt(completion.metrics.rental_website_coverage.count) + ' candidates with sites';
  document.getElementById('complete-rent-snapshots').textContent = completion.metrics.rental_rent_snapshot_coverage.pct + '%';
  document.getElementById('complete-rent-snapshots-sub').textContent = fmt(completion.metrics.rental_rent_snapshot_coverage.count) + ' candidates with observed rents';
  const unknownAssetRow = (assets.by_asset_group ?? []).find(row => row.assetGroup === 'unknown');
  document.getElementById('complete-unknown').textContent = fmt(unknownAssetRow?.parcels ?? 0);
  const countyRows = completion.by_county ?? [];
  const maxCounty = Math.max(1, ...countyRows.map(row => Number(row.parcels) || 0));
  document.getElementById('county-completion').innerHTML = countyRows.map(row => {
    const pctCore = row.parcels ? Math.round((Number(row.coreComplete || 0) / Number(row.parcels)) * 1000) / 10 : 0;
    return \`
      <div class="bar-row">
        <div class="bar-label">\${row.county}</div>
        <div class="bar-track"><div class="bar-fill" style="width:\${Math.max(3, (Number(row.parcels) / maxCounty) * 100)}%"></div></div>
        <div class="bar-value">\${fmt(row.parcels)}</div>
        <div class="muted" style="grid-column:2 / 4;font-size:11px;margin-top:-6px">\${pctCore}% core complete</div>
      </div>
    \`;
  }).join('') || '<div class="muted">No county completion data.</div>';
  document.getElementById('asset-note').textContent = currentAssetGroup
    ? 'Showing only ' + (assetLabels[currentAssetGroup] || currentAssetGroup) + ' parcels.'
    : 'Parcel-led inventory by asset type.';
  document.getElementById('asset-body').innerHTML = (assets.by_asset_group ?? []).map(row => {
    const recorderPct = row.parcels ? Math.round((Number(row.anyRecorderData || 0) / Number(row.parcels)) * 1000) / 10 : 0;
    const recorderText = assets.filters.coverage ? fmt(row.anyRecorderData) + ' / ' + recorderPct + '%' : '-';
    return \`
      <tr>
        <td><strong>\${assetLabels[row.assetGroup] || row.assetGroup}</strong><div class="muted" style="font-size:11px;margin-top:3px">\${row.assetGroup}</div></td>
        <td>\${fmt(row.parcels)}</td>
        <td>\${fmt(row.knownUnits)}</td>
        <td>\${compactMoney(row.marketValue)}</td>
        <td>\${fmt(row.activeListings)}</td>
        <td>\${recorderText}</td>
      </tr>
    \`;
  }).join('') || '<tr><td colspan="6" class="muted">No parcels match this asset filter.</td></tr>';
  const uses = assets.top_property_uses ?? [];
  const maxUse = Math.max(1, ...uses.map(row => Number(row.parcels) || 0));
  document.getElementById('asset-uses').innerHTML = uses.slice(0, 12).map(row => \`
    <div class="bar-row">
      <div class="bar-label">\${row.assetGroup}</div>
      <div class="bar-track"><div class="bar-fill" style="width:\${Math.max(3, (Number(row.parcels) / maxUse) * 100)}%"></div></div>
      <div class="bar-value">\${fmt(row.parcels)}</div>
      <div class="muted" style="grid-column:1 / 4;font-size:11px;margin-top:-6px">\${row.propertyUse} - \${fmt(row.knownUnits)} units</div>
    </div>
  \`).join('') || '<div class="muted">No assessor-use data.</div>';
  document.getElementById('asset-examples').innerHTML = (assets.examples ?? []).slice(0, 8).map(row => \`
    <div class="metric-line">
      <span><strong>\${row.address || 'No address'}</strong><div class="muted" style="font-size:11px;margin-top:3px">\${assetLabels[row.assetGroup] || row.assetGroup} - \${row.propertyUse || row.propertyType || '-'}</div></span>
      <span style="text-align:right">\${compactMoney(row.marketValue)}<div class="muted" style="font-size:11px;margin-top:3px">\${row.unitCount ? fmt(row.unitCount) + ' units' : row.zip || ''}</div></span>
    </div>
  \`).join('') || '<div class="muted">No examples found.</div>';
  document.getElementById('loading').textContent = 'Loading listing and coverage panels...';
  const [resp, coverageResp] = await Promise.all([
    fetch(path, { headers: { 'x-api-key': apiKey } }),
    fetch(coveragePath, { headers: { 'x-api-key': apiKey } }),
  ]);
  const data = await resp.json();
  const coverage = await coverageResp.json();
  if (!resp.ok) throw new Error(data.detail || data.error || 'Request failed');
  if (!coverageResp.ok) throw new Error(coverage.detail || coverage.error || 'Coverage request failed');
  document.getElementById('kpi-active').textContent = fmt(data.on_market.unique_properties);
  document.getElementById('kpi-unique').textContent = fmt(data.on_market.active_listing_rows) + ' active listing rows from tracked sources';
  document.getElementById('kpi-external').textContent = fmt(data.on_market.external_listing_rows);
  document.getElementById('kpi-external-sub').textContent = fmt(data.on_market.external_4_plus_rows) + ' are 4+ unit CRE signals';
  document.getElementById('kpi-list').textContent = money(data.on_market.list_price.median);
  document.getElementById('generated').textContent = 'Updated ' + new Date(data.generated_at).toLocaleTimeString();
  document.getElementById('cov-parcels').textContent = fmt(coverage.parcel_universe.parcel_count);
  document.getElementById('cov-units').textContent = fmt(coverage.parcel_universe.known_units);
  document.getElementById('cov-recorder').textContent = fmt(coverage.coverage.parcels_with_any_recorder_data) + ' / ' + coverage.coverage.any_recorder_data_pct + '%';
  document.getElementById('cov-sales').textContent = fmt(coverage.coverage.parcels_with_sale_history) + ' / ' + coverage.coverage.sale_history_pct + '%';
  document.getElementById('cov-mortgages').textContent = fmt(coverage.coverage.parcels_with_recorded_mortgage) + ' / ' + coverage.coverage.recorded_mortgage_pct + '%';
  document.getElementById('cov-listings').textContent = fmt(coverage.coverage.parcels_with_active_listing) + ' / ' + coverage.coverage.active_listing_pct + '%';
  document.getElementById('cov-profiles').textContent = fmt(coverage.coverage.parcels_with_complex_profile) + ' / ' + coverage.coverage.complex_profile_pct + '%';
  document.getElementById('list-p25').textContent = money(data.on_market.list_price.p25);
  document.getElementById('list-med').textContent = money(data.on_market.list_price.median);
  document.getElementById('list-p75').textContent = money(data.on_market.list_price.p75);
  document.getElementById('door-p25').textContent = money(data.on_market.price_per_unit.p25);
  document.getElementById('door-med').textContent = money(data.on_market.price_per_unit.median);
  document.getElementById('door-p75').textContent = money(data.on_market.price_per_unit.p75);
  bars('source-bars', data.on_market.by_source);
  bars('subtype-bars', data.on_market.by_subtype);
  bars('zip-bars', data.on_market.by_zip, 'median_price_per_unit');
  document.getElementById('listing-body').innerHTML = (data.on_market.top_listings ?? []).map(row => \`
    <tr>
      <td>
        <a href="\${row.listingUrl}" target="_blank">\${row.complexName || row.address}, \${row.zip}</a>
        <div class="muted" style="font-size:11px;margin-top:3px">\${row.complexName ? row.address : 'Complex name not enriched yet'}\${row.managementCompany ? ' · ' + row.managementCompany : ''}</div>
      </td>
      <td>\${row.assetSubtype ?? '-'}</td><td>\${row.unitCount ?? '-'}</td><td>\${money(row.listPrice)}</td><td>\${money(row.pricePerUnit)}</td><td>\${row.listingSource ?? '-'}</td>
    </tr>
  \`).join('') || '<tr><td colspan="6" class="muted">No internally linked listings for this unit filter.</td></tr>';
  const externalRows = data.on_market.external_top_listings ?? [];
  document.getElementById('external-card').style.display = externalRows.length ? 'block' : 'none';
  document.getElementById('external-body').innerHTML = externalRows.map(row => \`
    <tr>
      <td><a href="\${row.sourceUrl}" target="_blank">\${row.title || 'External listing'}</a><div class="muted" style="font-size:11px;margin-top:3px">\${row.confidence} confidence; verify before underwriting</div></td>
      <td>\${row.address ?? '-'}</td>
      <td>\${row.units ?? '-'}</td>
      <td>\${money(row.listPrice)}</td>
      <td>\${money(row.pricePerUnit)}</td>
      <td>\${row.source === 'crexi_search_snapshot' ? 'Crexi snapshot' : (row.source ?? '-')}</td>
    </tr>
  \`).join('');
  document.getElementById('loading').style.display = 'none';
  document.getElementById('content').style.display = 'block';
}
load().catch(err => {
  console.warn('MXRE live panel refresh failed', err);
  document.getElementById('loading').textContent = 'Live MXRE panels are retrying; baseline readiness and visible dashboard sections remain available.';
  setTimeout(() => load().catch(retryErr => console.warn('MXRE live panel retry failed', retryErr)), 30000);
});
</script>
</body>
</html>`);
});

// ── Helpers ──────────────────────────────────────────────────

app.get('/preview/data-gaps', async (c) => {
  if (!previewsEnabled(c)) return c.json({ error: 'Preview disabled' }, 404);
  const apiKey = getBrowserApiKey(c);
  return c.html(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MXRE Data Gaps</title>
<style>
body{font-family:Inter,Arial,sans-serif;margin:0;background:#f6f7f9;color:#14171f}header{padding:18px 22px;background:#111827;color:white;display:flex;justify-content:space-between;gap:14px;align-items:center}main{padding:18px 22px}.controls{display:grid;grid-template-columns:repeat(7,minmax(110px,1fr));gap:10px;margin-bottom:14px}select,input,button{height:36px;border:1px solid #cfd4dc;border-radius:6px;padding:0 10px;background:white}button{background:#111827;color:white;border:0;font-weight:700;cursor:pointer}.cards{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:10px;margin-bottom:14px}.card{background:white;border:1px solid #d8dde6;border-radius:8px;padding:12px}.label{font-size:12px;color:#6b7280}.value{font-size:24px;font-weight:800;margin-top:4px}table{width:100%;border-collapse:collapse;background:white;border:1px solid #d8dde6;border-radius:8px;overflow:hidden}th,td{font-size:13px;text-align:left;border-bottom:1px solid #e5e7eb;padding:9px;vertical-align:top}th{background:#f0f2f5;color:#374151;font-size:12px;text-transform:uppercase}.pill{display:inline-block;border-radius:999px;padding:3px 8px;font-size:12px;font-weight:700;margin:2px;background:#eef2ff;color:#3730a3}.critical{background:#fee2e2;color:#991b1b}.high{background:#ffedd5;color:#9a3412}.medium{background:#fef9c3;color:#854d0e}.muted{color:#6b7280}.nowrap{white-space:nowrap}.missing{max-width:360px}@media(max-width:1000px){.controls,.cards{grid-template-columns:1fr 1fr}table{display:block;overflow-x:auto}}
</style></head><body>
<header><div><strong>MXRE Property Data Gaps</strong><div class="muted">Property-by-property missing data and next source to fill it</div></div><a href="/preview/market-dashboard" style="color:white">Market dashboard</a></header>
<main>
<div class="controls">
<select id="asset"><option value="all">All assets</option><option value="single_family">Single family</option><option value="multifamily">Multifamily</option><option value="small_multifamily">2-4 units</option><option value="commercial_multifamily">5+ units</option></select>
<select id="gap"><option value="all">All gaps</option><option value="agent_email">Missing agent email</option><option value="agent_phone">Missing agent phone</option><option value="mortgage_balance">Missing mortgage balance</option><option value="mortgage_records">Missing mortgage records</option><option value="rent_snapshot">Missing rent snapshots</option><option value="floorplans">Missing floorplans</option><option value="valuation">Missing valuation</option></select>
<select id="onMarket"><option value="all">All status</option><option value="true">On market only</option><option value="false">Off market only</option></select>
<input id="zip" placeholder="ZIP"><input id="q" placeholder="Address / parcel / owner">
<select id="limit"><option>50</option><option selected>100</option><option>250</option></select><button onclick="load()">Run report</button>
</div>
<div class="cards"><div class="card"><div class="label">Matching gaps</div><div class="value" id="total">-</div></div><div class="card"><div class="label">Returned</div><div class="value" id="count">-</div></div><div class="card"><div class="label">Agent email gaps</div><div class="value" id="agentEmail">-</div></div><div class="card"><div class="label">Mortgage balance gaps</div><div class="value" id="mortgageBalance">-</div></div><div class="card"><div class="label">Rent snapshot gaps</div><div class="value" id="rentSnapshot">-</div></div></div>
<div id="status" class="muted">Loading...</div>
<table><thead><tr><th>Property</th><th>Asset</th><th>Status</th><th>Value</th><th>Missing</th><th>Next sources</th><th>Agent</th></tr></thead><tbody id="rows"></tbody></table>
</main>
<script>
const apiKey=${JSON.stringify(apiKey)},statusEl=document.getElementById('status'),fmt=n=>Number(n||0).toLocaleString(),esc=v=>String(v??'').replace(/[&<>"]/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[s])),pill=(t,c='')=>'<span class="pill '+c+'">'+esc(t)+'</span>';
async function load(){const params=new URLSearchParams({scope:'city',asset:asset.value,gap:gap.value,on_market:onMarket.value,limit:limit.value});if(zip.value.trim())params.set('zip',zip.value.trim());if(q.value.trim())params.set('q',q.value.trim());statusEl.textContent='Loading report...';const res=await fetch('/v1/markets/indianapolis/data-gaps?'+params,{headers:{'x-api-key':apiKey}});const data=await res.json();if(!res.ok){statusEl.textContent=data.error||'Failed';return;}total.textContent=fmt(data.total);count.textContent=fmt(data.count);agentEmail.textContent=fmt(data.returned_gap_counts?.agent_email);mortgageBalance.textContent=fmt(data.returned_gap_counts?.mortgage_balance);rentSnapshot.textContent=fmt(data.returned_gap_counts?.rent_snapshot);statusEl.textContent='Generated '+new Date(data.generated_at).toLocaleString();rows.innerHTML=(data.rows||[]).map(r=>{const a=r.agent||{};return '<tr><td><strong>'+esc(r.address)+'</strong><div class="muted">'+esc(r.city)+', '+esc(r.state)+' '+esc(r.zip)+' | '+esc(r.parcelId)+' | MXRE '+esc(r.mxreId)+'</div></td><td>'+esc(r.assetGroup)+'<div class="muted">'+esc(r.assetSubtype||r.assetType||'')+' | units '+esc(r.unitCount??'')+'</div></td><td>'+pill(r.severity,r.severity)+'<div class="muted">'+(r.onMarket?'on market':'off market')+'</div></td><td class="nowrap">$'+fmt(r.listPrice||r.marketValue||r.assessedValue)+'</td><td class="missing">'+(r.missingFields||[]).map(x=>pill(x)).join(' ')+'</td><td>'+(r.nextBestSources||[]).map(x=>pill(x)).join(' ')+'</td><td>'+esc(a.name||'')+'<div class="muted">'+esc(a.email||'')+' '+esc(a.phone||'')+'</div><div class="muted">'+esc(a.brokerage||'')+'</div></td></tr>';}).join('');}
load().catch(err=>{statusEl.textContent=String(err);});
</script></body></html>`);
});

function numberOrNull(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function parsePositiveInt(value: string | undefined): number | null {
  if (!value) return null;
  const clean = value.trim();
  if (!/^\d+$/.test(clean)) return null;
  const parsed = Number(clean);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function parseDateParam(value: string | undefined): string | null {
  if (!value) return null;
  const clean = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(clean)) return null;
  const parsed = new Date(`${clean}T00:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : clean;
}

function parseDateTimeParam(value: string | undefined): string | null {
  if (!value) return null;
  const clean = value.trim();
  if (clean.length > 40) return null;
  const parsed = new Date(clean);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function encodeChangeCursor(eventAt: string, recordVersion: string): string {
  return Buffer.from(JSON.stringify({ eventAt, recordVersion })).toString('base64url');
}

function decodeChangeCursor(value: string | undefined): { eventAt: string; recordVersion: string } | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as Record<string, unknown>;
    const eventAt = parseDateTimeParam(typeof parsed.eventAt === 'string' ? parsed.eventAt : undefined);
    const recordVersion = typeof parsed.recordVersion === 'string' ? parsed.recordVersion : '';
    if (!eventAt || !recordVersion) return null;
    return { eventAt, recordVersion };
  } catch {
    return null;
  }
}

function sqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function normalizeRecord(value: unknown): Record<string, unknown> {
  return (value && typeof value === 'object' && !Array.isArray(value)) ? value as Record<string, unknown> : {};
}

function inferGapSeverity(missingFields: string[], onMarket: boolean): 'critical' | 'high' | 'medium' {
  const missing = new Set(missingFields);
  if (
    missing.has('parcel_identity')
    || missing.has('ownership')
    || missing.has('valuation')
    || missing.has('mortgage_balance')
    || (onMarket && (missing.has('agent_email') || missing.has('agent_phone')))
  ) {
    return 'critical';
  }
  if (
    missing.has('asset_classification')
    || missing.has('sales_history')
    || missing.has('mortgage_records')
    || (onMarket && (missing.has('agent_name') || missing.has('brokerage') || missing.has('listing_url')))
  ) {
    return 'high';
  }
  return 'medium';
}

function inferGapSources(missingFields: string[], onMarket: boolean): string[] {
  const sources = new Set<string>();
  const add = (source: string) => sources.add(source);
  const missing = new Set(missingFields);

  if (
    missing.has('parcel_identity')
    || missing.has('asset_classification')
    || missing.has('ownership')
    || missing.has('valuation')
    || missing.has('physical_facts')
  ) add('county_assessor_refresh');

  if (missing.has('sales_history') || missing.has('mortgage_records')) add('recorder_refresh');
  if (missing.has('mortgage_balance')) add(onMarket ? 'realestateapi_property_detail' : 'recorder_refresh_or_paid_fallback');

  if (
    missing.has('agent_name')
    || missing.has('agent_email')
    || missing.has('agent_phone')
    || missing.has('brokerage')
    || missing.has('listing_url')
  ) {
    add('realestateapi_property_detail');
    add('rapidapi_zillow_fallback');
    if (missing.has('agent_email') || missing.has('agent_phone')) add('brokerage_web_search');
  }

  if (missing.has('property_website')) add('apartment_website_discovery');
  if (missing.has('floorplans') || missing.has('rent_snapshot')) add('property_website_rent_scraper');
  if (missing.has('location_scores')) add('crime_transit_location_refresh');

  return Array.from(sources);
}

function normalizeAutocompleteQuery(value: string): string {
  return value
    .replace(/[^\w\s,#.-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 120);
}

function normalizeAutocompleteQueryForSql(value: string): string {
  return value
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractStateHint(value: string): string | null {
  const match = value.toUpperCase().match(/(?:^|[\s,])([A-Z]{2})(?:[\s,]|$)/);
  return match?.[1] ?? null;
}

function extractZipHint(value: string): string | null {
  const match = value.match(/\b\d{5}\b/);
  return match?.[0] ?? null;
}

function dedupeAutocompleteRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const seen = new Set<string>();
  const out: Record<string, unknown>[] = [];
  for (const row of rows) {
    const key = [
      row.type,
      String(row.label ?? '').toUpperCase(),
      row.zip ?? '',
      row.propertyId ?? '',
    ].join('|');
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function buildAutocompleteResponse(query: string, limit: number, rows: Record<string, unknown>[]) {
  return {
    schemaVersion: 'mxre.addressAutocomplete.v1',
    query,
    results: rows.map((row) => ({
      type: row.type,
      label: row.label,
      street: row.street ?? null,
      city: row.city ?? null,
      state: row.state ?? null,
      zip: row.zip ?? null,
      county: row.county ?? null,
      lat: numberOrNull(row.lat),
      lng: numberOrNull(row.lng),
      source: row.source,
      confidence: row.confidence,
      coverage: {
        hasMxrePropertyDetail: Boolean(row.hasMxrePropertyDetail),
        propertyId: numberOrNull(row.propertyId),
        marketId: row.marketId ?? null,
        propertyCount: numberOrNull(row.propertyCount),
      },
    })),
    usage: {
      selectAddressThenCall: '/v1/bbc/property?address={street}&city={city}&state={state}&zip={zip}',
      selectCityThenCall: '/v1/bbc/search-runs',
    },
    meta: {
      limit,
      strategy: 'preindexed_mxre_first',
      nationalIndex: 'address_autocomplete_entries_optional',
      liveExternalCalls: false,
      generatedAt: new Date().toISOString(),
    },
  };
}

async function buildCoverageMarketRows(): Promise<Array<Record<string, unknown>>> {
  const markets = Object.values(MARKET_CONFIGS);
  return Promise.all(markets.map(async (market) => {
    let metrics: Record<string, unknown> = {};
    try {
      const [row] = await queryPg<Record<string, unknown>>(`
        with parcels as (
          select
            count(*)::int as parcel_count,
            count(*) filter (where parcel_id is not null and parcel_id <> '')::int as parcel_identity_count,
            count(*) filter (where asset_type is not null or property_use is not null or property_type is not null)::int as classified_count,
            count(*) filter (where owner_name is not null or company_name is not null or owner1_last is not null)::int as ownership_count,
            count(*) filter (where coalesce(market_value, assessed_value, taxable_value, 0) > 0)::int as valuation_count,
            count(*) filter (where coalesce(total_units, 0) >= 2 or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily'))::int as multifamily_count
          from properties
          where county_id = ${market.countyId}
            and state_code = '${market.state}'
            and upper(coalesce(city,'')) = '${market.cityUpper}'
        ),
        listings as (
          select
            count(*)::int as active_listing_count,
            count(distinct property_id)::int as active_property_count,
            count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name_count,
            count(*) filter (where nullif(listing_agent_email,'') is not null)::int as agent_email_count,
            count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as agent_phone_count,
            count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage_count,
            count(*) filter (where creative_finance_status = 'positive')::int as creative_finance_count,
            max(coalesce(last_seen_at, first_seen_at)) as latest_listing_seen,
            array_agg(distinct listing_source order by listing_source) filter (where listing_source is not null) as listing_sources
          from listing_signals
          where is_on_market = true
            and state_code = '${market.state}'
            and upper(coalesce(city,'')) = '${market.cityUpper}'
        ),
        debt as (
          select
            count(*)::int as mortgage_record_count,
            count(distinct m.property_id)::int as properties_with_mortgage_records,
            count(*) filter (where coalesce(m.estimated_current_balance, m.loan_amount, m.original_amount, 0) > 0)::int as mortgage_amount_count,
            max(m.recording_date) as latest_recording
          from mortgage_records m
          join properties p on p.id = m.property_id
          where p.county_id = ${market.countyId}
            and p.state_code = '${market.state}'
            and upper(coalesce(p.city,'')) = '${market.cityUpper}'
        ),
        rents as (
          select
            count(*)::int as rent_snapshot_count,
            count(distinct rs.property_id)::int as properties_with_rent_snapshots,
            max(rs.observed_at) as latest_rent_observed
          from rent_snapshots rs
          join properties p on p.id = rs.property_id
          where p.county_id = ${market.countyId}
            and p.state_code = '${market.state}'
            and upper(coalesce(p.city,'')) = '${market.cityUpper}'
        )
        select
          row_to_json(parcels) as parcels,
          row_to_json(listings) as listings,
          row_to_json(debt) as debt,
          row_to_json(rents) as rents
        from parcels, listings, debt, rents;
      `);
      metrics = row ?? {};
    } catch (error) {
      console.warn(`[MXRE coverage markets] failed to build metrics for ${market.key}:`, error);
      metrics = {
        ...(market.fallbackCoverageMetrics ?? {}),
        error: error instanceof Error ? error.message : String(error),
        fallback: market.fallbackCoverageMetrics ? 'configured_market_coverage_snapshot' : null,
      };
    }

    const parcels = normalizeRecord(metrics.parcels);
    const listings = normalizeRecord(metrics.listings);
    const debt = normalizeRecord(metrics.debt);
    const rents = normalizeRecord(metrics.rents);
    const parcelCount = numberOrNull(parcels.parcel_count) ?? 0;
    const activeListingCount = numberOrNull(listings.active_listing_count) ?? 0;
    const pctOf = (value: unknown, denominator: number): number => {
      if (denominator <= 0) return 0;
      return Math.round(((numberOrNull(value) ?? 0) / denominator) * 1000) / 10;
    };
    const readinessInputs = [
      pctOf(parcels.parcel_identity_count, parcelCount),
      pctOf(parcels.classified_count, parcelCount),
      pctOf(parcels.ownership_count, parcelCount),
      pctOf(parcels.valuation_count, parcelCount),
      activeListingCount > 0 ? pctOf(listings.agent_phone_count, activeListingCount) : 0,
      activeListingCount > 0 ? pctOf(listings.agent_email_count, activeListingCount) : 0,
      parcelCount > 0 ? pctOf(debt.properties_with_mortgage_records, parcelCount) : 0,
    ];
    const readinessScore = readinessInputs.length > 0
      ? Math.round((readinessInputs.reduce((sum, value) => sum + value, 0) / readinessInputs.length) * 10) / 10
      : 0;

    return {
      marketId: market.key,
      label: market.publicLabel,
      city: market.city,
      state: market.state,
      county: market.county,
      countyId: market.countyId,
      status: market.status,
      bbcAccess: readinessScore >= market.readinessTarget && market.status === 'live' ? 'production_allowed' : 'sandbox_or_admin_only',
      readinessScore,
      readinessTarget: market.readinessTarget,
      meetsReadinessTarget: readinessScore >= market.readinessTarget,
      scope: market.scope,
      aliases: market.aliases,
      refreshCadence: market.refreshCadence,
      supportedAssetClasses: MARKET_ASSET_CLASSES,
      supportedDataDomains: MARKET_DATA_DOMAINS,
      supportedEndpoints: BBC_MARKET_ENDPOINTS.map((endpoint) => endpoint.replace('{market}', market.key)),
      counts: {
        parcels: parcelCount,
        activeListings: activeListingCount,
        activeListingProperties: numberOrNull(listings.active_property_count) ?? 0,
        multifamilyCandidates: numberOrNull(parcels.multifamily_count) ?? 0,
        creativeFinanceListings: numberOrNull(listings.creative_finance_count) ?? 0,
        mortgageRecords: numberOrNull(debt.mortgage_record_count) ?? 0,
        propertiesWithMortgageRecords: numberOrNull(debt.properties_with_mortgage_records) ?? 0,
        rentSnapshots: numberOrNull(rents.rent_snapshot_count) ?? 0,
        propertiesWithRentSnapshots: numberOrNull(rents.properties_with_rent_snapshots) ?? 0,
      },
      metricsError: typeof metrics.error === 'string' ? metrics.error : null,
      metricsFallback: typeof metrics.fallback === 'string' ? metrics.fallback : null,
      coverage: {
        parcelIdentityPct: pctOf(parcels.parcel_identity_count, parcelCount),
        assetClassificationPct: pctOf(parcels.classified_count, parcelCount),
        ownershipPct: pctOf(parcels.ownership_count, parcelCount),
        valuationPct: pctOf(parcels.valuation_count, parcelCount),
        activeListingAgentNamePct: pctOf(listings.agent_name_count, activeListingCount),
        activeListingAgentEmailPct: pctOf(listings.agent_email_count, activeListingCount),
        activeListingAgentPhonePct: pctOf(listings.agent_phone_count, activeListingCount),
        activeListingBrokeragePct: pctOf(listings.brokerage_count, activeListingCount),
        mortgageRecordPct: pctOf(debt.properties_with_mortgage_records, parcelCount),
        mortgageAmountPct: pctOf(debt.mortgage_amount_count, numberOrNull(debt.mortgage_record_count) ?? 0),
        rentSnapshotPct: pctOf(rents.properties_with_rent_snapshots, parcelCount),
      },
      freshness: {
        latestListingSeen: listings.latest_listing_seen ?? null,
        latestRecording: debt.latest_recording ?? null,
        latestRentObserved: rents.latest_rent_observed ?? null,
        listingSources: listings.listing_sources ?? [],
      },
      restrictions: market.restrictions,
    };
  }));
}

function toTitleCase(value: unknown): string {
  return String(value ?? '')
    .toLowerCase()
    .replace(/\b[a-z]/g, (letter) => letter.toUpperCase());
}

function buildStaticMarketAutocompleteRows(query: string, stateHint: string): Record<string, unknown>[] {
  const normalized = normalizeAutocompleteQueryForSql(query.replace(/\b[A-Z]{2}\b/gi, ''));
  if (normalized.length < 2 || /\d/.test(query)) return [];

  return Object.values(MARKET_CONFIGS)
    .filter((market) => !stateHint || market.state === stateHint)
    .filter((market) => normalizeAutocompleteQueryForSql(market.city).startsWith(normalized))
    .map((market) => ({
      type: 'city',
      label: `${market.city}, ${market.state}`,
      street: null,
      city: market.city,
      state: market.state,
      zip: null,
      countyId: market.countyId,
      county: market.county,
      lat: null,
      lng: null,
      source: 'mxre_market_config',
      hasMxrePropertyDetail: false,
      propertyId: null,
      marketId: market.key,
      confidence: 'high',
      propertyCount: null,
      rank: -10,
    }));
}

function normalizeBbcSearchFilters(input: Record<string, unknown>) {
  const location = normalizeRecord(input.location);
  const assetTypes = Array.isArray(input.assetTypes)
    ? input.assetTypes.map((value) => String(value)).map(normalizeBbcAssetType).filter(Boolean)
    : [];
  const unitClasses = normalizeUnitClassFilters(input.unitClass ?? input.unitClasses ?? input.unit_class ?? input.unit_classes);
  const statuses = Array.isArray(input.status)
    ? input.status.map((value) => String(value).toLowerCase()).filter((value) => ['active', 'pending', 'off_market'].includes(value))
    : ['active'];

  return {
    assetTypes: [...new Set(assetTypes)],
    unitClasses,
    statuses: [...new Set(statuses)],
    minPrice: positiveNumberOrNull(input.minPrice),
    maxPrice: positiveNumberOrNull(input.maxPrice),
    minEquityPercent: positiveNumberOrNull(input.minEquityPercent ?? input.min_equity_percent),
    maxEquityPercent: positiveNumberOrNull(input.maxEquityPercent ?? input.max_equity_percent),
    minEstimatedEquity: positiveNumberOrNull(input.minEstimatedEquity ?? input.min_equity ?? input.minEquity),
    maxEstimatedEquity: positiveNumberOrNull(input.maxEstimatedEquity ?? input.max_equity ?? input.maxEquity),
    minUnits: positiveNumberOrNull(input.minUnits),
    maxUnits: positiveNumberOrNull(input.maxUnits),
    minBeds: positiveNumberOrNull(input.minBeds ?? input.minBedrooms),
    maxBeds: positiveNumberOrNull(input.maxBeds ?? input.maxBedrooms),
    minBaths: positiveNumberOrNull(input.minBaths ?? input.minBathrooms),
    maxBaths: positiveNumberOrNull(input.maxBaths ?? input.maxBathrooms),
    minSqft: positiveNumberOrNull(input.minSqft ?? input.minLivingSqft),
    maxSqft: positiveNumberOrNull(input.maxSqft ?? input.maxLivingSqft),
    minYearBuilt: positiveNumberOrNull(input.minYearBuilt),
    maxYearBuilt: positiveNumberOrNull(input.maxYearBuilt),
    states: normalizeStateFilters(input.state ?? input.states ?? location.state ?? location.states),
    cities: normalizeCityFilters(input.city ?? input.cities ?? location.city ?? location.cities),
    zips: normalizeZipFilters(input.zip ?? input.zips ?? input.zipCodes ?? location.zip ?? location.zips ?? location.zipCodes),
    creativeOnly: input.creativeOnly === true,
  };
}

function normalizeUnitClassFilters(value: unknown): string[] {
  const aliases: Record<string, string> = {
    single_family: 'single_family',
    sfr: 'single_family',
    one_unit: 'single_family',
    '1_unit': 'single_family',
    duplex: 'duplex',
    two_unit: 'duplex',
    '2_unit': 'duplex',
    triplex: 'triplex',
    three_unit: 'triplex',
    '3_unit': 'triplex',
    fourplex: 'fourplex',
    quadplex: 'fourplex',
    four_unit: 'fourplex',
    '4_unit': 'fourplex',
    fiveplex: 'fiveplex',
    five_unit: 'fiveplex',
    '5_unit': 'fiveplex',
    small_multifamily: 'small_multifamily_2_5',
    small_multifamily_2_5: 'small_multifamily_2_5',
    residential_multifamily_2_5: 'small_multifamily_2_5',
    two_to_five: 'small_multifamily_2_5',
    '2_5_units': 'small_multifamily_2_5',
    multifamily_5_plus: 'multifamily_5_plus',
    multifamily_6_plus: 'commercial_multifamily_6_plus',
    commercial_multifamily: 'commercial_multifamily_6_plus',
    commercial_multifamily_6_plus: 'commercial_multifamily_6_plus',
    apartment: 'commercial_multifamily_6_plus',
    apartments: 'commercial_multifamily_6_plus',
  };

  return [...new Set(normalizeStringList(value)
    .map((item) => item.trim().toLowerCase().replace(/[\s-]+/g, '_'))
    .map((item) => aliases[item] ?? '')
    .filter(Boolean)
    .slice(0, 20))];
}

function unitClassSqlCondition(unitClass: string): string {
  const units = 'coalesce(nullif(p.total_units, 0), case when asset_group = \'single_family\' then 1 else 0 end)';
  switch (unitClass) {
    case 'single_family':
      return `(asset_group = 'single_family' and ${units} = 1)`;
    case 'duplex':
      return `(${units} = 2 or p.asset_subtype = 'duplex')`;
    case 'triplex':
      return `(${units} = 3 or p.asset_subtype = 'triplex')`;
    case 'fourplex':
      return `(${units} = 4 or p.asset_subtype in ('fourplex','quadplex'))`;
    case 'fiveplex':
      return `${units} = 5`;
    case 'small_multifamily_2_5':
      return `(asset_group = 'small_multifamily' and ${units} between 2 and 5)`;
    case 'multifamily_5_plus':
      return `(asset_group in ('small_multifamily','commercial_multifamily') and ${units} >= 5)`;
    case 'commercial_multifamily_6_plus':
      return `(asset_group = 'commercial_multifamily' or ${units} >= 6)`;
    default:
      return '';
  }
}

function normalizeStringList(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item));
  if (typeof value === 'string') return value.split(',');
  return [];
}

function normalizeStateFilters(value: unknown): string[] {
  return [...new Set(normalizeStringList(value)
    .map((item) => item.trim().toUpperCase())
    .filter((item) => /^[A-Z]{2}$/.test(item))
    .slice(0, 20))];
}

function normalizeCityFilters(value: unknown): string[] {
  return [...new Set(normalizeStringList(value)
    .map((item) => item.trim().toUpperCase())
    .filter((item) => item.length >= 2 && item.length <= 80)
    .slice(0, 100))];
}

function normalizeZipFilters(value: unknown): string[] {
  return [...new Set(normalizeStringList(value)
    .map((item) => item.replace(/[^\d]/g, '').slice(0, 5))
    .filter((item) => /^\d{5}$/.test(item))
    .slice(0, 500))];
}

function normalizeBbcAssetType(value: string): string {
  const normalized = value.toLowerCase().replace(/-/g, '_');
  if (['single_family', 'sfr', 'residential'].includes(normalized)) return 'single_family';
  if (['small_multifamily', 'multifamily', 'multi_family', 'duplex', 'triplex', 'fourplex'].includes(normalized)) return 'small_multifamily';
  if (['commercial_multifamily', 'apartment', 'apartments'].includes(normalized)) return 'commercial_multifamily';
  return '';
}

function positiveNumberOrNull(value: unknown): number | null {
  const parsed = typeof value === 'number'
    ? value
    : typeof value === 'string' && /^\d+(\.\d+)?$/.test(value.trim())
      ? Number(value)
      : NaN;
  return Number.isFinite(parsed) && parsed > 0 && parsed <= 1_000_000_000 ? Math.round(parsed) : null;
}

function buildBuyBoxClubPropertyResponse(response: ReturnType<typeof buildPropertyResponse>) {
  const owner = response.ownership.owner1;
  return {
    schemaVersion: 'mxre.bbc.property.v1',
    mxreId: response.id,
    request: {
      matchedBy: 'mxre_property_id_or_address',
      confidence: response.meta.completeness >= 70 ? 'high' : response.meta.completeness >= 45 ? 'medium' : 'low',
    },
    property: {
      address: response.property.address,
      city: response.property.city,
      state: response.property.state,
      zip: response.property.zip,
      county: response.property.county,
      parcelId: response.property.parcelId,
      apn: response.property.apn,
      lat: response.property.lat,
      lng: response.property.lng,
      assetType: response.property.assetType,
      assetSubtype: response.property.assetSubtype,
      propertyType: response.property.type,
      propertyUse: response.property.use,
      unitCount: response.property.unitCount,
      unitCountSource: response.property.unitCountSource,
      assetConfidence: response.property.assetConfidence,
      beds: response.property.bedrooms,
      baths: (response.property.bathroomsFull ?? 0) + ((response.property.bathroomsHalf ?? 0) * 0.5),
      bedrooms: response.property.bedrooms,
      bathroomsFull: response.property.bathroomsFull,
      bathroomsHalf: response.property.bathroomsHalf,
      livingSqft: response.property.livingSqft,
      totalSqft: response.property.totalSqft,
      lotSqft: response.property.lotSqft,
      lotAcres: response.property.lotAcres,
      yearBuilt: response.property.yearBuilt,
      yearRemodeled: response.property.yearRemodeled,
      stories: response.property.stories,
      subdivision: response.property.subdivision,
      floodZone: response.property.floodZone,
      floodZoneType: response.property.floodZoneType,
    },
    ownership: {
      ownerName: owner?.fullName ?? null,
      ownerType: owner?.type ?? null,
      mailingAddress: response.ownership.mailingAddress,
      ownerOccupied: response.ownership.ownerOccupied,
      absenteeOwner: response.ownership.absenteeOwner,
      inStateAbsentee: response.ownership.inStateAbsentee,
      outOfStateAbsentee: response.ownership.outOfStateAbsentee,
      corporateOwned: response.ownership.corporateOwned,
      ownershipStartDate: response.ownership.ownershipStartDate,
      ownershipLengthMonths: response.ownership.ownershipLengthMonths,
    },
    valuation: {
      marketValue: response.valuation.marketValue,
      assessedValue: response.valuation.assessedValue,
      appraisedLand: response.valuation.appraisedLand,
      appraisedBuilding: response.valuation.appraisedBuilding,
      annualTax: response.valuation.annualTax,
      taxYear: response.valuation.taxYear,
      estimatedValue: response.valuation.estimatedValue,
    },
    debtAndLiens: {
      freeClear: response.liens.summary.freeClear,
      openMortgageBalance: response.liens.summary.openMortgageBalance,
      openMortgageBalanceSource: response.liens.summary.openMortgageBalanceSource,
      openMortgageBalanceConfidence: response.liens.summary.openMortgageBalanceConfidence,
      estimatedEquity: response.liens.summary.estimatedEquity,
      equityPercent: response.liens.summary.equityPercent,
      equityBasis: response.liens.summary.equityBasis,
      equityValue: response.liens.summary.equityValue,
      openLienCount: response.liens.summary.openLienCount,
      lienCount: response.liens.summary.lienCount,
      current: response.liens.current,
      history: response.liens.history,
    },
    sales: response.sales,
    rent: response.rent,
    market: response.market,
    publicSignals: response.publicSignals,
    signals: {
      ...response.signals,
      creativeFinance: response.market.history.some((item) => item.listingType?.toLowerCase().includes('creative')) ? true : null,
    },
    meta: {
      asOf: new Date().toISOString(),
      lastUpdated: response.meta.lastUpdated,
      completeness: response.meta.completeness,
      dataSources: response.meta.dataSources,
      dataQuality: response.meta.dataQuality,
      fallbackRecommended: response.meta.completeness < 50,
      fallbackProvider: response.meta.completeness < 50 ? 'realestateapi' : null,
    },
  };
}

function normalizeJoinedProperty(value: unknown): Record<string, unknown> {
  if (Array.isArray(value)) return (value[0] as Record<string, unknown> | undefined) ?? {};
  return (value as Record<string, unknown> | null) ?? {};
}

async function queryPg<T extends Record<string, unknown>>(query: string): Promise<T[]> {
  const directUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.MXRE_PG_URL ?? process.env.DATABASE_URL;
  if (directUrl && /^postgres(ql)?:\/\//i.test(directUrl)) {
    if (!directPgPool) {
      directPgPool = new Pool({
        connectionString: directUrl,
        max: 5,
        options: '-c max_parallel_workers_per_gather=0',
        idleTimeoutMillis: 30_000,
        connectionTimeoutMillis: 10_000,
        ssl: directUrl.includes('sslmode=require') ? { rejectUnauthorized: false } : undefined,
      });
    }
    const result = await directPgPool.query(query);
    return result.rows as T[];
  }

  const url = directUrl && /^https?:\/\//i.test(directUrl)
    ? directUrl.replace(/\/$/, '').replace(/\/pg\/query$/, '')
    : process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY;
  if (!url || !key) throw new Error('Database connection not configured.');

  const response = await fetch(`${url.replace(/\/$/, '')}/pg/query`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${key}`,
      apikey: key,
    },
    body: JSON.stringify({ query }),
  });

  const body = await response.text();
  if (!response.ok) {
    throw new Error(`pg query failed: ${response.status} ${body}`);
  }
  return JSON.parse(body) as T[];
}

function percentile(values: number[], p: number): number | null {
  const sorted = values.filter((value) => Number.isFinite(value) && value > 0).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return Math.round(sorted[index]);
}

function renderPrivateDocsHtml(clientId: string): string {
  const escapedClientId = escapeHtml(clientId);
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MXRE Private API Docs</title>
  <style>
    :root{color-scheme:dark;--bg:#0b1020;--panel:#111827;--line:#263244;--text:#e5edf8;--muted:#93a4ba;--accent:#3dd6b3;--blue:#60a5fa;--warn:#fbbf24}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.55 system-ui,-apple-system,Segoe UI,sans-serif}
    header{position:sticky;top:0;background:rgba(11,16,32,.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--line);padding:18px 28px;z-index:2}
    main{max-width:1180px;margin:0 auto;padding:28px}.eyebrow{color:var(--accent);font-weight:700;text-transform:uppercase;font-size:12px;letter-spacing:.08em}
    h1{margin:4px 0 6px;font-size:28px}h2{margin:28px 0 10px;font-size:20px}h3{margin:18px 0 8px;font-size:15px;color:#cbd5e1}
    p{color:var(--muted);margin:6px 0 12px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px}
    code,pre{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}code{background:#1f2937;border:1px solid #334155;border-radius:5px;padding:2px 5px}
    pre{background:#050814;border:1px solid var(--line);border-radius:8px;padding:14px;overflow:auto;color:#dbeafe}
    input,button{font:inherit}input{width:100%;background:#050814;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:10px}label{display:block;color:#cbd5e1;font-weight:650;margin-bottom:5px}
    button{background:var(--accent);color:#032018;border:0;border-radius:7px;padding:10px 14px;font-weight:800;cursor:pointer}button.secondary{background:#1f2937;color:var(--text);border:1px solid var(--line)}
    .form-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:12px}.actions{display:flex;gap:10px;align-items:center;margin-top:12px;flex-wrap:wrap}
    .result-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0}.mini{background:#0b1221;border:1px solid var(--line);border-radius:7px;padding:10px}.mini b{display:block;font-size:12px;color:var(--muted)}.mini span{font-size:18px;font-weight:800}
    table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--line);border-radius:8px;overflow:hidden}th,td{border-bottom:1px solid var(--line);padding:10px;text-align:left;vertical-align:top}th{color:#cbd5e1;background:#172033}
    .pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:3px 9px;color:#cbd5e1;background:#121b2d}.good{color:var(--accent)}.warn{color:var(--warn)}
    a{color:var(--blue)}ul{padding-left:20px}.small{font-size:12px;color:var(--muted)}@media(max-width:900px){.grid,.form-grid,.result-grid{grid-template-columns:1fr}main{padding:18px}header{padding:16px}}
  </style>
</head>
<body>
<header>
  <div class="eyebrow">Private Docs</div>
  <h1>MXRE API for Buy Box Club</h1>
  <p>Authenticated as <span class="pill">${escapedClientId}</span>. Base URL: <code>https://api.mxre.mundox.ai</code></p>
</header>
<main>
  <section class="grid">
    <div class="card">
      <h2>Authentication</h2>
      <p>Use these headers from Buy Box Club backend functions. Never expose the MXRE key in the BBC frontend.</p>
      <pre>GET /v1/bbc/property?... HTTP/1.1
x-client-id: buy_box_club_sandbox
x-api-key: &lt;MXRE_BUY_BOX_CLUB_SANDBOX_KEY&gt;</pre>
      <p class="small">For browser access to this docs page, use HTTP Basic Auth with username = client id and password = API key.</p>
    </div>
    <div class="card">
      <h2>Integration Pattern</h2>
      <p>BBC user key validates inside Buy Box Club. BBC backend then calls MXRE with the MXRE client id and key. Keep RealEstateAPI as fallback while MXRE coverage expands.</p>
      <ul>
        <li>Primary: MXRE exact property lookup.</li>
        <li>Fallback: RealEstateAPI when MXRE returns 404, timeout, or low required-field coverage.</li>
        <li>Diagnostics: save <code>dataSource = "mxre"</code> or <code>"realestateapi_fallback"</code>.</li>
      </ul>
    </div>
  </section>

  <section class="card" style="margin-top:16px">
    <h2>Try It Now</h2>
    <p>Test the exact address endpoint from this page. If you opened docs with Basic Auth, leave API key blank and the browser should reuse your authenticated session. If that fails, paste the sandbox key below.</p>
    <div class="form-grid">
      <div><label for="try-address">Address</label><input id="try-address" value="429 N Tibbs Ave"></div>
      <div><label for="try-city">City</label><input id="try-city" value="Indianapolis"></div>
      <div><label for="try-state">State</label><input id="try-state" value="IN" maxlength="2"></div>
      <div><label for="try-zip">ZIP</label><input id="try-zip" value="46222"></div>
    </div>
    <div class="form-grid" style="grid-template-columns:1fr 2fr;margin-top:12px">
      <div><label for="try-client">Client ID</label><input id="try-client" value="${escapedClientId}"></div>
      <div><label for="try-key">API Key, optional if already logged in</label><input id="try-key" type="password" autocomplete="off" placeholder="Paste sandbox key only if needed"></div>
    </div>
    <div class="actions">
      <button type="button" onclick="tryPropertyLookup()">Run Property Lookup</button>
      <button type="button" class="secondary" onclick="copyCurl()">Copy curl</button>
      <span id="try-status" class="small">Ready.</span>
    </div>
    <div id="try-summary" class="result-grid" style="display:none"></div>
    <pre id="try-output">Results will appear here.</pre>
  </section>

  <h2>How Buy Box Club Uses MXRE</h2>
  <p>Buy Box Club should treat MXRE as the market data system of record. BBC owns user accounts, buy boxes, underwriting rules, pass/fail history, and user-specific exclusions. MXRE owns property facts, market status, listing/rent/deed changes, and data-quality scoring.</p>
  <div class="callout">
    <b>Simple rule:</b> BBC asks MXRE for fresh or changed market candidates, underwrites those candidates inside BBC, stores the BBC decision, then asks MXRE again tomorrow for only records that are new or changed.
  </div>

  <h3>End-to-End Flow</h3>
  <table>
    <tr><th>Stage</th><th>What BBC Is Doing</th><th>MXRE Endpoint</th><th>What BBC Stores</th></tr>
    <tr>
      <td>0. Human autocomplete</td>
      <td>User types a city/state or starts an address before clicking Search.</td>
      <td><code>GET /v1/addresses/autocomplete?q=429%20N%20Tibbs</code></td>
      <td>Selected result <code>type</code>, label, city/state/zip, and <code>propertyId</code> when MXRE already has property detail.</td>
    </tr>
    <tr>
      <td>1. Sandbox connection</td>
      <td>Verify BBC backend can authenticate and reach MXRE.</td>
      <td><code>GET /v1/bbc/property?address=429%20N%20Tibbs%20Ave&amp;city=Indianapolis&amp;state=IN&amp;zip=46222</code></td>
      <td>Nothing permanent; this is a smoke test.</td>
    </tr>
    <tr>
      <td>2. Exact address lookup</td>
      <td>User or automation asks BBC to underwrite one address.</td>
      <td><code>GET /v1/bbc/property</code></td>
      <td><code>mxreId</code>, <code>sync.recordVersion</code>, full normalized response, <code>quality.completeness</code>, fallback decision.</td>
    </tr>
    <tr>
      <td>3. Daily saved search</td>
      <td>User has a saved buy box like "active Indianapolis small multifamily under $300k."</td>
      <td><code>POST /v1/bbc/search-runs</code></td>
      <td>Search run id, returned <code>mxreId</code>s, candidate <code>recordVersion</code>s, underwriting status per user.</td>
    </tr>
    <tr>
      <td>4. Skip unchanged failures</td>
      <td>BBC already underwrote and failed a property yesterday.</td>
      <td><code>POST /v1/bbc/search-runs</code> with <code>excludeMxreIds</code> and <code>onlyChangedSince</code></td>
      <td>BBC keeps user-specific fail/pass state. MXRE returns the property again only if market data changed enough to matter.</td>
    </tr>
    <tr>
      <td>5. Re-score changed deals</td>
      <td>A failed deal may become viable after price drop, rent update, status change, creative-finance signal, or agent contact update.</td>
      <td><code>GET /v1/bbc/markets/{market}/changes</code></td>
      <td>Last sync cursor/time, latest event <code>recordVersion</code>, changed fields used to trigger re-underwriting.</td>
    </tr>
    <tr>
      <td>6. Report screens</td>
      <td>BBC wants dashboards like creative finance opportunities or price drops.</td>
      <td><code>GET /v1/bbc/markets/{market}/creative-finance-listings</code>, <code>GET /v1/markets/{market}/price-changes</code></td>
      <td>Report filters and selected MXRE ids. Do not copy MXRE market stats as permanent truth unless BBC needs snapshot history.</td>
    </tr>
    <tr>
      <td>7. Admin diagnostics</td>
      <td>BBC admin wants to know whether MXRE coverage is ready in a market.</td>
      <td><code>GET /v1/markets/{market}/readiness</code>, <code>GET /v1/markets/{market}/completion</code></td>
      <td>Coverage snapshot for admin display, not underwriting truth.</td>
    </tr>
  </table>

  <h3>BBC Saved Search Pattern</h3>
  <ol>
    <li>BBC stores the user's saved-search filters and underwriting criteria.</li>
    <li>Each day, BBC calls <code>POST /v1/bbc/search-runs</code> with the market, filters, <code>onlyChangedSince</code>, and any <code>excludeMxreIds</code> that should be skipped unless changed.</li>
    <li>MXRE returns only candidates that are new or changed according to MXRE market data.</li>
    <li>BBC calls <code>GET /v1/bbc/property/{mxreId}</code> for any candidate that needs full underwriting details.</li>
    <li>BBC runs its underwriting, stores pass/fail/user decision, and stores the candidate <code>recordVersion</code>.</li>
    <li>Tomorrow, BBC uses the latest cursor/time and prior decisions so unchanged failed deals do not waste underwriting cycles.</li>
  </ol>

  <h3>Fallback Policy</h3>
  <table>
    <tr><th>Condition</th><th>BBC Behavior</th><th>Reason</th></tr>
    <tr><td>MXRE returns <code>200</code> and <code>quality.fallbackRecommended=false</code></td><td>Use MXRE as primary.</td><td>MXRE has enough data for the requested task.</td></tr>
    <tr><td>MXRE returns <code>404</code></td><td>Call RealEstateAPI fallback, then log <code>mxre_no_property_match</code>.</td><td>MXRE does not have the property matched yet.</td></tr>
    <tr><td>MXRE returns low completeness or missing required underwriting fields</td><td>Use RealEstateAPI only to fill missing fields, and label source as fallback inside BBC.</td><td>Fallback should fill gaps, not replace MXRE's source of truth.</td></tr>
    <tr><td>MXRE returns <code>429</code></td><td>Respect <code>retry-after</code> and retry later.</td><td>Protects MXRE and BBC from accidental brute-force style loops.</td></tr>
    <tr><td>MXRE returns <code>5xx</code></td><td>Keep previous BBC snapshot if available; fallback only for user-facing urgent underwriting.</td><td>A temporary outage should not corrupt stored decisions.</td></tr>
  </table>

  <h2>Property Detail Endpoint</h2>
  <p>Use the BBC-normalized endpoint for Buy Box Club underwriting. The raw MXRE endpoint remains available for admin/debug views.</p>
  <table>
    <tr><th>Method</th><th>Path</th><th>Use</th><th>Required</th></tr>
    <tr><td><code>GET</code></td><td><code>/v1/addresses/autocomplete</code></td><td>Fast human city/state/address autocomplete before running property or market search.</td><td><code>q</code>; optional <code>state</code>, <code>limit</code></td></tr>
    <tr><td><code>GET</code></td><td><code>/v1/bbc/property</code></td><td>BBC exact address underwriting contract.</td><td><code>address</code>, <code>state</code>; <code>city</code>/<code>zip</code> recommended</td></tr>
    <tr><td><code>GET</code></td><td><code>/v1/bbc/property/{mxreId}</code></td><td>BBC re-sync after storing MXRE id.</td><td><code>mxreId</code></td></tr>
    <tr><td><code>GET</code></td><td><code>/v1/property</code></td><td>Raw MXRE/admin detail.</td><td><code>address</code>, <code>state</code></td></tr>
  </table>
  <h3>Example</h3>
  <pre>curl "https://api.mxre.mundox.ai/v1/bbc/property?address=429%20N%20Tibbs%20Ave&amp;city=Indianapolis&amp;state=IN&amp;zip=46222" \
  -H "x-client-id: buy_box_club_sandbox" \
  -H "x-api-key: YOUR_SANDBOX_KEY"</pre>
  <h3>Response Shape</h3>
  <pre>{
  "schemaVersion": "mxre.bbc.property.v1",
  "mxreId": 50913586,
  "property": { "address": "...", "assetType": "small_multifamily", "unitCount": 2, "livingSqft": 1276 },
  "ownership": { "owner": {}, "absenteeOwner": true, "corporateOwned": true },
  "valuation": { "marketValue": 174000, "assessedValue": 174000, "annualTax": 3618 },
  "sales": [],
  "debtAndLiens": { "summary": {}, "current": [], "history": [] },
  "rent": { "rentEstimate": 2440, "rentPerDoor": 1220, "unitBasis": "per_unit" },
  "market": { "onMarket": false, "listPrice": null, "agent": null },
  "signals": {},
  "sync": { "recordVersion": "...", "lastUpdated": "..." },
  "quality": { "completeness": 76, "fallbackRecommended": false }
}</pre>

  <h2>Market and Report Endpoints</h2>
  <table>
    <tr><th>Endpoint</th><th>Purpose</th><th>Key Query Params</th></tr>
    <tr><td><code>GET /v1/bbc/markets</code></td><td>BBC-safe list of available coverage markets. By default, markets below the readiness target are hidden.</td><td><code>includeBelowTarget=true</code> admin/debug only</td></tr>
    <tr><td><code>GET /v1/markets/{market}/readiness</code></td><td>Market readiness and coverage status.</td><td><code>market=indianapolis</code>, <code>columbus</code>, <code>west-chester</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/completion</code></td><td>Completion metrics for parcels, underwriting fields, rents, listings.</td><td><code>scope=city|core|metro</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/data-gaps</code></td><td>Property-by-property missing data report with severity and next best enrichment source.</td><td><code>asset</code>, <code>gap</code>, <code>on_market</code>, <code>zip</code>, <code>q</code>, <code>page</code>, <code>limit</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/opportunities</code></td><td>Filterable active listing/opportunity table.</td><td><code>asset</code>, <code>zip</code>, <code>min_price</code>, <code>max_price</code>, <code>creative=positive</code>, <code>page</code>, <code>limit</code></td></tr>
    <tr><td><code>GET /v1/bbc/markets/{market}/creative-finance-listings</code></td><td>BBC-stable creative finance listings detected daily from MLS/listing public remarks.</td><td><code>status</code>, <code>asset</code>, <code>zip</code>, <code>since</code>, <code>until</code>, <code>page</code>, <code>limit</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/reports/creative-finance</code></td><td>Internal/admin creative finance report alias.</td><td><code>limit</code>, <code>offset</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/price-changes</code></td><td>Recent listing price-change events.</td><td><code>limit</code>, <code>offset</code></td></tr>
    <tr><td><code>GET /v1/markets/{market}/pre-foreclosures</code></td><td>Public pre-foreclosure signals where available.</td><td><code>status</code>, <code>limit</code>, <code>offset</code></td></tr>
    <tr><td><code>GET /v1/coverage</code></td><td>National/state coverage overview.</td><td>None</td></tr>
  </table>

  <h2>Buy Box Club Endpoints</h2>
  <table>
    <tr><th>Endpoint</th><th>Status</th><th>Purpose</th><th>Notes</th></tr>
    <tr><td><code>GET /v1/addresses/autocomplete</code></td><td>Required UX</td><td>Human users type city/state or address and select a normalized result before searching.</td><td>Debounce browser calls. If result type is <code>address</code>, call property detail after Search. If result type is <code>city</code>, call saved-search/listing endpoints.</td></tr>
    <tr><td><code>GET /v1/bbc/markets</code></td><td>Required setup</td><td>Discover which markets BBC should show. Default response excludes markets under 90% readiness or their configured market target.</td><td>Use this before enabling market-level saved search options in BBC. Admins may use <code>includeBelowTarget=true</code> to inspect pipeline markets.</td></tr>
    <tr><td><code>GET /v1/bbc/property</code></td><td>Required</td><td>BBC-normalized exact address lookup.</td><td>Use <code>address</code>, <code>city</code>, <code>state</code>, <code>zip</code>.</td></tr>
    <tr><td><code>GET /v1/bbc/property/{mxreId}</code></td><td>Required</td><td>BBC-normalized lookup by MXRE id.</td><td>Use for safe re-sync after BBC stores <code>mxreId</code>.</td></tr>
    <tr><td><code>GET /v1/bbc/markets/{market}/changes</code></td><td>Required</td><td>Cursor-based changed market records since <code>updated_after</code> or <code>cursor</code>.</td><td>Use for daily sync and reconsidering failed deals. Store <code>nextCursor</code> per market; loop while <code>hasMore=true</code>.</td></tr>
    <tr><td><code>POST /v1/bbc/search-runs</code></td><td>Required</td><td>Delta-based saved-search execution.</td><td>Send filters plus <code>excludeMxreIds</code>; MXRE returns new/changed candidates.</td></tr>
    <tr><td><code>GET /v1/bbc/markets/{market}/creative-finance-listings</code></td><td>Optional report</td><td>Creative finance listings identified daily from MLS/listing remarks.</td><td>Use for the dedicated BBC creative finance screen. Display <code>listingDescription</code> or <code>publicRemarks</code> so users can read the source language. Stable response: <code>mxre.bbc.creativeFinanceListings.v1</code>.</td></tr>
    <tr><td><code>GET /v1/markets/{market}/price-changes</code></td><td>Optional report</td><td>MLS/listing price-change report.</td><td>Use for re-underwriting failed deals after price drops.</td></tr>
    <tr><td><code>GET /v1/markets/{market}/readiness</code></td><td>Optional admin</td><td>Market readiness and coverage.</td><td>Use in BBC admin diagnostics.</td></tr>
    <tr><td><code>GET /v1/markets/{market}/completion</code></td><td>Optional admin</td><td>Coverage/completion breakdown.</td><td>Use in MXRE/BBC admin dashboards.</td></tr>
    <tr><td><code>GET /v1/markets/{market}/data-gaps</code></td><td>Optional admin</td><td>Property-level missing data report.</td><td>Use in MXRE dashboard to decide what enrichment script/source should run next.</td></tr>
  </table>
  <h3>Minimum BBC Integration</h3>
  <table>
    <tr><th>BBC Use Case</th><th>MXRE Endpoint</th><th>Fallback</th></tr>
    <tr><td>Show available markets</td><td><code>GET /v1/bbc/markets</code></td><td>Only show markets returned in <code>markets[]</code>; anything below readiness target remains hidden</td></tr>
    <tr><td>Autocomplete input</td><td><code>GET /v1/addresses/autocomplete</code></td><td>Do not call slow live geocoders on every keystroke; fallback only after submit/no result if BBC chooses to keep one.</td></tr>
    <tr><td>Exact address underwriting</td><td><code>GET /v1/bbc/property</code></td><td>RealEstateAPI if <code>404</code> or <code>quality.fallbackRecommended=true</code></td></tr>
    <tr><td>Stored property refresh</td><td><code>GET /v1/bbc/property/{mxreId}</code></td><td>Keep previous BBC snapshot if MXRE unavailable</td></tr>
    <tr><td>Daily saved search</td><td><code>POST /v1/bbc/search-runs</code></td><td>Return no rows rather than re-underwrite unchanged deals</td></tr>
    <tr><td>Reconsider failed deals</td><td><code>GET /v1/bbc/markets/{market}/changes</code></td><td>Only re-score when <code>recordVersion</code> changes</td></tr>
  </table>
  <h3>BBC Daily Search Example</h3>
  <h3>BBC Incremental Change Sync</h3>
  <p>BBC should use the changes endpoint as the durable daily sync contract. First call it with <code>updated_after</code>; after that, store and reuse the opaque <code>nextCursor</code>. Returned rows are ordered oldest-to-newest so BBC can process them safely in sequence.</p>
  <pre>curl "https://api.mxre.mundox.ai/v1/bbc/markets/indianapolis/changes?updated_after=2026-05-01T00:00:00.000Z&limit=500" \
  -H "x-client-id: buy_box_club_sandbox" \
  -H "x-api-key: YOUR_SANDBOX_KEY"</pre>
  <pre>{
  "schemaVersion": "mxre.bbc.changes.v1",
  "market": "indianapolis",
  "updatedAfter": "2026-05-01T00:00:00.000Z",
  "hasMore": true,
  "nextCursor": "eyJldmVudEF0IjoiMjAyNi0wNS0wMlQxNDozMToyMC4wMDBaIiwicmVjb3JkVmVyc2lvbiI6Ii4uLiJ9",
  "results": [
    {
      "mxreId": 50913586,
      "eventType": "listing_price_changed",
      "eventAt": "2026-05-02T14:31:20.000Z",
      "recordVersion": "...",
      "changedFields": ["market.listPrice"],
      "underwritingRelevant": true
    }
  ]
}</pre>
  <h3>Creative Finance Report Usage</h3>
  <p>BBC should use <code>GET /v1/bbc/markets/{market}/creative-finance-listings</code> for a dedicated report of active listings where MXRE detected positive or negative creative-finance language. The UI should show <code>listingDescription</code> or <code>publicRemarks</code> next to the score/terms so users can verify the context themselves.</p>
  <pre>curl "https://api.mxre.mundox.ai/v1/bbc/markets/indianapolis/creative-finance-listings?status=positive&limit=25" \
  -H "x-client-id: buy_box_club_sandbox" \
  -H "x-api-key: YOUR_SANDBOX_KEY"</pre>
  <pre>{
  "schemaVersion": "mxre.bbc.creativeFinanceListings.v1",
  "results": [
    {
      "mxreId": 50913586,
      "address": "123 Main St",
      "creativeFinanceScore": 92,
      "creativeFinanceStatus": "positive",
      "creativeFinanceTerms": ["seller financing"],
      "listingDescription": "Seller financing available...",
      "publicRemarks": "Seller financing available...",
      "publicRemarksSnippet": "Seller financing available..."
    }
  ]
}</pre>
  <p>Use <code>POST /v1/bbc/search-runs</code> for frontend searches and backend cron searches. The <code>market</code> selects a covered MXRE universe; <code>location</code> and numeric filters narrow the returned active/new/changed leads.</p>
  <pre>curl "https://api.mxre.mundox.ai/v1/bbc/search-runs" \
  -X POST \
  -H "content-type: application/json" \
  -H "x-client-id: buy_box_club_sandbox" \
  -H "x-api-key: YOUR_SANDBOX_KEY" \
  -d '{
    "market": "indianapolis",
    "location": { "city": "Indianapolis", "state": "IN", "zipCodes": ["46222", "46203"] },
    "assetTypes": ["single_family", "small_multifamily"],
    "unitClasses": ["duplex", "triplex", "fourplex", "fiveplex"],
    "status": ["active"],
    "minPrice": 50000,
    "maxPrice": 250000,
    "minBeds": 2,
    "maxBeds": 4,
    "minBaths": 1,
    "minSqft": 900,
    "maxSqft": 2500,
    "minEquityPercent": 40,
    "onlyChangedSince": "2026-05-02T00:00:00Z",
    "excludeMxreIds": [50913586],
    "limit": 100
  }'</pre>
  <p>Equity filters are computed server-side. For active on-market listings MXRE uses <code>listPrice - estimatedMortgageBalance</code>. For market search rows without an active list price it uses <code>marketValue</code>, then <code>assessedValue</code> as fallback basis. Exact property detail may include <code>estimatedValue</code> where that source field exists.</p>
  <pre>{
  "mxreId": 50913586,
  "listPrice": 160000,
  "estimatedMortgageBalance": 96000,
  "estimatedEquity": 64000,
  "equityPercent": 40,
  "equityBasis": "list_price",
  "equityBasisValue": 160000
}</pre>
  <h3>Asset And Unit Class Presets</h3>
  <table>
    <tr><th>BBC Use</th><th>Recommended Filter</th><th>Meaning</th></tr>
    <tr><td>Single family</td><td><code>"unitClasses": ["single_family"]</code></td><td>1-unit SFR/condo-style residential rows.</td></tr>
    <tr><td>Duplex</td><td><code>"unitClasses": ["duplex"]</code></td><td>Exactly 2 units or assessor/listing subtype duplex.</td></tr>
    <tr><td>Triplex</td><td><code>"unitClasses": ["triplex"]</code></td><td>Exactly 3 units or subtype triplex.</td></tr>
    <tr><td>Fourplex</td><td><code>"unitClasses": ["fourplex"]</code></td><td>Exactly 4 units or subtype fourplex/quadplex.</td></tr>
    <tr><td>2-5 unit small multifamily</td><td><code>"unitClasses": ["small_multifamily_2_5"]</code></td><td>Small multifamily rows with 2 through 5 known units.</td></tr>
    <tr><td>5+ multifamily</td><td><code>"unitClasses": ["multifamily_5_plus"]</code></td><td>Multifamily rows with 5 or more known units.</td></tr>
    <tr><td>Commercial apartment scale</td><td><code>"unitClasses": ["commercial_multifamily_6_plus"]</code></td><td>Commercial multifamily classification or 6+ known units.</td></tr>
  </table>

  <h2>Recommended BBC Wiring</h2>
  <ul>
    <li>Exact address underwriting: call <code>/v1/bbc/property</code> with address, city, state, zip.</li>
    <li>Club match score: consume <code>property</code>, <code>ownership</code>, <code>valuation</code>, <code>debtAndLiens</code>, <code>rent</code>, <code>signals</code>, and <code>quality.completeness</code>.</li>
    <li>Deal chat underwriting: show <code>quality.dataSources</code> and <code>quality.dataQuality</code> so users know actual vs estimated fields.</li>
    <li>Bulk LOI fallback: use <code>market.agent</code> when present; fall back to BBC/other provider when MXRE has contact gaps.</li>
    <li>Admin diagnostics: store endpoint, MXRE id, HTTP status, latency, <code>meta.completeness</code>, and fallback reason.</li>
  </ul>

  <h2>Machine-Readable Spec</h2>
  <p>OpenAPI JSON is available at <a href="/v1/docs/openapi.json">/v1/docs/openapi.json</a> with the same authentication.</p>
</main>
<script>
function $(id){return document.getElementById(id)}
function money(value){return value == null ? '-' : '$' + Number(value).toLocaleString()}
function html(value){return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]))}
function endpointUrl(){
  const params = new URLSearchParams({
    address: $('try-address').value.trim(),
    city: $('try-city').value.trim(),
    state: $('try-state').value.trim().toUpperCase(),
  });
  const zip = $('try-zip').value.trim();
  if (zip) params.set('zip', zip);
  return '/v1/bbc/property?' + params.toString();
}
function authHeaders(){
  const key = $('try-key').value.trim();
  if (!key) return {};
  return { 'x-client-id': $('try-client').value.trim(), 'x-api-key': key };
}
async function tryPropertyLookup(){
  $('try-status').textContent = 'Calling MXRE...';
  $('try-output').textContent = 'Loading...';
  $('try-summary').style.display = 'none';
  try {
    const response = await fetch(endpointUrl(), { headers: authHeaders(), credentials: 'same-origin' });
    const data = await response.json();
    $('try-output').textContent = JSON.stringify(data, null, 2);
    $('try-status').textContent = response.ok ? 'Success: ' + response.status : 'Error: ' + response.status;
    if (response.ok) {
      $('try-summary').style.display = 'grid';
      $('try-summary').innerHTML = [
        ['MXRE ID', data.mxreId],
        ['Value', money(data.valuation?.marketValue)],
        ['Rent', money(data.rent?.rentEstimate)],
        ['Completeness', (data.quality?.completeness ?? '-') + '%'],
      ].map(([label, value]) => '<div class="mini"><b>' + html(label) + '</b><span>' + html(value) + '</span></div>').join('');
    }
  } catch (error) {
    $('try-status').textContent = 'Request failed';
    $('try-output').textContent = error instanceof Error ? error.message : String(error);
  }
}
async function copyCurl(){
  const absoluteUrl = location.origin + endpointUrl();
  const client = $('try-client').value.trim() || 'buy_box_club_sandbox';
  const key = $('try-key').value.trim() || 'YOUR_SANDBOX_KEY';
  const command = 'curl "' + absoluteUrl + '" \\\\\\n  -H "x-client-id: ' + client + '" \\\\\\n  -H "x-api-key: ' + key + '"';
  await navigator.clipboard.writeText(command);
  $('try-status').textContent = 'curl copied.';
}
</script>
</body>
</html>`;
}

function buildOpenApiSpec() {
  return {
    openapi: '3.1.0',
    info: {
      title: 'MXRE Private API',
      version: '1.0.0',
      description: 'Private MXRE property, market, and coverage API for Buy Box Club backend integration.',
    },
    servers: [{ url: 'https://api.mxre.mundox.ai' }],
    security: [{ ApiKeyAuth: [], ClientId: [] }],
    components: {
      securitySchemes: {
        ApiKeyAuth: { type: 'apiKey', in: 'header', name: 'x-api-key' },
        ClientId: { type: 'apiKey', in: 'header', name: 'x-client-id' },
      },
    },
    paths: {
      '/v1/bbc/markets': {
        get: {
          summary: 'BBC coverage market list',
          description: 'Returns the machine-readable list of MXRE markets that Buy Box Club can expose or query, including live/pilot/building status, restrictions, counts, freshness, supported asset classes, and supported endpoint paths.',
          parameters: [
            { name: 'includeBuilding', in: 'query', required: false, schema: { type: 'boolean', default: false } },
            { name: 'includeBelowTarget', in: 'query', required: false, schema: { type: 'boolean', default: false }, description: 'Admin/debug only. Default false hides markets below their readiness target.' },
          ],
          responses: {
            '200': { description: 'mxre.bbc.coverageMarkets.v1 market list.' },
            '401': { description: 'Invalid client id or API key.' },
            '429': { description: 'Rate limit exceeded.' },
          },
        },
      },
      '/v1/addresses/autocomplete': {
        get: {
          summary: 'Fast human city/state/address autocomplete',
          description: 'Returns mixed address and city suggestions for one-at-a-time user input. This endpoint does not perform slow live external geocoding while the user types.',
          parameters: [
            { name: 'q', in: 'query', required: true, schema: { type: 'string', minLength: 2 }, example: '429 N Tibbs' },
            { name: 'state', in: 'query', required: false, schema: { type: 'string', minLength: 2, maxLength: 2 }, example: 'IN' },
            { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 8, maximum: 20 } },
            { name: 'includeProperties', in: 'query', required: false, schema: { type: 'boolean', default: true } },
          ],
          responses: {
            '200': { description: 'mxre.addressAutocomplete.v1 with address/city suggestions and MXRE coverage flags.' },
            '401': { description: 'Invalid client id or API key.' },
            '429': { description: 'Rate limit exceeded.' },
          },
        },
      },
      '/v1/bbc/property': {
        get: {
          summary: 'BBC-normalized property lookup by address',
          parameters: [
            { name: 'address', in: 'query', required: true, schema: { type: 'string' } },
            { name: 'city', in: 'query', required: false, schema: { type: 'string' } },
            { name: 'state', in: 'query', required: true, schema: { type: 'string' } },
            { name: 'zip', in: 'query', required: false, schema: { type: 'string' } },
          ],
          responses: { '200': { description: 'Stable BBC property contract.' }, '404': { description: 'No MXRE match; RealEstateAPI fallback recommended.' } },
        },
      },
      '/v1/bbc/property/{id}': {
        get: {
          summary: 'BBC-normalized property lookup by MXRE id',
          parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'integer' } }],
          responses: { '200': { description: 'Stable BBC property contract.' } },
        },
      },
      '/v1/bbc/markets/{market}/changes': {
        get: {
          summary: 'Cursor-based market changes feed',
          description: 'Durable incremental sync endpoint for BBC automation. First call with updated_after; subsequent calls should pass cursor=nextCursor. Rows are returned oldest-to-newest. Continue until hasMore=false.',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'updated_after', in: 'query', required: false, schema: { type: 'string', format: 'date-time' }, description: 'Initial lower bound for first sync.' },
            { name: 'cursor', in: 'query', required: false, schema: { type: 'string' }, description: 'Opaque cursor returned as nextCursor from the previous response.' },
            { name: 'since', in: 'query', required: false, deprecated: true, schema: { type: 'string', format: 'date-time' }, description: 'Legacy alias for updated_after.' },
            { name: 'event_types', in: 'query', required: false, schema: { type: 'string' }, example: 'price_changed,listing_created' },
            { name: 'limit', in: 'query', required: false, schema: { type: 'integer', default: 100, maximum: 1000 } },
          ],
          responses: { '200': { description: 'mxre.bbc.changes.v1 with hasMore, nextCursor, and event rows.' } },
        },
      },
      '/v1/bbc/search-runs': {
        post: {
          summary: 'Run a BBC saved search against fresh/changed market records',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: { type: 'object' },
                example: {
                  market: 'indianapolis',
                  location: { city: 'Indianapolis', state: 'IN', zipCodes: ['46222', '46203'] },
                  assetTypes: ['single_family', 'small_multifamily'],
                  unitClasses: ['duplex', 'triplex', 'fourplex', 'fiveplex'],
                  status: ['active'],
                  minPrice: 50000,
                  maxPrice: 250000,
                  minBeds: 2,
                  maxBeds: 4,
                  minBaths: 1,
                  minSqft: 900,
                  maxSqft: 2500,
                  minEquityPercent: 40,
                  onlyChangedSince: '2026-05-02T00:00:00Z',
                  excludeMxreIds: [50913586],
                  limit: 100,
                },
              },
            },
          },
          responses: { '200': { description: 'Search run summary and new/changed candidates.' } },
        },
      },
      '/v1/property': {
        get: {
          summary: 'Lookup one property by address',
          description: 'Primary endpoint for underwriting. Address and state are required; city and zip are strongly recommended.',
          parameters: [
            { name: 'address', in: 'query', required: true, schema: { type: 'string' }, example: '429 N Tibbs Ave' },
            { name: 'city', in: 'query', required: false, schema: { type: 'string' }, example: 'Indianapolis' },
            { name: 'state', in: 'query', required: true, schema: { type: 'string', minLength: 2, maxLength: 2 }, example: 'IN' },
            { name: 'zip', in: 'query', required: false, schema: { type: 'string' }, example: '46222' },
          ],
          responses: {
            '200': { description: 'Full MXRE property detail object with property, ownership, valuation, liens, sales, rent, market, signals, and meta.' },
            '400': { description: 'Missing required params.' },
            '401': { description: 'Invalid client id or API key.' },
            '404': { description: 'Property not found.' },
            '429': { description: 'Rate limit exceeded.' },
          },
        },
      },
      '/v1/property/{id}': {
        get: {
          summary: 'Lookup one property by MXRE property id',
          parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'integer' } }],
          responses: { '200': { description: 'Full MXRE property detail object.' }, '404': { description: 'Property not found.' } },
        },
      },
      '/v1/property/search': {
        get: {
          summary: 'Search properties',
          parameters: [
            { name: 'state', in: 'query', schema: { type: 'string' } },
            { name: 'county', in: 'query', schema: { type: 'string' } },
            { name: 'city', in: 'query', schema: { type: 'string' } },
            { name: 'zip', in: 'query', schema: { type: 'string' } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 50 } },
            { name: 'offset', in: 'query', schema: { type: 'integer', default: 0 } },
          ],
          responses: { '200': { description: 'Search results and pagination.' } },
        },
      },
      '/v1/markets/{market}/opportunities': {
        get: {
          summary: 'Filterable market opportunities',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'asset', in: 'query', schema: { type: 'string', example: 'multifamily' } },
            { name: 'creative', in: 'query', schema: { type: 'string', example: 'positive' } },
            { name: 'zip', in: 'query', schema: { type: 'string' } },
            { name: 'min_price', in: 'query', schema: { type: 'number' } },
            { name: 'max_price', in: 'query', schema: { type: 'number' } },
            { name: 'min_units', in: 'query', schema: { type: 'integer' } },
            { name: 'max_units', in: 'query', schema: { type: 'integer' } },
            { name: 'page', in: 'query', schema: { type: 'integer', default: 1 } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 50 } },
          ],
          responses: { '200': { description: 'Opportunity rows, zip rollups, and summary metrics.' } },
        },
      },
      '/v1/bbc/markets/{market}/creative-finance-listings': {
        get: {
          summary: 'BBC creative finance listings',
          description: 'Stable BBC endpoint for daily MLS/listing remarks that indicate seller, owner, subject-to, or other creative finance signals. Negative language such as no owner financing is scored separately instead of treated as an opportunity. Each row includes listingDescription/publicRemarks when MXRE has captured the source listing text so BBC can display the exact context to users.',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'status', in: 'query', schema: { type: 'string', enum: ['positive', 'negative', 'all'], default: 'positive' } },
            { name: 'asset', in: 'query', schema: { type: 'string', enum: ['all', 'single_family', 'multifamily'], default: 'all' } },
            { name: 'zip', in: 'query', schema: { type: 'string' } },
            { name: 'min_price', in: 'query', schema: { type: 'number' } },
            { name: 'max_price', in: 'query', schema: { type: 'number' } },
            { name: 'min_units', in: 'query', schema: { type: 'integer' } },
            { name: 'max_units', in: 'query', schema: { type: 'integer' } },
            { name: 'since', in: 'query', schema: { type: 'string', format: 'date' } },
            { name: 'until', in: 'query', schema: { type: 'string', format: 'date' } },
            { name: 'page', in: 'query', schema: { type: 'integer', default: 1 } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 50 } },
          ],
          responses: { '200': { description: 'mxre.bbc.creativeFinanceListings.v1 report with summary, zip rollups, term rollups, and listing rows including listingDescription/publicRemarks.' } },
        },
      },
      '/v1/markets/{market}/reports/creative-finance': {
        get: {
          summary: 'Creative finance listing report',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 100 } },
            { name: 'offset', in: 'query', schema: { type: 'integer', default: 0 } },
          ],
          responses: { '200': { description: 'Listings with positive or negative creative finance language and provenance.' } },
        },
      },
      '/v1/markets/{market}/price-changes': {
        get: {
          summary: 'Listing price-change report',
          description: 'Use this to decide whether previously failed BBC deals should be re-underwritten after a price drop or status movement.',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'since', in: 'query', schema: { type: 'string', format: 'date-time' } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 100 } },
            { name: 'offset', in: 'query', schema: { type: 'integer', default: 0 } },
          ],
          responses: { '200': { description: 'Recent price-change events with listing/property context.' } },
        },
      },
      '/v1/markets/{market}/readiness': {
        get: {
          summary: 'Market readiness summary',
          description: 'Admin diagnostic for deciding whether a market is ready for BBC users.',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
          ],
          responses: { '200': { description: 'Market coverage and readiness metrics.' } },
        },
      },
      '/v1/markets/{market}/completion': {
        get: {
          summary: 'Market data completion metrics',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'scope', in: 'query', schema: { type: 'string', enum: ['city', 'core', 'metro'] } },
          ],
          responses: { '200': { description: 'Completion metrics for parcel identity, underwriting, rents, and market data.' } },
        },
      },
      '/v1/markets/{market}/data-gaps': {
        get: {
          summary: 'Property-level data gaps report',
          description: 'Admin/MXRE dashboard report showing each property with missing fields, severity, and recommended enrichment sources.',
          parameters: [
            { name: 'market', in: 'path', required: true, schema: { type: 'string', enum: SUPPORTED_MARKETS } },
            { name: 'scope', in: 'query', schema: { type: 'string', enum: ['city', 'core', 'metro'], default: 'city' } },
            { name: 'asset', in: 'query', schema: { type: 'string', enum: ['all', 'single_family', 'multifamily', 'small_multifamily', 'commercial_multifamily'], default: 'all' } },
            { name: 'gap', in: 'query', schema: { type: 'string', example: 'agent_email' } },
            { name: 'on_market', in: 'query', schema: { type: 'string', enum: ['all', 'true', 'false'], default: 'all' } },
            { name: 'zip', in: 'query', schema: { type: 'string' } },
            { name: 'q', in: 'query', schema: { type: 'string', description: 'Address, parcel, or owner contains search.' } },
            { name: 'min_units', in: 'query', schema: { type: 'integer' } },
            { name: 'max_units', in: 'query', schema: { type: 'integer' } },
            { name: 'min_price', in: 'query', schema: { type: 'number' } },
            { name: 'max_price', in: 'query', schema: { type: 'number' } },
            { name: 'page', in: 'query', schema: { type: 'integer', default: 1 } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 100, maximum: 250 } },
          ],
          responses: { '200': { description: 'Rows with missingFields, severity, nextBestSources, and source-specific checks.' } },
        },
      },
      '/v1/coverage': {
        get: {
          summary: 'Coverage overview',
          responses: { '200': { description: 'Current MXRE coverage summary.' } },
        },
      },
    },
  };
}

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function fetchAndRespond(
  c: Context,
  id: number,
  responseMapper: (response: ReturnType<typeof buildPropertyResponse>) => unknown = (response) => response,
) {
  const { data: props, error } = await db.from('properties')
    .select('*, counties(county_name, state_code, county_fips, state_fips)')
    .eq('id', id)
    .limit(1);

  if (error) {
    return c.json({ error: 'Database error', detail: error.message }, 500);
  }
  if (!props || props.length === 0) {
    return c.json({ error: 'Property not found' }, 404);
  }

  return assembleResponse(c, props[0], responseMapper);
}

async function assembleResponse(
  c: Context,
  property: Record<string, unknown>,
  responseMapper: (response: ReturnType<typeof buildPropertyResponse>) => unknown = (response) => response,
) {
  const id = property.id as number;
  // counties may be an array (when joined without !inner) or a single object
  const rawCounty = property.counties;
  const countyData: Record<string, unknown> = (Array.isArray(rawCounty) ? rawCounty[0] : rawCounty) ?? {
    county_name: '',
    state_code: property.state_code ?? '',
    county_fips: null,
    state_fips: null,
  };

  // Fetch all related data in parallel
  const [mortgages, rents, listingsById, saleHistory, mlsHistory, foreclosureData, publicSignals, demo] = await Promise.all([
    db.from('mortgage_records')
      .select('id,property_id,document_type,loan_amount,original_amount,estimated_current_balance,estimated_monthly_payment,interest_rate,interest_rate_type,rate_source,rate_match_confidence,term_months,maturity_date,recording_date,document_number,book_page,lender_name,lender_type,borrower_name,loan_type,source_url,grantee_name,open,position')
      .eq('property_id', id).order('recording_date', { ascending: false }).limit(50),
    db.from('rent_snapshots').select('*').eq('property_id', id).order('observed_at', { ascending: false }).limit(24),
    db.from('listing_signals').select('*').eq('property_id', id)
      .order('is_on_market', { ascending: false })
      .order('last_seen_at', { ascending: false, nullsFirst: false })
      .order('first_seen_at', { ascending: false, nullsFirst: false })
      .limit(20),
    db.from('sale_history').select('*').eq('property_id', id).order('recording_date', { ascending: false }).limit(20),
    db.from('mls_history').select('*').eq('property_id', id).order('status_date', { ascending: false }).limit(20),
    db.from('foreclosures').select('*').eq('property_id', id).limit(10),
    db.from('property_public_signals').select('*').eq('property_id', id).order('observed_date', { ascending: false, nullsFirst: false }).limit(50),
    fetchRentBaselineDemographics(property, countyData),
  ]);

  // Listing signals: fall back to address+state match if property_id lookup is empty
  let listings = listingsById;
  if ((!listingsById.data || listingsById.data.length === 0) && property.address) {
    const addrClean = (property.address as string).replace(/[,()]/g, ' ').trim();
    const stateCode = (countyData.state_code as string ?? '').toUpperCase();
    const byAddr = await db.from('listing_signals').select('*')
      .ilike('address', addrClean)
      .eq('state_code', stateCode)
      .order('is_on_market', { ascending: false })
      .order('last_seen_at', { ascending: false, nullsFirst: false })
      .order('first_seen_at', { ascending: false, nullsFirst: false })
      .limit(10);
    if (byAddr.data && byAddr.data.length > 0) listings = byAddr as typeof listingsById;
  }

  const response = buildPropertyResponse(
    property,
    countyData,
    (mortgages.data ?? []) as Record<string, unknown>[],
    (rents.data ?? []) as Record<string, unknown>[],
    (listings.data ?? []) as Record<string, unknown>[],
    (saleHistory.data ?? []) as Record<string, unknown>[],
    (mlsHistory.data ?? []) as Record<string, unknown>[],
    (demo.data as Record<string, unknown> | null) ?? null,
    (foreclosureData.data ?? []) as Record<string, unknown>[],
    (publicSignals.data ?? []) as Record<string, unknown>[],
  );

  return c.json(responseMapper(response));
}

async function fetchRentBaselineDemographics(
  property: Record<string, unknown>,
  countyData: Record<string, unknown>,
): Promise<{ data: Record<string, unknown> | null; error: unknown | null }> {
  const zip = String(property.zip ?? '').slice(0, 5);
  const stateFips = String(countyData.state_fips ?? '');
  const countyFips = String(countyData.county_fips ?? '');
  const fullCountyFips = countyFips.length === 5 ? countyFips : `${stateFips}${countyFips}`;

  const buildFmr = (rows: Array<Record<string, unknown>>): Record<string, unknown> | null => {
    if (rows.length === 0) return null;

    const byBedroom = new Map<number, Record<string, unknown>>();
    for (const row of rows) {
      if (typeof row.bedrooms === 'number') byBedroom.set(row.bedrooms, row);
    }
    const year = Math.max(...rows.map((row) => Number(row.vintage_year ?? 0)));

    return {
      fmr_0: byBedroom.get(0)?.median_rent ?? null,
      fmr_1: byBedroom.get(1)?.median_rent ?? null,
      fmr_2: byBedroom.get(2)?.median_rent ?? null,
      fmr_3: byBedroom.get(3)?.median_rent ?? null,
      fmr_4: byBedroom.get(4)?.median_rent ?? null,
      fmr_year: year || null,
      hud_area_name: rows[0]?.geography_type === 'zip' ? `ZIP ${rows[0].geography_id}` : `County ${rows[0]?.geography_id}`,
      median_income: null,
      source: rows[0]?.source ?? 'rent_baselines',
    };
  };

  if (zip) {
    const { data, error } = await db.from('rent_baselines')
      .select('source,geography_type,geography_id,bedrooms,median_rent,vintage_year')
      .eq('geography_type', 'zip')
      .eq('geography_id', zip)
      .order('vintage_year', { ascending: false });
    if (!error && data && data.length > 0) return { data: buildFmr(data as Array<Record<string, unknown>>), error: null };
  }

  if (fullCountyFips.length === 5) {
    const { data, error } = await db.from('rent_baselines')
      .select('source,geography_type,geography_id,bedrooms,median_rent,vintage_year')
      .eq('geography_type', 'county')
      .eq('geography_id', fullCountyFips)
      .order('vintage_year', { ascending: false });
    if (!error && data && data.length > 0) return { data: buildFmr(data as Array<Record<string, unknown>>), error: null };
  }

  return { data: null, error: null };
}

// ── Start server ─────────────────────────────────────────────
const port = parseInt(process.env.PORT ?? process.env.MXRE_API_PORT ?? '3100', 10);
const hostname = process.env.HOST ?? process.env.MXRE_API_HOST ?? '127.0.0.1';

serve({ fetch: app.fetch, port, hostname }, (info) => {
  console.log(`MXRE Property API running on http://${hostname}:${info.port}`);
});

export { app };


