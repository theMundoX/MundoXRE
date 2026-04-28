import 'dotenv/config';
import { Hono, type Context } from 'hono';
import { serve } from '@hono/node-server';
import { getDb } from '../db/client.js';
import { buildPropertyResponse } from './transforms/build-response.js';
import type { PropertySummary } from './types.js';

const app = new Hono();
const db = getDb();

// ── Auth middleware (skip for /health) ────────────────────────
app.use('*', async (c, next) => {
  if (c.req.path === '/health' || c.req.path === '/' || c.req.path === '/dashboard' || c.req.path === '/preview/market-dashboard') return next();

  const apiKey = c.req.header('x-api-key');
  const expected = process.env.MXRE_API_KEY;

  if (!expected) {
    return c.json({ error: 'Server misconfigured: MXRE_API_KEY not set' }, 500);
  }
  if (!apiKey || apiKey !== expected) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  return next();
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
  const limit = Math.min(parseInt(c.req.query('limit') ?? '50', 10), 500);
  const offset = parseInt(c.req.query('offset') ?? '0', 10);

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
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
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
      listing_brokerage,
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
    .eq('properties.county_id', 797583)
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
    .eq('properties.county_id', 797583)
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
        listingBrokerage: row.listing_brokerage ?? null,
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
    market: 'indianapolis',
    geography: { city: 'Indianapolis', county: 'Marion', state: 'IN', countyId: 797583 },
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
  const market = c.req.param('market').toLowerCase();
  if (!['indianapolis', 'indy'].includes(market)) {
    return c.json({ error: 'Unsupported market', supported_markets: ['indianapolis'] }, 400);
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
      where county_id = 797583
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
        and p.county_id = 797583
        and p.asset_type in ('small_multifamily', 'apartment', 'commercial_multifamily')
        ${activeUnitFilterSql}
    ),
    external_active as (
      select *
      from external_market_listings
      where market = 'indianapolis'
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
    market: 'indianapolis',
    geography: { city: 'Indianapolis', county: 'Marion', state: 'IN', countyId: 797583 },
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
function fmt(n) { return n?.toLocaleString('en-US') ?? '—'; }

async function load() {
  document.getElementById('last-updated').textContent = 'Loading...';
  try {
    const [cov, ing, mf] = await Promise.all([
      fetch('/v1/coverage', { headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' } }).then(r => r.json()),
      fetch('/v1/ingest-status', { headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' } }).then(r => r.json()),
      fetch('/v1/markets/indianapolis/multifamily/on-market?limit=25', { headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' } }).then(r => r.json()),
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
      headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' },
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
      headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' },
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
  main { padding:22px 24px 32px; max-width:1440px; margin:0 auto; }
  .kpi-grid { display:grid; grid-template-columns:repeat(6,minmax(130px,1fr)); gap:12px; margin-bottom:18px; }
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
  @media (max-width:1100px) { .kpi-grid { grid-template-columns:repeat(3,minmax(150px,1fr)); } .layout { grid-template-columns:1fr; } }
  @media (max-width:620px) { .topbar { align-items:flex-start; flex-direction:column; } main { padding:14px; } .kpi-grid { grid-template-columns:1fr 1fr; } .kpi-value { font-size:22px; } th:nth-child(3),td:nth-child(3),th:nth-child(5),td:nth-child(5){display:none;} }
</style>
</head>
<body>
<div class="topbar">
  <div>
    <h1>Indianapolis Multifamily Market</h1>
    <div class="subtitle">Buy Box Club market dashboard preview powered by MXRE</div>
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;">
    <span class="pill">Marion County, IN</span>
    <span class="pill" id="generated">Loading</span>
  </div>
</div>
<main>
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;flex-wrap:wrap;">
    <div class="muted" id="filter-note">Showing all multifamily on-market records.</div>
    <div class="seg" aria-label="Unit size filter">
      <button id="filter-all" class="active" onclick="setUnitFilter(null)">All MF</button>
      <button id="filter-2" onclick="setUnitFilter(2)">2+ Units</button>
      <button id="filter-3" onclick="setUnitFilter(3)">3+ Units</button>
      <button id="filter-4" onclick="setUnitFilter(4)">4+ Units</button>
    </div>
  </div>
  <div id="loading" class="card loading">Loading market dashboard...</div>
  <div id="content" style="display:none;">
    <div class="kpi-grid">
      <div class="card"><div class="kpi-label">MF Properties</div><div class="kpi-value" id="kpi-props">-</div><div class="kpi-sub">classified inventory</div></div>
      <div class="card"><div class="kpi-label">Known Units</div><div class="kpi-value" id="kpi-units">-</div><div class="kpi-sub">assessor-derived units</div></div>
      <div class="card"><div class="kpi-label">Recorder Coverage</div><div class="kpi-value" id="kpi-recorder">-</div><div class="kpi-sub" id="kpi-recorder-sub">sale or mortgage records</div></div>
      <div class="card"><div class="kpi-label">Internal Listings</div><div class="kpi-value green" id="kpi-active">-</div><div class="kpi-sub" id="kpi-unique">- unique properties</div></div>
      <div class="card"><div class="kpi-label">External CRE</div><div class="kpi-value amber" id="kpi-external">-</div><div class="kpi-sub" id="kpi-external-sub">unverified observations</div></div>
      <div class="card"><div class="kpi-label">Median List</div><div class="kpi-value" id="kpi-list">-</div><div class="kpi-sub">active multifamily</div></div>
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
const apiKey = '${process.env.MXRE_API_KEY ?? ''}';
const fmt = (n) => n == null ? '-' : Number(n).toLocaleString('en-US');
const money = (n) => n == null ? '-' : '$' + fmt(n);
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
function setUnitFilter(minUnits) {
  currentMinUnits = minUnits;
  for (const id of ['filter-all', 'filter-2', 'filter-3', 'filter-4']) document.getElementById(id).classList.remove('active');
  document.getElementById(minUnits ? 'filter-' + minUnits : 'filter-all').classList.add('active');
  document.getElementById('content').style.display = 'none';
  document.getElementById('loading').style.display = 'block';
  document.getElementById('loading').textContent = 'Loading market dashboard...';
  load();
}
async function load() {
  const params = new URLSearchParams();
  if (currentMinUnits) params.set('min_units', String(currentMinUnits));
  const path = '/v1/markets/indianapolis/dashboard' + (params.toString() ? '?' + params.toString() : '');
  const coveragePath = '/v1/markets/indianapolis/multifamily/coverage' + (params.toString() ? '?' + params.toString() : '');
  const [resp, coverageResp] = await Promise.all([
    fetch(path, { headers: { 'x-api-key': apiKey } }),
    fetch(coveragePath, { headers: { 'x-api-key': apiKey } }),
  ]);
  const data = await resp.json();
  const coverage = await coverageResp.json();
  if (!resp.ok) throw new Error(data.detail || data.error || 'Request failed');
  if (!coverageResp.ok) throw new Error(coverage.detail || coverage.error || 'Coverage request failed');
  document.getElementById('filter-note').textContent = currentMinUnits ? 'Showing Indianapolis multifamily with at least ' + currentMinUnits + ' units.' : 'Showing all multifamily on-market records.';
  document.getElementById('kpi-props').textContent = fmt(data.inventory.total_multifamily_properties);
  document.getElementById('kpi-units').textContent = fmt(data.inventory.known_multifamily_units);
  document.getElementById('kpi-recorder').textContent = coverage.coverage.any_recorder_data_pct + '%';
  document.getElementById('kpi-recorder-sub').textContent = fmt(coverage.coverage.parcels_with_any_recorder_data) + ' parcels with sale/mortgage data';
  document.getElementById('kpi-active').textContent = fmt(data.on_market.active_listing_rows);
  document.getElementById('kpi-unique').textContent = fmt(data.on_market.unique_properties) + ' unique properties';
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
load().catch(err => { document.getElementById('loading').textContent = err.message; });
</script>
</body>
</html>`);
});

// ── Helpers ──────────────────────────────────────────────────

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
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function normalizeJoinedProperty(value: unknown): Record<string, unknown> {
  if (Array.isArray(value)) return (value[0] as Record<string, unknown> | undefined) ?? {};
  return (value as Record<string, unknown> | null) ?? {};
}

async function queryPg<T extends Record<string, unknown>>(query: string): Promise<T[]> {
  const url = process.env.SUPABASE_URL;
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

async function fetchAndRespond(c: Context, id: number) {
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

  return assembleResponse(c, props[0]);
}

async function assembleResponse(c: Context, property: Record<string, unknown>) {
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
    db.from('listing_signals').select('*').eq('property_id', id).order('first_seen_at', { ascending: false }).limit(20),
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
      .order('first_seen_at', { ascending: false })
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

  return c.json(response);
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
const port = parseInt(process.env.MXRE_API_PORT ?? '3100', 10);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`MXRE Property API running on http://localhost:${info.port}`);
});

export { app };
