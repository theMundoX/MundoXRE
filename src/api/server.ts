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
  if (c.req.path === '/health' || c.req.path === '/' || c.req.path === '/dashboard') return next();

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

// ── Ingest status (reads supervisor logs) ─────────────────────

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

<script>
function fmt(n) { return n?.toLocaleString('en-US') ?? '—'; }

async function load() {
  document.getElementById('last-updated').textContent = 'Loading...';
  try {
    const [cov, ing] = await Promise.all([
      fetch('/v1/coverage', { headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' } }).then(r => r.json()),
      fetch('/v1/ingest-status', { headers: { 'x-api-key': '${process.env.MXRE_API_KEY ?? ''}' } }).then(r => r.json()),
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

    document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch (e) {
    document.getElementById('last-updated').textContent = 'Error: ' + e.message;
  }
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

// ── Helpers ──────────────────────────────────────────────────

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
