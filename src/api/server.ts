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
  if (c.req.path === '/health' || c.req.path === '/') return next();

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
  const [mortgages, rents, listingsById, saleHistory, mlsHistory, foreclosureData, demo] = await Promise.all([
    db.from('mortgage_records')
      .select('id,property_id,document_type,loan_amount,original_amount,estimated_current_balance,estimated_monthly_payment,interest_rate,interest_rate_type,rate_source,rate_match_confidence,term_months,maturity_date,recording_date,document_number,book_page,lender_name,lender_type,borrower_name,loan_type,source_url,grantee_name,open,position')
      .eq('property_id', id).order('recording_date', { ascending: false }).limit(50),
    db.from('rent_snapshots').select('*').eq('property_id', id).order('observed_at', { ascending: false }).limit(24),
    db.from('listing_signals').select('*').eq('property_id', id).order('first_seen_at', { ascending: false }).limit(20),
    db.from('sale_history').select('*').eq('property_id', id).order('recording_date', { ascending: false }).limit(20),
    db.from('mls_history').select('*').eq('property_id', id).order('status_date', { ascending: false }).limit(20),
    db.from('foreclosures').select('*').eq('property_id', id).limit(10),
    property.zip
      ? db.from('zip_demographics').select('fmr_0,fmr_1,fmr_2,fmr_3,fmr_4,fmr_year,hud_area_name,median_income').eq('zip', property.zip as string).single()
      : Promise.resolve({ data: null, error: null }),
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
  );

  return c.json(response);
}

// ── Start server ─────────────────────────────────────────────
const port = parseInt(process.env.MXRE_API_PORT ?? '3100', 10);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`MXRE Property API running on http://localhost:${info.port}`);
});

export { app };
