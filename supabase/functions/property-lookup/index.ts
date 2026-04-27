// MXRE Property Lookup — fuzzy address search + full per-property profile.
//
// Two modes:
//   GET ?q=<address|owner|parcel>           → top-10 candidate matches
//   GET ?id=<property_id>                    → full property profile
//
// The full profile bundles:
//   - properties row (every column)
//   - county lookup (name, state, FIPS)
//   - linked mortgage_records (latest first)
//   - rent_baselines for the property's zip + county (ACS, HUD FMR, HUD SAFMR)
//   - distress signals (open mortgages, lis_pendens, etc — from doc types)

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const MXRE_PG_URL   = Deno.env.get("MXRE_PG_URL") ?? "";
const MXRE_SVC_KEY  = Deno.env.get("MXRE_SUPABASE_SERVICE_KEY") ?? "";
const SUPABASE_URL  = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_ANON = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

async function requireAuth(req: Request): Promise<{ error: Response } | { ok: true }> {
  const auth = req.headers.get("Authorization");
  if (!auth?.startsWith("Bearer ")) {
    return { error: new Response(JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { "Content-Type": "application/json" } }) };
  }
  const client = createClient(SUPABASE_URL, SUPABASE_ANON, {
    auth: { persistSession: false },
    global: { headers: { Authorization: auth } },
  });
  const { error } = await client.auth.getUser();
  if (error) {
    return { error: new Response(JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { "Content-Type": "application/json" } }) };
  }
  return { ok: true };
}

async function pg(q: string): Promise<any[]> {
  const res = await fetch(MXRE_PG_URL, {
    method: "POST",
    headers: { apikey: MXRE_SVC_KEY, Authorization: `Bearer ${MXRE_SVC_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`pg-meta ${res.status}: ${await res.text()}`);
  return res.json();
}

const sqlEscape = (s: string) => s.replace(/'/g, "''");

// Default scope: Marion County, IN (county_id 797583). Properties has no
// indexed full-text — scoping by county uses idx_prop_county and reduces
// the candidate set from 98M → 583K, fast enough for ILIKE.
const DEFAULT_COUNTY_ID = 797583;

async function search(qRaw: string, countyId: number) {
  const q = qRaw.trim();
  if (q.length < 2) return [];
  const esc = sqlEscape(q.toUpperCase());
  const countyFilter = `p.county_id = ${countyId}`;

  // Parcel exact (uses unique idx (county_id, parcel_id))
  if (/^[\d-]+$/.test(q)) {
    const exact = await pg(`
      SELECT p.id, p.parcel_id, p.address, p.city, p.state_code, p.zip,
             p.owner_name, p.market_value, p.year_built, p.bedrooms,
             c.county_name, (c.state_fips || c.county_fips) AS county_fips
      FROM properties p LEFT JOIN counties c ON p.county_id = c.id
      WHERE ${countyFilter} AND p.parcel_id = '${esc}' LIMIT 10
    `);
    if (exact.length > 0) return exact;
  }
  // Fuzzy address + owner (within scoped county)
  return pg(`
    SELECT p.id, p.parcel_id, p.address, p.city, p.state_code, p.zip,
           p.owner_name, p.market_value, p.year_built, p.bedrooms,
           c.county_name, (c.state_fips || c.county_fips) AS county_fips
    FROM properties p LEFT JOIN counties c ON p.county_id = c.id
    WHERE ${countyFilter}
      AND (upper(p.address) ILIKE '%${esc}%' OR upper(p.owner_name) ILIKE '%${esc}%')
    ORDER BY (p.market_value IS NULL), p.market_value DESC NULLS LAST
    LIMIT 25
  `);
}

async function fullProfile(id: number) {
  const [propRows, mortgageRows] = await Promise.all([
    pg(`
      SELECT p.*, c.county_name, c.state_fips, c.county_fips,
             (c.state_fips || c.county_fips) AS county_fips_5
      FROM properties p LEFT JOIN counties c ON p.county_id = c.id
      WHERE p.id = ${id}
    `),
    pg(`
      SELECT id, document_type, recording_date, original_amount, loan_amount,
             estimated_current_balance, lender_name, borrower_name,
             interest_rate, term_months, maturity_date, document_number,
             open, position, deed_type, source_url
      FROM mortgage_records
      WHERE property_id = ${id}
      ORDER BY recording_date DESC NULLS LAST
      LIMIT 50
    `),
  ]);

  const property = propRows[0];
  if (!property) return { error: "not found" };

  const fips5 = property.county_fips_5;
  const zip   = property.zip ? String(property.zip).slice(0, 5) : null;

  // Pull rent baselines, rent observations, portfolio, and HMDA tract stats in parallel
  const [baselines, rentObservations, portfolioRows, hmdaRows] = await Promise.all([
    // Rent baselines (ACS + HUD FMR) for zip and county
    (() => {
      const filters: string[] = [];
      if (fips5) filters.push(`(geography_type='county' AND geography_id='${fips5}')`);
      if (zip)   filters.push(`(geography_type='zip'    AND geography_id='${zip}')`);
      return filters.length > 0
        ? pg(`SELECT source, geography_type, geography_id, bedrooms, median_rent, vintage_year, observed_at
              FROM rent_baselines WHERE ${filters.join(" OR ")}
              ORDER BY source, bedrooms NULLS FIRST`).catch(() => [])
        : Promise.resolve([]);
    })(),

    // Actual scraped rent observations for this property
    pg(`SELECT observed_at, beds, baths, sqft, asking_rent, effective_rent, available_count, days_on_market
        FROM rent_snapshots WHERE property_id = ${id}
        ORDER BY observed_at DESC LIMIT 20`).catch(() => []),

    // Owner portfolio — other properties owned by same entity (corporate owners only)
    (() => {
      const ownerName = property.owner_name as string | null;
      if (!ownerName || !property.corporate_owned) return Promise.resolve([]);
      const esc = sqlEscape(ownerName);
      return pg(`
        SELECT p.id, p.address, p.city, p.state_code, p.zip,
               p.market_value, p.assessed_value, p.property_use, p.year_built,
               p.total_sqft, p.bedrooms, p.bathrooms, p.county_id,
               p.corporate_owned, p.absentee_owner,
               c.county_name, c.state_fips
        FROM properties p LEFT JOIN counties c ON p.county_id = c.id
        WHERE p.owner_name = '${esc}' AND p.id != ${id}
        ORDER BY p.market_value DESC NULLS LAST
        LIMIT 100
      `).catch(() => []);
    })(),

    // HMDA census-tract stats — loan activity around this property's neighbourhood
    (() => {
      const tract = property.census_tract as string | null;
      if (!tract) return Promise.resolve([]);
      const esc = sqlEscape(tract);
      return pg(`
        SELECT year, loan_purpose, loan_type, lien_status, occupancy_type,
               COUNT(*)::int AS origination_count,
               ROUND(AVG(loan_amount))::int AS avg_loan_amount,
               ROUND(AVG(property_value))::int AS avg_property_value
        FROM hmda_originations
        WHERE census_tract = '${esc}'
        GROUP BY year, loan_purpose, loan_type, lien_status, occupancy_type
        ORDER BY year DESC, origination_count DESC
        LIMIT 200
      `).catch(() => []);
    })(),
  ]);

  // Distress flags
  const distress: string[] = [];
  const docTypes = new Set(mortgageRows.map((m: any) => m.document_type));
  if (docTypes.has("lis_pendens"))        distress.push("lis_pendens");
  if (docTypes.has("foreclosure"))        distress.push("foreclosure");
  if (docTypes.has("notice_of_default"))  distress.push("notice_of_default");
  if (docTypes.has("tax_lien") || docTypes.has("federal_tax_lien")) distress.push("tax_lien");
  if (docTypes.has("mechanics_lien"))     distress.push("mechanics_lien");
  if (docTypes.has("judgment"))           distress.push("judgment");

  // Open mortgages
  const openMortgages = mortgageRows.filter((m: any) =>
    (m.document_type === "mortgage" || m.document_type === "deed_of_trust") && m.open !== false,
  );

  // Equity & LTV
  const totalDebt = openMortgages.reduce((s: number, m: any) =>
    s + (Number(m.estimated_current_balance) || Number(m.original_amount) || 0), 0);
  const marketValue    = Number(property.market_value) || Number(property.assessed_value) || 0;
  const estimatedEquity = marketValue > 0 ? marketValue - totalDebt : null;
  const ltv             = marketValue > 0 && totalDebt > 0 ? totalDebt / marketValue : null;

  // Owner portfolio summary
  const portfolioValue = portfolioRows.reduce((s: number, p: any) =>
    s + (Number(p.market_value) || 0), 0);
  const ownerPortfolio = portfolioRows.length > 0 ? {
    entity:               property.owner_name,
    total_other_properties: portfolioRows.length,
    total_portfolio_value:  portfolioValue + marketValue,
    states:               [...new Set(portfolioRows.map((p: any) => p.state_code))],
    properties:           portfolioRows,
  } : null;

  return {
    property,
    mortgage_records:    mortgageRows,
    open_mortgages:      openMortgages,
    rent_baselines:      baselines,
    rent_observations:   rentObservations,
    owner_portfolio:     ownerPortfolio,
    hmda_tract_stats:    hmdaRows,
    distress_flags:      distress,
    derived: {
      total_open_debt:        totalDebt,
      estimated_equity:       estimatedEquity,
      ltv_ratio:              ltv,
      market_value:           marketValue,
      portfolio_property_count: portfolioRows.length + (marketValue > 0 ? 1 : 0),
    },
  };
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const authResult = await requireAuth(req);
  if ("error" in authResult) return authResult.error;

  try {
    const url = new URL(req.url);
    const t0 = performance.now();
    let payload: any;

    const idParam = url.searchParams.get("id");
    const qParam = url.searchParams.get("q");

    if (idParam) {
      const id = parseInt(idParam, 10);
      if (!Number.isFinite(id)) throw new Error("invalid id");
      payload = await fullProfile(id);
    } else if (qParam) {
      const cidRaw = url.searchParams.get("county_id");
      const cid = cidRaw ? parseInt(cidRaw, 10) : DEFAULT_COUNTY_ID;
      payload = { results: await search(qParam, cid), county_id: cid };
    } else {
      return new Response(JSON.stringify({ error: "missing q or id" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    return new Response(JSON.stringify({
      generated_at: new Date().toISOString(),
      query_ms: Math.round(performance.now() - t0),
      ...payload,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err.message ?? String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
