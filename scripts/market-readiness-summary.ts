#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const arg = (name: string, fallback?: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const STATE = (arg("state", "OH") ?? "OH").toUpperCase();
const CITY = (arg("city", "COLUMBUS") ?? "COLUMBUS").toUpperCase();
const COUNTY_ID = Number(arg("county_id", "1698985"));

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

const pct = (value: unknown, total: unknown) => {
  const n = Number(value ?? 0);
  const d = Number(total ?? 0);
  return d > 0 ? Math.round((n / d) * 10000) / 100 : 0;
};

async function main() {
  const citySql = CITY.replace(/'/g, "''");
  const stateSql = STATE.replace(/'/g, "''");
  const propWhere = `county_id = ${COUNTY_ID} and state_code = '${stateSql}' and upper(coalesce(city,'')) like '%${citySql}%'`;
  const listingWhere = `is_on_market = true and state_code = '${stateSql}' and upper(coalesce(city,'')) = '${citySql}'`;
  const mfWhere = `${propWhere} and (coalesce(total_units,1) >= 2 or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily'))`;

  const [parcels] = await pg(`
    select count(*)::int as parcel_count,
           count(*) filter (where asset_type is not null)::int as classified_count,
           count(*) filter (where total_units is not null)::int as unit_count_count,
           count(*) filter (where asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily') or coalesce(total_units,0) >= 2)::int as multifamily_asset_count
      from properties
     where ${propWhere};
  `);

  const [listings] = await pg(`
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
     where ${listingWhere};
  `);

  const [mf] = await pg(`
    with mf as (
      select id from properties where ${mfWhere}
    )
    select count(distinct mf.id)::int as multifamily_complex_count,
           count(distinct pw.property_id)::int as complexes_with_websites,
           count(distinct fp.property_id)::int as complexes_with_floorplans,
           count(distinct fp.id)::int as floorplan_rows,
           count(distinct rs.property_id)::int as complexes_with_rent_snapshots,
           count(distinct rs.id)::int as rent_snapshot_rows,
           max(rs.observed_at) as latest_rent_observed
      from mf
      left join property_websites pw on pw.property_id = mf.id and pw.active = true
      left join floorplans fp on fp.property_id = mf.id
      left join rent_snapshots rs on rs.property_id = mf.id;
  `);

  const active = Number(listings.active_listing_count ?? 0);
  const parcelCount = Number(parcels.parcel_count ?? 0);
  const mfCount = Number(mf.multifamily_complex_count ?? 0);

  console.log(JSON.stringify({
    market: `${CITY}, ${STATE}`,
    generated_at: new Date().toISOString(),
    active_listing_count: active,
    listing_source_coverage: {
      source_count: Number(listings.listing_source_count ?? 0),
      sources: listings.listing_sources ?? [],
      redfin_detail_rows: Number(listings.redfin_detail_count ?? 0),
    },
    agent_coverage: {
      name: Number(listings.agent_name_count ?? 0),
      phone: Number(listings.agent_phone_count ?? 0),
      email: Number(listings.agent_email_count ?? 0),
      brokerage: Number(listings.brokerage_count ?? 0),
      pct: {
        name: pct(listings.agent_name_count, active),
        phone: pct(listings.agent_phone_count, active),
        email: pct(listings.agent_email_count, active),
        brokerage: pct(listings.brokerage_count, active),
      },
    },
    creative_finance_count: Number(listings.creative_finance_count ?? 0),
    parcel_count: parcelCount,
    asset_classification_coverage: {
      classified_count: Number(parcels.classified_count ?? 0),
      unit_count_count: Number(parcels.unit_count_count ?? 0),
      multifamily_asset_count: Number(parcels.multifamily_asset_count ?? 0),
      pct_classified: pct(parcels.classified_count, parcelCount),
      pct_unit_count: pct(parcels.unit_count_count, parcelCount),
    },
    multifamily_coverage: {
      complex_count: mfCount,
      complexes_with_websites: Number(mf.complexes_with_websites ?? 0),
      complexes_with_floorplans: Number(mf.complexes_with_floorplans ?? 0),
      floorplan_rows: Number(mf.floorplan_rows ?? 0),
      complexes_with_rent_snapshots: Number(mf.complexes_with_rent_snapshots ?? 0),
      rent_snapshot_rows: Number(mf.rent_snapshot_rows ?? 0),
      latest_rent_observed: mf.latest_rent_observed ?? null,
      pct_website: pct(mf.complexes_with_websites, mfCount),
      pct_floorplans: pct(mf.complexes_with_floorplans, mfCount),
      pct_rent_snapshot: pct(mf.complexes_with_rent_snapshots, mfCount),
    },
    readiness: {
      dashboard_api_ready: active > 0 && parcelCount > 0 && Number(parcels.classified_count ?? 0) > 0,
      remaining_gaps: [
        active === 0 ? `active listing ingestion did not return ${CITY}, ${STATE} rows` : null,
        Number(listings.agent_phone_count ?? 0) === 0 ? "agent phone coverage is empty" : null,
        Number(listings.agent_email_count ?? 0) === 0 ? "verified public agent email coverage is empty" : null,
        Number(mf.complexes_with_rent_snapshots ?? 0) === 0 ? "multifamily rent snapshots are empty" : null,
      ].filter(Boolean),
    },
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
