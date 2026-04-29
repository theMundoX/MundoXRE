#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
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
  const [summary] = await pg(`
    with mf as (
      select p.id, p.asset_type, p.total_units
      from properties p
      where p.county_id = 797583
        and upper(coalesce(p.city,'')) like '%INDIANAPOLIS%'
        and (coalesce(p.total_units,1) >= 2 or p.asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily'))
    ), rents as (
      select property_id,
             count(*) as rent_rows,
             count(*) filter (where beds = 0) as studio_rows,
             count(*) filter (where beds = 1) as one_bed_rows,
             count(*) filter (where beds = 2) as two_bed_rows,
             count(*) filter (where beds = 3) as three_bed_rows,
             max(observed_at) as latest_rent_observed
      from rent_snapshots
      where property_id in (select id from mf)
      group by property_id
    ), floorplans as (
      select property_id, count(*) as floorplan_rows
      from floorplans
      where property_id in (select id from mf)
      group by property_id
    ), listings as (
      select property_id, count(*) filter (where is_on_market = true) as active_listing_rows
      from listing_signals
      where property_id in (select id from mf)
      group by property_id
    ), profiles as (
      select property_id,
             count(*) filter (where nullif(complex_name,'') is not null) as profile_name_rows,
             count(*) filter (where nullif(website,'') is not null) as profile_website_rows
      from property_complex_profiles
      where property_id in (select id from mf)
      group by property_id
    )
    select
      count(*)::int as mf_parcels,
      sum(coalesce(total_units,1))::int as mf_units,
      count(*) filter (where coalesce(total_units,0) >= 4 or asset_type in ('apartment','commercial_multifamily'))::int as four_plus_parcels,
      count(*) filter (where coalesce(l.active_listing_rows,0) > 0)::int as active_linked_mf_parcels,
      coalesce(sum(l.active_listing_rows),0)::int as active_linked_mf_rows,
      count(*) filter (where coalesce(pr.profile_name_rows,0) > 0)::int as parcels_with_complex_name,
      count(*) filter (where coalesce(pr.profile_website_rows,0) > 0)::int as parcels_with_website,
      count(*) filter (where coalesce(fp.floorplan_rows,0) > 0)::int as parcels_with_floorplans,
      coalesce(sum(fp.floorplan_rows),0)::int as floorplan_rows,
      count(*) filter (where coalesce(r.rent_rows,0) > 0)::int as parcels_with_any_rent_snapshot,
      coalesce(sum(r.rent_rows),0)::int as rent_snapshot_rows,
      count(*) filter (where coalesce(r.studio_rows,0) > 0)::int as parcels_with_studio_rent,
      count(*) filter (where coalesce(r.one_bed_rows,0) > 0)::int as parcels_with_1br_rent,
      count(*) filter (where coalesce(r.two_bed_rows,0) > 0)::int as parcels_with_2br_rent,
      count(*) filter (where coalesce(r.three_bed_rows,0) > 0)::int as parcels_with_3br_rent,
      max(r.latest_rent_observed) as latest_rent_observed
    from mf
    left join rents r on r.property_id = mf.id
    left join floorplans fp on fp.property_id = mf.id
    left join listings l on l.property_id = mf.id
    left join profiles pr on pr.property_id = mf.id;
  `);

  const total = Number(summary.mf_parcels ?? 0);
  const report = {
    market: "indianapolis",
    generated_at: new Date().toISOString(),
    ...summary,
    coverage_pct: {
      complex_name: pct(summary.parcels_with_complex_name, total),
      website: pct(summary.parcels_with_website, total),
      floorplans: pct(summary.parcels_with_floorplans, total),
      any_rent_snapshot: pct(summary.parcels_with_any_rent_snapshot, total),
      studio_rent: pct(summary.parcels_with_studio_rent, total),
      one_bed_rent: pct(summary.parcels_with_1br_rent, total),
      two_bed_rent: pct(summary.parcels_with_2br_rent, total),
      three_bed_rent: pct(summary.parcels_with_3br_rent, total),
    },
  };

  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
