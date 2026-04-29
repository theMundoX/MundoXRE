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
  // Keep this on indexed columns. Free-text property_use scans on the full
  // Indianapolis property table were causing pg/query read timeouts.
  const mfWhere = `
    county_id = 797583
    and (
      coalesce(total_units,1) >= 2
      or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily')
    )
  `;

  const [summary] = await pg(`
    with mf as (
      select id, asset_type, total_units
      from properties
      where ${mfWhere}
    )
    select
      count(*)::int as mf_parcels,
      sum(coalesce(total_units,1))::int as mf_units,
      count(*) filter (where coalesce(total_units,0) >= 4 or asset_type in ('apartment','commercial_multifamily'))::int as four_plus_parcels
    from mf;
  `);

  const mfIds = await pg(`select id from properties where ${mfWhere};`);
  const ids = mfIds.map(row => Number(row.id)).filter(Number.isFinite);
  const total = Number(summary.mf_parcels ?? ids.length);
  const chunks: number[][] = [];
  for (let i = 0; i < ids.length; i += 250) chunks.push(ids.slice(i, i + 250));

  const metrics = {
    active_linked_mf_parcels: 0,
    active_linked_mf_rows: 0,
    parcels_with_complex_name: 0,
    parcels_with_website: 0,
    parcels_with_floorplans: 0,
    floorplan_rows: 0,
    parcels_with_any_rent_snapshot: 0,
    rent_snapshot_rows: 0,
    parcels_with_studio_rent: 0,
    parcels_with_1br_rent: 0,
    parcels_with_2br_rent: 0,
    parcels_with_3br_rent: 0,
    latest_rent_observed: null as unknown,
  };

  for (let index = 0; index < chunks.length; index++) {
    const chunk = chunks[index];
    const idList = chunk.join(",");
    const [listings] = await pg(`
      select count(distinct property_id)::int as parcels, count(*)::int as rows
      from listing_signals
      where is_on_market = true and property_id in (${idList});
    `);
    const [profiles] = await pg(`
      select
        count(distinct property_id) filter (where nullif(complex_name,'') is not null)::int as names,
        count(distinct property_id) filter (where nullif(website,'') is not null)::int as websites
      from property_complex_profiles
      where property_id in (${idList});
    `);
    const [floorplans] = await pg(`
      select count(distinct property_id)::int as parcels, count(*)::int as rows
      from floorplans
      where property_id in (${idList});
    `);
    const [rents] = await pg(`
      select
        count(distinct property_id)::int as parcels,
        count(*)::int as rows,
        count(distinct property_id) filter (where beds = 0)::int as studio,
        count(distinct property_id) filter (where beds = 1)::int as one_bed,
        count(distinct property_id) filter (where beds = 2)::int as two_bed,
        count(distinct property_id) filter (where beds = 3)::int as three_bed,
        max(observed_at) as latest
      from rent_snapshots
      where property_id in (${idList});
    `);

    metrics.active_linked_mf_parcels += Number(listings.parcels ?? 0);
    metrics.active_linked_mf_rows += Number(listings.rows ?? 0);
    metrics.parcels_with_complex_name += Number(profiles.names ?? 0);
    metrics.parcels_with_website += Number(profiles.websites ?? 0);
    metrics.parcels_with_floorplans += Number(floorplans.parcels ?? 0);
    metrics.floorplan_rows += Number(floorplans.rows ?? 0);
    metrics.parcels_with_any_rent_snapshot += Number(rents.parcels ?? 0);
    metrics.rent_snapshot_rows += Number(rents.rows ?? 0);
    metrics.parcels_with_studio_rent += Number(rents.studio ?? 0);
    metrics.parcels_with_1br_rent += Number(rents.one_bed ?? 0);
    metrics.parcels_with_2br_rent += Number(rents.two_bed ?? 0);
    metrics.parcels_with_3br_rent += Number(rents.three_bed ?? 0);
    if (rents.latest && (!metrics.latest_rent_observed || String(rents.latest) > String(metrics.latest_rent_observed))) {
      metrics.latest_rent_observed = rents.latest;
    }
    if ((index + 1) % 10 === 0) {
      console.log(`  audited ${index + 1}/${chunks.length} multifamily chunks`);
    }
  }

  const report = {
    market: "indianapolis",
    scope: "marion_county_indexed_multifamily_universe",
    generated_at: new Date().toISOString(),
    ...summary,
    ...metrics,
    coverage_pct: {
      complex_name: pct(metrics.parcels_with_complex_name, total),
      website: pct(metrics.parcels_with_website, total),
      floorplans: pct(metrics.parcels_with_floorplans, total),
      any_rent_snapshot: pct(metrics.parcels_with_any_rent_snapshot, total),
      studio_rent: pct(metrics.parcels_with_studio_rent, total),
      one_bed_rent: pct(metrics.parcels_with_1br_rent, total),
      two_bed_rent: pct(metrics.parcels_with_2br_rent, total),
      three_bed_rent: pct(metrics.parcels_with_3br_rent, total),
    },
  };

  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
