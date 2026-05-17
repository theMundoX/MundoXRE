#!/usr/bin/env tsx
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const PG_URL = firstEnv("MXRE_PG_URL")
  ?? `${(firstEnv("SUPABASE_URL") ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY") ?? "";
const arg = (name: string, fallback?: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const STATE = (arg("state", "IN") ?? "IN").toUpperCase();
const CITY = (arg("city", "INDIANAPOLIS") ?? "INDIANAPOLIS").toUpperCase();
const COUNTY_ID = arg("county_id", "797583");

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
    county_id = ${Number(COUNTY_ID)}
    and upper(coalesce(city,'')) like '%${CITY.replace(/'/g, "''")}%'
    and (
      coalesce(total_units,1) >= 2
      or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily')
    )
  `;

  const ids: number[] = [];
  const summary = {
    mf_parcels: 0,
    mf_units: 0,
    four_plus_parcels: 0,
  };
  let lastId = 0;
  while (true) {
    const page = await pg(`
      select id, asset_type, total_units
      from properties
      where ${mfWhere}
        and id > ${lastId}
      order by id
      limit 1000;
    `);
    if (page.length === 0) break;
    for (const row of page) {
      const id = Number(row.id);
      if (Number.isFinite(id)) {
        ids.push(id);
        if (id > lastId) lastId = id;
      }
      const units = Number(row.total_units ?? 1);
      const assetType = String(row.asset_type ?? "");
      summary.mf_parcels += 1;
      summary.mf_units += Number.isFinite(units) && units > 0 ? units : 1;
      if ((Number.isFinite(units) && units >= 4) || ["apartment", "commercial_multifamily"].includes(assetType)) {
        summary.four_plus_parcels += 1;
      }
    }
    if (page.length < 1000) break;
    console.log(`  loaded ${ids.length.toLocaleString()} multifamily ids`);
  }
  const total = Number(summary.mf_parcels ?? ids.length);
  const chunks: number[][] = [];
  for (let i = 0; i < ids.length; i += 100) chunks.push(ids.slice(i, i + 100));

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
    market: `${CITY.toLowerCase()}, ${STATE}`,
    scope: `${CITY.toLowerCase()}_${STATE.toLowerCase()}_indexed_multifamily_universe`,
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
