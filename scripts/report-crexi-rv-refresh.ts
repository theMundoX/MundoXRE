#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { makeDbClient } from "./lib/db.ts";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const SINCE_HOURS = Math.max(1, Number.parseInt(arg("since-hours", "24") ?? "24", 10));
const ASSET_CLASS = (arg("asset-class", "mobile_home_rv") ?? "mobile_home_rv").toLowerCase();
const WRITE_LOG = !process.argv.includes("--no-write");
const assetClassFilter = ASSET_CLASS === "all" ? "" : `and asset_class = '${ASSET_CLASS.replace(/'/g, "''")}'`;

async function main() {
  const db = await makeDbClient();
  const since = new Date(Date.now() - SINCE_HOURS * 60 * 60 * 1000).toISOString();
  const [summary] = (await db.query(`
    select
      count(*)::int as total_rows,
      count(*) filter (where first_seen_at >= '${since}'::timestamptz)::int as new_rows,
      count(*) filter (where last_seen_at >= '${since}'::timestamptz)::int as refreshed_rows,
      count(*) filter (where coalesce(last_seen_at, observed_at, created_at) < '${since}'::timestamptz)::int as stale_rows,
      count(*) filter (where status = 'removed_candidate')::int as removed_candidate_rows,
      count(*) filter (where status = 'removed')::int as removed_rows,
      count(*) filter (where status = 'removed' and nullif(raw->>'removed_confirmed_at', '')::timestamptz >= '${since}'::timestamptz)::int as newly_removed_rows,
      count(*) filter (
        where nullif(raw->>'updated_on', '') is not null
          and nullif(raw->>'previous_updated_on', '') is not null
          and (raw->>'updated_on')::timestamptz > (raw->>'previous_updated_on')::timestamptz
      )::int as source_updated_rows,
      count(*) filter (where list_price is not null)::int as with_price,
      count(*) filter (where noi is not null)::int as with_noi,
      count(*) filter (where cap_rate is not null)::int as with_cap_rate,
      count(*) filter (where units is not null and units > 0)::int as with_units,
      count(*) filter (where nullif(raw->>'total_units', '') is not null)::int as with_standard_total_units,
      count(*) filter (where nullif(raw->>'unit_count_status', '') is not null)::int as with_unit_count_status,
      count(*) filter (where raw->>'marketing_description' is not null)::int as with_marketing_description,
      count(*) filter (where raw->>'investment_highlights' is not null)::int as with_investment_highlights,
      count(*) filter (where raw->>'building_class_status' = 'explicit_value')::int as with_building_class,
      count(*) filter (where raw->>'building_class_status' = 'needs_review')::int as with_building_class_needs_review,
      count(*) filter (where asset_class = 'multifamily' and raw->'multifamily' is not null)::int as with_multifamily_stats,
      count(*) filter (where asset_class = 'multifamily' and nullif(raw->>'apartment_units', '') is not null)::int as with_multifamily_units,
      count(*) filter (where asset_class = 'multifamily' and nullif(raw->>'building_count', '') is not null)::int as with_multifamily_buildings,
      count(*) filter (where asset_class = 'multifamily' and nullif(raw->>'year_built', '') is not null)::int as with_multifamily_year_built,
      count(*) filter (where asset_class = 'multifamily' and nullif(raw->>'occupancy_pct', '') is not null)::int as with_multifamily_occupancy,
      count(*) filter (where asset_class = 'multifamily' and nullif(raw->>'price_per_sqft', '') is not null)::int as with_multifamily_price_per_sqft,
      count(*) filter (where raw->'unit_mix' is not null and exists (
        select 1 from jsonb_each_text(raw->'unit_mix') e where e.value is not null and e.value <> ''
      ))::int as with_unit_mix
    from external_market_listings
    where source = 'crexi_rapidapi'
      ${assetClassFilter};
  `)).rows;
  const byMarket = (await db.query(`
    select asset_class,
           market,
           count(*)::int as rows,
           count(*) filter (where first_seen_at >= '${since}'::timestamptz)::int as new_rows,
           count(*) filter (where last_seen_at >= '${since}'::timestamptz)::int as refreshed_rows,
           count(*) filter (where coalesce(last_seen_at, observed_at, created_at) < '${since}'::timestamptz)::int as stale_rows,
           count(*) filter (where status = 'removed_candidate')::int as removed_candidate_rows,
           count(*) filter (where status = 'removed')::int as removed_rows
      from external_market_listings
     where source = 'crexi_rapidapi'
       ${assetClassFilter}
     group by asset_class, market
     order by refreshed_rows desc, rows desc, asset_class, market;
  `)).rows;
  const bySubAssetType = (await db.query(`
    select asset_class,
           coalesce(raw->>'sub_asset_type', raw->>'sub_type', 'unknown') as sub_asset_type,
           count(*)::int as rows,
           count(*) filter (where first_seen_at >= '${since}'::timestamptz)::int as new_rows,
           count(*) filter (where last_seen_at >= '${since}'::timestamptz)::int as refreshed_rows,
           count(*) filter (where coalesce(last_seen_at, observed_at, created_at) < '${since}'::timestamptz)::int as stale_rows
      from external_market_listings
     where source = 'crexi_rapidapi'
       ${assetClassFilter}
     group by asset_class, coalesce(raw->>'sub_asset_type', raw->>'sub_type', 'unknown')
     order by rows desc, asset_class, sub_asset_type;
  `)).rows;
  const byBuildingClass = (await db.query(`
    select asset_class,
           coalesce(raw->>'building_class', 'unknown') as building_class,
           coalesce(raw->>'building_class_status', 'no_data') as building_class_status,
           count(*)::int as rows,
           count(*) filter (where first_seen_at >= '${since}'::timestamptz)::int as new_rows,
           count(*) filter (where last_seen_at >= '${since}'::timestamptz)::int as refreshed_rows
      from external_market_listings
     where source = 'crexi_rapidapi'
       ${assetClassFilter}
     group by asset_class, coalesce(raw->>'building_class', 'unknown'), coalesce(raw->>'building_class_status', 'no_data')
     order by rows desc, asset_class, building_class_status, building_class;
  `)).rows;
  const changedExamples = (await db.query(`
    select asset_class, market, title, source_url, list_price, noi, cap_rate, units,
           nullif(raw->>'total_units', '')::int as total_units,
           raw->>'unit_count_status' as unit_count_status,
           raw->>'unit_count_source' as unit_count_source,
           raw->>'sub_asset_type' as sub_asset_type,
           raw->>'class' as source_class,
           raw->>'building_class' as building_class,
           raw->>'building_class_status' as building_class_status,
           raw->>'updated_on' as source_updated_on,
           raw->>'previous_updated_on' as previous_source_updated_on,
           raw->'unit_mix' as unit_mix,
           raw->'multifamily' as multifamily,
           nullif(raw->>'apartment_units', '')::int as apartment_units,
           nullif(raw->>'building_count', '')::int as building_count,
           nullif(raw->>'story_count', '')::int as story_count,
           nullif(raw->>'year_built', '')::int as year_built,
           nullif(raw->>'occupancy_pct', '')::numeric as occupancy_pct,
           raw->>'occupancy_status' as occupancy_status,
           nullif(raw->>'price_per_sqft', '')::numeric as price_per_sqft,
           nullif(raw->>'avg_unit_sqft', '')::numeric as avg_unit_sqft
     from external_market_listings
     where source = 'crexi_rapidapi'
       ${assetClassFilter}
       and (
         first_seen_at >= '${since}'::timestamptz
         or last_seen_at >= '${since}'::timestamptz
         or (
           nullif(raw->>'updated_on', '') is not null
           and nullif(raw->>'previous_updated_on', '') is not null
           and (raw->>'updated_on')::timestamptz > (raw->>'previous_updated_on')::timestamptz
         )
       )
     order by last_seen_at desc nulls last, id desc
     limit 25;
  `)).rows;
  await db.end();

  const report = {
    schemaVersion: "mxre.crexiRvRefreshReport.v1",
    generatedAt: new Date().toISOString(),
    assetClass: ASSET_CLASS,
    since,
    sinceHours: SINCE_HOURS,
    summary,
    byMarket,
    bySubAssetType,
    byBuildingClass,
    changedExamples,
  };

  if (WRITE_LOG) {
    const dir = join(process.cwd(), "logs", "crexi-rv-refresh");
    await mkdir(dir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    await writeFile(join(dir, `crexi-rv-refresh-${stamp}.json`), JSON.stringify(report, null, 2));
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
