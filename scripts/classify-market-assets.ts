#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const DRY_RUN = process.argv.includes("--dry-run");
const ACTIVE_LISTINGS_ONLY = process.argv.includes("--active-listings-only");

const arg = (name: string, fallback?: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const STATE = (arg("state", "OH") ?? "OH").toUpperCase();
const CITY = (arg("city", "COLUMBUS") ?? "COLUMBUS").toUpperCase();
const COUNTY_ID = Number(arg("county_id", "1698985"));
const BATCH_SIZE = Math.max(100, Number(arg("batch-size", "2500")));

async function pg(query: string): Promise<Record<string, unknown>[]> {
  if (DRY_RUN && /^\s*(update|insert|delete)/i.test(query)) {
    console.log(`[dry-run] ${query.replace(/\s+/g, " ").slice(0, 220)}...`);
    return [];
  }
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

async function updateBatched(label: string, setSql: string, whereSql: string) {
  console.log(`\n${label}`);
  let total = 0;

  if (DRY_RUN) {
    console.log(`  dry-run only; skipped heavy candidate scan`);
    return;
  }

  while (true) {
    const rows = await pg(`
      with target as (
        select id from properties
        where ${whereSql}
        limit ${BATCH_SIZE}
      ),
      changed as (
        update properties p
           set ${setSql}, updated_at = now()
          from target
         where p.id = target.id
         returning p.id
      )
      select count(*)::int as updated from changed;
    `);
    const count = Number(rows[0]?.updated ?? 0);
    total += count;
    process.stdout.write(`\r  updated ${total.toLocaleString()}   `);
    if (count === 0) break;
  }
  console.log();
}

async function main() {
  const stateSql = STATE.replace(/'/g, "''");
  const citySql = CITY.replace(/'/g, "''");
  const marketWhere = ACTIVE_LISTINGS_ONLY
    ? `
      id in (
        select distinct property_id
          from listing_signals
         where is_on_market = true
           and state_code = '${stateSql}'
           and upper(coalesce(city,'')) = '${citySql}'
           and property_id is not null
      )
      and state_code = '${stateSql}'
    `
    : `
      county_id = ${COUNTY_ID}
      and state_code = '${stateSql}'
      and upper(coalesce(city,'')) like '%${citySql}%'
    `;

  console.log("MXRE - Market asset classification");
  console.log(`Market: ${CITY}, ${STATE}`);
  console.log(`County ID: ${COUNTY_ID}`);
  console.log(`Scope: ${ACTIVE_LISTINGS_ONLY ? "active linked on-market properties" : "county/city parcel subset"}`);
  console.log(`Dry run: ${DRY_RUN}`);

  await updateBatched(
    "Franklin/CAGIS multifamily class codes",
    `asset_type = 'small_multifamily',
     asset_subtype = coalesce(asset_subtype, 'multifamily_unknown'),
     total_units = case when coalesce(total_units,0) >= 2 then total_units else null end,
     unit_count_source = coalesce(unit_count_source, 'assessor_class_code'),
     asset_confidence = coalesce(asset_confidence, 'medium'),
     is_sfr = false,
     is_apartment = coalesce(is_apartment, false)`,
    `${marketWhere}
     and lower(coalesce(property_type,'')) like '%multifamily%'
     and asset_type is distinct from 'small_multifamily'`,
  );

  await updateBatched(
    "Apartment flags from property type / website discovery",
    `asset_type = 'apartment',
     asset_subtype = coalesce(asset_subtype, 'apartment_community'),
     total_units = case when coalesce(total_units,0) > 0 then total_units else null end,
     unit_count_source = coalesce(unit_count_source, case when coalesce(total_units,0) > 0 then 'assessor_explicit' else 'unknown' end),
     asset_confidence = coalesce(asset_confidence, 'medium'),
     is_apartment = true,
     is_sfr = false`,
    `${marketWhere}
     and (
       is_apartment is true
       or lower(coalesce(property_type,'')) like '%apartment%'
       or coalesce(total_units,0) >= 5
     )
     and asset_type is distinct from 'apartment'`,
  );

  await updateBatched(
    "Residential single-unit defaults",
    `asset_type = 'residential',
     asset_subtype = case
       when lower(coalesce(property_type,'')) like '%condo%' or is_condo is true then 'condo'
       else 'sfr'
     end,
     total_units = case when coalesce(total_units,0) > 0 then total_units else 1 end,
     unit_count_source = coalesce(unit_count_source, 'inferred_single_unit'),
     asset_confidence = coalesce(asset_confidence, 'medium'),
     is_sfr = case when lower(coalesce(property_type,'')) like '%condo%' or is_condo is true then false else true end`,
    `${marketWhere}
     and asset_type is null
     and lower(coalesce(property_type,'')) in ('single_family','residential','condo')`,
  );

  await updateBatched(
    "Other classified parcel defaults",
    `asset_type = coalesce(nullif(lower(property_type), ''), 'other'),
     asset_subtype = coalesce(asset_subtype, nullif(lower(property_type), ''), 'unknown'),
     total_units = case when coalesce(total_units,0) > 0 then total_units else total_units end,
     unit_count_source = coalesce(unit_count_source, 'unknown'),
     asset_confidence = coalesce(asset_confidence, 'low')`,
    `${marketWhere}
     and asset_type is null`,
  );

  const summary = await pg(`
    select asset_type, asset_subtype, unit_count_source, asset_confidence,
           count(*)::int as properties,
           sum(coalesce(total_units,0))::int as units
      from properties
     where ${marketWhere}
     group by 1,2,3,4
     order by properties desc
     limit 25;
  `);

  console.log("\nSummary:");
  console.log(JSON.stringify(summary, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
