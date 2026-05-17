#!/usr/bin/env tsx
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

const arg = (name: string, fallback?: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const STATE = (arg("state", "OH") ?? "OH").toUpperCase();
const CITY = (arg("city", "COLUMBUS") ?? "COLUMBUS").toUpperCase();
const COUNTY_ID = arg("county_id", "1698985");
hydrateWindowsUserEnv();
const PG_URL = firstEnv("MXRE_PG_URL")
  ?? `${(firstEnv("SUPABASE_URL") ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY") ?? "";

type Row = Record<string, unknown>;

function sql(value: string): string {
  return value.replace(/'/g, "''");
}

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  if (!PG_URL || !PG_KEY) {
    throw new Error("Missing MXRE_PG_URL/SUPABASE_URL or SUPABASE_SERVICE_KEY");
  }
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
  return response.json() as Promise<T[]>;
}

const pct = (value: unknown, total: unknown) => {
  const n = Number(value ?? 0);
  const d = Number(total ?? 0);
  return d > 0 ? Math.round((n / d) * 1000) / 10 : 0;
};

async function main() {
  const stateSql = sql(STATE);
  const citySql = sql(CITY);
  const countySql = Number(COUNTY_ID || 0);
  const listingWhere = `is_on_market = true and state_code = '${stateSql}' and upper(coalesce(city,'')) = '${citySql}'`;
  const activeProperties = `
    select distinct property_id
      from listing_signals
     where ${listingWhere}
       and property_id is not null
  `;

  const [summary] = await pg(`
    with active as (${activeProperties}),
    mortgage_sources as (
      select
        count(*)::int as mortgage_record_rows,
        count(distinct property_id)::int as properties_with_any_mortgage_record,
        count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_rows,
        count(distinct property_id) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as properties_with_amounts,
        count(*) filter (where source_url = 'realestateapi')::int as realestateapi_rows,
        count(distinct property_id) filter (where source_url = 'realestateapi')::int as realestateapi_properties,
        count(*) filter (where source_url <> 'realestateapi')::int as public_rows,
        count(distinct property_id) filter (where source_url <> 'realestateapi')::int as public_properties,
        count(distinct property_id) filter (where source_url <> 'realestateapi' and property_id in (
          select property_id from mortgage_records where source_url = 'realestateapi' and property_id in (select property_id from active)
        ))::int as overlap_properties
      from mortgage_records
      where property_id in (select property_id from active)
    ),
    reapi as (
      select
        count(*)::int as cached_rows,
        count(distinct r.property_id)::int as cached_properties,
        count(*) filter (where coalesce(r.status, 'ok') = 'ok')::int as ok_rows,
        count(*) filter (
          where jsonb_typeof(r.response_body) = 'object'
            and r.response_body <> '{}'::jsonb
            and coalesce(
              r.response_body->>'id',
              r.response_body->>'propertyId',
              r.response_body->>'apn',
              r.response_body->>'address',
              r.response_body->>'formattedAddress',
              r.response_body->>'owner1FullName'
            ) is not null
        )::int as valid_identity_rows,
        count(*) filter (
          where jsonb_typeof(r.response_body) = 'object'
            and r.response_body <> '{}'::jsonb
            and coalesce(
              r.response_body->>'id',
              r.response_body->>'propertyId',
              r.response_body->>'apn',
              r.response_body->>'address',
              r.response_body->>'formattedAddress',
              r.response_body->>'owner1FullName'
            ) is not null
            and coalesce(jsonb_array_length(case when jsonb_typeof(r.response_body->'currentMortgages') = 'array' then r.response_body->'currentMortgages' else '[]'::jsonb end), 0) = 0
            and coalesce(nullif(regexp_replace(coalesce(r.response_body->>'estimatedMortgageBalance', r.response_body->>'openMortgageBalance', '0'), '[^0-9.-]', '', 'g'), '')::numeric, 0) = 0
        )::int as valid_free_clear_rows,
        count(*) filter (where nullif(r.response_body->>'estimatedEquity','') is not null)::int as rows_with_response_equity,
        count(*) filter (where nullif(r.response_body->>'estimatedMortgageBalance','') is not null or nullif(r.response_body->>'openMortgageBalance','') is not null)::int as rows_with_response_balance
      from realestateapi_property_details r
      join active a on a.property_id = r.property_id
    ),
    covered as (
      select property_id, 'mortgage_record'::text as coverage_type
        from mortgage_records
       where property_id in (select property_id from active)
      union all
      select r.property_id, 'realestateapi_free_clear'::text as coverage_type
        from realestateapi_property_details r
       where r.property_id in (select property_id from active)
         and jsonb_typeof(r.response_body) = 'object'
         and r.response_body <> '{}'::jsonb
         and coalesce(
           r.response_body->>'id',
           r.response_body->>'propertyId',
           r.response_body->>'apn',
           r.response_body->>'address',
           r.response_body->>'formattedAddress',
           r.response_body->>'owner1FullName'
         ) is not null
         and coalesce(jsonb_array_length(case when jsonb_typeof(r.response_body->'currentMortgages') = 'array' then r.response_body->'currentMortgages' else '[]'::jsonb end), 0) = 0
         and coalesce(nullif(regexp_replace(coalesce(r.response_body->>'estimatedMortgageBalance', r.response_body->>'openMortgageBalance', '0'), '[^0-9.-]', '', 'g'), '')::numeric, 0) = 0
    ),
    coverage as (
      select count(distinct property_id)::int as exact_covered_properties,
             count(distinct property_id) filter (where coverage_type = 'mortgage_record')::int as covered_by_mortgage_record,
             count(distinct property_id) filter (where coverage_type = 'realestateapi_free_clear')::int as covered_by_free_clear,
             count(distinct property_id) filter (
               where property_id in (
                 select property_id from covered where coverage_type = 'mortgage_record'
               )
               and property_id in (
                 select property_id from covered where coverage_type = 'realestateapi_free_clear'
               )
             )::int as overlap_mortgage_record_and_free_clear
        from covered
    ),
    active_count as (
      select count(*)::int as active_properties from active
    )
    select row_to_json(active_count) as active,
           row_to_json(mortgage_sources) as mortgage_records,
           row_to_json(reapi) as realestateapi_cache,
           row_to_json(coverage) as coverage
      from active_count, mortgage_sources, reapi, coverage;
  `);

  const sourceRows = await pg(`
    with active as (${activeProperties})
    select
      case when source_url = 'realestateapi' then 'RealEstateAPI' else coalesce(source_url, 'unknown_public_source') end as source_label,
      count(*)::int as mortgage_record_rows,
      count(distinct property_id)::int as properties_with_records,
      count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_rows,
      count(distinct property_id) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as properties_with_amounts,
      max(recording_date)::text as latest_recording
    from mortgage_records
    where property_id in (select property_id from active)
    group by 1
    order by mortgage_record_rows desc;
  `);

  const countyRecorder = countySql > 0 ? await pg(`
    select
      case when source_url = 'realestateapi' then 'RealEstateAPI' else coalesce(source_url, 'unknown_public_source') end as source_label,
      count(*)::int as mortgage_record_rows,
      count(distinct property_id)::int as properties_with_records,
      count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_rows,
      count(distinct property_id) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as properties_with_amounts,
      max(recording_date)::text as latest_recording
    from mortgage_records m
    join properties p on p.id = m.property_id
    where p.county_id = ${countySql}
      and p.state_code = '${stateSql}'
      and upper(coalesce(p.city,'')) = '${citySql}'
    group by 1
    order by mortgage_record_rows desc;
  `) : [];

  const active = summary.active as Row;
  const mr = summary.mortgage_records as Row;
  const reapi = summary.realestateapi_cache as Row;
  const coverage = summary.coverage as Row;

  console.log(JSON.stringify({
    market: `${CITY}, ${STATE}`,
    generated_at: new Date().toISOString(),
    active_listing_properties: Number(active.active_properties ?? 0),
    active_listing_debt_coverage: {
      properties_with_mortgage_record_rows: Number(mr.properties_with_any_mortgage_record ?? 0),
      valid_realestateapi_free_clear_properties: Number(reapi.valid_free_clear_rows ?? 0),
      overlap_mortgage_record_and_free_clear: Number(coverage.overlap_mortgage_record_and_free_clear ?? 0),
      exact_covered_properties: Number(coverage.exact_covered_properties ?? 0),
      unknown_properties: Math.max(0, Number(active.active_properties ?? 0) - Number(coverage.exact_covered_properties ?? 0)),
      exact_coverage_pct: pct(coverage.exact_covered_properties, active.active_properties),
      note: "Free-clear proof is debt coverage but is intentionally not inserted as a mortgage_records row.",
    },
    mortgage_records_summary: mr,
    active_listing_source_split: sourceRows,
    realestateapi_property_detail_cache: reapi,
    city_property_recorder_split: countyRecorder,
  }, null, 2));

}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
