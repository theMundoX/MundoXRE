#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const databaseUrl = process.env.MXRE_DIRECT_PG_URL
  ?? process.env.MXRE_PG_URL
  ?? process.env.DATABASE_URL
  ?? process.env.POSTGRES_URL;

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, MXRE_PG_URL, DATABASE_URL, or POSTGRES_URL.");

const client = new Client({ connectionString: databaseUrl });
await client.connect();

try {
  const { rows } = await client.query(`
    with active as (
      select distinct p.id
      from listing_signals l
      join properties p on p.id = l.property_id
      where l.is_on_market = true
        and p.state_code = $1
        and upper(coalesce(p.city,'')) = $2
    ),
    listing as (
      select
        count(*) as active_rows,
        count(distinct property_id) as active_properties,
        count(*) filter (where nullif(listing_agent_name,'') is not null) as mxre_agent_name,
        count(*) filter (where nullif(listing_agent_email,'') is not null) as mxre_agent_email,
        count(*) filter (where nullif(listing_agent_phone,'') is not null) as mxre_agent_phone,
        count(*) filter (where nullif(listing_brokerage,'') is not null) as mxre_brokerage,
        count(*) filter (where agent_contact_source = 'realestateapi') as reapi_agent_rows,
        count(*) filter (where agent_contact_source like 'zillow_api%') as zillow_agent_rows
      from listing_signals l
      join active a on a.id = l.property_id
      where l.is_on_market = true
    ),
    cache as (
      select
        count(*) as reapi_cached_properties,
        count(*) filter (where response_body ? 'mlsHistory') as reapi_has_mls_history,
        count(*) filter (where response_body ? 'currentMortgages') as reapi_has_current_mortgages,
        count(*) filter (where nullif(response_body->>'estimatedMortgageBalance','') is not null) as reapi_has_estimated_balance,
        count(*) filter (where nullif(response_body->>'estimatedMortgagePayment','') is not null) as reapi_has_estimated_payment,
        count(*) filter (where nullif(response_body->>'estimatedEquity','') is not null) as reapi_has_estimated_equity
      from realestateapi_property_details r
      join active a on a.id = r.property_id
    ),
    mortgages as (
      select
        count(distinct property_id) filter (where source_url = 'realestateapi') as properties_with_reapi_mortgages,
        count(distinct property_id) filter (where source_url = 'realestateapi' and estimated_current_balance is not null) as properties_with_reapi_balance,
        count(*) filter (where source_url = 'realestateapi') as reapi_mortgage_rows,
        count(*) filter (where source_url = 'realestateapi' and open is true) as reapi_open_mortgage_rows
      from mortgage_records m
      join active a on a.id = m.property_id
    ),
    mls as (
      select
        count(distinct property_id) filter (where listing_source = 'realestateapi') as properties_with_reapi_mls_history,
        count(*) filter (where listing_source = 'realestateapi') as reapi_mls_rows,
        count(*) filter (where listing_source = 'realestateapi' and nullif(agent_email,'') is not null) as reapi_mls_rows_with_email
      from mls_history h
      join active a on a.id = h.property_id
    )
    select *
    from listing, cache, mortgages, mls
  `, [state, city]);

  const row = rows[0] ?? {};
  const activeProperties = Number(row.active_properties ?? 0) || 1;
  const pct = (value: unknown) => Math.round((Number(value ?? 0) / activeProperties) * 10000) / 100;
  console.log(JSON.stringify({
    market: `${city.toLowerCase()}, ${state}`,
    generated_at: new Date().toISOString(),
    ...row,
    coverage_pct_by_active_property: {
      mxre_agent_name: pct(row.mxre_agent_name),
      mxre_agent_email: pct(row.mxre_agent_email),
      mxre_agent_phone: pct(row.mxre_agent_phone),
      mxre_brokerage: pct(row.mxre_brokerage),
      reapi_cached_properties: pct(row.reapi_cached_properties),
      reapi_has_current_mortgages: pct(row.reapi_has_current_mortgages),
      reapi_has_estimated_balance: pct(row.reapi_has_estimated_balance),
      properties_with_reapi_balance: pct(row.properties_with_reapi_balance),
      properties_with_reapi_mls_history: pct(row.properties_with_reapi_mls_history),
    },
  }, null, 2));
} finally {
  await client.end();
}
