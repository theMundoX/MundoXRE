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
      select distinct on (p.id)
        p.id,
        l.listing_agent_email,
        l.listing_agent_phone,
        l.listing_agent_name,
        l.listing_brokerage,
        r.property_id as reapi_cached,
        exists (
          select 1 from mortgage_records mr
          where mr.property_id = p.id
            and mr.source_url = 'realestateapi'
            and mr.estimated_current_balance is not null
        ) as has_reapi_balance,
        exists (
          select 1 from mls_history mh
          where mh.property_id = p.id
            and mh.listing_source = 'realestateapi'
        ) as has_reapi_mls_history
      from listing_signals l
      join properties p on p.id = l.property_id
      left join realestateapi_property_details r on r.property_id = p.id
      where l.is_on_market = true
        and p.state_code = $1
        and upper(coalesce(p.city,'')) = $2
      order by p.id, l.last_seen_at desc nulls last, l.updated_at desc nulls last
    )
    select
      count(*) as active_properties,
      count(*) filter (where reapi_cached is null) as needs_reapi_first_call,
      count(*) filter (
        where reapi_cached is null
          and (
            nullif(listing_agent_email,'') is null
            or nullif(listing_agent_phone,'') is null
            or not has_reapi_balance
            or not has_reapi_mls_history
          )
      ) as reapi_paid_call_candidates,
      count(*) filter (
        where reapi_cached is not null
          and (
            nullif(listing_agent_email,'') is null
            or nullif(listing_agent_phone,'') is null
            or nullif(listing_agent_name,'') is null
            or nullif(listing_brokerage,'') is null
          )
      ) as rapidapi_fallback_candidates
    from active
  `, [state, city]);

  console.log(JSON.stringify({
    market: `${city.toLowerCase()}, ${state}`,
    generated_at: new Date().toISOString(),
    ...rows[0],
  }, null, 2));
} finally {
  await client.end();
}
