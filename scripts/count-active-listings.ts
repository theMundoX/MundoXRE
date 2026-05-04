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
    select
      count(*) filter (where l.is_on_market = true) as active_listing_rows,
      count(distinct l.property_id) filter (where l.is_on_market = true) as active_properties,
      count(distinct p.id) filter (where l.is_on_market = true and r.property_id is not null) as active_properties_with_reapi_cache
    from listing_signals l
    join properties p on p.id = l.property_id
    left join realestateapi_property_details r on r.property_id = p.id
    where p.state_code = $1
      and upper(coalesce(p.city,'')) = $2
  `, [state, city]);

  const bySource = await client.query(`
    select coalesce(l.listing_source, 'unknown') as listing_source,
      count(*) as active_rows,
      count(distinct l.property_id) as active_properties
    from listing_signals l
    join properties p on p.id = l.property_id
    where l.is_on_market = true
      and p.state_code = $1
      and upper(coalesce(p.city,'')) = $2
    group by coalesce(l.listing_source, 'unknown')
    order by active_rows desc
  `, [state, city]);

  console.log(JSON.stringify({
    market: `${city.toLowerCase()}, ${state}`,
    generated_at: new Date().toISOString(),
    summary: rows[0] ?? {},
    bySource: bySource.rows,
  }, null, 2));
} finally {
  await client.end();
}
