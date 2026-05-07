#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

const args = process.argv.slice(2);
hydrateWindowsUserEnv();
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const databaseUrl = firstEnv("MXRE_DIRECT_PG_URL", "MXRE_PG_URL", "DATABASE_URL", "POSTGRES_URL");

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, MXRE_PG_URL, DATABASE_URL, or POSTGRES_URL.");

type Queryable = {
  query<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<{ rows: T[] }>;
  end(): Promise<void>;
};

function sqlLiteral(value: unknown): string {
  if (value == null) return "null";
  if (Array.isArray(value)) return `array[${value.map(sqlLiteral).join(",")}]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return dollarQuotedString(String(value));
}

function dollarQuotedString(value: string): string {
  let tag = "mxre";
  while (value.includes(`$${tag}$`)) tag = `${tag}x`;
  return `$${tag}$${value}$${tag}$`;
}

function bindSql(query: string, params: unknown[] = []): string {
  const templated = params.reduceRight((sql, _value, index) => {
    const token = new RegExp(`\\$${index + 1}(?!\\d)`, "g");
    return sql.replace(token, `__MXRE_PARAM_${index + 1}__`);
  }, query);
  return params.reduce((sql, value, index) =>
    sql.replaceAll(`__MXRE_PARAM_${index + 1}__`, sqlLiteral(value)), templated);
}

function makeClient(): Queryable {
  if (/^https?:\/\//i.test(databaseUrl ?? "")) {
    const endpoint = databaseUrl.replace(/\/$/, "");
    const key = process.env.SUPABASE_SERVICE_KEY ?? "";
    return {
      async query<T = Record<string, unknown>>(query: string, params: unknown[] = []) {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
          body: JSON.stringify({ query: bindSql(query, params) }),
          signal: AbortSignal.timeout(120_000),
        });
        if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
        const body = await response.json();
        return { rows: Array.isArray(body) ? body as T[] : [] };
      },
      async end() {},
    };
  }
  return new Client({ connectionString: databaseUrl }) as unknown as Queryable;
}

const client = makeClient();
if (!/^https?:\/\//i.test(databaseUrl ?? "")) {
  await (client as unknown as Client).connect();
}

try {
  const { rows } = await client.query(`
    with active as (
      select distinct on (p.id)
        p.id,
        l.listing_agent_email,
        l.listing_agent_phone,
        l.listing_agent_name,
        l.listing_brokerage,
        coalesce(l.raw, '{}'::jsonb)->'zillow_rapidapi_detail' is not null as has_zillow_rapidapi_detail,
        coalesce(l.raw, '{}'::jsonb)->'zillow_rapidapi_contact' is not null as has_zillow_rapidapi_contact,
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
      ,
      count(*) filter (
        where reapi_cached is not null
          and not has_zillow_rapidapi_detail
          and (
            nullif(listing_agent_email,'') is null
            or nullif(listing_agent_phone,'') is null
            or nullif(listing_agent_name,'') is null
            or nullif(listing_brokerage,'') is null
          )
      ) as rapidapi_fallback_unattempted_candidates,
      count(*) filter (
        where reapi_cached is not null
          and has_zillow_rapidapi_detail
          and (
            nullif(listing_agent_email,'') is null
            or nullif(listing_agent_phone,'') is null
            or nullif(listing_agent_name,'') is null
            or nullif(listing_brokerage,'') is null
          )
      ) as rapidapi_fallback_attempted_still_missing
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
