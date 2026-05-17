#!/usr/bin/env tsx
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const PG_URL = firstEnv("MXRE_PG_URL")
  ?? `${(firstEnv("SUPABASE_URL") ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY") ?? "";
const arg = (name: string, fallback: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const STATE = arg("state", "IN").toUpperCase();
const CITY = arg("city", "INDIANAPOLIS").toUpperCase();

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
  const sql = (value: string) => `'${value.replace(/'/g, "''")}'`;
  const [summary] = await pg(`
    select
      count(*)::int as active_listing_rows,
      count(distinct property_id)::int as active_properties,
      count(*) filter (where nullif(listing_agent_name,'') is not null)::int as rows_with_agent_name,
      count(*) filter (where nullif(listing_agent_first_name,'') is not null and nullif(listing_agent_last_name,'') is not null)::int as rows_with_agent_first_last,
      count(*) filter (where nullif(listing_agent_email,'') is not null)::int as rows_with_agent_email,
      count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as rows_with_agent_phone,
      count(*) filter (where nullif(listing_brokerage,'') is not null)::int as rows_with_brokerage,
      count(*) filter (
        where nullif(listing_agent_name,'') is not null
          and nullif(listing_agent_email,'') is not null
          and nullif(listing_agent_phone,'') is not null
          and nullif(listing_brokerage,'') is not null
      )::int as rows_agent_contact_complete,
      count(*) filter (where creative_finance_status = 'positive')::int as creative_positive,
      count(*) filter (where creative_finance_status = 'negative')::int as creative_negative,
      count(*) filter (where creative_finance_status is null)::int as creative_missing,
      count(*) filter (where creative_finance_score is not null)::int as creative_scored
    from listing_signals
    where is_on_market = true
      and state_code = ${sql(STATE)}
      and upper(city) = ${sql(CITY)};
  `);

  const total = Number(summary.active_listing_rows ?? 0);
  console.log(JSON.stringify({
    market: `${CITY.toLowerCase()}, ${STATE}`,
    generated_at: new Date().toISOString(),
    ...summary,
    coverage_pct: {
      agent_name: pct(summary.rows_with_agent_name, total),
      agent_first_last: pct(summary.rows_with_agent_first_last, total),
      agent_email: pct(summary.rows_with_agent_email, total),
      agent_phone: pct(summary.rows_with_agent_phone, total),
      brokerage: pct(summary.rows_with_brokerage, total),
      agent_contact_complete: pct(summary.rows_agent_contact_complete, total),
      creative_scored: pct(summary.creative_scored, total),
    },
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
