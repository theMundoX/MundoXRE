#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const MARKET = process.argv.find(a => a.startsWith("--market="))?.split("=")[1]?.toLowerCase() ?? "indianapolis";
const DAYS = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--days="))?.split("=")[1] ?? "7", 10));
const LIMIT = Math.max(1, Math.min(500, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "50", 10)));

async function pg<T extends Record<string, unknown>>(query: string): Promise<T[]> {
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

async function main() {
  if (!["indianapolis", "indy"].includes(MARKET)) throw new Error("Only --market=indianapolis is supported right now.");

  const rows = await pg(`
    select
      e.event_at,
      e.address,
      e.city,
      e.state_code,
      e.zip,
      e.listing_source,
      e.listing_url,
      e.previous_list_price,
      e.list_price,
      (e.previous_list_price - e.list_price)::numeric as price_drop,
      case
        when e.previous_list_price > 0 then round(((e.previous_list_price - e.list_price) / e.previous_list_price) * 100, 2)
        else null
      end as price_drop_pct,
      e.listing_agent_name,
      e.listing_brokerage
    from listing_signal_events e
    where e.event_type = 'price_changed'
      and e.list_price is not null
      and e.previous_list_price is not null
      and e.list_price < e.previous_list_price
      and e.state_code = 'IN'
      and e.city ilike '%INDIANAPOLIS%'
      and e.event_at >= now() - interval '${DAYS} days'
    order by (e.previous_list_price - e.list_price) desc nulls last, e.event_at desc
    limit ${LIMIT};
  `);

  console.log(JSON.stringify({
    market: "indianapolis",
    days: DAYS,
    count: rows.length,
    rows,
    generated_at: new Date().toISOString(),
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
