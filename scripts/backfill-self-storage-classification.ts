#!/usr/bin/env tsx
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const basePgUrl = (firstEnv("MXRE_PG_URL") ?? firstEnv("SUPABASE_URL") ?? "").replace(/\/$/, "");
const PG_URL = basePgUrl.endsWith("/pg/query") ? basePgUrl : `${basePgUrl}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY") ?? "";
const DRY_RUN = process.argv.includes("--dry-run");

type Market = {
  key: string;
  city: string;
  state: string;
  countyId: number;
};

const MARKETS: Market[] = [
  { key: "dallas", city: "DALLAS", state: "TX", countyId: 7 },
  { key: "indianapolis", city: "INDIANAPOLIS", state: "IN", countyId: 797583 },
  { key: "columbus", city: "COLUMBUS", state: "OH", countyId: 1698985 },
  { key: "dayton", city: "DAYTON", state: "OH", countyId: 1698991 },
  { key: "toledo", city: "TOLEDO", state: "OH", countyId: 2338836 },
  { key: "san-antonio", city: "SAN ANTONIO", state: "TX", countyId: 1741238 },
  { key: "memphis", city: "MEMPHIS", state: "TN", countyId: 1741244 },
  { key: "cleveland", city: "CLEVELAND", state: "OH", countyId: 1698988 },
  { key: "akron", city: "AKRON", state: "OH", countyId: 1698989 },
  { key: "fort-wayne", city: "FORT WAYNE", state: "IN", countyId: 797481 },
  { key: "south-bend", city: "SOUTH BEND", state: "IN", countyId: 797737 },
  { key: "peoria", city: "PEORIA", state: "IL", countyId: 2338837 },
  { key: "west-chester", city: "WEST CHESTER", state: "PA", countyId: 817175 },
  { key: "birmingham", city: "BIRMINGHAM", state: "AL", countyId: 1973348 },
  { key: "detroit", city: "DETROIT", state: "MI", countyId: 1973412 },
];

async function pg<T extends Record<string, unknown> = Record<string, unknown>>(query: string): Promise<T[]> {
  if (DRY_RUN && /^\s*update/i.test(query)) {
    console.log(`[dry-run] ${query.replace(/\s+/g, " ").slice(0, 220)}...`);
    return [] as T[];
  }
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

function sql(value: string): string {
  return value.replace(/'/g, "''");
}

async function main() {
  console.log("MXRE - self-storage classification backfill");
  console.log(`Dry run: ${DRY_RUN}`);

  const summary: Record<string, unknown>[] = [];
  for (const market of MARKETS) {
    const where = `
      county_id = ${market.countyId}
      and state_code = '${sql(market.state)}'
      and upper(coalesce(city,'')) like '%${sql(market.city)}%'
      and (
        coalesce(property_use,'') ~* '(MINI[- ]?WAREHOUSE|SELF[- ]?STORAGE|MINI[- ]?STORAGE|STORAGE UNITS?)'
        or lower(coalesce(asset_type,'') || ' ' || coalesce(asset_subtype,'') || ' ' || coalesce(property_type,'') || ' ' || coalesce(property_use,'')) like '%self%storage%'
        or lower(coalesce(asset_type,'') || ' ' || coalesce(asset_subtype,'') || ' ' || coalesce(property_type,'') || ' ' || coalesce(property_use,'')) like '%mini%warehouse%'
      )
    `;

    const before = await pg<{ rows: number }>(`select count(*)::int as rows from properties where ${where};`);
    const changed = await pg<{ updated: number }>(`
      update properties
         set asset_type = 'self_storage',
             asset_subtype = 'self_storage',
             total_units = null,
             unit_count_source = 'not_applicable',
             asset_confidence = 'high',
             is_sfr = false,
             is_apartment = false,
             updated_at = now()
       where ${where}
         and (
           asset_type is distinct from 'self_storage'
           or asset_subtype is distinct from 'self_storage'
           or unit_count_source is distinct from 'not_applicable'
         )
       returning 1 as updated;
    `);
    const after = await pg<{ rows: number }>(`
      select count(*)::int as rows
        from properties
       where county_id = ${market.countyId}
         and state_code = '${sql(market.state)}'
         and upper(coalesce(city,'')) like '%${sql(market.city)}%'
         and asset_type = 'self_storage';
    `);
    const updated = DRY_RUN ? 0 : changed.length;
    const item = { market: market.key, candidates: before[0]?.rows ?? 0, updated, selfStorageRows: after[0]?.rows ?? 0 };
    summary.push(item);
    console.log(JSON.stringify(item));
  }

  console.log(JSON.stringify({ summary }, null, 2));
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
