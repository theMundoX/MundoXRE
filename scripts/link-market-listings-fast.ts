#!/usr/bin/env tsx
/**
 * Fast market listing linker.
 *
 * Purpose:
 * - link active listing_signals to existing property rows by unique exact address + ZIP
 * - optionally create clearly sourced listing-backed property shells when no parcel row exists
 * - keep shell records distinguishable from assessor/parcel records for later reconciliation
 *
 * This is intentionally set-based and market-parameterized so daily refreshes do not stall
 * on per-row in-memory matching.
 */
import "dotenv/config";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const basePgUrl = (process.env.MXRE_PG_URL || process.env.SUPABASE_URL || "").replace(/\/$/, "");
const PG_URL = basePgUrl.endsWith("/pg/query") ? basePgUrl : `${basePgUrl}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

const args = process.argv.slice(2);
const arg = (name: string) => args.find(value => value.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const flag = (name: string) => args.includes(`--${name}`);

const STATE = (arg("state") ?? "").toUpperCase();
const CITY = (arg("city") ?? "").toUpperCase();
const COUNTY_ID = Number(arg("county_id") ?? arg("county-id"));
const SOURCE = arg("source") ?? "redfin";
const CREATE_SHELLS = flag("create-shells");
const DRY_RUN = flag("dry-run");

if (!STATE || !CITY || !Number.isFinite(COUNTY_ID)) {
  console.error("Usage: npx tsx scripts/link-market-listings-fast.ts --state=OH --city=COLUMBUS --county_id=1698985 [--source=redfin] [--create-shells] [--dry-run]");
  process.exit(1);
}

type Row = Record<string, unknown>;

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(180_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

function sql(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function countState(label: string) {
  const [row] = await pg(`
    select ${sql(label)} as label,
           count(*)::int as active_rows,
           count(*) filter (where property_id is null)::int as unlinked_rows,
           count(distinct property_id)::int as active_properties
      from listing_signals
     where is_on_market = true
       and state_code = ${sql(STATE)}
       and upper(coalesce(city,'')) = ${sql(CITY)};
  `);
  return row;
}

async function linkExactUnique() {
  if (DRY_RUN) {
    return pg(`
      with matches as (
        select ls.id listing_id, count(p.id)::int candidates
          from listing_signals ls
          join properties p
            on p.county_id = ${COUNTY_ID}
           and p.state_code = ${sql(STATE)}
           and p.zip = ls.zip
           and upper(trim(p.address)) = upper(trim(ls.address))
         where ls.is_on_market = true
           and ls.state_code = ${sql(STATE)}
           and upper(coalesce(ls.city,'')) = ${sql(CITY)}
           and ls.property_id is null
         group by ls.id
      )
      select count(*) filter (where candidates = 1)::int as would_link,
             count(*) filter (where candidates > 1)::int as ambiguous
        from matches;
    `);
  }

  return pg(`
    with unique_matches as (
      select listing_id, property_id
        from (
          select ls.id listing_id, min(p.id)::bigint property_id, count(p.id)::int candidates
            from listing_signals ls
            join properties p
              on p.county_id = ${COUNTY_ID}
             and p.state_code = ${sql(STATE)}
             and p.zip = ls.zip
             and upper(trim(p.address)) = upper(trim(ls.address))
           where ls.is_on_market = true
             and ls.state_code = ${sql(STATE)}
             and upper(coalesce(ls.city,'')) = ${sql(CITY)}
             and ls.property_id is null
           group by ls.id
        ) m
       where candidates = 1
    ),
    updated as (
      update listing_signals ls
         set property_id = unique_matches.property_id,
             updated_at = now()
        from unique_matches
       where ls.id = unique_matches.listing_id
         and ls.property_id is null
      returning ls.id
    )
    select count(*)::int as linked_exact_unique from updated;
  `);
}

async function createListingShells() {
  if (!CREATE_SHELLS) return [{ inserted_shells: 0 }];
  const shellSource = `listing_signal_shell:${SOURCE}`;

  if (DRY_RUN) {
    return pg(`
      with candidates as (
        select distinct upper(trim(address)) as address, zip
          from listing_signals ls
         where ls.is_on_market = true
           and ls.state_code = ${sql(STATE)}
           and upper(coalesce(ls.city,'')) = ${sql(CITY)}
           and ls.property_id is null
           and nullif(ls.address,'') is not null
           and nullif(ls.zip,'') is not null
      ),
      missing as (
        select c.*
          from candidates c
         where not exists (
           select 1 from properties p
            where p.county_id = ${COUNTY_ID}
              and p.state_code = ${sql(STATE)}
              and p.zip = c.zip
              and upper(trim(p.address)) = c.address
         )
      )
      select count(*)::int as would_insert_shells from missing;
    `);
  }

  return pg(`
    with candidates as (
      select upper(trim(address)) as address,
             zip,
             min(mls_list_price)::int as list_price,
             min(listing_url) as listing_url
        from listing_signals ls
       where ls.is_on_market = true
         and ls.state_code = ${sql(STATE)}
         and upper(coalesce(ls.city,'')) = ${sql(CITY)}
         and ls.property_id is null
         and nullif(ls.address,'') is not null
         and nullif(ls.zip,'') is not null
       group by upper(trim(address)), zip
    ),
    missing as (
      select c.*
        from candidates c
       where not exists (
         select 1 from properties p
          where p.county_id = ${COUNTY_ID}
            and p.state_code = ${sql(STATE)}
            and p.zip = c.zip
            and upper(trim(p.address)) = c.address
       )
    ),
    inserted as (
      insert into properties (
        county_id, parcel_id, address, city, state_code, zip,
        property_type, market_value, source, assessor_url, created_at, updated_at,
        record_status, asset_type, asset_confidence
      )
      select ${COUNTY_ID},
             null,
             address,
             ${sql(CITY)},
             ${sql(STATE)},
             zip,
             'listing_backed_property',
             nullif(list_price, 0),
             ${sql(shellSource)},
             listing_url,
             now(),
             now(),
             'active_listing_shell',
             'unknown',
             'listing_only'
        from missing
      returning id
    )
    select count(*)::int as inserted_shells from inserted;
  `);
}

async function main() {
  console.log("MXRE fast market listing linker");
  console.log(JSON.stringify({ state: STATE, city: CITY, county_id: COUNTY_ID, source: SOURCE, create_shells: CREATE_SHELLS, dry_run: DRY_RUN }, null, 2));
  const before = await countState("before");
  const exact = await linkExactUnique();
  const shells = await createListingShells();
  const afterShellExact = CREATE_SHELLS ? await linkExactUnique() : [{ linked_exact_unique: 0 }];
  const after = await countState("after");
  console.log(JSON.stringify({ before, exact: exact[0], shells: shells[0], after_shell_exact: afterShellExact[0], after }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
