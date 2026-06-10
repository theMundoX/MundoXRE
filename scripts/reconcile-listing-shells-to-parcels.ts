#!/usr/bin/env tsx
/**
 * Reconcile listing-backed shell properties back to real parcel/assessor properties.
 *
 * When listing_signals are linked to a shell property (source like listing_signal_shell:*),
 * and an exact unique non-shell property exists for the same county/state/address/ZIP with
 * a non-null parcel_id, re-point the listing_signals.property_id to the real property.
 *
 * This keeps on-market layers usable while slowly collapsing shells back into true parcels
 * as parcel/property ingests catch up.
 *
 * Usage:
 *   npx tsx scripts/reconcile-listing-shells-to-parcels.ts --state=IN --city=INDIANAPOLIS --county_id=797583
 *   npx tsx scripts/reconcile-listing-shells-to-parcels.ts --state=TX --city=DALLAS --county_id=7 --source=redfin
 */
import "dotenv/config";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const basePgUrl = (process.env.MXRE_PG_URL || process.env.SUPABASE_URL || "").replace(/\/$/, "");
const PG_URL = basePgUrl.endsWith("/pg/query") ? basePgUrl : `${basePgUrl}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

const args = process.argv.slice(2);
const arg = (name: string) => args.find((value) => value.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const flag = (name: string) => args.includes(`--${name}`);

const STATE = (arg("state") ?? "").toUpperCase();
const CITY = (arg("city") ?? "").toUpperCase();
const COUNTY_ID = Number(arg("county_id") ?? arg("county-id"));
const SOURCE = arg("source") ?? "redfin";
const DRY_RUN = flag("dry-run");
const MATCH_WITHOUT_ZIP = flag("match-without-zip");
const ANY_SHELL_SOURCE = flag("any-shell-source");
const ZIPS = (arg("zips") ?? "")
  .split(",")
  .map((zip) => zip.trim())
  .filter(Boolean);

if (!STATE || !CITY || !Number.isFinite(COUNTY_ID)) {
  console.error(
    "Usage: npx tsx scripts/reconcile-listing-shells-to-parcels.ts --state=IN --city=INDIANAPOLIS --county_id=797583 [--source=redfin] [--zips=46201,46202] [--any-shell-source] [--dry-run]",
  );
  process.exit(1);
}

type Row = Record<string, unknown>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5 * 60_000);
    try {
      const response = await fetch(PG_URL, {
        method: "POST",
        headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
        signal: controller.signal,
      });
      if (!response.ok) {
        const body = await response.text();
        const transient = response.status === 408 || response.status === 429 || response.status >= 500;
        if (transient && attempt < maxAttempts) {
          await sleep(500 * attempt);
          continue;
        }
        throw new Error(`pg/query ${response.status}: ${body}`);
      }
      return response.json() as Promise<T[]>;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const transient = message.includes("aborted") || message.includes("AbortError") || message.includes("timeout");
      if (transient && attempt < maxAttempts) {
        await sleep(500 * attempt);
        continue;
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
  return [];
}

function sql(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function main() {
  console.log("MXRE reconcile listing shells -> parcels");
  console.log(JSON.stringify({ state: STATE, city: CITY, county_id: COUNTY_ID, source: SOURCE, dry_run: DRY_RUN, match_without_zip: MATCH_WITHOUT_ZIP }, null, 2));

  const shellSource = `listing_signal_shell:${SOURCE}`;
  const shellSourceLike = "listing_signal_shell:%";
  const marketPredicate = ZIPS.length
    ? `(upper(coalesce(ls.city,'')) = ${sql(CITY)} or ls.zip in (${ZIPS.map(sql).join(",")}))`
    : `upper(coalesce(ls.city,'')) = ${sql(CITY)}`;
  const shellPredicate = ANY_SHELL_SOURCE
    ? `(p_shell.record_status = 'active_listing_shell' or coalesce(p_shell.source,'') like ${sql(shellSourceLike)})`
    : `p_shell.source = ${sql(shellSource)}`;
  const BATCH = Math.max(100, Number(arg("batch") ?? "2000"));
  const MAX_BATCHES = Math.max(1, Number(arg("max-batches") ?? "50"));

  if (DRY_RUN) {
    const [row] = await pg<{ shell_linked_active: number }>(`
      select count(*)::int as shell_linked_active
        from listing_signals ls
        join properties p_shell
          on p_shell.id = ls.property_id
       where ls.is_on_market = true
         and ls.state_code = ${sql(STATE)}
         and ${marketPredicate}
         and p_shell.county_id = ${COUNTY_ID}
         and ${shellPredicate};
    `);
    console.log(JSON.stringify({ shell_linked_active: Number(row?.shell_linked_active ?? 0) || 0 }, null, 2));
    return;
  }

  type ShellLinkedListing = { id: number; address: string; zip: string };
  type PropertyCandidate = { id: number; address: string; zip: string };

  const normalizeKey = (address: string, zip: string) => `${address.toUpperCase().trim()}|${zip.trim()}`;

  let total = 0;

  for (let batch = 1; batch <= MAX_BATCHES; batch++) {
    const listings = await pg<ShellLinkedListing>(`
      select ls.id::bigint as id,
             upper(trim(ls.address)) as address,
             coalesce(ls.zip, '') as zip
        from listing_signals ls
        join properties p_shell
         on p_shell.id = ls.property_id
       where ls.is_on_market = true
         and ls.state_code = ${sql(STATE)}
         and ${marketPredicate}
         and ls.property_id is not null
         and p_shell.county_id = ${COUNTY_ID}
         and ${shellPredicate}
         and nullif(ls.address,'') is not null
         and (${MATCH_WITHOUT_ZIP ? "true" : "nullif(ls.zip,'') is not null"})
       order by ls.id
       limit ${BATCH};
    `);

    if (listings.length === 0) {
      console.log(JSON.stringify({ batch, reconciled: 0, total }, null, 2));
      break;
    }

    const addresses = [...new Set(listings.map((l) => l.address))];
    const zips = [...new Set(listings.map((l) => l.zip).filter(Boolean))];
    const addrSql = addresses.map(sql).join(",");
    const zipSql = zips.length ? zips.map(sql).join(",") : "''";

    const candidates = await pg<PropertyCandidate>(`
      select p.id::bigint as id,
             upper(trim(p.address)) as address,
             coalesce(p.zip, '') as zip
        from properties p
       where p.county_id = ${COUNTY_ID}
         and p.state_code = ${sql(STATE)}
         and coalesce(p.source,'') not like ${sql(shellSourceLike)}
         and coalesce(p.record_status,'') <> 'active_listing_shell'
         and upper(trim(p.address)) in (${addrSql})
         and (${MATCH_WITHOUT_ZIP ? "true" : `p.zip in (${zipSql})`});
    `);

    const candidateIdsByKey = new Map<string, number[]>();
    const candidateIdsByAddress = new Map<string, number[]>();
    for (const candidate of candidates) {
      const key = normalizeKey(String(candidate.address ?? ""), String(candidate.zip ?? ""));
      const list = candidateIdsByKey.get(key) ?? [];
      list.push(Number(candidate.id));
      candidateIdsByKey.set(key, list);
      const addressKey = String(candidate.address ?? "").toUpperCase().trim();
      const addressList = candidateIdsByAddress.get(addressKey) ?? [];
      addressList.push(Number(candidate.id));
      candidateIdsByAddress.set(addressKey, addressList);
    }

    const updates: Array<{ listingId: number; propertyId: number }> = [];
    for (const listing of listings) {
      const key = normalizeKey(String(listing.address ?? ""), String(listing.zip ?? ""));
      const hit = MATCH_WITHOUT_ZIP
        ? candidateIdsByAddress.get(String(listing.address ?? "").toUpperCase().trim()) ?? []
        : candidateIdsByKey.get(key) ?? [];
      if (hit.length === 1) updates.push({ listingId: Number(listing.id), propertyId: hit[0] });
    }

    if (updates.length === 0) {
      console.log(JSON.stringify({ batch, reconciled: 0, total }, null, 2));
      break;
    }

    const values = updates.map((u) => `(${u.listingId}::bigint, ${u.propertyId}::bigint)`).join(",\n");
    const [row] = await pg<{ reconciled: number }>(`
      with updates(listing_id, property_id) as (
        values
        ${values}
      ),
      updated as (
        update listing_signals ls
           set property_id = updates.property_id,
               updated_at = now()
          from updates
         where ls.id = updates.listing_id
        returning ls.id
      )
      select count(*)::int as reconciled from updated;
    `);

    const reconciled = Number(row?.reconciled ?? 0) || 0;
    total += reconciled;
    console.log(JSON.stringify({ batch, reconciled, total }, null, 2));
    if (reconciled === 0) break;
  }
}

main().catch((error) => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
