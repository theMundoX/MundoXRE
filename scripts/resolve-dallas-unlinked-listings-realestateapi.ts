#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = process.env.MXRE_PG_URL
  ?? `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const API_KEY = process.env.REALESTATEAPI_KEY
  ?? process.env.REALESTATE_API_KEY
  ?? process.env.REALESTATEAPI_API_KEY;

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const LIMIT = Math.min(Math.max(Number(valueArg("limit") ?? "100"), 1), 2500);
const MAX_CALLS = Math.min(Math.max(Number(valueArg("max-calls") ?? String(LIMIT)), 0), LIMIT);

type Row = Record<string, any>;

function valueArg(name: string) {
  const prefix = `--${name}=`;
  return args.find(arg => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
}

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

function sql(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function normalize(value: string): string {
  return value
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[.,]/g, " ")
    .replace(/#/g, " UNIT ")
    .replace(/\b(APARTMENT|APT|UNIT|STE|SUITE|BLDG|BUILDING)\b/g, " UNIT ")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bPLAZA\b/g, "PLZ")
    .replace(/\bSPRINGS\b/g, "SPGS")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/\s+/g, " ")
    .trim();
}

function addressKeys(value: string): string[] {
  const full = normalize(value);
  const unitMatch = full.match(/\s+UNIT\s+([A-Z0-9-]+)\s*$/);
  const unit = unitMatch?.[1];
  const base = full.replace(/\s+UNIT\s+\S+.*$/, "").trim();
  const keys = [full];
  if (unit) {
    keys.push(`${base} ${unit}`);
    if (/^\d+$/.test(unit)) keys.push(`${base} ${unit.padStart(4, "0")}`);
  }
  return [...new Set(keys.filter(key => key.length >= 5))];
}

function parcelKey(value: unknown): string | null {
  const text = String(value ?? "").replace(/[^A-Za-z0-9]/g, "").toUpperCase();
  return text ? text.replace(/^0+/, "") : null;
}

function parcelExactKey(value: unknown): string | null {
  const text = String(value ?? "").replace(/[^A-Za-z0-9]/g, "").toUpperCase();
  return text || null;
}

function responseAddress(response: Row): { address: string | null; city: string | null; zip: string | null } {
  const info = response.propertyInfo?.address ?? {};
  return {
    address: stringOrNull(info.address) ?? stringOrNull(info.label),
    city: stringOrNull(info.city),
    zip: stringOrNull(info.zip),
  };
}

function stringOrNull(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value).trim();
  return text ? text : null;
}

async function callRealEstateApi(address: string): Promise<Row> {
  if (!API_KEY) throw new Error("Set REALESTATEAPI_KEY before making paid RealEstateAPI calls.");
  const response = await fetch("https://api.realestateapi.com/v2/PropertyDetail", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
      "x-api-key": API_KEY,
      "x-user-id": "mxre-unlinked-listing-resolution",
    },
    body: JSON.stringify({ address, exact_match: true, comps: false }),
    signal: AbortSignal.timeout(45_000),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`RealEstateAPI ${response.status}: ${text.slice(0, 500)}`);
  const parsed = JSON.parse(text);
  const data = parsed?.data;
  return data && typeof data === "object" && !Array.isArray(data) ? data : parsed;
}

async function main() {
  console.log("MXRE - Dallas unlinked listing RealEstateAPI resolver");
  console.log(JSON.stringify({ dry_run: DRY_RUN, limit: LIMIT, max_calls: MAX_CALLS }, null, 2));

  const listings = await pg(`
    select id, address, city, state_code, zip, listing_url
      from listing_signals
     where is_on_market = true
       and state_code = 'TX'
       and upper(coalesce(city,'')) = 'DALLAS'
       and property_id is null
       and nullif(address,'') is not null
       and not (raw ? 'realestateapi_unlinked_resolution')
     order by last_seen_at desc nulls last, updated_at desc nulls last, id
     limit ${LIMIT};
  `);

  let apiCalls = 0;
  let matched = 0;
  let unmatched = 0;
  let ambiguous = 0;
  let failed = 0;

  for (const listing of listings) {
    if (apiCalls >= MAX_CALLS && !DRY_RUN) break;
    const searchAddress = [listing.address, listing.city, listing.state_code, listing.zip].filter(Boolean).join(", ");
    try {
      if (DRY_RUN) {
        console.log(JSON.stringify({ wouldCall: searchAddress, listing_id: listing.id }));
        continue;
      }
      const response = await callRealEstateApi(searchAddress);
      apiCalls++;
      const propertyId = await resolvePropertyId(response, listing);
      const resolution = {
        provider: "realestateapi",
        observed_at: new Date().toISOString(),
        search_address: searchAddress,
        matched_property_id: propertyId,
        match_status: propertyId ? "matched" : "unmatched",
        response,
      };

      await pg(`
        update listing_signals
           set raw = jsonb_set(coalesce(raw,'{}'::jsonb), '{realestateapi_unlinked_resolution}', ${sql(JSON.stringify(resolution))}::jsonb, true),
               property_id = coalesce(property_id, ${propertyId ?? "null"}),
               updated_at = now()
         where id = ${Number(listing.id)}
           and property_id is null;
      `);

      if (propertyId) {
        matched++;
        await pg(`
          insert into property_enrichment_queue(property_id, provider, reason, status, priority, next_run_at)
          values (${propertyId}, 'realestateapi', 'missing_property_detail', 'queued', 5, now())
          on conflict(property_id, provider, reason)
          do update set status = 'queued', priority = least(property_enrichment_queue.priority, excluded.priority), next_run_at = now(), updated_at = now();
        `);
      } else {
        unmatched++;
      }
      await sleep(350);
    } catch (error) {
      failed++;
      await pg(`
        update listing_signals
           set raw = jsonb_set(coalesce(raw,'{}'::jsonb), '{realestateapi_unlinked_resolution_error}', ${sql(JSON.stringify({
             provider: "realestateapi",
             observed_at: new Date().toISOString(),
             search_address: searchAddress,
             error: error instanceof Error ? error.message : String(error),
           }))}::jsonb, true),
               updated_at = now()
         where id = ${Number(listing.id)};
      `);
      console.error(`Failed listing ${listing.id}:`, error instanceof Error ? error.message : error);
    }
  }

  console.log(JSON.stringify({ scanned: listings.length, apiCalls, matched, unmatched, ambiguous, failed }, null, 2));

  async function resolvePropertyId(response: Row, listing: Row): Promise<number | null> {
    const exactApnKeys = [
      parcelExactKey(response.lotInfo?.apn),
      parcelExactKey(response.lotInfo?.apnUnformatted),
      parcelExactKey(response.propertyInfo?.parcelAccountNumber),
    ].filter(Boolean);
    const strippedApnKeys = [
      parcelKey(response.lotInfo?.apn),
      parcelKey(response.lotInfo?.apnUnformatted),
      parcelKey(response.propertyInfo?.parcelAccountNumber),
    ].filter(Boolean);
    if (exactApnKeys.length) {
      const rows = await pg<{ id: number }>(`
        select id
          from properties
         where county_id = 7
           and state_code = 'TX'
           and regexp_replace(upper(coalesce(parcel_id,'')), '[^A-Z0-9]', '', 'g') in (${exactApnKeys.map(sql).join(",")})
         limit 2;
      `);
      if (rows.length === 1) return Number(rows[0].id);
      if (rows.length > 1) {
        ambiguous++;
        return null;
      }
    }
    if (strippedApnKeys.length) {
      const rows = await pg<{ id: number }>(`
        select id
          from properties
         where county_id = 7
           and state_code = 'TX'
           and regexp_replace(regexp_replace(upper(coalesce(parcel_id,'')), '[^A-Z0-9]', '', 'g'), '^0+', '') in (${strippedApnKeys.map(sql).join(",")})
         limit 2;
      `);
      if (rows.length === 1) return Number(rows[0].id);
      if (rows.length > 1) {
        ambiguous++;
        return null;
      }
    }

    const returned = responseAddress(response);
    const zip = returned.zip ?? stringOrNull(listing.zip);
    const sourceAddress = returned.address ?? stringOrNull(listing.address);
    if (!zip || !sourceAddress) return null;
    if (/\bLOT\b/i.test(String(listing.address ?? "")) && exactApnKeys.length) return null;
    const keys = addressKeys(sourceAddress);
    const rows = await pg<{ id: number; address: string }>(`
      select id, address
        from properties
       where county_id = 7
         and state_code = 'TX'
         and zip = ${sql(zip)}
         and nullif(address,'') is not null
         and upper(split_part(address, ' ', 1)) = ${sql(normalize(sourceAddress).split(" ")[0])};
    `);
    const candidates = rows.filter(row => addressKeys(String(row.address ?? "")).some(key => keys.includes(key)));
    const ids = [...new Set(candidates.map(row => Number(row.id)))];
    if (ids.length === 1) return ids[0];
    if (ids.length > 1) ambiguous++;
    return null;
  }
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
