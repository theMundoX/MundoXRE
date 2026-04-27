#!/usr/bin/env tsx
/**
 * MXRE — Cook County, IL (Chicago) assessor ingest
 *
 * Sources (Cook County Open Data / Socrata):
 *   CAMA (beds/baths/sqft/year_built): dataset x54s-btds
 *   Parcel addresses + owner names:     dataset 3723-97qp
 *
 * Covers ~1.8M parcels in Cook County (Chicago + suburbs).
 * Downloads current-year data only; uses ON CONFLICT to upsert on
 * (county_id, parcel_id, source).
 *
 * Usage:
 *   npx tsx scripts/ingest-cook-county-il.ts
 *   npx tsx scripts/ingest-cook-county-il.ts --year=2025
 *   npx tsx scripts/ingest-cook-county-il.ts --dry-run
 *   npx tsx scripts/ingest-cook-county-il.ts --limit=10000
 */

import "dotenv/config";

const argv    = process.argv.slice(2);
const getArg  = (n: string) => argv.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => argv.includes(`--${n}`);

const YEAR    = getArg("year") ? parseInt(getArg("year")!) : 2026;
const DRY_RUN = hasFlag("dry-run");
const MAX_ROWS = getArg("limit") ? parseInt(getArg("limit")!) : Infinity;

// Socrata API base — no auth token required for public data, but rate-limited
const SOCRATA_BASE = "https://datacatalog.cookcountyil.gov/resource";
const PAGE_SIZE    = 50_000;

// ─── DB ────────────────────────────────────────────────────────────────────

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(query: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(55_000),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return res.json();
}

// ─── Socrata paged fetch ────────────────────────────────────────────────────

async function* socrataPages<T>(
  datasetId: string,
  fields: string,
  where: string,
  orderBy: string,
): AsyncGenerator<T[]> {
  let offset = 0;
  let fetched = 0;
  while (true) {
    const url = new URL(`${SOCRATA_BASE}/${datasetId}.json`);
    url.searchParams.set("$select",  fields);
    url.searchParams.set("$where",   where);
    url.searchParams.set("$order",   orderBy);
    url.searchParams.set("$limit",   String(PAGE_SIZE));
    url.searchParams.set("$offset",  String(offset));

    const res = await fetch(url.toString(), {
      headers: { "Accept": "application/json", "User-Agent": "MXRE-Ingest/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!res.ok) throw new Error(`Socrata ${res.status} for ${datasetId}: ${await res.text()}`);
    const rows = await res.json() as T[];
    if (rows.length === 0) break;
    yield rows;
    fetched += rows.length;
    if (fetched >= MAX_ROWS) break;
    offset += rows.length;
    if (rows.length < PAGE_SIZE) break;
    // Brief pause to avoid Socrata rate-limits (5 req/s for unauthenticated)
    await new Promise(r => setTimeout(r, 250));
  }
}

// ─── Types ──────────────────────────────────────────────────────────────────

interface CamaRow {
  pin: string;
  year: string;
  char_beds: string;
  char_fbath: string;
  char_hbath: string;
  char_bldg_sf: string;
  char_yrblt: string;
}

interface AddrRow {
  pin: string;
  year: string;
  prop_address_full: string;
  prop_address_city_name: string;
  prop_address_state: string;
  prop_address_zipcode_1: string;
  owner_address_name: string;
  mail_address_name: string;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — Cook County, IL assessor ingest");
  console.log("═".repeat(60));
  console.log(`Year    : ${YEAR}`);
  console.log(`Dry run : ${DRY_RUN}`);
  console.log();

  // Resolve county_id
  const countyRows = await pg(`
    SELECT id AS county_id FROM counties
    WHERE state_code = 'IL' AND county_name ILIKE '%Cook%'
    LIMIT 1
  `);
  if (!countyRows.length) throw new Error("Cook County not found in counties table");
  const COUNTY_ID = countyRows[0].county_id as number;
  console.log(`Cook County ID: ${COUNTY_ID}`);

  // ── Step 1: Load CAMA data (beds/baths/sqft/year_built) ──────────────────
  console.log(`\nFetching CAMA data (year=${YEAR}) from x54s-btds...`);
  const cama = new Map<string, CamaRow>();

  let camaCount = 0;
  for await (const page of socrataPages<CamaRow>(
    "x54s-btds",
    "pin,year,char_beds,char_fbath,char_hbath,char_bldg_sf,char_yrblt",
    `year = ${YEAR}`,
    "pin ASC",
  )) {
    for (const r of page) {
      // Take max sqft row when a parcel has multiple buildings
      const existing = cama.get(r.pin);
      const newSqft = parseFloat(r.char_bldg_sf) || 0;
      const oldSqft = existing ? parseFloat(existing.char_bldg_sf) || 0 : 0;
      if (!existing || newSqft > oldSqft) {
        cama.set(r.pin, r);
      }
    }
    camaCount += page.length;
    process.stdout.write(`\r  Loaded ${camaCount.toLocaleString()} CAMA rows → ${cama.size.toLocaleString()} unique PINs`);
    if (camaCount >= MAX_ROWS) break;
  }
  console.log(`\n  Done: ${cama.size.toLocaleString()} unique PINs with CAMA data`);

  // ── Step 2: Load parcel addresses + owner names ───────────────────────────
  console.log(`\nFetching parcel addresses (year=${YEAR}) from 3723-97qp...`);
  const addrs = new Map<string, AddrRow>();

  let addrCount = 0;
  for await (const page of socrataPages<AddrRow>(
    "3723-97qp",
    "pin,year,prop_address_full,prop_address_city_name,prop_address_state,prop_address_zipcode_1,owner_address_name,mail_address_name",
    `year = ${YEAR}`,
    "pin ASC",
  )) {
    for (const r of page) addrs.set(r.pin, r);
    addrCount += page.length;
    process.stdout.write(`\r  Loaded ${addrCount.toLocaleString()} address rows`);
    if (addrCount >= MAX_ROWS) break;
  }
  console.log(`\n  Done: ${addrs.size.toLocaleString()} unique PINs with address data`);

  // ── Step 3: Merge and upsert ───────────────────────────────────────────────
  // Join on PIN — all unique PINs from either dataset
  const allPins = new Set([...cama.keys(), ...addrs.keys()]);
  console.log(`\nTotal unique PINs to upsert: ${allPins.size.toLocaleString()}`);

  if (DRY_RUN) {
    const sample = [...allPins].slice(0, 3);
    for (const pin of sample) {
      const c = cama.get(pin);
      const a = addrs.get(pin);
      console.log(`  PIN ${pin}: beds=${c?.char_beds} sqft=${c?.char_bldg_sf} yr=${c?.char_yrblt} owner=${a?.owner_address_name} addr=${a?.prop_address_full}`);
    }
    console.log("  [dry-run] No writes performed.");
    return;
  }

  const BATCH     = 200;
  const esc       = (v: unknown) => v == null ? "NULL" : `'${String(v).replace(/'/g, "''")}'`;
  const INT_MAX   = 2_147_483_647;
  const escNum    = (v: string | undefined) => { const n = parseFloat(v ?? ""); return isNaN(n) || n <= 0 ? "NULL" : String(Math.min(Math.round(n), INT_MAX)); };
  const escYear   = (v: string | undefined) => { const n = parseInt(v ?? ""); return n > 1800 && n <= 2030 ? String(n) : "NULL"; };
  const escBaths  = (f: string | undefined, h: string | undefined) => {
    const full = parseFloat(f ?? "") || 0;
    const half = parseFloat(h ?? "") || 0;
    const total = full + half * 0.5;
    return total > 0 ? String(total) : "NULL";
  };

  const COLS = [
    "county_id","parcel_id","source","state_code","address","city","zip",
    "owner_name","bedrooms","bathrooms","total_sqft","year_built",
    "created_at","updated_at",
  ].join(",");

  let inserted = 0, batches = 0, errors = 0;
  const pins = [...allPins];

  for (let i = 0; i < pins.length; i += BATCH) {
    const chunk = pins.slice(i, i + BATCH);
    const vals  = chunk.map(pin => {
      const c = cama.get(pin);
      const a = addrs.get(pin);
      return `(${[
        COUNTY_ID,
        esc(pin),
        esc("cook-county-assessor"),
        esc("IL"),
        esc(a?.prop_address_full || ""),
        esc(a?.prop_address_city_name || ""),
        esc(a?.prop_address_zipcode_1 || ""),
        esc(a?.owner_address_name || a?.mail_address_name || null),
        escNum(c?.char_beds),
        escBaths(c?.char_fbath, c?.char_hbath),
        escNum(c?.char_bldg_sf),
        escYear(c?.char_yrblt),
        "NOW()",
        "NOW()",
      ].join(",")})`;
    }).join(",");

    const sql = `
      INSERT INTO properties (${COLS}) VALUES ${vals}
      ON CONFLICT (county_id, parcel_id) DO UPDATE SET
        source     = EXCLUDED.source,
        address    = EXCLUDED.address,
        city       = EXCLUDED.city,
        zip        = EXCLUDED.zip,
        owner_name = EXCLUDED.owner_name,
        bedrooms   = COALESCE(EXCLUDED.bedrooms,   properties.bedrooms),
        bathrooms  = COALESCE(EXCLUDED.bathrooms,  properties.bathrooms),
        total_sqft = COALESCE(EXCLUDED.total_sqft, properties.total_sqft),
        year_built = COALESCE(EXCLUDED.year_built, properties.year_built),
        updated_at = NOW()
    `;

    try {
      await pg(sql);
      inserted += chunk.length;
    } catch (e: any) {
      errors++;
      if (errors <= 3) console.error(`\n  Batch error: ${(e as Error).message.slice(0, 120)}`);
    }

    batches++;
    if (batches % 50 === 0) {
      process.stdout.write(`\r  Upserted ${inserted.toLocaleString()} / ${pins.length.toLocaleString()} (${Math.round(inserted/pins.length*100)}%)  errors=${errors}`);
    }
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`Cook County upsert complete.`);
  console.log(`  Upserted : ${inserted.toLocaleString()} rows`);
  console.log(`  Errors   : ${errors}`);

  // Quick coverage report
  const check = await pg(`
    SELECT COUNT(*)::int AS total,
           COUNT(bedrooms)::int AS has_beds,
           COUNT(total_sqft)::int AS has_sqft,
           COUNT(year_built)::int AS has_yr,
           COUNT(owner_name)::int AS has_owner
    FROM properties
    WHERE county_id = ${COUNTY_ID}
      AND source = 'cook-county-assessor'
  `).catch(() => []);
  if (check.length) {
    const r = check[0];
    const pct = (n: number, d: number) => d > 0 ? `${Math.round(n/d*100)}%` : "0%";
    console.log(`\nCoverage: ${r.total?.toLocaleString()} rows | beds=${pct(r.has_beds,r.total)} sqft=${pct(r.has_sqft,r.total)} yr=${pct(r.has_yr,r.total)} owner=${pct(r.has_owner,r.total)}`);
  }
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
