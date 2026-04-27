#!/usr/bin/env tsx
/**
 * MXRE — NYC PLUTO (Primary Land Use Tax Lot Output) ingest
 *
 * Source: NYC Department of City Planning, data.cityofnewyork.us
 * Dataset: 64uk-42ks (updated quarterly, ~880K tax lots)
 *
 * Covers all 5 NYC boroughs → 5 counties:
 *   MN = Manhattan (New York County)
 *   BK = Brooklyn  (Kings County)
 *   QN = Queens    (Queens County)
 *   BX = Bronx     (Bronx County)
 *   SI = Staten Island (Richmond County)
 *
 * Fields: BBL, owner name, address, year_built, sqft, units,
 *         assessed land/total values, lat/lng, building class, land use
 *
 * Usage:
 *   npx tsx scripts/ingest-nyc-pluto.ts
 *   npx tsx scripts/ingest-nyc-pluto.ts --dry-run
 *   npx tsx scripts/ingest-nyc-pluto.ts --borough=MN    # Manhattan only
 */

import "dotenv/config";

const argv    = process.argv.slice(2);
const getArg  = (n: string) => argv.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => argv.includes(`--${n}`);

const DRY_RUN    = hasFlag("dry-run");
const BOROUGH    = getArg("borough")?.toUpperCase();  // MN | BK | QN | BX | SI

const SOCRATA_BASE = "https://data.cityofnewyork.us/resource";
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

// ─── Borough → county mapping ───────────────────────────────────────────────

const BOROUGH_STATE_COUNTY: Record<string, { state: string; countyName: string }> = {
  MN: { state: "NY", countyName: "New York" },
  BK: { state: "NY", countyName: "Kings" },
  QN: { state: "NY", countyName: "Queens" },
  BX: { state: "NY", countyName: "Bronx" },
  SI: { state: "NY", countyName: "Richmond" },
};

// ─── Socrata paged fetch ────────────────────────────────────────────────────

async function* socrataPages<T>(
  datasetId: string,
  fields: string,
  where: string,
  orderBy: string,
): AsyncGenerator<T[]> {
  let offset = 0;
  while (true) {
    const url = new URL(`${SOCRATA_BASE}/${datasetId}.json`);
    url.searchParams.set("$select",  fields);
    if (where) url.searchParams.set("$where",  where);
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
    offset += rows.length;
    if (rows.length < PAGE_SIZE) break;
    await new Promise(r => setTimeout(r, 200));
  }
}

interface PlutoRow {
  bbl: string;
  borough: string;
  address: string;
  zipcode: string;
  ownername: string;
  yearbuilt: string;
  numfloors: string;
  unitsres: string;
  unitstotal: string;
  bldgarea: string;
  lotarea: string;
  assessland: string;
  assesstot: string;
  latitude: string;
  longitude: string;
  bldgclass: string;
  landuse: string;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — NYC PLUTO ingest");
  console.log("═".repeat(60));
  console.log(`Borough : ${BOROUGH ?? "ALL"}`);
  console.log(`Dry run : ${DRY_RUN}`);
  console.log();

  // Resolve county IDs for all 5 boroughs
  const boroughs = BOROUGH ? [BOROUGH] : ["MN", "BK", "QN", "BX", "SI"];
  const countyIds: Record<string, number> = {};

  for (const b of boroughs) {
    const { state, countyName } = BOROUGH_STATE_COUNTY[b];
    const rows = await pg(`
      SELECT id AS county_id FROM counties
      WHERE state_code = '${state}' AND county_name ILIKE '%${countyName}%'
      LIMIT 1
    `);
    if (!rows.length) { console.warn(`  WARNING: ${countyName} County not found in DB`); continue; }
    countyIds[b] = rows[0].county_id;
    console.log(`  ${b} → ${countyName} County (id=${rows[0].county_id})`);
  }

  const boroughFilter = boroughs.map(b => `'${b}'`).join(",");
  const where = `borough IN (${boroughFilter})`;

  const FIELDS = [
    "bbl","borough","address","zipcode","ownername",
    "yearbuilt","numfloors","unitsres","unitstotal",
    "bldgarea","lotarea","assessland","assesstot",
    "latitude","longitude","bldgclass","landuse",
  ].join(",");

  console.log(`\nFetching PLUTO lots...`);

  const esc    = (v: unknown) => v == null ? "NULL" : `'${String(v).replace(/'/g, "''")}'`;
  const INT_MAX = 2_147_483_647;
  const escNum = (v: string | undefined) => { const n = parseFloat(v ?? ""); return isNaN(n) || n <= 0 ? "NULL" : String(Math.min(Math.round(n), INT_MAX)); };
  const escYear = (v: string | undefined) => { const n = parseInt(v ?? ""); return n > 1800 && n <= 2030 ? String(n) : "NULL"; };
  const escDec  = (v: string | undefined, digits = 6) => { const n = parseFloat(v ?? ""); return isNaN(n) ? "NULL" : n.toFixed(digits); };

  const COLS = [
    "county_id","parcel_id","source","state_code","address","city","zip",
    "owner_name","year_built","total_sqft","lot_sqft","assessed_value","land_value",
    "latitude","longitude","total_units",
    "created_at","updated_at",
  ].join(",");

  let inserted = 0, errors = 0, total = 0;
  const buffer: PlutoRow[] = [];
  const BATCH = 200;

  async function flushBuffer() {
    if (!buffer.length) return;
    const vals = buffer.map(r => {
      const cid = countyIds[r.borough];
      if (!cid) return null;
      return `(${[
        cid,
        esc(String(Math.round(parseFloat(r.bbl ?? "0")))),  // BBL as parcel_id
        esc("nyc-pluto"),
        esc("NY"),
        esc(r.address || ""),
        esc("New York"),
        esc(r.zipcode || ""),
        esc(r.ownername || null),
        escYear(r.yearbuilt),
        escNum(r.bldgarea),
        escNum(r.lotarea),
        escNum(r.assesstot),
        escNum(r.assessland),
        escDec(r.latitude),
        escDec(r.longitude),
        escNum(r.unitstotal || r.unitsres),
        "NOW()",
        "NOW()",
      ].join(",")})`;
    }).filter(Boolean).join(",");

    if (!vals) { buffer.length = 0; return; }

    const sql = `
      INSERT INTO properties (${COLS}) VALUES ${vals}
      ON CONFLICT (county_id, parcel_id) DO UPDATE SET
        source       = EXCLUDED.source,
        address      = EXCLUDED.address,
        zip          = EXCLUDED.zip,
        owner_name   = EXCLUDED.owner_name,
        year_built   = COALESCE(EXCLUDED.year_built,    properties.year_built),
        total_sqft   = COALESCE(EXCLUDED.total_sqft,    properties.total_sqft),
        lot_sqft     = COALESCE(EXCLUDED.lot_sqft,      properties.lot_sqft),
        assessed_value = COALESCE(EXCLUDED.assessed_value, properties.assessed_value),
        land_value   = COALESCE(EXCLUDED.land_value,    properties.land_value),
        latitude     = COALESCE(EXCLUDED.latitude,      properties.latitude),
        longitude    = COALESCE(EXCLUDED.longitude,     properties.longitude),
        total_units  = COALESCE(EXCLUDED.total_units,    properties.total_units),
        updated_at   = NOW()
    `;
    try {
      await pg(sql);
      inserted += buffer.length;
    } catch (e: any) {
      errors++;
      if (errors <= 3) console.error(`\n  Batch error: ${(e as Error).message.slice(0, 200)}`);
    }
    buffer.length = 0;
  }

  for await (const page of socrataPages<PlutoRow>("64uk-42ks", FIELDS, where, "bbl ASC")) {
    for (const row of page) {
      buffer.push(row);
      if (buffer.length >= BATCH) {
        if (!DRY_RUN) await flushBuffer();
        else buffer.length = 0;
      }
      total++;
    }
    process.stdout.write(`\r  Fetched ${total.toLocaleString()} lots, inserted ${inserted.toLocaleString()}, errors=${errors}`);
  }
  if (!DRY_RUN) await flushBuffer();

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`NYC PLUTO ingest complete.`);
  console.log(`  Total lots  : ${total.toLocaleString()}`);
  console.log(`  Inserted    : ${inserted.toLocaleString()}`);
  console.log(`  Errors      : ${errors}`);

  if (DRY_RUN) return;

  // Coverage report per borough
  for (const b of boroughs) {
    const cid = countyIds[b];
    if (!cid) continue;
    const check = await pg(`
      SELECT COUNT(*)::int AS total,
             COUNT(year_built)::int AS has_yr,
             COUNT(total_sqft)::int AS has_sqft,
             COUNT(owner_name)::int AS has_owner,
             COUNT(assessed_value)::int AS has_val,
             COUNT(latitude)::int AS has_geo
      FROM properties
      WHERE county_id = ${cid} AND source = 'nyc-pluto'
    `).catch(() => []);
    if (check.length) {
      const r = check[0];
      const pct = (n: number, d: number) => d > 0 ? `${Math.round(n/d*100)}%` : "0%";
      const { countyName } = BOROUGH_STATE_COUNTY[b];
      console.log(`  ${b} (${countyName}): ${r.total?.toLocaleString()} rows | yr=${pct(r.has_yr,r.total)} sqft=${pct(r.has_sqft,r.total)} owner=${pct(r.has_owner,r.total)} val=${pct(r.has_val,r.total)} geo=${pct(r.has_geo,r.total)}`);
    }
  }
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
