#!/usr/bin/env tsx
/**
 * Tier 1 rent baseline — Census ACS 5-year (B25031: median gross rent by bedrooms).
 *
 * Free, official, no auth. Pulls county-level median gross rent overall + by
 * bedroom count (studio, 1BR, 2BR, 3BR, 4BR, 5BR+) for any list of counties
 * and writes to `rent_baselines`.
 *
 * Usage:
 *   npx tsx scripts/ingest-rent-baselines-acs.ts                    # default: Marion only
 *   npx tsx scripts/ingest-rent-baselines-acs.ts --counties=18097,18057,18063
 *   npx tsx scripts/ingest-rent-baselines-acs.ts --states=IN        # all counties in Indiana
 *   npx tsx scripts/ingest-rent-baselines-acs.ts --states=IN,OH,IL  # multi-state
 *   npx tsx scripts/ingest-rent-baselines-acs.ts --year=2022
 *   npx tsx scripts/ingest-rent-baselines-acs.ts --all-midwest      # legacy: 19 mxre-midwest FIPS
 */

import "dotenv/config";

const args = process.argv.slice(2);
const getArg = (n: string) => args.find((a) => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => args.includes(`--${n}`);

const VINTAGE = parseInt(getArg("year") ?? "2022", 10); // ACS 5-year ending year

// State FIPS lookup for 2-letter abbreviations
const STATE_FIPS: Record<string, string> = {
  AL:"01",AK:"02",AZ:"04",AR:"05",CA:"06",CO:"08",CT:"09",DE:"10",FL:"12",GA:"13",
  HI:"15",ID:"16",IL:"17",IN:"18",IA:"19",KS:"20",KY:"21",LA:"22",ME:"23",MD:"24",
  MA:"25",MI:"26",MN:"27",MS:"28",MO:"29",MT:"30",NE:"31",NV:"32",NH:"33",NJ:"34",
  NM:"35",NY:"36",NC:"37",ND:"38",OH:"39",OK:"40",OR:"41",PA:"42",RI:"44",SC:"45",
  SD:"46",TN:"47",TX:"48",UT:"49",VT:"50",VA:"51",WA:"53",WV:"54",WI:"55",WY:"56",
  DC:"11",PR:"72",
};

const MIDWEST_FIPS = [
  "19113", "19013", "19163",
  "39045", "39055", "39125", "39049", "39061", "39113",
  "26125", "26009",
  "18097", "18057", "18063", "18081", "18059", "18089", "18003", "18011",
];

// Resolve target counties
let counties: string[] | null = null; // null = use per-state wildcard fetch
let targetStates: string[] | null = null;

if (hasFlag("all-midwest")) {
  counties = MIDWEST_FIPS;
} else if (getArg("counties")) {
  counties = getArg("counties")!.split(",").map(s => s.trim());
} else if (getArg("states")) {
  targetStates = getArg("states")!.split(",").map(s => s.trim().toUpperCase());
} else {
  counties = ["18097"]; // Marion / Indianapolis default
}

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
if (!PG_URL || !PG_KEY) { console.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing"); process.exit(1); }

async function pg(query: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`pg-meta ${res.status}: ${await res.text()}`);
  return res.json();
}

async function ensureTable() {
  await pg(`
    CREATE TABLE IF NOT EXISTS rent_baselines (
      id SERIAL PRIMARY KEY,
      source TEXT NOT NULL,
      geography_type TEXT NOT NULL,
      geography_id TEXT NOT NULL,
      bedrooms INTEGER,
      median_rent INTEGER NOT NULL,
      vintage_year INTEGER NOT NULL,
      observed_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(source, geography_type, geography_id, bedrooms, vintage_year)
    );
    CREATE INDEX IF NOT EXISTS idx_rent_baselines_geo
      ON rent_baselines(geography_type, geography_id);
  `);
}

// B25031 cells: 001=overall, 002=studio, 003=1BR, 004=2BR, 005=3BR, 006=4BR, 007=5BR+
const BEDROOM_MAP: Record<string, number | null> = {
  B25031_001E: null,
  B25031_002E: 0,
  B25031_003E: 1,
  B25031_004E: 2,
  B25031_005E: 3,
  B25031_006E: 4,
  B25031_007E: 5,
};
const ACS_VARS = Object.keys(BEDROOM_MAP).join(",");

async function fetchCountyRents(fips5: string) {
  const state = fips5.slice(0, 2);
  const county = fips5.slice(2);
  const url = `https://api.census.gov/data/${VINTAGE}/acs/acs5?get=NAME,${ACS_VARS}&for=county:${county}&in=state:${state}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`ACS ${res.status} for ${fips5}: ${await res.text()}`);
  const rows: string[][] = await res.json();
  if (!rows || rows.length < 2) return null;
  const [header, data] = rows;
  const out: Record<string, string> = {};
  header.forEach((h, i) => (out[h] = data[i]));
  return out;
}

/** Fetch all counties in a state via wildcard in one request. Returns array of {fips5, row} */
async function fetchAllCountiesInState(stateFips: string): Promise<Array<{fips5: string, row: Record<string, string>}>> {
  const url = `https://api.census.gov/data/${VINTAGE}/acs/acs5?get=NAME,${ACS_VARS}&for=county:*&in=state:${stateFips}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`ACS ${res.status} for state ${stateFips}: ${await res.text()}`);
  const rows: string[][] = await res.json();
  if (!rows || rows.length < 2) return [];
  const header = rows[0];
  const stateIdx = header.indexOf("state");
  const countyIdx = header.indexOf("county");
  return rows.slice(1).map(data => {
    const row: Record<string, string> = {};
    header.forEach((h, i) => (row[h] = data[i]));
    const fips5 = (data[stateIdx] ?? stateFips).padStart(2, "0") + (data[countyIdx] ?? "").padStart(3, "0");
    return { fips5, row };
  });
}

function buildInsertRows(fips5: string, row: Record<string, string>): string[] {
  const sets: string[] = [];
  for (const [varName, beds] of Object.entries(BEDROOM_MAP)) {
    const raw = row[varName];
    if (raw == null || raw === "" || raw === "null" || raw === "-") continue;
    const v = parseInt(raw, 10);
    if (!Number.isFinite(v) || v <= 0) continue;
    const bedSql = beds == null ? "NULL" : String(beds);
    sets.push(`('acs_b25031','county','${fips5}',${bedSql},${v},${VINTAGE})`);
  }
  return sets;
}

async function main() {
  console.log("MXRE — ACS rent baseline ingest (B25031, median gross rent by bedrooms)");
  await ensureTable();

  let inserted = 0;

  if (targetStates) {
    console.log(`  Vintage: ${VINTAGE} | States: ${targetStates.join(", ")} (all counties)`);
    for (const abbr of targetStates) {
      const stateFips = STATE_FIPS[abbr];
      if (!stateFips) { console.error(`  Unknown state: ${abbr}`); continue; }
      try {
        const entries = await fetchAllCountiesInState(stateFips);
        const allRows: string[] = [];
        for (const { fips5, row } of entries) {
          allRows.push(...buildInsertRows(fips5, row));
        }
        if (allRows.length > 0) {
          // Chunk into 500-row batches
          for (let i = 0; i < allRows.length; i += 500) {
            await pg(`
              INSERT INTO rent_baselines (source, geography_type, geography_id, bedrooms, median_rent, vintage_year)
              VALUES ${allRows.slice(i, i + 500).join(",")}
              ON CONFLICT (source, geography_type, geography_id, bedrooms, vintage_year)
              DO UPDATE SET median_rent=EXCLUDED.median_rent, observed_at=now();
            `);
          }
          inserted += allRows.length;
          console.log(`  ${abbr}: ${entries.length} counties, ${allRows.length} rows`);
        }
      } catch (e) {
        console.error(`  ${abbr}: ${(e as Error).message}`);
      }
    }
  } else {
    const fipsList = counties!;
    console.log(`  Vintage: ${VINTAGE} | Counties: ${fipsList.length}`);
    for (const fips of fipsList) {
      try {
        const row = await fetchCountyRents(fips);
        if (!row) { console.log(`  ${fips}: no data`); continue; }
        const sets = buildInsertRows(fips, row);
        if (sets.length === 0) { console.log(`  ${fips}: no usable values`); continue; }
        await pg(`
          INSERT INTO rent_baselines (source, geography_type, geography_id, bedrooms, median_rent, vintage_year)
          VALUES ${sets.join(",")}
          ON CONFLICT (source, geography_type, geography_id, bedrooms, vintage_year)
          DO UPDATE SET median_rent=EXCLUDED.median_rent, observed_at=now();
        `);
        inserted += sets.length;
        console.log(`  ${fips} (${row.NAME}): ${sets.length} rows`);
      } catch (e) {
        console.error(`  ${fips}: ${(e as Error).message}`);
      }
    }
  }

  console.log(`\nDone. ${inserted} rows upserted into rent_baselines.`);
}

main().catch((e) => { console.error(e); process.exit(1); });
