#!/usr/bin/env tsx
/**
 * Tier 1 rent baseline — HUD Fair Market Rent (FMR) + Small Area FMR (SAFMR).
 *
 * Two layers:
 *   1. FMR  — county/metro-level, updated annually
 *   2. SAFMR — ZIP-code-level for designated metros (Indianapolis IS one)
 *              Comes through the same fmr/data endpoint when smallarea_status=1
 *
 * Source: HUD User API (https://www.huduser.gov/portal/dataset/fmr-api.html).
 * Requires a free API token. Set HUD_TOKEN in .env.
 *
 * Usage:
 *   npx tsx scripts/ingest-hud-fmr.ts                     # IN only
 *   npx tsx scripts/ingest-hud-fmr.ts --year=2025
 *   npx tsx scripts/ingest-hud-fmr.ts --states=IN,OH,MI
 */

import "dotenv/config";

const args = process.argv.slice(2);
const getArg = (n: string) => args.find((a) => a.startsWith(`--${n}=`))?.split("=")[1];

const YEAR = parseInt(getArg("year") ?? "2025", 10);
const STATES = (getArg("states") ?? "IN").split(",").map((s) => s.trim().toUpperCase());

const HUD_TOKEN = process.env.HUD_TOKEN ?? "";
if (!HUD_TOKEN) {
  console.error("HUD_TOKEN missing. Get a free token at https://www.huduser.gov/portal/dataset/fmr-api.html");
  process.exit(1);
}

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(q: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
  });
  if (!res.ok) throw new Error(`pg-meta ${res.status}: ${await res.text()}`);
  return res.json();
}

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function hud(path: string, attempt = 1): Promise<any> {
  const res = await fetch(`https://www.huduser.gov/hudapi/public/${path}`, {
    headers: { Authorization: `Bearer ${HUD_TOKEN}` },
    signal: AbortSignal.timeout(30_000),
  });

  if (res.status === 429) {
    if (attempt > 6) throw new Error(`HUD 429 after ${attempt} retries: ${path}`);
    const delay = Math.min(60_000, 2000 * Math.pow(2, attempt - 1)) + Math.random() * 1000;
    console.log(`    429 rate-limited — waiting ${(delay / 1000).toFixed(1)}s (attempt ${attempt})`);
    await sleep(delay);
    return hud(path, attempt + 1);
  }

  if (!res.ok) throw new Error(`HUD ${res.status} ${path}: ${(await res.text()).slice(0, 200)}`);
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
    CREATE INDEX IF NOT EXISTS idx_rent_baselines_geo ON rent_baselines(geography_type, geography_id);
  `);
}

// HUD FMR field names → bedroom count
const FMR_BEDROOM_MAP: Record<string, number> = {
  "Efficiency": 0, "One-Bedroom": 1, "Two-Bedroom": 2, "Three-Bedroom": 3, "Four-Bedroom": 4,
};

async function upsert(rows: string[]) {
  for (let i = 0; i < rows.length; i += 500) {
    await pg(`
      INSERT INTO rent_baselines (source, geography_type, geography_id, bedrooms, median_rent, vintage_year)
      VALUES ${rows.slice(i, i + 500).join(",")}
      ON CONFLICT (source, geography_type, geography_id, bedrooms, vintage_year)
      DO UPDATE SET median_rent=EXCLUDED.median_rent, observed_at=now();
    `);
  }
}

async function ingestState(state: string) {
  // fmr/listCounties returns both county-level AND metro-level entity IDs
  const list: any[] = await hud(`fmr/listCounties/${state}`);
  console.log(`  ${state}: ${list.length} entities`);

  let countyRows = 0;
  let safmrRows = 0;
  let errors = 0;

  for (const c of list) {
    // Normalize FIPS to 5 digits (HUD sometimes returns 10-char codes)
    const fipsClean = String(c.fips_code ?? "").replace(/\D/g, "").slice(0, 5);
    if (!fipsClean) continue;

    // Rate-limit: 1 request per second to avoid 429s
    await sleep(1100);

    try {
      const data = await hud(`fmr/data/${c.fips_code}?year=${YEAR}`);
      const basicdata = data?.data?.basicdata;
      if (!basicdata) continue;

      const records = Array.isArray(basicdata) ? basicdata : [basicdata];
      const isSafmr = data?.data?.smallarea_status === "1" || data?.data?.smallarea_status === 1;

      // Deduplicate by (fips, bedrooms) for county-level FMR
      const fmrMap = new Map<string, string>();
      // Deduplicate by (zip, bedrooms) for ZIP-level SAFMR
      const safmrMap = new Map<string, string>();

      for (const r of records) {
        const zip = String(r.zip_code ?? "").trim();
        const isZipRow = /^\d{5}$/.test(zip);

        if (isSafmr && isZipRow) {
          // ZIP-level SAFMR row: store as hud_safmr
          for (const [name, beds] of Object.entries(FMR_BEDROOM_MAP)) {
            const v = parseInt(r[name], 10);
            if (Number.isFinite(v) && v > 0) {
              safmrMap.set(`${zip}|${beds}`, `('hud_safmr','zip','${zip}',${beds},${v},${YEAR})`);
            }
          }
        } else {
          // County/MSA aggregate row: store as hud_fmr
          for (const [name, beds] of Object.entries(FMR_BEDROOM_MAP)) {
            const v = parseInt(r[name], 10);
            if (Number.isFinite(v) && v > 0) {
              fmrMap.set(`${fipsClean}|${beds}`, `('hud_fmr','county','${fipsClean}',${beds},${v},${YEAR})`);
            }
          }
        }
      }

      const fmrInserts = [...fmrMap.values()];
      if (fmrInserts.length > 0) {
        await upsert(fmrInserts);
        countyRows += fmrInserts.length;
      }

      if (safmrMap.size > 0) {
        await upsert([...safmrMap.values()]);
        safmrRows += safmrMap.size;
        console.log(`    SAFMR ${c.county_name ?? c.fips_code}: ${safmrMap.size} ZIP rows`);
      }
    } catch (e) {
      errors++;
      console.error(`    ${fipsClean} ${c.county_name ?? ""}: ${(e as Error).message.slice(0, 100)}`);
    }
  }

  console.log(`  ${state}: ${countyRows} FMR rows, ${safmrRows} SAFMR rows, ${errors} errors`);
}

async function main() {
  console.log(`MXRE — HUD FMR + SAFMR ingest`);
  console.log(`  Year: ${YEAR} | States: ${STATES.join(", ")}`);
  await ensureTable();
  for (const s of STATES) {
    console.log(`\n[${s}]`);
    await ingestState(s);
  }
  console.log("\nDone.");
}

main().catch((e) => { console.error(e); process.exit(1); });
