/**
 * Load HMDA Reporter Panel CSVs into hmda_lender_crosswalk.
 *
 * Input: data/hmda/panels/{YEAR}_public_panel_csv.csv for YEARS below.
 * Table: hmda_lender_crosswalk (lei PK, respondent_name, respondent_name_normalized,
 *        activity_year, agency_code, assets)
 *
 * Upsert keyed by lei. On conflict we keep the row with the most recent
 * activity_year so the crosswalk always reflects the latest known name
 * and asset size for each institution.
 *
 * Run:  node scripts/load-hmda-panels.mjs
 */
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import pkg from "pg";
import { normalizeLenderName } from "./lib/normalize-lender.mjs";

const { Pool } = pkg;

const YEARS = [2018, 2019, 2020, 2021, 2022, 2023];
const PANEL_DIR = path.resolve("data/hmda/panels");
const BATCH = 500;

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 4,
  connectionTimeoutMillis: 15000,
});

// Minimal CSV splitter that respects double-quoted fields.
function splitCsv(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQ = false;
        }
      } else {
        cur += c;
      }
    } else {
      if (c === ",") {
        out.push(cur);
        cur = "";
      } else if (c === '"') {
        inQ = true;
      } else {
        cur += c;
      }
    }
  }
  out.push(cur);
  return out;
}

async function flushBatch(client, rows) {
  if (rows.length === 0) return;
  const cols = ["lei", "respondent_name", "respondent_name_normalized", "activity_year", "agency_code", "assets"];
  const values = [];
  const placeholders = [];
  rows.forEach((r, i) => {
    const base = i * cols.length;
    placeholders.push(`($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6})`);
    values.push(r.lei, r.respondent_name, r.respondent_name_normalized, r.activity_year, r.agency_code, r.assets);
  });
  const sql = `
    INSERT INTO hmda_lender_crosswalk
      (${cols.join(",")})
    VALUES ${placeholders.join(",")}
    ON CONFLICT (lei) DO UPDATE SET
      respondent_name            = EXCLUDED.respondent_name,
      respondent_name_normalized = EXCLUDED.respondent_name_normalized,
      activity_year              = EXCLUDED.activity_year,
      agency_code                = EXCLUDED.agency_code,
      assets                     = EXCLUDED.assets
    WHERE EXCLUDED.activity_year >= hmda_lender_crosswalk.activity_year
  `;
  await client.query(sql, values);
}

async function loadYear(client, year) {
  const file = path.join(PANEL_DIR, `${year}_public_panel_csv.csv`);
  if (!fs.existsSync(file)) {
    console.warn(`  SKIP ${year} (not found: ${file})`);
    return { read: 0, skipped: 0 };
  }
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  let header = null;
  let idxLei = -1;
  let idxName = -1;
  let idxYear = -1;
  let idxAgency = -1;
  let idxAssets = -1;
  let read = 0;
  let skipped = 0;
  let batch = [];
  for await (const line of rl) {
    if (!line.trim()) continue;
    if (!header) {
      header = splitCsv(line).map((h) => h.trim().toLowerCase());
      idxLei = header.indexOf("lei");
      idxName = header.indexOf("respondent_name");
      idxYear = header.indexOf("activity_year");
      idxAgency = header.indexOf("agency_code");
      idxAssets = header.indexOf("assets");
      if (idxLei < 0 || idxName < 0) {
        throw new Error(`${year}: missing lei/respondent_name in header: ${header.join(",")}`);
      }
      continue;
    }
    const f = splitCsv(line);
    const lei = (f[idxLei] || "").trim();
    const name = (f[idxName] || "").trim();
    if (!lei || lei === "-1" || !name) {
      skipped++;
      continue;
    }
    const norm = normalizeLenderName(name);
    const yr = idxYear >= 0 ? parseInt(f[idxYear], 10) || year : year;
    const agency = idxAgency >= 0 ? parseInt(f[idxAgency], 10) || null : null;
    const assetsRaw = idxAssets >= 0 ? f[idxAssets] : null;
    const assets = assetsRaw && assetsRaw !== "-1" && assetsRaw !== "" ? parseInt(assetsRaw, 10) || null : null;
    batch.push({
      lei,
      respondent_name: name,
      respondent_name_normalized: norm,
      activity_year: yr,
      agency_code: agency,
      assets,
    });
    read++;
    if (batch.length >= BATCH) {
      await flushBatch(client, batch);
      batch = [];
    }
  }
  await flushBatch(client, batch);
  return { read, skipped };
}

async function main() {
  const client = await pool.connect();
  try {
    console.log("Loading HMDA panels into hmda_lender_crosswalk...");
    const before = await client.query("SELECT COUNT(*)::int AS n FROM hmda_lender_crosswalk");
    console.log(`  rows before: ${before.rows[0].n}`);
    for (const y of YEARS) {
      console.log(`  ${y}...`);
      const r = await loadYear(client, y);
      console.log(`    read=${r.read} skipped=${r.skipped}`);
    }
    const after = await client.query("SELECT COUNT(*)::int AS n FROM hmda_lender_crosswalk");
    console.log(`  rows after: ${after.rows[0].n}`);
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
