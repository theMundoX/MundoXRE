#!/usr/bin/env node
/**
 * RATE PIPELINE ORCHESTRATOR
 *
 * Runs the full rate ingest + backfill sequence end-to-end, autonomously.
 * Intended to be launched in the background and left alone until completion.
 *
 * Sequence:
 *   1. For each HMDA year 2018-2023:
 *      - Wait for the CSV to be fully downloaded (file size stable for 30s)
 *      - Load it into hmda_lar via scripts/load-hmda-year.mjs (child process)
 *      - Verify row count landed
 *   2. After all years loaded, run scripts/backfill-rates-from-hmda.mjs --apply
 *   3. Write a coverage report to data/rate-coverage.json
 *   4. Exit
 *
 * Every major step logs to data/rate-pipeline.log and the console.
 */
import { spawn } from "child_process";
import { existsSync, statSync, appendFileSync, writeFileSync } from "fs";
import { setTimeout as sleep } from "timers/promises";
import pkg from "pg";
const { Pool } = pkg;

const YEARS = [2018, 2019, 2020, 2021, 2022, 2023];
const DATA_DIR = "C:/Users/msanc/mxre/data/hmda";
const LOG_PATH = "C:/Users/msanc/mxre/data/rate-pipeline.log";

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 2,
  statement_timeout: 0,
  connectionTimeoutMillis: 30000,
});

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  appendFileSync(LOG_PATH, line + "\n");
}

function runChild(cmd, args, label) {
  return new Promise((resolve, reject) => {
    log(`START ${label}: ${cmd} ${args.join(" ")}`);
    const child = spawn(cmd, args, { cwd: "C:/Users/msanc/mxre", shell: true });
    let lastOut = "";
    child.stdout.on("data", (d) => {
      const s = d.toString();
      lastOut = s;
      process.stdout.write(`  [${label}] ${s}`);
    });
    child.stderr.on("data", (d) => {
      process.stderr.write(`  [${label}-err] ${d}`);
    });
    child.on("exit", (code) => {
      log(`END ${label}: exit=${code}`);
      if (code === 0) resolve(lastOut);
      else reject(new Error(`${label} exited with code ${code}`));
    });
    child.on("error", (e) => reject(e));
  });
}

async function waitForStableFile(path, stableSeconds = 30, maxWaitMinutes = 240) {
  const maxMs = maxWaitMinutes * 60_000;
  const start = Date.now();
  let lastSize = -1;
  let stableSince = null;
  let lastKeepalive = Date.now();
  while (Date.now() - start < maxMs) {
    if (!existsSync(path)) {
      await sleep(10_000);
      continue;
    }
    const size = statSync(path).size;
    if (size !== lastSize) {
      lastSize = size;
      stableSince = Date.now();
      log(`  ${path} size=${(size / 1024 / 1024).toFixed(0)} MB (still growing)`);
    } else if (stableSince && Date.now() - stableSince >= stableSeconds * 1000) {
      log(`  ${path} stable at ${(size / 1024 / 1024).toFixed(0)} MB`);
      return size;
    }
    // Keepalive: ping postgres every 60s to prevent Supavisor from reaping the idle pool
    if (Date.now() - lastKeepalive >= 60_000) {
      try {
        const c = await pool.connect();
        await c.query("SELECT 1");
        c.release();
        lastKeepalive = Date.now();
      } catch (e) {
        log(`  keepalive ping failed: ${e.message}`);
      }
    }
    await sleep(15_000);
  }
  throw new Error(`timeout waiting for ${path} to stabilize`);
}

async function hmdaRowCount(year) {
  const c = await pool.connect();
  try {
    const r = await c.query(
      `SELECT COUNT(*)::bigint AS n FROM hmda_lar WHERE activity_year = $1`,
      [year]
    );
    return Number(r.rows[0].n);
  } finally {
    c.release();
  }
}

async function rateCoverage() {
  const c = await pool.connect();
  try {
    const r = await c.query(
      `SELECT
         COUNT(*) AS total,
         COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly')  AS pmms,
         COUNT(*) FILTER (WHERE rate_source = 'hmda_match')   AS hmda,
         COUNT(*) FILTER (WHERE rate_source = 'agency_match') AS agency,
         COUNT(*) FILTER (WHERE rate_source = 'estimated')    AS estimated,
         COUNT(*) FILTER (WHERE interest_rate IS NULL)        AS no_rate
       FROM mortgage_records`
    );
    return r.rows[0];
  } finally {
    c.release();
  }
}

async function loadYearIfNeeded(year) {
  const path = `${DATA_DIR}/hmda_${year}_originated.csv`;
  const existing = await hmdaRowCount(year);
  if (existing > 0) {
    log(`HMDA ${year}: already has ${existing.toLocaleString()} rows in hmda_lar, skipping load`);
    return existing;
  }
  log(`Waiting for ${path} to finish downloading...`);
  await waitForStableFile(path);
  log(`HMDA ${year}: starting load...`);
  await runChild("node", ["scripts/load-hmda-year.mjs", String(year)], `hmda-${year}`);
  const after = await hmdaRowCount(year);
  log(`HMDA ${year}: loaded ${after.toLocaleString()} rows`);
  return after;
}

async function main() {
  log("============================================================");
  log("RATE PIPELINE ORCHESTRATOR starting");
  log("============================================================");

  const startCov = await rateCoverage();
  log(`START coverage: ${JSON.stringify(startCov)}`);

  // Phase 1: load each HMDA year sequentially (download runs in parallel elsewhere)
  for (const year of YEARS) {
    try {
      await loadYearIfNeeded(year);
    } catch (e) {
      log(`ERROR loading HMDA ${year}: ${e.message}`);
      // continue to next year — partial data is still useful
    }
  }

  // Check totals in hmda_lar
  const totals = {};
  for (const y of YEARS) totals[y] = (await hmdaRowCount(y)).toLocaleString();
  log(`HMDA row counts: ${JSON.stringify(totals)}`);

  // Phase 2: run the fuzzy matcher against mortgage_records
  log("Running fuzzy matcher dry run (1000 rows) to sanity-check match rate...");
  await runChild("node", ["scripts/backfill-rates-from-hmda.mjs", "--limit", "1000"], "matcher-dry");

  log("Running FULL fuzzy matcher apply...");
  await runChild("node", ["scripts/backfill-rates-from-hmda.mjs", "--apply"], "matcher-apply");

  const endCov = await rateCoverage();
  log(`END coverage: ${JSON.stringify(endCov)}`);

  writeFileSync(
    "C:/Users/msanc/mxre/data/rate-coverage.json",
    JSON.stringify({ started_at: new Date().toISOString(), start: startCov, end: endCov }, null, 2)
  );

  log("ORCHESTRATOR COMPLETE");
  await pool.end();
}

main().catch((e) => {
  log(`FATAL: ${e.message}\n${e.stack}`);
  pool.end().catch(() => {});
  process.exit(1);
});
