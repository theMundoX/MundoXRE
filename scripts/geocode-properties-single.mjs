#!/usr/bin/env node
/**
 * GEOCODER v2 — Census single-address endpoint with parallel workers.
 *
 * Uses the JSON endpoint:
 *   https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=...&benchmark=Public_AR_Current&format=json
 *
 * This avoids the multipart-batch endpoint that rejected our Node form data.
 *
 * Strategy:
 *   - Pull a batch of N pending properties (latitude IS NULL, address NOT NULL)
 *   - Run M parallel workers, each fetching one address per HTTP call
 *   - Apply matches via bulk UPDATE (in batches of 100 to amortize round trips)
 *   - Repeat until no more pending
 *
 * Throughput target: ~100-200 addresses/sec at 20-30 workers.
 *
 * Usage:
 *   node scripts/geocode-properties-single.mjs                # run continuously
 *   node scripts/geocode-properties-single.mjs --max 50000    # cap rows
 *   WORKERS=30 node scripts/geocode-properties-single.mjs
 */
import pkg from "pg";
const { Pool } = pkg;

const WORKERS = parseInt(process.env.WORKERS || "20");
const FETCH_BATCH = 500; // pull this many pending props per DB query
const APPLY_BATCH = 100; // bulk-UPDATE this many results at once
const MAX_ARG = process.argv.indexOf("--max");
const MAX_ROWS = MAX_ARG >= 0 ? parseInt(process.argv[MAX_ARG + 1]) : Infinity;

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 4,
  statement_timeout: 60000,
  keepAlive: true,
});

const STATS = {
  pulled: 0,
  matched: 0,
  no_match: 0,
  errors: 0,
  applied: 0,
  start: Date.now(),
};

function buildAddressString(row) {
  const parts = [];
  if (row.address) parts.push(row.address);
  if (row.city) parts.push(row.city);
  if (row.state_code) parts.push(row.state_code);
  if (row.zip) parts.push(row.zip);
  return parts.join(", ").replace(/[^\x20-\x7E]/g, " ").replace(/\s+/g, " ").trim();
}

async function geocodeOne(addr) {
  const url = `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=${encodeURIComponent(addr)}&benchmark=Public_AR_Current&format=json`;
  const r = await fetch(url, {
    headers: { "User-Agent": "MXRE/1.0", Accept: "application/json" },
    signal: AbortSignal.timeout(20_000),
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  const m = j.result?.addressMatches?.[0];
  if (!m) return null;
  return { lat: m.coordinates.y, lng: m.coordinates.x };
}

const queue = [];
const results = [];
let queueDoneFn = () => {};
let queueDonePromise = null;

async function dbFetcher() {
  while (STATS.pulled < MAX_ROWS) {
    const c = await pool.connect();
    try {
      const r = await c.query(
        `SELECT id, address, city, state_code, zip
           FROM properties
          WHERE latitude IS NULL
            AND address IS NOT NULL
            AND state_code IS NOT NULL
          LIMIT $1`,
        [FETCH_BATCH]
      );
      if (!r.rows.length) {
        console.log("[fetcher] no more pending rows");
        return;
      }
      for (const row of r.rows) queue.push(row);
      STATS.pulled += r.rows.length;
    } finally {
      c.release();
    }
    // Wait until queue drains a bit before pulling more
    while (queue.length > FETCH_BATCH * 0.5 && STATS.pulled < MAX_ROWS) {
      await new Promise((r) => setTimeout(r, 250));
    }
  }
}

async function dbApplier() {
  while (true) {
    if (results.length >= APPLY_BATCH || (results.length > 0 && STATS.pulled >= MAX_ROWS)) {
      const batch = results.splice(0, APPLY_BATCH);
      const c = await pool.connect();
      try {
        const values = batch.map((_, i) => `($${i * 3 + 1}::int, $${i * 3 + 2}::numeric, $${i * 3 + 3}::numeric)`).join(",");
        const params = [];
        for (const m of batch) {
          params.push(m.id, m.lat, m.lng);
        }
        await c.query(
          `UPDATE properties SET latitude = v.lat, longitude = v.lng
             FROM (VALUES ${values}) AS v(id, lat, lng)
            WHERE properties.id = v.id`,
          params
        );
        STATS.applied += batch.length;
      } catch (e) {
        console.error(`[applier] ${e.message}`);
      } finally {
        c.release();
      }
    } else {
      await new Promise((r) => setTimeout(r, 250));
      // exit when no more work coming
      if (STATS.pulled >= MAX_ROWS && queue.length === 0 && results.length === 0) {
        return;
      }
    }
  }
}

async function worker(id) {
  while (true) {
    const row = queue.shift();
    if (!row) {
      // Wait for more work or exit
      await new Promise((r) => setTimeout(r, 500));
      if (STATS.pulled >= MAX_ROWS && queue.length === 0) return;
      continue;
    }
    const addr = buildAddressString(row);
    if (!addr) {
      STATS.no_match++;
      continue;
    }
    try {
      const m = await geocodeOne(addr);
      if (m) {
        results.push({ id: row.id, lat: m.lat, lng: m.lng });
        STATS.matched++;
      } else {
        STATS.no_match++;
      }
    } catch (e) {
      STATS.errors++;
    }
  }
}

function statusLog() {
  const setIv = setInterval(() => {
    const elapsed = (Date.now() - STATS.start) / 1000;
    const rate = (STATS.matched / elapsed).toFixed(0);
    console.log(
      `[status] pulled=${STATS.pulled} queue=${queue.length} matched=${STATS.matched} no_match=${STATS.no_match} errors=${STATS.errors} applied=${STATS.applied} ${rate}/s ${elapsed.toFixed(0)}s elapsed`
    );
  }, 10_000);
  return () => clearInterval(setIv);
}

async function main() {
  console.log(`Single-address geocoder: workers=${WORKERS} max=${MAX_ROWS === Infinity ? "∞" : MAX_ROWS}`);
  const stopStatus = statusLog();

  // Skip the COUNT(*) — too slow on 66.8M rows without an index. Just start working.

  const workers = Array.from({ length: WORKERS }, (_, i) => worker(i + 1));
  const fetcher = dbFetcher();
  const applier = dbApplier();

  await fetcher;
  console.log("[fetcher] done");
  await Promise.all(workers);
  console.log("[workers] done");
  await applier;
  console.log("[applier] done");
  stopStatus();

  console.log(`\n=== DONE ===\n${JSON.stringify(STATS, null, 2)}`);
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  pool.end().catch(() => {});
  process.exit(1);
});
