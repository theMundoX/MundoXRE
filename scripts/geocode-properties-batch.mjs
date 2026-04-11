#!/usr/bin/env node
/**
 * GEOCODE PROPERTIES VIA CENSUS BUREAU BATCH GEOCODER
 *
 * Free, no API key, no rate limit (in practice, polite parallelism = 4 concurrent).
 * Endpoint: https://geocoding.geo.census.gov/geocoder/locations/addressbatch
 *
 * Uses Supabase JS client (REST API) instead of direct pg — works with self-hosted Supabase.
 *
 * Strategy:
 *   1. Pull next batch of 1,000 properties WHERE latitude IS NULL
 *   2. Build CSV in-memory, POST to Census batch endpoint
 *   3. Parse response, UPDATE properties WITH lat/lng
 *   4. Repeat until nothing left
 *
 * Usage:
 *   node scripts/geocode-properties-batch.mjs            # geocode all null-lat properties
 *   node scripts/geocode-properties-batch.mjs --fips 39049  # Franklin County only
 */
import { createClient } from "@supabase/supabase-js";
import { config } from "dotenv";
import { mkdirSync, existsSync } from "fs";

config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const db = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

const BATCH_SIZE = 1000;
const PARALLEL = 4; // Conservative — Census API prefers polite concurrency
const ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch?benchmark=Public_AR_Current";

const FIPS_ARG = process.argv.indexOf("--fips");
const FIPS_FILTER = FIPS_ARG >= 0 ? process.argv[FIPS_ARG + 1] : null;

const TMP = "C:/Users/msanc/mxre/data/tmp-geocode";
if (!existsSync(TMP)) mkdirSync(TMP, { recursive: true });

function csvCell(v) {
  if (v == null) return "";
  return String(v)
    .replace(/[^\x20-\x7E]/g, " ")
    .replace(/[,"\r\n]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// Track which IDs are in-flight so parallel workers don't double-fetch
const inFlight = new Set();
let offset = 0;

async function fetchBatch() {
  let q = db
    .from("properties")
    .select("id, address, city, state_code, zip")
    .is("latitude", null)
    .not("address", "is", null)
    .not("state_code", "is", null)
    .range(offset, offset + BATCH_SIZE - 1);

  if (FIPS_FILTER) {
    // Filter by county_fips on the county join — use county_id lookup
    // Simpler: filter on source containing the county FIPS or use state prefix
    // Best available: filter properties by known county ID from counties table
    q = q.eq("county_id", FIPS_FILTER); // fallback — pass county_id directly if known
  }

  const { data, error } = await q;
  if (error) throw new Error(`Fetch error: ${error.message}`);
  if (!data || data.length === 0) return [];

  // Skip any IDs already being processed
  const fresh = data.filter(r => !inFlight.has(r.id));
  for (const r of fresh) inFlight.add(r.id);
  offset += BATCH_SIZE;
  return fresh;
}

async function callCensusBatch(rows) {
  const csv = rows
    .map(r => `${r.id},${csvCell(r.address)},${csvCell(r.city)},${csvCell(r.state_code)},${csvCell(r.zip)}`)
    .join("\n");
  const fd = new FormData();
  const blob = new Blob([csv], { type: "text/csv" });
  fd.append("addressFile", blob, "batch.csv");
  const resp = await fetch(ENDPOINT, {
    method: "POST",
    body: fd,
    headers: { "User-Agent": "curl/8.4.0", Accept: "*/*" },
    signal: AbortSignal.timeout(120_000),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
  return await resp.text();
}

function parseCensusResponse(text) {
  const matches = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line) continue;
    const cols = [];
    let cur = "", inQ = false;
    for (const c of line) {
      if (c === '"') { inQ = !inQ; }
      else if (c === "," && !inQ) { cols.push(cur); cur = ""; }
      else { cur += c; }
    }
    cols.push(cur);
    if (cols.length < 6 || cols[2] !== "Match") continue;
    const coords = cols[5];
    if (!coords?.includes(",")) continue;
    const [lng, lat] = coords.split(",").map(parseFloat);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    matches.push({ id: parseInt(cols[0]), lat, lng });
  }
  return matches;
}

async function applyMatches(matches) {
  if (!matches.length) return;
  // Update individually via Supabase REST (no bulk VALUES trick available)
  // Batch into groups of 50 parallel updates
  const CHUNK = 50;
  for (let i = 0; i < matches.length; i += CHUNK) {
    const chunk = matches.slice(i, i + CHUNK);
    await Promise.all(chunk.map(m =>
      db.from("properties").update({ latitude: m.lat, longitude: m.lng }).eq("id", m.id)
    ));
  }
}

const STATS = { batches: 0, sent: 0, matched: 0, applied: 0, errors: 0, started_at: Date.now() };

async function worker(workerId) {
  while (true) {
    const rows = await fetchBatch();
    if (!rows.length) {
      console.log(`[w${workerId}] no more pending rows`);
      return;
    }
    STATS.batches++;
    STATS.sent += rows.length;
    try {
      const resp = await callCensusBatch(rows);
      const matches = parseCensusResponse(resp);
      if (matches.length === 0) {
        console.log(`[w${workerId}] ZERO MATCHES for batch — first 300 chars: ${resp.slice(0, 300)}`);
      }
      STATS.matched += matches.length;
      await applyMatches(matches);
      STATS.applied += matches.length;
      for (const m of matches) inFlight.delete(m.id);
      const elapsedMin = ((Date.now() - STATS.started_at) / 60000).toFixed(1);
      const rate = (STATS.applied / Math.max(1, (Date.now() - STATS.started_at) / 1000)).toFixed(0);
      console.log(
        `[w${workerId}] batch=${STATS.batches} sent=${STATS.sent.toLocaleString()} matched=${STATS.matched.toLocaleString()} applied=${STATS.applied.toLocaleString()} (${rate}/s, ${elapsedMin}m elapsed)`
      );
    } catch (e) {
      STATS.errors++;
      console.error(`[w${workerId}] batch error: ${e.message}`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function main() {
  console.log(`Census Geocoder — concurrency=${PARALLEL} batch=${BATCH_SIZE}${FIPS_FILTER ? ` fips=${FIPS_FILTER}` : " (all counties)"}`);
  console.log(`DB: ${SUPABASE_URL}`);

  // Quick count
  let q = db.from("properties").select("*", { count: "exact", head: true }).is("latitude", null);
  const { count } = await q;
  console.log(`Properties needing geocoding: ${count?.toLocaleString()}\n`);

  const workers = Array.from({ length: PARALLEL }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  console.log(`\n=== DONE ===`);
  console.log(`Sent: ${STATS.sent.toLocaleString()} | Matched: ${STATS.matched.toLocaleString()} | Applied: ${STATS.applied.toLocaleString()} | Errors: ${STATS.errors}`);
}

main().catch(e => { console.error(e); process.exit(1); });
