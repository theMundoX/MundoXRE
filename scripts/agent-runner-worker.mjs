#!/usr/bin/env node
/**
 * MXRE Agent Runner Worker — SELF-HOSTED, LOCAL OLLAMA ONLY.
 *
 * Runs on the box that has Ollama (3090 / VPS / laptop). Zero paid APIs.
 *
 * Architecture:
 *   Supabase Edge Function queues a row in `agent_runs` (status='pending').
 *   This worker subscribes via Supabase Realtime → sees new rows → runs the
 *   ReAct loop against local Ollama → writes results back to Supabase.
 *
 *   Event bus on the MXRE side (pg_notify 'mxre_events') also routes into
 *   agent_runs via the mxre-event-bus edge function, so recorder filings
 *   trigger agents within seconds.
 *
 * Run:
 *   node scripts/agent-runner-worker.mjs
 *
 * Env needed (put in .env):
 *   VS_SUPABASE_URL             (Venture Studio project URL)
 *   VS_SUPABASE_SERVICE_ROLE_KEY
 *   MXRE_SUPABASE_URL           (self-hosted)
 *   MXRE_SUPABASE_SERVICE_KEY
 *   OLLAMA_URL                  (default: http://localhost:11434)
 */
import { config as loadEnv } from "dotenv";
import { createClient } from "@supabase/supabase-js";
import path from "node:path";
import { spawn } from "node:child_process";
import fs from "node:fs";
// Load .env.worker specifically (not the default .env which is for ingest scripts)
loadEnv({ path: path.join(process.cwd(), ".env.worker") });

// Credentials come from env ONLY. Never hardcoded. Worker runs on a trusted host
// (your VPS or 3090 box) — same trust domain as a database admin script.
const VS_URL   = process.env.VS_SUPABASE_URL;
const VS_KEY   = process.env.VS_SUPABASE_SERVICE_ROLE_KEY;
const MXRE_PG  = (process.env.MXRE_SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const MXRE_SVC = process.env.MXRE_SUPABASE_SERVICE_KEY;
const OLLAMA   = process.env.OLLAMA_URL ?? "http://localhost:11434";

// Refuse to start without a complete config — fail-closed.
for (const [k, v] of Object.entries({ VS_SUPABASE_URL: VS_URL, VS_SUPABASE_SERVICE_ROLE_KEY: VS_KEY, MXRE_SUPABASE_URL: process.env.MXRE_SUPABASE_URL, MXRE_SUPABASE_SERVICE_KEY: MXRE_SVC })) {
  if (!v) { console.error(`✗ Missing required env: ${k}`); process.exit(1); }
}
// Ollama must be local-only — refuse public endpoints.
if (!/^https?:\/\/(127\.|localhost|10\.|192\.168\.|100\.|\[?::1]?:)/.test(OLLAMA)) {
  console.error(`✗ OLLAMA_URL must be localhost / LAN / Tailscale. Got: ${OLLAMA}`);
  process.exit(1);
}

if (!VS_KEY || !MXRE_SVC) {
  console.error("Set VS_SUPABASE_SERVICE_ROLE_KEY and MXRE_SUPABASE_SERVICE_KEY in .env");
  process.exit(1);
}

const supa = createClient(VS_URL, VS_KEY, { auth: { persistSession: false } });

const MAX_ITERS = 6;
const LOOP_THRESHOLD = 3;

// ── Schema doc — pinned in every agent prompt to stop column hallucination ──
// Pulled live via `npm run schema:dump` (see scripts/dump-schema.mjs). Update
// this constant when migrations land.
const MXRE_SCHEMA_DOC = `
DATABASE: PostgreSQL 15 (self-hosted Supabase). Use Postgres syntax ONLY:
- Date math: NOW() - INTERVAL '7 days'   NOT DATE_SUB / CURDATE / GETDATE
- String concat: ||                       NOT CONCAT()
- Limit: LIMIT N                          NOT TOP N
- Booleans: true / false                  NOT 1/0

TABLES (selected columns; * for full set):

properties (98M rows)
  id integer PK, county_id integer FK→counties.id, parcel_id text,
  address text, city text, state_code char(2), zip text,
  lat numeric, lng numeric, msa text,
  property_type text, total_units int, year_built int, total_sqft int,
  is_sfr bool, is_apartment bool, is_condo bool,
  owner_name text, mgmt_company text,
  assessed_value int, market_value int, last_sale_price int, last_sale_date date,
  property_tax int, annual_tax int, tax_year int,
  bedrooms int, bathrooms numeric, total_rooms int,
  corporate_owned bool, absentee_owner bool, owner_occupied bool,
  legal_description text, subdivision text,
  created_at timestamptz, updated_at timestamptz, last_seen_at timestamptz
  NOTE: properties has NO county_fips column. To filter by FIPS, JOIN counties.

counties
  id int PK, state_fips char(2), county_fips char(3),  -- 3-digit suffix only!
  state_code char(2), county_name text, msa text, active bool
  Full FIPS = state_fips || county_fips. Marion/Indianapolis = '18' || '097' = '18097'.

mortgage_records (9M rows)
  id int PK, property_id int FK→properties.id, document_type text,
  recording_date date, loan_amount int, original_amount int,
  lender_name text, borrower_name text, document_number text,
  county_fips char(5),  -- HAS county_fips directly (5-digit full)
  document_type IN ('mortgage','lien','deed_of_trust','satisfaction','release','assignment','foreclosure','lis_pendens')
  open bool, position int, interest_rate numeric

hmda_lar (51M rows)
  id bigint PK, activity_year smallint, lei text,
  state_code char(2), county_code char(3),  -- HMDA uses split codes
  loan_amount numeric, interest_rate numeric, property_value numeric,
  action_taken smallint, loan_type smallint, loan_purpose smallint

rent_snapshots (13M rows)
  id int PK, property_id int FK, observed_at date,
  beds int, baths int, sqft int, asking_rent int, effective_rent int

listing_signals
  id bigint PK, property_id bigint FK, address text, city text, state_code char(2),
  is_on_market bool, mls_list_price int, days_on_market int,
  listing_source text, first_seen_at timestamptz, delisted_at timestamptz

entities / entity_relationships (knowledge graph)
  entities: id text PK, entity_type text, name text, aliases text[]
  entity_relationships: from_entity, to_entity, relationship_type, property_id text, county_fips text

COMMON PATTERNS:
-- All Indianapolis (Marion County) properties:
SELECT p.* FROM properties p JOIN counties c ON p.county_id=c.id
WHERE c.state_fips='18' AND c.county_fips='097' LIMIT 100;

-- Recent foreclosures in a FIPS:
SELECT * FROM mortgage_records
WHERE county_fips='18097' AND document_type='foreclosure'
  AND recording_date >= NOW() - INTERVAL '30 days'
ORDER BY recording_date DESC LIMIT 50;

-- Property count by county for a state:
SELECT c.county_name, count(p.id) FROM counties c
LEFT JOIN properties p ON p.county_id=c.id
WHERE c.state_fips='18' GROUP BY c.county_name ORDER BY 2 DESC;
`.trim();

// ── Tools ────────────────────────────────────────────────────────────────────

async function toolQueryMxre(sql) {
  const clean = sql.trim().toLowerCase();
  if (!clean.startsWith("select") && !clean.startsWith("with")) throw new Error("SELECT/WITH only");
  const res = await fetch(MXRE_PG, {
    method: "POST",
    headers: { apikey: MXRE_SVC, Authorization: `Bearer ${MXRE_SVC}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: sql }),
  });
  if (!res.ok) throw new Error(`mxre ${res.status}: ${await res.text()}`);
  return await res.json();
}

async function toolPostMessage(from, to, content, run_id) {
  const { data: perm } = await supa.from("agent_permissions")
    .select("allowed").eq("from_agent", from).eq("to_agent", to).eq("capability", "message").maybeSingle();
  if (!perm?.allowed) throw new Error(`${from} → ${to} message not permitted`);
  const { data, error } = await supa.from("agent_conversations").insert({
    agent_id: from, to_agent_id: to, role: "agent", content, run_id,
  }).select().single();
  if (error) throw error;
  return { message_id: data.id };
}

async function toolCreateTask(from, to, title, description) {
  const { data: perm } = await supa.from("agent_permissions")
    .select("allowed").eq("from_agent", from).eq("to_agent", to).eq("capability", "assign_task").maybeSingle();
  if (!perm?.allowed) throw new Error(`${from} → ${to} assign_task not permitted`);
  const { data, error } = await supa.from("agent_tasks").insert({
    title, description, assigned_to: to, assigned_by: from, status: "pending",
  }).select().single();
  if (error) throw error;
  return { task_id: data.id };
}

// run_ingest — agents (Ryder) can spawn the actual scraping pipeline.
// Resolves county_fips → county/state via the counties table, then spawns
// `tsx scripts/ingest-county.ts --county=NAME --state=ST`. Returns a job id
// the agent can later check status on.
//
// Concurrency: max INGEST_CONCURRENCY simultaneous jobs (default 2). Beyond
// that, returns 'queued' status — the daemon will dequeue.
const ingestJobs = new Map();  // jobId → { pid, status, fips, county, state, started, log }
const INGEST_CONCURRENCY = Number(process.env.INGEST_CONCURRENCY ?? 2);
const INGEST_LOG_DIR = path.join(process.cwd(), "logs", "agent-ingest");
fs.mkdirSync(INGEST_LOG_DIR, { recursive: true });

async function resolveFips(county_fips) {
  if (!/^\d{5}$/.test(county_fips)) throw new Error(`county_fips must be 5 digits, got: ${county_fips}`);
  const stateFips = county_fips.slice(0, 2);
  const countyFips = county_fips.slice(2);
  const rows = await toolQueryMxre(
    `SELECT county_name, state_code FROM counties WHERE state_fips='${stateFips}' AND county_fips='${countyFips}' LIMIT 1`,
  );
  if (!rows?.length) throw new Error(`unknown FIPS: ${county_fips}`);
  return { county: rows[0].county_name, state: rows[0].state_code };
}

async function toolRunIngest(from, args) {
  const { county_fips, mode = "full" } = args ?? {};
  if (!county_fips) throw new Error("run_ingest requires county_fips");

  // Permission gate — head-of-division agents only by default.
  const { data: perm } = await supa.from("agent_permissions")
    .select("allowed").eq("from_agent", from).eq("to_agent", "system").eq("capability", "run_ingest").maybeSingle();
  if (!perm?.allowed) throw new Error(`${from} not permitted to run_ingest (need agent_permissions row from→system capability=run_ingest)`);

  // Concurrency check
  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) {
    return { status: "rejected", reason: `at concurrency cap ${INGEST_CONCURRENCY}; try later`, active };
  }

  const { county, state } = await resolveFips(county_fips);
  const jobId = `${county_fips}-${Date.now().toString(36)}`;
  const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const args2 = ["scripts/ingest-county.ts", `--county=${county}`, `--state=${state}`];
  if (mode === "dry") args2.push("--dry-run");

  // Windows: `spawn('npx', ...)` fails with ENOENT because npx is a .cmd shim,
  // not an .exe. Use shell: true so Windows resolves through cmd.exe; on Linux
  // this is a no-op since `npx` resolves via PATH normally.
  const child = spawn("npx", ["tsx", ...args2], {
    cwd: process.cwd(),
    detached: false,
    stdio: ["ignore", logFd, logFd],
    env: process.env,
    shell: true,
  });

  // CRITICAL: error handler on the child must be attached BEFORE the process
  // can fail. Without this an ENOENT (binary not found) becomes an unhandled
  // 'error' event and crashes the WORKER itself, not just the child.
  child.on("error", (err) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
    console.error(`  ✗ run_ingest spawn failed for ${county_fips}: ${err.message}`);
  });

  ingestJobs.set(jobId, {
    pid: child.pid, status: "running", fips: county_fips, county, state,
    started: Date.now(), log: logPath, started_by: from,
  });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
    console.log(`  • ingest job ${jobId} exited code ${code}`);
  });

  // Persist to managed Supabase so Command Center can show it
  await supa.from("agent_tasks").insert({
    title: `Ingest ${county} County, ${state} (FIPS ${county_fips})`,
    description: `run_ingest spawned by ${from}. Job ${jobId}, PID ${child.pid}, log: ${logPath}`,
    assigned_to: from, assigned_by: from,
    status: "running",
    metadata: { county_fips, county, state, job_id: jobId, pid: child.pid, kind: "ingest" },
  });

  return { status: "started", job_id: jobId, pid: child.pid, county, state, log: logPath };
}

// run_enrich — fill missing owner/value/property_use for a county from its
// public GIS/assessor source. Currently handles Marion County (18097) via
// xmaps.indy.gov ArcGIS. Additional counties can be added as new scripts land.
async function toolRunEnrich(from, args) {
  const { county_fips, source } = args ?? {};
  if (!county_fips) throw new Error("run_enrich requires county_fips");

  const ENRICH_SCRIPTS = {
    "18097": { script: "scripts/enrich-marion-arcgis.ts", extra: source ? [`--source=${source}`] : [] },
  };

  const cfg = ENRICH_SCRIPTS[county_fips];
  if (!cfg) return { status: "unsupported", reason: `No enrichment script for FIPS ${county_fips}` };

  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) return { status: "rejected", reason: "at concurrency cap" };

  const jobId = `enrich-${county_fips}-${Date.now().toString(36)}`;
  const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const child = spawn("npx", ["tsx", cfg.script, ...cfg.extra], {
    cwd: process.cwd(), detached: false,
    stdio: ["ignore", logFd, logFd], env: process.env, shell: true,
  });
  child.on("error", (err) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });
  ingestJobs.set(jobId, { pid: child.pid, status: "running", fips: county_fips, started: Date.now(), log: logPath, started_by: from });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
    console.log(`  • enrich job ${jobId} exited code ${code}`);
  });

  await supa.from("agent_tasks").insert({
    title: `Enrich properties for FIPS ${county_fips}`,
    description: `run_enrich spawned by ${from}. Job ${jobId}, log: ${logPath}`,
    assigned_to: from, assigned_by: from, status: "running",
    metadata: { county_fips, job_id: jobId, pid: child.pid, kind: "enrich" },
  });

  return { status: "started", job_id: jobId, pid: child.pid, log: logPath };
}

// run_refresh — refresh market-rate rent baselines (ACS, HUD FMR/SAFMR) for
// a state. Keeps the rent estimate cards in the UI current.
// type: "acs" | "hud" | "all" (default "all")
// states: comma-separated 2-letter abbreviations, e.g. "IN,OH"
async function toolRunRefresh(from, args) {
  const { type = "all", states } = args ?? {};
  if (!states) throw new Error("run_refresh requires states (e.g. 'IN' or 'IN,OH')");

  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) return { status: "rejected", reason: "at concurrency cap" };

  const jobs = [];

  const spawnRefresh = (label, scriptArgs) => {
    const jobId = `refresh-${label}-${Date.now().toString(36)}`;
    const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
    const logFd = fs.openSync(logPath, "a");
    const child = spawn("npx", ["tsx", ...scriptArgs], {
      cwd: process.cwd(), detached: false,
      stdio: ["ignore", logFd, logFd], env: process.env, shell: true,
    });
    child.on("error", (err) => {
      const job = ingestJobs.get(jobId);
      if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
      try { fs.closeSync(logFd); } catch {}
    });
    ingestJobs.set(jobId, { pid: child.pid, status: "running", started: Date.now(), log: logPath, started_by: from });
    child.on("exit", (code) => {
      const job = ingestJobs.get(jobId);
      if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
      try { fs.closeSync(logFd); } catch {}
      console.log(`  • refresh job ${jobId} exited code ${code}`);
    });
    jobs.push({ job_id: jobId, pid: child.pid, log: logPath });
    return jobId;
  };

  if (type === "acs" || type === "all") {
    spawnRefresh(`acs-${states}`, ["scripts/ingest-rent-baselines-acs.ts", `--states=${states}`]);
  }
  if (type === "hud" || type === "all") {
    spawnRefresh(`hud-${states}`, ["scripts/ingest-hud-fmr.ts", `--states=${states}`]);
  }

  await supa.from("agent_tasks").insert({
    title: `Refresh rent baselines — ${type.toUpperCase()} — ${states}`,
    description: `run_refresh spawned by ${from}. ${jobs.length} job(s).`,
    assigned_to: from, assigned_by: from, status: "running",
    metadata: { type, states, jobs, kind: "refresh" },
  });

  return { status: "started", jobs };
}

// run_sdf — ingest Indiana DLGF Sales Disclosure Form deed transfer records.
// Downloads bulk ZIP(s) from stats.indiana.edu and upserts into mortgage_records
// as document_type='deed'. Covers all 92 Indiana counties.
// args: { state="IN", from_year?, to_year?, county? }
//   state: 2-letter code (only "IN" supported today; future-proof)
//   from_year / to_year: default current year for routine updates; 2008-2025 for back-fill
//   county: optional county name filter (default: all counties in state)
async function toolRunSdf(from, args) {
  const { state = "IN", from_year, to_year, county } = args ?? {};

  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) return { status: "rejected", reason: "at concurrency cap" };

  const jobId = `sdf-${state}-${Date.now().toString(36)}`;
  const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const scriptArgs = ["tsx", "scripts/ingest-dlgf-sdf.ts"];
  if (!county) scriptArgs.push("--all-counties");
  else scriptArgs.push(`--county=${county}`);
  if (from_year) scriptArgs.push(`--from-year=${from_year}`);
  if (to_year)   scriptArgs.push(`--to-year=${to_year}`);

  const child = spawn("npx", scriptArgs, { stdio: ["ignore", logFd, logFd], cwd: process.cwd() });
  child.on("error", (err) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });
  ingestJobs.set(jobId, { pid: child.pid, status: "running", state, started: Date.now(), log: logPath, started_by: from });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });

  await supa.from("agent_tasks").insert({
    title: `DLGF SDF deed ingest — ${state}${county ? ` / ${county}` : " / all counties"}${from_year ? ` (${from_year}–${to_year ?? "present"})` : ""}`,
    description: `run_sdf spawned by ${from}. Job ${jobId}, log: ${logPath}`,
    assigned_to: from, assigned_by: from, status: "running",
    metadata: { state, from_year, to_year, county, job_id: jobId, pid: child.pid, kind: "sdf" },
  });

  return { status: "started", job_id: jobId, pid: child.pid, log: logPath };
}

// run_investor_liens — search Fidlar DirectSearch by business name for every
// corporate-owned Marion County property. Fills mortgage/lien history gaps that
// the 200-result date-range cap leaves behind. No cost. Runs incrementally.
// args: { limit?, from_year?, to_year?, name? }
async function toolRunInvestorLiens(from, args) {
  const { limit, from_year, to_year, name } = args ?? {};

  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) return { status: "rejected", reason: "at concurrency cap" };

  const jobId = `investor-liens-${Date.now().toString(36)}`;
  const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const scriptArgs = ["tsx", "scripts/fidlar-investor-lien-search.ts"];
  if (limit)     scriptArgs.push(`--limit=${limit}`);
  if (from_year) scriptArgs.push(`--from-year=${from_year}`);
  if (to_year)   scriptArgs.push(`--to-year=${to_year}`);
  if (name)      scriptArgs.push(`--name=${name}`);

  const child = spawn("npx", scriptArgs, { stdio: ["ignore", logFd, logFd], cwd: process.cwd() });
  child.on("error", (err) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });
  ingestJobs.set(jobId, { pid: child.pid, status: "running", started: Date.now(), log: logPath, started_by: from });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });

  await supa.from("agent_tasks").insert({
    title: `Fidlar investor lien back-fill — Marion County${name ? ` / ${name}` : ""}`,
    description: `run_investor_liens spawned by ${from}. Job ${jobId}, log: ${logPath}`,
    assigned_to: from, assigned_by: from, status: "running",
    metadata: { limit, from_year, to_year, name, job_id: jobId, pid: child.pid, kind: "investor_liens" },
  });

  return { status: "started", job_id: jobId, pid: child.pid, log: logPath };
}

// run_hmda — ingest CFPB HMDA mortgage origination data for one or more states.
// Free, no auth. Populates hmda_originations table with census-tract-level stats
// (lender market share, loan volumes, FHA/VA/conventional mix, investment %).
// args: { states?, from_year?, to_year? }
//   states: comma-separated 2-letter codes (default "IN")
async function toolRunHmda(from, args) {
  const { states = "IN", from_year, to_year } = args ?? {};

  const active = [...ingestJobs.values()].filter(j => j.status === "running").length;
  if (active >= INGEST_CONCURRENCY) return { status: "rejected", reason: "at concurrency cap" };

  const jobId = `hmda-${states.replace(/,/g, "-")}-${Date.now().toString(36)}`;
  const logPath = path.join(INGEST_LOG_DIR, `${jobId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const scriptArgs = ["tsx", "scripts/ingest-hmda.ts", `--states=${states}`];
  if (from_year) scriptArgs.push(`--from-year=${from_year}`);
  if (to_year)   scriptArgs.push(`--to-year=${to_year}`);

  const child = spawn("npx", scriptArgs, { stdio: ["ignore", logFd, logFd], cwd: process.cwd() });
  child.on("error", (err) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = "failed"; job.spawn_error = err.message; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });
  ingestJobs.set(jobId, { pid: child.pid, status: "running", started: Date.now(), log: logPath, started_by: from });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    try { fs.closeSync(logFd); } catch {}
  });

  await supa.from("agent_tasks").insert({
    title: `HMDA originations ingest — ${states}${from_year ? ` (${from_year}–${to_year ?? "present"})` : ""}`,
    description: `run_hmda spawned by ${from}. Job ${jobId}, log: ${logPath}`,
    assigned_to: from, assigned_by: from, status: "running",
    metadata: { states, from_year, to_year, job_id: jobId, pid: child.pid, kind: "hmda" },
  });

  return { status: "started", job_id: jobId, pid: child.pid, log: logPath };
}

// ── Nightly data freshness schedule ─────────────────────────────────────────
// Runs without any agent needing to ask. Keeps rent baselines and Marion
// enrichment current. Fires once per day around 2am local time.

let lastNightlyRun = null;

function scheduleNightly() {
  setInterval(() => {
    const now = new Date();
    const hour = now.getHours();
    const dateStr = now.toDateString();
    if (hour === 2 && lastNightlyRun !== dateStr) {
      lastNightlyRun = dateStr;
      console.log(`[nightly] starting scheduled refresh at ${now.toISOString()}`);
      // Refresh Indiana rent baselines (ACS + HUD)
      toolRunRefresh("system:scheduler", { type: "all", states: "IN" })
        .then(r => console.log("[nightly] refresh jobs started:", JSON.stringify(r)))
        .catch(e => console.error("[nightly] refresh error:", e.message));
      // Re-enrich Marion County (picks up any new parcels with null owner_name)
      toolRunEnrich("system:scheduler", { county_fips: "18097" })
        .then(r => console.log("[nightly] enrich job started:", JSON.stringify(r)))
        .catch(e => console.error("[nightly] enrich error:", e.message));
      // Nightly investor lien back-fill: searches new corporate owners added since last run
      toolRunInvestorLiens("system:scheduler", {})
        .then(r => console.log("[nightly] investor liens started:", JSON.stringify(r)))
        .catch(e => console.error("[nightly] investor liens error:", e.message));
      // Weekly SDF update: re-ingest current year every Friday (DLGF refreshes Fridays 10am)
      if (now.getDay() === 5) {
        const curYear = now.getFullYear();
        toolRunSdf("system:scheduler", { state: "IN", from_year: curYear, to_year: curYear })
          .then(r => console.log("[nightly] SDF update started:", JSON.stringify(r)))
          .catch(e => console.error("[nightly] SDF error:", e.message));
      }
    }
  }, 60_000); // check every minute
}

// ── Circuit breaker ──────────────────────────────────────────────────────────

async function checkBreaker(agent_id) {
  const { data } = await supa.from("agent_health").select("*").eq("agent_id", agent_id).maybeSingle();
  return { open: data?.breaker_state === "open", data };
}

async function recordFailure(agent_id, reason) {
  const { data } = await supa.from("agent_health").select("*").eq("agent_id", agent_id).maybeSingle();
  const consecutive = (data?.consecutive_failures ?? 0) + 1;
  const threshold = data?.auto_halt_on_failures ?? 3;
  const tripped = consecutive >= threshold;
  await supa.from("agent_health").upsert({
    agent_id, consecutive_failures: consecutive,
    total_failures_24h: (data?.total_failures_24h ?? 0) + 1,
    last_failure_at: new Date().toISOString(),
    last_failure_reason: String(reason).slice(0, 500),
    breaker_state: tripped ? "open" : (data?.breaker_state ?? "closed"),
    tripped_at: tripped ? new Date().toISOString() : data?.tripped_at,
    tripped_by: tripped ? "system" : data?.tripped_by,
    updated_at: new Date().toISOString(),
  });
  if (tripped && data?.escalate_to) {
    await supa.from("agent_conversations").insert({
      agent_id: data.escalate_to, role: "system",
      content: `⚠️ Breaker tripped on ${agent_id}: ${String(reason).slice(0, 300)}`,
    });
  }
}

async function recordSuccess(agent_id) {
  await supa.from("agent_health")
    .update({ consecutive_failures: 0, updated_at: new Date().toISOString() })
    .eq("agent_id", agent_id);
}

// ── Ollama ───────────────────────────────────────────────────────────────────

// Robust JSON tool-call extraction. Models wrap JSON in ```json fences,
// emit trailing prose, or pad with commentary. Strategy:
//  1. Strip markdown fences
//  2. Walk forward from each '{' and use depth tracking to find the matching '}'
//  3. Return the first balanced JSON object that parses AND has a recognized shape.
function parseToolCall(text) {
  if (!text) return null;
  // Strip ```json ... ``` and ``` ... ``` fences if present
  const fenceMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidates = [];
  if (fenceMatch) candidates.push(fenceMatch[1]);
  candidates.push(text);
  for (const blob of candidates) {
    for (let i = 0; i < blob.length; i++) {
      if (blob[i] !== "{") continue;
      let depth = 0, inStr = false, esc = false;
      for (let j = i; j < blob.length; j++) {
        const ch = blob[j];
        if (esc) { esc = false; continue; }
        if (ch === "\\") { esc = true; continue; }
        if (ch === '"') { inStr = !inStr; continue; }
        if (inStr) continue;
        if (ch === "{") depth++;
        else if (ch === "}") {
          depth--;
          if (depth === 0) {
            const slice = blob.slice(i, j + 1);
            try {
              const obj = JSON.parse(slice);
              if (obj && (obj.tool || obj.done)) return obj;
            } catch {}
            break;
          }
        }
      }
    }
  }
  return null;
}

async function ollama(model, messages) {
  // keep_alive=60s — Ollama unloads the model 60s after the last request, so
  // we don't pile multiple models in memory across runs. Critical on
  // memory-tight boxes (a 32B qwen pin can eat 20GB+ for hours otherwise).
  const res = await fetch(`${OLLAMA}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model, messages, stream: false,
      keep_alive: process.env.OLLAMA_KEEP_ALIVE ?? "60s",
      options: { temperature: 0.3 },
    }),
  });
  if (!res.ok) throw new Error(`ollama ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return { content: data.message?.content ?? "", in: data.prompt_eval_count ?? 0, out: data.eval_count ?? 0 };
}

// ── Runner ───────────────────────────────────────────────────────────────────

async function executeRun(run) {
  console.log(`▶ ${run.id.slice(0, 8)} ${run.agent_id} trigger=${run.trigger}${run.shadow ? " SHADOW" : ""}`);

  const brk = await checkBreaker(run.agent_id);
  if (brk.open) {
    await supa.from("agent_runs").update({
      status: "killed", error: `breaker open: ${brk.data?.last_failure_reason ?? "unknown"}`,
      finished_at: new Date().toISOString(),
    }).eq("id", run.id);
    console.log(`  ✗ breaker open`);
    return;
  }

  const { data: agent } = await supa.from("agents").select("*").eq("id", run.agent_id).maybeSingle();
  if (!agent) {
    await supa.from("agent_runs").update({ status: "failed", error: "agent not found", finished_at: new Date().toISOString() }).eq("id", run.id);
    return;
  }

  const toolCalls = [];
  const signatures = [];
  const citations = [];
  let totalIn = 0, totalOut = 0;
  let output = "";
  let error = null;

  try {
    // 1. Pre-plan (forced for small models — pentagi supervision)
    const plan = await ollama(agent.model_primary, [{
      role: "system",
      content: `${agent.system_prompt}\n\nTrigger: ${run.trigger}\nContext: ${JSON.stringify(run.trigger_source ?? {}).slice(0, 800)}\n\nBefore acting, write a concise 2-5 sentence plan of what you will investigate. Then STOP.`,
    }]);
    totalIn += plan.in; totalOut += plan.out;
    await supa.from("agent_runs").update({ plan: plan.content }).eq("id", run.id);

    // 2. ReAct loop (simplified — one iteration to keep this worker tight)
    for (let iter = 0; iter < MAX_ITERS; iter++) {
      const msg = await ollama(agent.model_primary, [
        { role: "system", content:
`${agent.system_prompt}

${MXRE_SCHEMA_DOC}

TOOLS:
- query_mxre({sql}): SELECT-only against MXRE Postgres. Use the schema above. NEVER invent columns. NEVER use MySQL syntax (DATE_SUB, CURDATE, GETDATE, CONCAT, TOP).
- post_message({to, content}): message another agent
- create_task({assigned_to, title, description}): assign work to an agent
- run_ingest({county_fips, mode?}): spawn the ingest pipeline for a county. county_fips is 5-digit (e.g. "18097" for Marion/Indianapolis). mode "full" (default) or "dry". Use this when a county has missing/stale data. Heads-of-division only.
- run_enrich({county_fips, source?}): fill missing owner_name/market_value/property_use for a county from its public GIS source. Currently supports "18097" (Marion). source optionally filters to "in-data-harvest-parcels" or "assessor".
- run_refresh({states, type?}): refresh rent baselines (ACS + HUD FMR/SAFMR) for one or more states. states is comma-separated (e.g. "IN" or "IN,OH"). type "acs" | "hud" | "all" (default "all"). Use when rent estimate cards look stale.
- run_sdf({state, from_year?, to_year?, county?}): ingest Indiana DLGF Sales Disclosure Form deed transfer records for all counties. state="IN". Omit from_year/to_year for current year only (routine weekly update). Pass from_year=2008, to_year=2025 for full historical back-fill. Runs in background; takes 2-4 hours for full back-fill.
- run_investor_liens({limit?, from_year?, to_year?, name?}): search Fidlar DirectSearch by business name for all corporate-owned Marion County properties — fills mortgage/lien history gaps that the 200/day cap leaves. Free. Incremental (skips already-ingested docs). name= targets a single entity for spot lookups.
- run_hmda({states?, from_year?, to_year?}): ingest CFPB HMDA mortgage origination data into hmda_originations. Free, nationwide. Powers census-tract analytics (lender market share, loan volumes, FHA/VA/conventional mix). states defaults to "IN". HMDA has no property address so records are tract-level, not parcel-level.

OUTPUT FORMAT — strict JSON, one tool call per turn:
{"tool":"<name>","args":{...}}      ← to act
{"done":true,"summary":"..."}        ← when finished

Rules:
- If a query returns 0 rows for a county that should have data, BEFORE assuming "no data" check schema (the table column might be wrong) OR call run_ingest to load it.
- Quote string literals with single quotes in SQL: WHERE state_fips='18'
- Always LIMIT your queries (≤500 rows) — the DB has 100M+ rows.` },
        { role: "user", content: `Plan: ${plan.content}\n\nIteration ${iter + 1}. What's the next action?` },
      ]);
      totalIn += msg.in; totalOut += msg.out;

      let call = parseToolCall(msg.content);
      if (!call) { output += `Iter ${iter}: no parsable tool call. Raw: ${msg.content.slice(0, 300)}\n`; break; }
      if (call.done) { output += `Done: ${call.summary ?? ""}\n`; break; }

      const sig = `${call.tool}:${JSON.stringify(call.args ?? {})}`;
      if (signatures.filter(s => s === sig).length >= LOOP_THRESHOLD) throw new Error(`loop detected on ${call.tool}`);
      signatures.push(sig);

      const t0 = Date.now();
      let result;
      if (call.tool === "query_mxre") {
        const rows = await toolQueryMxre(call.args?.sql ?? "");
        result = { rows: rows.length, sample: rows.slice(0, 3) };
        if (!run.shadow) {
          citations.push({ run_id: run.id, source_table: "mxre_query",
            source_ids: [String(call.args.sql).slice(0, 200)],
            claim: plan.content.slice(0, 500) });
        }
      } else if (call.tool === "post_message" && !run.shadow) {
        result = await toolPostMessage(run.agent_id, call.args.to, call.args.content, run.id);
      } else if (call.tool === "create_task" && !run.shadow) {
        result = await toolCreateTask(run.agent_id, call.args.assigned_to, call.args.title, call.args.description);
      } else if (call.tool === "run_ingest" && !run.shadow) {
        result = await toolRunIngest(run.agent_id, call.args);
      } else if (call.tool === "run_enrich" && !run.shadow) {
        result = await toolRunEnrich(run.agent_id, call.args);
      } else if (call.tool === "run_refresh" && !run.shadow) {
        result = await toolRunRefresh(run.agent_id, call.args);
      } else if (call.tool === "run_sdf" && !run.shadow) {
        result = await toolRunSdf(run.agent_id, call.args);
      } else if (call.tool === "run_investor_liens" && !run.shadow) {
        result = await toolRunInvestorLiens(run.agent_id, call.args);
      } else if (call.tool === "run_hmda" && !run.shadow) {
        result = await toolRunHmda(run.agent_id, call.args);
      } else if (run.shadow) {
        result = { status: "shadow", would_call: call };
      } else {
        result = { error: `unknown tool: ${call.tool}` };
      }

      toolCalls.push({ tool: call.tool, args: call.args, result, ms: Date.now() - t0 });
      output += `[${call.tool}] ${JSON.stringify(result).slice(0, 200)}\n`;

      if (call.tool !== "query_mxre") break; // break after a non-read action
    }

    if (citations.length) await supa.from("agent_citations").insert(citations);
    await supa.from("agent_conversations").insert({ agent_id: run.agent_id, role: "agent", content: output, run_id: run.id });
    await supa.from("agent_heartbeats").upsert({ agent_id: run.agent_id, status: "idle",
      current_task_summary: `last run: ${run.trigger}`, last_ping: new Date().toISOString() });
    await recordSuccess(run.agent_id);
    console.log(`  ✓ ${toolCalls.length} tool calls, ${totalIn + totalOut} tokens`);
  } catch (err) {
    error = err.message ?? String(err);
    await recordFailure(run.agent_id, error);
    console.log(`  ✗ ${error}`);
  }

  await supa.from("agent_runs").update({
    finished_at: new Date().toISOString(),
    status: error ? "failed" : "succeeded",
    error, tool_calls: toolCalls, output,
    tokens_input: totalIn, tokens_output: totalOut,
    duration_ms: Date.now() - new Date(run.started_at).getTime(),
  }).eq("id", run.id);
}

// ── Main loop: subscribe to pending runs + poll for stragglers ──────────────

async function pollPending() {
  const { data } = await supa.from("agent_runs")
    .select("*").eq("status", "running")
    .is("finished_at", null)
    .lt("started_at", new Date(Date.now() - 30_000).toISOString())  // stale claim recovery
    .limit(5);
  for (const run of data ?? []) await executeRun(run);
}

async function main() {
  console.log("MXRE Agent Runner Worker");
  console.log(`  Venture Studio: ${VS_URL}`);
  console.log(`  MXRE Supabase:  ${MXRE_PG.replace("/pg/query", "")}`);
  console.log(`  Ollama:         ${OLLAMA}`);
  console.log("");

  // Realtime: react to INSERTs on agent_runs
  supa.channel("agent-runs-listener")
    .on("postgres_changes", { event: "INSERT", schema: "public", table: "agent_runs" }, async (payload) => {
      const run = payload.new;
      if (run.status === "running") await executeRun(run);
    })
    .subscribe((status) => console.log(`Realtime: ${status}`));

  // Poll every 30s as a safety net
  setInterval(pollPending, 30_000);
  pollPending();

  // Nightly data freshness schedule
  scheduleNightly();
  console.log("Nightly schedule active — rent baselines + Marion enrich will run at 2am.");
}

main().catch((e) => { console.error(e); process.exit(1); });
