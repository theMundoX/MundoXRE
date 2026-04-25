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
  const child = spawn("npx", ["tsx", ...args2], {
    cwd: process.cwd(),
    detached: false,
    stdio: ["ignore", logFd, logFd],
    env: process.env,
  });

  ingestJobs.set(jobId, {
    pid: child.pid, status: "running", fips: county_fips, county, state,
    started: Date.now(), log: logPath, started_by: from,
  });
  child.on("exit", (code) => {
    const job = ingestJobs.get(jobId);
    if (job) { job.status = code === 0 ? "succeeded" : "failed"; job.exit_code = code; job.finished = Date.now(); }
    fs.closeSync(logFd);
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

async function ollama(model, messages) {
  const res = await fetch(`${OLLAMA}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model, messages, stream: false, options: { temperature: 0.3 } }),
    // allow local calls with no timeout baked in
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

      let call;
      try { const m = msg.content.match(/\{[\s\S]*\}/); call = m ? JSON.parse(m[0]) : null; } catch { call = null; }
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
}

main().catch((e) => { console.error(e); process.exit(1); });
