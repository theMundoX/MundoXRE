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
        { role: "system", content: `${agent.system_prompt}\n\nYou have tools:\n- query_mxre(sql): SELECT against MXRE DB (tables: properties, mortgage_records, hmda_lar, rent_snapshots, listing_signals, entities, entity_relationships)\n- post_message(to, content): message another agent\n- create_task(assigned_to, title, description): assign work\n\nOutput JSON: {"tool":"<name>","args":{...}} or {"done":true,"summary":"..."}\nOne tool call per turn.` },
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
