// ryder-command-center — aggregated read-only view of the WHOLE MXRE division.
// Called by the dashboard every 5s for real-time pulse. No mutations, no LLM,
// just a single JSON blob with everything Mundo needs to see at a glance.
import { createClient } from "npm:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const MXRE_PG      = Deno.env.get("MXRE_PG_URL") ?? "";
const MXRE_SVC     = Deno.env.get("MXRE_SUPABASE_SERVICE_KEY")!;
const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

async function mxre(sql: string) {
  const res = await fetch(MXRE_PG, {
    method: "POST",
    headers: { apikey: MXRE_SVC, Authorization: `Bearer ${MXRE_SVC}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: sql }),
  });
  return res.ok ? await res.json() : [];
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  try {
    const MXRE_AGENTS = ["ryder","mxre-intel","lien-agent","listing-agent","rent-agent",
      "mxre-texas","mxre-southeast","mxre-midwest","mxre-southwest","mxre-northeast"];

    const since24h = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
    const since1h  = new Date(Date.now() - 3600 * 1000).toISOString();
    const since5m  = new Date(Date.now() - 5 * 60 * 1000).toISOString();

    const [
      agents, health, heartbeats, tasksActive, tasksAll24h,
      conversations, runs24h, citations24h,
      deals, approvals, orchLogs, federated,
      tokenUsage24h, opportunities, divisions,
      mxreVolume,
    ] = await Promise.all([
      supa.from("agents").select("id,name,role,status,model_primary,model_fallbacks,avatar_color,reports_to").in("id", MXRE_AGENTS),
      supa.from("agent_health").select("*").in("agent_id", MXRE_AGENTS),
      supa.from("agent_heartbeats").select("*").in("agent_id", MXRE_AGENTS),
      supa.from("agent_tasks").select("id,title,description,assigned_to,status,priority,created_at,claimed_at,metadata").in("assigned_to", MXRE_AGENTS).in("status", ["backlog","pending","claimed","running"]).order("created_at", { ascending: false }).limit(60),
      supa.from("agent_tasks").select("id,title,assigned_to,status,created_at,completed_at").in("assigned_to", MXRE_AGENTS).gte("created_at", since24h).order("created_at", { ascending: false }).limit(80),
      supa.from("agent_conversations").select("id,agent_id,to_agent_id,role,content,created_at").in("agent_id", MXRE_AGENTS).order("created_at", { ascending: false }).limit(20),
      supa.from("agent_runs").select("id,agent_id,status,trigger,tokens_input,tokens_output,duration_ms,started_at,shadow").in("agent_id", MXRE_AGENTS).gte("started_at", since24h).order("started_at", { ascending: false }).limit(100),
      supa.from("agent_citations").select("id,created_at").gte("created_at", since24h),
      supa.from("deals").select("id,status,address,county_fips,sourced_by_agent,purchase_price_cents,actual_profit_cents,sourced_at").order("created_at", { ascending: false }).limit(20),
      supa.from("approval_queue").select("id,requested_by,title,description,priority,created_at,status").eq("status", "pending").in("requested_by", MXRE_AGENTS).order("created_at", { ascending: false }).limit(20),
      supa.from("intel_orchestration_log").select("id,cycle_started_at,tasks_created,messages_sent,reasoning,duration_ms,pipeline_health_snapshot").order("cycle_started_at", { ascending: false }).limit(10),
      supa.from("federated_outcomes").select("pattern_key,outcome_type,win_rate,members_seen_count,median_profit_cents").order("win_rate", { ascending: false, nullsFirst: false }).limit(10),
      supa.from("agent_token_usage").select("agent_id,model,provider,input_tokens,output_tokens,cost_usd,created_at").in("agent_id", MXRE_AGENTS).gte("created_at", since24h),
      // NEW: cross-org opportunities + divisions inventory
      supa.from("opportunities").select("*").order("created_at", { ascending: false }).limit(40),
      supa.from("divisions").select("*").order("id"),
      mxre(`
        SELECT
          -- TOTALS (from pg_stat — instant, even for 98M-row tables)
          (SELECT n_live_tup::bigint FROM pg_stat_user_tables WHERE relname='properties')       AS properties_total,
          (SELECT n_live_tup::bigint FROM pg_stat_user_tables WHERE relname='hmda_lar')         AS hmda_total,
          (SELECT n_live_tup::bigint FROM pg_stat_user_tables WHERE relname='mortgage_records') AS recorder_total,
          (SELECT n_live_tup::bigint FROM pg_stat_user_tables WHERE relname='rent_snapshots')   AS rent_total,
          (SELECT n_live_tup::bigint FROM pg_stat_user_tables WHERE relname='listing_signals')  AS listing_total,
          -- VELOCITY (rows added in time windows)
          (SELECT count(*) FROM mortgage_records WHERE recording_date >= (now() - interval '1 hour')::date)::int   AS docs_1h,
          (SELECT count(*) FROM mortgage_records WHERE recording_date >= (now() - interval '24 hours')::date)::int AS docs_24h,
          (SELECT count(*) FROM mortgage_records WHERE recording_date >= (now() - interval '7 days')::date)::int   AS docs_7d,
          -- KG SCALE
          (SELECT count(*) FROM entities)::int AS entities,
          (SELECT count(*) FROM entity_relationships)::int AS relationships,
          -- COVERAGE
          (SELECT count(DISTINCT county_fips) FROM mortgage_records WHERE county_fips IS NOT NULL)::int AS counties_covered
      `),
    ]);

    // Compute derived metrics
    const runsByStatus = { succeeded: 0, failed: 0, running: 0, killed: 0, shadow: 0 };
    const runsByAgent: Record<string, number> = {};
    let runsTokens = 0;
    for (const r of runs24h.data ?? []) {
      runsByStatus[r.status as keyof typeof runsByStatus] = (runsByStatus[r.status as keyof typeof runsByStatus] ?? 0) + 1;
      runsByAgent[r.agent_id] = (runsByAgent[r.agent_id] ?? 0) + 1;
      runsTokens += (r.tokens_input ?? 0) + (r.tokens_output ?? 0);
    }

    const tasksByAgent: Record<string, { pending: number; claimed: number; running: number; backlog: number }> = {};
    for (const t of tasksActive.data ?? []) {
      if (!tasksByAgent[t.assigned_to]) tasksByAgent[t.assigned_to] = { pending: 0, claimed: 0, running: 0, backlog: 0 };
      (tasksByAgent[t.assigned_to] as any)[t.status] = ((tasksByAgent[t.assigned_to] as any)[t.status] ?? 0) + 1;
    }

    // Kanban columns for the division — grouped by status with stuck detection
    const STUCK_MS = 15 * 60 * 1000;
    const kanban: Record<string, any[]> = { backlog: [], in_progress: [], stuck: [], completed: [] };
    for (const t of tasksActive.data ?? []) {
      const card = { id: t.id, title: t.title, description: t.description, assigned_to: t.assigned_to, status: t.status, priority: t.priority, created_at: t.created_at, claimed_at: t.claimed_at, keepalive: (t.metadata as any)?.keepalive === true };
      if (t.status === "backlog" || t.status === "pending") {
        kanban.backlog.push(card);
      } else if (t.status === "claimed" || t.status === "running") {
        const claimedAge = t.claimed_at ? Date.now() - new Date(t.claimed_at).getTime() : 0;
        if (claimedAge > STUCK_MS) kanban.stuck.push(card);
        else kanban.in_progress.push(card);
      }
    }
    for (const t of (tasksAll24h.data ?? []).filter(t => t.status === "completed").slice(0, 30)) {
      kanban.completed.push({ id: t.id, title: t.title, assigned_to: t.assigned_to, status: t.status, completed_at: t.completed_at });
    }

    // Token burn by model + by agent (aggregated across all sources: agent_token_usage + agent_runs)
    const tokensByModel: Record<string, { input: number; output: number; cost_usd: number; calls: number }> = {};
    const tokensByAgent: Record<string, { input: number; output: number; cost_usd: number; calls: number }> = {};
    for (const u of tokenUsage24h.data ?? []) {
      const m = u.model ?? "unknown";
      if (!tokensByModel[m]) tokensByModel[m] = { input: 0, output: 0, cost_usd: 0, calls: 0 };
      tokensByModel[m].input  += u.input_tokens  ?? 0;
      tokensByModel[m].output += u.output_tokens ?? 0;
      tokensByModel[m].cost_usd += Number(u.cost_usd ?? 0);
      tokensByModel[m].calls  += 1;
      const a = u.agent_id ?? "unknown";
      if (!tokensByAgent[a]) tokensByAgent[a] = { input: 0, output: 0, cost_usd: 0, calls: 0 };
      tokensByAgent[a].input  += u.input_tokens  ?? 0;
      tokensByAgent[a].output += u.output_tokens ?? 0;
      tokensByAgent[a].cost_usd += Number(u.cost_usd ?? 0);
      tokensByAgent[a].calls  += 1;
    }
    // Also fold in tokens from agent_runs (worker writes here)
    for (const r of runs24h.data ?? []) {
      const agent = (agents.data ?? []).find((a: any) => a.id === r.agent_id);
      const m = agent?.model_primary ?? "unknown";
      if (!tokensByModel[m]) tokensByModel[m] = { input: 0, output: 0, cost_usd: 0, calls: 0 };
      tokensByModel[m].input  += r.tokens_input  ?? 0;
      tokensByModel[m].output += r.tokens_output ?? 0;
      tokensByModel[m].calls  += 1;
      if (!tokensByAgent[r.agent_id]) tokensByAgent[r.agent_id] = { input: 0, output: 0, cost_usd: 0, calls: 0 };
      tokensByAgent[r.agent_id].input  += r.tokens_input  ?? 0;
      tokensByAgent[r.agent_id].output += r.tokens_output ?? 0;
      tokensByAgent[r.agent_id].calls  += 1;
    }
    const totalTokensAllSources = Object.values(tokensByModel).reduce((s, m) => s + m.input + m.output, 0);

    const dealStats = {
      prospects:  deals.data?.filter(d => d.status === "prospect").length ?? 0,
      contracted: deals.data?.filter(d => d.status === "contracted").length ?? 0,
      closed:     deals.data?.filter(d => d.status === "closed").length ?? 0,
      attributed_profit_cents: deals.data?.filter(d => d.status === "closed")
        .reduce((s, d) => s + (d.actual_profit_cents ?? 0), 0) ?? 0,
    };

    const lastOrch = orchLogs.data?.[0];
    const nextOrchDue = lastOrch
      ? new Date(new Date(lastOrch.cycle_started_at).getTime() + 5 * 60 * 1000).toISOString()
      : null;

    const breakersTripped = (health.data ?? []).filter(h => h.breaker_state === "open");
    const agentsWorking   = (heartbeats.data ?? []).filter(h => h.status === "working").length;
    const agentsIdle      = (heartbeats.data ?? []).filter(h => h.status === "idle").length;

    return new Response(JSON.stringify({
      generated_at: new Date().toISOString(),
      summary: {
        agents_total: agents.data?.length ?? 0,
        agents_working: agentsWorking,
        agents_idle: agentsIdle,
        breakers_tripped: breakersTripped.length,
        tasks_active_total: tasksActive.data?.length ?? 0,
        tasks_completed_24h: (tasksAll24h.data ?? []).filter(t => t.status === "completed").length,
        runs_24h_total: runs24h.data?.length ?? 0,
        runs_24h_succeeded: runsByStatus.succeeded,
        runs_24h_failed: runsByStatus.failed,
        tokens_24h: totalTokensAllSources,
        citations_24h: citations24h.data?.length ?? 0,
        conversations_24h: conversations.data?.length ?? 0,
        approvals_pending: approvals.data?.length ?? 0,
        next_orchestration_at: nextOrchDue,
        last_orchestration: lastOrch ? {
          at: lastOrch.cycle_started_at,
          tasks_created: lastOrch.tasks_created,
          messages_sent: lastOrch.messages_sent,
          reasoning: lastOrch.reasoning,
          duration_ms: lastOrch.duration_ms,
        } : null,
      },
      mxre_pipeline: mxreVolume?.[0] ?? {},
      agents: agents.data ?? [],
      health: health.data ?? [],
      heartbeats: heartbeats.data ?? [],
      runs_by_agent: runsByAgent,
      runs_by_status: runsByStatus,
      tasks_by_agent: tasksByAgent,
      tasks_active: tasksActive.data ?? [],
      recent_conversations: conversations.data ?? [],
      recent_runs: (runs24h.data ?? []).slice(0, 20),
      deals_recent: deals.data ?? [],
      deals_stats: dealStats,
      approvals_pending: approvals.data ?? [],
      orchestration_log: orchLogs.data ?? [],
      federated_patterns: federated.data ?? [],
      breakers_tripped: breakersTripped,
      // New: kanban + token breakdowns
      kanban,
      tokens_by_model: tokensByModel,
      tokens_by_agent: tokensByAgent,
      // Cross-org
      opportunities: opportunities.data ?? [],
      divisions: divisions.data ?? [],
    }), { headers: { ...cors, "Content-Type": "application/json" } });
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err.message ?? String(err) }),
      { status: 500, headers: { ...cors, "Content-Type": "application/json" } });
  }
});
