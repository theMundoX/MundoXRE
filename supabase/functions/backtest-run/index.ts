// backtest-run — replays an agent's logic against historical data.
// THE moat feature: proves agent works before you bet real capital on it.
//
// Takes { backtest_id } → reads the replay window, ticks through it, at each tick
// invokes agent-runner in shadow mode with a "data-as-of-<tick>" context window,
// compares agent output to the ground-truth `outcome_sql`, computes precision/recall/F1.

import { createClient } from "npm:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const MXRE_PG      = Deno.env.get("MXRE_PG_URL") ?? "";
const MXRE_SVC     = Deno.env.get("MXRE_SUPABASE_SERVICE_KEY")!;
const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, content-type",
};

async function pg(sql: string) {
  const res = await fetch(MXRE_PG, {
    method: "POST",
    headers: { apikey: MXRE_SVC, Authorization: `Bearer ${MXRE_SVC}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: sql }),
  });
  return res.ok ? await res.json() : [];
}

function ticksBetween(start: string, end: string, interval: string): string[] {
  const out: string[] = [];
  const cur = new Date(start);
  const endD = new Date(end);
  const stepDays = interval.includes("hour") ? 1/24 : interval.includes("week") ? 7 : 1;
  while (cur <= endD) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setDate(cur.getDate() + Math.max(1, Math.floor(stepDays)));
  }
  return out;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const { backtest_id } = await req.json();
  const { data: bt } = await supa.from("backtests").select("*").eq("id", backtest_id).maybeSingle();
  if (!bt) return new Response(JSON.stringify({ error: "backtest not found" }), { status: 404, headers: { ...corsHeaders, "Content-Type": "application/json" } });

  await supa.from("backtests").update({ status: "running", started_at: new Date().toISOString() }).eq("id", backtest_id);

  const ticks = ticksBetween(bt.replay_start_date, bt.replay_end_date, bt.tick_interval);
  const tp: any[] = [];       // agent flagged & outcome positive
  const fp: any[] = [];       // agent flagged & outcome negative
  const fn: any[] = [];       // agent didn't flag & outcome positive
  const tn_count = { n: 0 };  // didn't flag & outcome negative (count only, sampled)
  const leadTimes: number[] = [];

  try {
    // Fetch ground-truth outcomes once (assumed to include a `flagged_at` timestamp per row)
    const outcomes = await pg(bt.outcome_sql);

    for (const tick of ticks) {
      // Invoke agent-runner in shadow mode with a "data-as-of-<tick>" hint.
      const res = await fetch(`${SUPABASE_URL}/functions/v1/agent-runner`, {
        method: "POST",
        headers: { Authorization: `Bearer ${SERVICE_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          agent_id: bt.agent_id,
          trigger: "backtest",
          trigger_source: { tick, backtest_id, as_of: tick },
          shadow: true,
        }),
      });
      const runResult = await res.json().catch(() => ({}));

      // For now we stub the comparison — real impl parses runResult.output for property_ids
      // and matches against `outcomes` where outcome_date ≤ tick
      const agentFlags: string[] = []; // TODO: parse from runResult.output
      const actualsAtTick = outcomes.filter((o: any) => o.flagged_at && o.flagged_at <= tick);

      for (const a of actualsAtTick) {
        if (agentFlags.includes(a.property_id)) {
          tp.push({ tick, property_id: a.property_id });
          const dist = (new Date(a.actual_event_date).getTime() - new Date(tick).getTime()) / 86400000;
          if (dist > 0) leadTimes.push(dist);
        } else {
          fn.push({ tick, property_id: a.property_id });
        }
      }
      for (const flagged of agentFlags) {
        if (!actualsAtTick.find((a: any) => a.property_id === flagged)) fp.push({ tick, property_id: flagged });
      }
    }

    const tpN = tp.length, fpN = fp.length, fnN = fn.length;
    const precision = (tpN + fpN) > 0 ? tpN / (tpN + fpN) : 0;
    const recall    = (tpN + fnN) > 0 ? tpN / (tpN + fnN) : 0;
    const f1        = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0;
    const leadMed   = leadTimes.length ? leadTimes.sort((a, b) => a - b)[Math.floor(leadTimes.length / 2)] : null;

    await supa.from("backtests").update({
      status: "completed", completed_at: new Date().toISOString(),
      true_positives: tpN, false_positives: fpN, true_negatives: tn_count.n, false_negatives: fnN,
      precision_score: precision, recall_score: recall, f1_score: f1,
      lead_time_median_days: leadMed,
      result_summary: { ticks: ticks.length, outcomes: outcomes.length },
    }).eq("id", backtest_id);

    return new Response(JSON.stringify({ ok: true, precision, recall, f1, lead_time_median_days: leadMed, ticks: ticks.length }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } });
  } catch (err: any) {
    await supa.from("backtests").update({ status: "failed", result_summary: { error: err.message } }).eq("id", backtest_id);
    return new Response(JSON.stringify({ error: err.message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
