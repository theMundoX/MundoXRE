// mxre-event-bus — the event-driven dispatcher.
// Called by a webhook or worker that reads pg_notify('mxre_events', ...) from the
// MXRE Supabase. Looks up which agents subscribe to the event_type, invokes
// agent-runner for each one. This is the "new recorder filing → lien agent
// fires in seconds" pattern.
import { createClient } from "npm:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, content-type",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const { event_type, source, source_id, payload } = await req.json();
  if (!event_type) {
    return new Response(JSON.stringify({ error: "missing event_type" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }

  // 1. Persist the event
  const { data: evt } = await supa.from("agent_events").insert({
    event_type, source, source_id, payload, status: "pending",
  }).select().single();

  // 2. Find subscribed agents
  const { data: subs } = await supa.from("agent_subscriptions")
    .select("agent_id").eq("event_type", event_type).eq("enabled", true);

  const dispatched: string[] = [];
  for (const sub of subs ?? []) {
    // 3. Fire agent-runner (fire-and-forget — don't await)
    fetch(`${SUPABASE_URL}/functions/v1/agent-runner`, {
      method: "POST",
      headers: { Authorization: `Bearer ${SERVICE_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        agent_id: sub.agent_id,
        trigger: "event",
        trigger_source: { event_type, source, source_id, payload, event_id: evt?.id },
      }),
    }).catch(err => console.error(`dispatch to ${sub.agent_id}:`, err));
    dispatched.push(sub.agent_id);
  }

  // 4. Mark dispatched
  if (evt) {
    await supa.from("agent_events").update({
      dispatched_to: dispatched, status: dispatched.length ? "claimed" : "expired",
      claimed_at: new Date().toISOString(),
    }).eq("id", evt.id);
  }

  return new Response(JSON.stringify({ event_id: evt?.id, dispatched, count: dispatched.length }),
    { headers: { ...corsHeaders, "Content-Type": "application/json" } });
});
