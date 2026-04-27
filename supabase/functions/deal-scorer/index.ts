// deal-scorer — finds distressed/high-equity patterns in MXRE data and writes
// them as deals (status='prospect') + cross-division opportunities.
// Rule-based, no LLM. Runs via pg_cron every 10 min.
import { createClient } from "npm:@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const MXRE_PG      = Deno.env.get("MXRE_PG_URL") ?? "";
const MXRE_SVC     = Deno.env.get("MXRE_SUPABASE_SERVICE_KEY")!;
const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "authorization, content-type" };

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
  const t0 = Date.now();

  // PATTERN 1: Lender distress clusters — recent foreclosure/lis-pendens spikes
  const distressClusters = await mxre(`
    SELECT lender_name, county_fips,
           count(*) AS events,
           (array_agg(document_type))[1:3] AS types
    FROM mortgage_records
    WHERE recording_date >= (now() - interval '30 days')::date
      AND document_type IN ('foreclosure','lis_pendens','assignment')
      AND lender_name IS NOT NULL
      AND county_fips IS NOT NULL
    GROUP BY 1, 2
    HAVING count(*) >= 5
    ORDER BY events DESC
    LIMIT 10;
  `);

  // PATTERN 2: High-equity distressed properties — old mortgages, tax liens, satisfactions implying ownership flux
  const highEquity = await mxre(`
    SELECT m.property_id, m.county_fips, m.borrower_name,
           m.original_amount, m.recording_date, m.document_type
    FROM mortgage_records m
    WHERE m.property_id IS NOT NULL
      AND m.document_type IN ('satisfaction','release','discharge','lien','tax_lien')
      AND m.recording_date >= (now() - interval '60 days')::date
      AND m.original_amount IS NOT NULL
      AND m.original_amount < 150000
    ORDER BY m.recording_date DESC
    LIMIT 20;
  `);

  // PATTERN 3: Unlinked record spikes — Lien needs to clean up
  const unlinkedSpike = await mxre(`
    SELECT county_fips, count(*) AS unlinked
    FROM mortgage_records
    WHERE property_id IS NULL AND county_fips IS NOT NULL
    GROUP BY 1 HAVING count(*) > 5000
    ORDER BY 2 DESC LIMIT 5;
  `);

  let dealsCreated = 0;
  let opportunitiesCreated = 0;

  // Turn distress clusters into opportunities (cross-division: RE + content + sales)
  for (const c of distressClusters) {
    const { data: existing } = await supa.from("opportunities")
      .select("id").eq("title", `Distress cluster: ${c.lender_name} / ${c.county_fips}`)
      .eq("status", "open").limit(1);
    if (existing && existing.length > 0) continue;

    await supa.from("opportunities").insert({
      discovered_by: "lien-agent",
      discovered_in: "real-estate",
      opportunity_type: "deal_pattern",
      title: `Distress cluster: ${c.lender_name} / ${c.county_fips}`,
      description: `${c.events} foreclosure/lis-pendens/assignment events from ${c.lender_name} in county ${c.county_fips} over the last 30 days. Types: ${(c.types ?? []).join(", ")}.`,
      relevant_divisions: ["real-estate", "content", "sales"],
      urgency: c.events >= 20 ? "high" : "medium",
      confidence: 0.85,
      source_citations: { table: "mortgage_records", filter: `lender_name='${c.lender_name}' AND county_fips='${c.county_fips}'` },
    });
    opportunitiesCreated++;
  }

  // Turn high-equity candidates into deals (prospects)
  for (const p of highEquity) {
    if (!p.property_id) continue;
    const { data: existing } = await supa.from("deals")
      .select("id").eq("property_id", p.property_id).limit(1);
    if (existing && existing.length > 0) continue;

    await supa.from("deals").insert({
      property_id: p.property_id,
      county_fips: p.county_fips,
      status: "prospect",
      sourced_by_agent: "lien-agent",
      sourced_at: new Date().toISOString(),
      outcome_notes: `Surfaced by deal-scorer. Signal: ${p.document_type} recorded ${p.recording_date}. Borrower: ${p.borrower_name ?? "(unknown)"}. Original amount: ${p.original_amount ? "$" + p.original_amount : "n/a"}.`,
    });
    dealsCreated++;
  }

  // Unlinked spikes → opportunity for Lien to tackle (division-internal but posted)
  for (const u of unlinkedSpike) {
    const { data: existing } = await supa.from("opportunities")
      .select("id").eq("title", `Unlink backlog: ${u.county_fips}`).eq("status", "open").limit(1);
    if (existing && existing.length > 0) continue;
    await supa.from("opportunities").insert({
      discovered_by: "mxre-intel",
      discovered_in: "real-estate",
      opportunity_type: "risk",
      title: `Unlink backlog: ${u.county_fips}`,
      description: `${u.unlinked} recorder documents have no property_id in ${u.county_fips}. Linking them unlocks deal discovery in that county.`,
      relevant_divisions: ["real-estate"],
      urgency: u.unlinked > 50000 ? "high" : "medium",
      confidence: 1.0,
    });
    opportunitiesCreated++;
  }

  return new Response(JSON.stringify({
    deals_created: dealsCreated,
    opportunities_created: opportunitiesCreated,
    patterns_scanned: { distress: distressClusters.length, high_equity: highEquity.length, unlinked: unlinkedSpike.length },
    duration_ms: Date.now() - t0,
  }), { headers: { ...cors, "Content-Type": "application/json" } });
});
