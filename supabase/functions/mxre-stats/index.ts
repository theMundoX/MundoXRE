// MXRE stats proxy — queries the self-hosted MXRE Supabase (Contabo VPS) via
// pg-meta and returns agent-specific stats. The MXRE service key never leaves
// the server, so the frontend can poll this safely.
//
// Called as: GET /mxre-stats?agent=mxre-intel
//   agent ∈ { ryder, mxre-intel, lien-agent, listing-agent, rent-agent,
//             mxre-texas, mxre-southeast, mxre-midwest, mxre-southwest, mxre-northeast }

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

const MXRE_PG_URL   = Deno.env.get("MXRE_PG_URL") ?? "";
const MXRE_SVC_KEY  = Deno.env.get("MXRE_SUPABASE_SERVICE_KEY") ?? "";
const SUPABASE_URL  = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_ANON = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

async function requireAuth(req: Request): Promise<{ error: Response } | { ok: true }> {
  const auth = req.headers.get("Authorization");
  if (!auth?.startsWith("Bearer ")) {
    return { error: new Response(JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { "Content-Type": "application/json" } }) };
  }
  const client = createClient(SUPABASE_URL, SUPABASE_ANON, {
    auth: { persistSession: false },
    global: { headers: { Authorization: auth } },
  });
  const { error } = await client.auth.getUser();
  if (error) {
    return { error: new Response(JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { "Content-Type": "application/json" } }) };
  }
  return { ok: true };
}

// FIPS groupings per region
const REGION_FIPS: Record<string, string[]> = {
  "mxre-texas":     ["48015", "48147", "48167", "48265", "48365"],
  "mxre-southeast": ["37119", "37183", "13121"], // Mecklenburg NC, Wake NC, Fulton GA
  "mxre-midwest":   [
    // Iowa (Polk, Linn, Scott)
    "19113", "19013", "19163",
    // Ohio (Cuyahoga, Franklin, Hamilton, Lucas)
    "39045", "39055", "39125", "39049", "39061", "39113",
    // Michigan (Wayne, Oakland)
    "26125", "26009",
    // Indiana (Marion=Indianapolis + donut counties Hamilton/Hendricks/Johnson/Hancock + Lake/Allen)
    "18097", "18057", "18063", "18081", "18059", "18089", "18003", "18011",
  ],
  "mxre-southwest": ["04013"], // Maricopa AZ
  "mxre-northeast": ["33001","33003","33005","33007","33009","33011","33013","33015","33017","33019"],
};

async function pg(query: string): Promise<any[]> {
  const res = await fetch(MXRE_PG_URL, {
    method: "POST",
    headers: {
      "apikey": MXRE_SVC_KEY,
      "Authorization": `Bearer ${MXRE_SVC_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`pg-meta ${res.status}: ${await res.text()}`);
  return res.json();
}

// ── Stats builders ───────────────────────────────────────────────────────────

async function overviewStats() {
  // Use pg_stat_user_tables for fast estimates on huge tables (98M+ properties)
  const [tables, docTypes, withAmt, recentDocs, fips] = await Promise.all([
    pg(`SELECT relname, n_live_tup, pg_total_relation_size(relid) AS size_bytes
        FROM pg_stat_user_tables
        WHERE schemaname='public'
          AND relname IN ('properties','hmda_lar','rent_snapshots','mortgage_records',
                          'listing_signals','mortgage_lender_lei_map','mortgage_rate_matches',
                          'agency_lld','pmms_weekly','counties')`),
    pg(`SELECT document_type, count(*)::bigint as c FROM mortgage_records GROUP BY 1 ORDER BY 2 DESC LIMIT 10`),
    pg(`SELECT count(*)::bigint as c FROM mortgage_records WHERE original_amount IS NOT NULL`),
    pg(`SELECT county_fips, recording_date, document_type, original_amount FROM mortgage_records ORDER BY recording_date DESC NULLS LAST LIMIT 8`),
    pg(`SELECT count(DISTINCT county_fips)::bigint as c FROM mortgage_records WHERE county_fips IS NOT NULL`),
  ]);

  const tableMap: Record<string, { rows: number; size_bytes: number }> = {};
  tables.forEach((t: any) => {
    tableMap[t.relname] = { rows: Number(t.n_live_tup), size_bytes: Number(t.size_bytes) };
  });

  const totalRecords =
    (tableMap.properties?.rows ?? 0) +
    (tableMap.hmda_lar?.rows ?? 0) +
    (tableMap.rent_snapshots?.rows ?? 0) +
    (tableMap.mortgage_records?.rows ?? 0) +
    (tableMap.listing_signals?.rows ?? 0);

  return {
    total_records: totalRecords,
    properties: tableMap.properties?.rows ?? 0,
    hmda_lar: tableMap.hmda_lar?.rows ?? 0,
    rent_snapshots: tableMap.rent_snapshots?.rows ?? 0,
    mortgage_records: tableMap.mortgage_records?.rows ?? 0,
    listing_signals: tableMap.listing_signals?.rows ?? 0,
    tables: tableMap,
    doc_types: docTypes,
    with_amount: Number(withAmt?.[0]?.c ?? 0),
    counties: Number(fips?.[0]?.c ?? 0),
    recent: recentDocs,
  };
}

async function lienStats() {
  const [byType, byCounty, recent, withAmt] = await Promise.all([
    pg(`SELECT document_type, count(*)::bigint as c FROM mortgage_records
        WHERE document_type IN ('mortgage','lien','deed_of_trust','satisfaction','release','discharge','assignment')
        GROUP BY 1 ORDER BY 2 DESC`),
    pg(`SELECT county_fips, count(*)::bigint as c FROM mortgage_records
        WHERE county_fips IS NOT NULL AND document_type IN ('mortgage','lien','deed_of_trust')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10`),
    pg(`SELECT county_fips, recording_date, document_type, lender_name, original_amount
        FROM mortgage_records
        WHERE document_type IN ('mortgage','lien','deed_of_trust')
          AND recording_date IS NOT NULL
        ORDER BY recording_date DESC LIMIT 8`),
    pg(`SELECT count(*)::bigint as c FROM mortgage_records WHERE original_amount IS NOT NULL`),
  ]);
  return { by_type: byType, by_county: byCounty, recent, with_amount: Number(withAmt?.[0]?.c ?? 0) };
}

async function listingStats() {
  const [total, byStatus, recent] = await Promise.all([
    pg(`SELECT count(*)::bigint as c FROM listing_signals`).catch(() => [{ c: 0 }]),
    pg(`SELECT status, count(*)::bigint as c FROM listing_signals GROUP BY 1 ORDER BY 2 DESC LIMIT 6`).catch(() => []),
    pg(`SELECT source, signal_type, detected_at FROM listing_signals ORDER BY detected_at DESC LIMIT 8`).catch(() => []),
  ]);
  return { total: Number(total?.[0]?.c ?? 0), by_status: byStatus, recent };
}

async function rentStats() {
  const [snapshots, estimates] = await Promise.all([
    pg(`SELECT count(*)::bigint as c FROM rent_snapshot`).catch(() => [{ c: 0 }]),
    pg(`SELECT count(*)::bigint as c FROM rent_estimate`).catch(() => [{ c: 0 }]),
  ]);
  return { snapshots: Number(snapshots?.[0]?.c ?? 0), estimates: Number(estimates?.[0]?.c ?? 0) };
}

async function regionStats(fips: string[]) {
  const fipsList = fips.map(f => `'${f}'`).join(",");
  const [docs, liens, props] = await Promise.all([
    pg(`SELECT county_fips, count(*)::bigint as c FROM mortgage_records
        WHERE county_fips IN (${fipsList}) GROUP BY 1 ORDER BY 2 DESC`),
    pg(`SELECT county_fips, count(*)::bigint as c FROM mortgage_records
        WHERE county_fips IN (${fipsList})
          AND document_type IN ('mortgage','lien','deed_of_trust')
        GROUP BY 1 ORDER BY 2 DESC`),
    // properties has no county_fips — must JOIN counties (where state_fips||county_fips makes the 5-digit code).
    pg(`SELECT count(*)::bigint as c FROM properties p
        JOIN counties c ON p.county_id = c.id
        WHERE (c.state_fips || c.county_fips) IN (${fipsList})`).catch(() => [{ c: 0 }]),
  ]);
  const totalDocs = docs.reduce((s, r) => s + Number(r.c || 0), 0);
  const totalLiens = liens.reduce((s, r) => s + Number(r.c || 0), 0);
  return { by_county: docs, liens_by_county: liens, total_docs: totalDocs, total_liens: totalLiens, properties: Number(props?.[0]?.c ?? 0) };
}

// ── HTTP handler ─────────────────────────────────────────────────────────────

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const authResult = await requireAuth(req);
  if ("error" in authResult) return authResult.error;

  try {
    const url = new URL(req.url);
    const agent = url.searchParams.get("agent") ?? "mxre-intel";

    let payload: any;
    const t0 = performance.now();

    if (agent === "ryder" || agent === "mxre-intel") {
      payload = await overviewStats();
    } else if (agent === "lien-agent") {
      payload = await lienStats();
    } else if (agent === "listing-agent") {
      payload = await listingStats();
    } else if (agent === "rent-agent") {
      payload = await rentStats();
    } else if (REGION_FIPS[agent]) {
      payload = await regionStats(REGION_FIPS[agent]);
    } else {
      payload = { error: "unknown agent" };
    }

    return new Response(
      JSON.stringify({
        agent,
        generated_at: new Date().toISOString(),
        query_ms: Math.round(performance.now() - t0),
        data: payload,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (err: any) {
    return new Response(
      JSON.stringify({ error: err.message ?? String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
