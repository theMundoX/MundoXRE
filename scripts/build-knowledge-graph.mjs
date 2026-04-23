#!/usr/bin/env node
/**
 * Knowledge graph builder — populates entities + entity_relationships from
 * mortgage_records and properties. Runs on the VPS (where the data lives) —
 * avoids network transfer of millions of rows.
 *
 * Pass 1: Extract unique borrower + lender names → entities (with canonical forms)
 * Pass 2: Emit borrower→property (owns), lender→borrower (borrower_at),
 *         borrower→borrower (same_address) edges
 *
 * Safe to re-run — uses ON CONFLICT upserts.
 *
 * Run:
 *   node scripts/build-knowledge-graph.mjs
 *   node scripts/build-knowledge-graph.mjs --county=39113   (limit to one county)
 *   node scripts/build-knowledge-graph.mjs --limit=100000
 */
import { config as loadEnv } from "dotenv";
loadEnv();

const MXRE_PG  = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const MXRE_SVC = process.env.SUPABASE_SERVICE_KEY;
if (!MXRE_SVC) { console.error("Set SUPABASE_SERVICE_KEY in .env"); process.exit(1); }

const args = process.argv.slice(2);
const countyFilter = args.find((a) => a.startsWith("--county="))?.split("=")[1];
const limit = parseInt(args.find((a) => a.startsWith("--limit="))?.split("=")[1] ?? "0", 10);

async function pg(sql, timeoutMs = 120_000) {
  const res = await fetch(MXRE_PG, {
    method: "POST",
    headers: { apikey: MXRE_SVC, Authorization: `Bearer ${MXRE_SVC}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: sql }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return await res.json();
}

// Canonicalize an entity name → deterministic ID.
function canon(s, type) {
  const clean = String(s ?? "")
    .toLowerCase()
    .replace(/[.,&]/g, " ")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 60);
  return clean ? `${type}:${clean}` : null;
}

function entityType(name) {
  const s = String(name ?? "").toLowerCase();
  if (/\b(llc|inc|corp|corporation|co\b|trust|bank|mortgage|fargo|chase|lending|credit\s*union|financial|servicing)\b/.test(s)) return "llc";
  if (/\b(federal|county|state|city|ihfa)\b/.test(s)) return "agency";
  return "person";
}

// ── Phase 1: entities ───────────────────────────────────────────────────────

async function ingestEntities() {
  console.log("Phase 1: extracting unique lenders + borrowers…");

  const where = [];
  if (countyFilter) where.push(`county_fips = '${countyFilter}'`);
  where.push("(lender_name IS NOT NULL OR borrower_name IS NOT NULL)");
  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  // Lenders
  const lenders = await pg(`
    SELECT lender_name, count(*)::int AS c, min(recording_date) AS first_seen, max(recording_date) AS last_seen
    FROM mortgage_records
    ${whereClause}
    AND lender_name IS NOT NULL
    GROUP BY 1
    HAVING length(lender_name) >= 3
    ${limit ? `LIMIT ${limit}` : ""};
  `);
  console.log(`  ${lenders.length} unique lender names`);

  // Borrowers
  const borrowers = await pg(`
    SELECT borrower_name, count(*)::int AS c, min(recording_date) AS first_seen, max(recording_date) AS last_seen
    FROM mortgage_records
    ${whereClause}
    AND borrower_name IS NOT NULL
    GROUP BY 1
    HAVING length(borrower_name) >= 3
    ${limit ? `LIMIT ${limit}` : ""};
  `);
  console.log(`  ${borrowers.length} unique borrower names`);

  // Upsert in batches
  const all = [
    ...lenders.map((r) => ({ name: r.lender_name, kind: "lender", ...r })),
    ...borrowers.map((r) => ({ name: r.borrower_name, kind: "borrower", ...r })),
  ];

  const batches = [];
  for (let i = 0; i < all.length; i += 500) batches.push(all.slice(i, i + 500));

  let inserted = 0;
  for (const [idx, batch] of batches.entries()) {
    // Deduplicate by canonical id within this batch — different raw strings can collapse to same id
    const seen = new Map();
    for (const e of batch) {
      const type = e.kind === "lender" ? "lender" : entityType(e.name);
      const id = canon(e.name, type);
      if (!id) continue;
      const existing = seen.get(id);
      if (!existing) {
        seen.set(id, { id, type, name: e.name, c: e.c, first_seen: e.first_seen, last_seen: e.last_seen });
      } else {
        existing.c += e.c;
        if (e.first_seen && (!existing.first_seen || e.first_seen < existing.first_seen)) existing.first_seen = e.first_seen;
        if (e.last_seen && (!existing.last_seen || e.last_seen > existing.last_seen))   existing.last_seen  = e.last_seen;
      }
    }
    const values = [...seen.values()]
      .map((e) => `('${e.id}','${e.type}','${e.name.replace(/'/g, "''")}',${e.c},'${e.first_seen ?? "1970-01-01"}','${e.last_seen ?? "1970-01-01"}')`)
      .join(",");
    if (!values) continue;

    await pg(`
      INSERT INTO entities (id, entity_type, name, occurrences, first_seen, last_seen)
      VALUES ${values}
      ON CONFLICT (id) DO UPDATE SET
        occurrences = entities.occurrences + EXCLUDED.occurrences,
        last_seen = GREATEST(entities.last_seen, EXCLUDED.last_seen);
    `);
    inserted += batch.length;
    process.stdout.write(`\r  batch ${idx + 1}/${batches.length} (${inserted} rows)`);
  }
  console.log("\n  ✓ entities upserted");
}

// ── Phase 2: relationships ──────────────────────────────────────────────────

async function ingestRelationships() {
  console.log("\nPhase 2: extracting relationships…");
  const where = [];
  if (countyFilter) where.push(`county_fips = '${countyFilter}'`);
  where.push("borrower_name IS NOT NULL AND lender_name IS NOT NULL");
  where.push("property_id IS NOT NULL");
  const whereClause = `WHERE ${where.join(" AND ")}`;

  // lender → borrower relationships (borrower_at)
  const rels = await pg(`
    SELECT borrower_name, lender_name, county_fips, property_id,
           min(recording_date) AS first_obs, max(recording_date) AS last_obs, count(*)::int AS c
    FROM mortgage_records
    ${whereClause}
    AND document_type IN ('mortgage','deed_of_trust')
    GROUP BY 1,2,3,4
    ${limit ? `LIMIT ${limit}` : ""};
  `);
  console.log(`  ${rels.length} lender↔borrower↔property triples`);

  const batches = [];
  for (let i = 0; i < rels.length; i += 300) batches.push(rels.slice(i, i + 300));

  for (const [idx, batch] of batches.entries()) {
    // Dedup edges by (from, to, type) within this batch
    const edgeMap = new Map();
    for (const r of batch) {
      const lenderId = canon(r.lender_name, "lender");
      const borrowerId = canon(r.borrower_name, entityType(r.borrower_name));
      if (!lenderId || !borrowerId) continue;
      // Only lender→borrower edges for now. Property-owns edges require property
      // entities to exist in `entities` table — will add in a later pass.
      const edges = [
        { from: lenderId, to: borrowerId, type: "borrower_at", pid: r.property_id, fips: r.county_fips, first: r.first_obs, last: r.last_obs },
      ];
      for (const e of edges) {
        const key = `${e.from}|${e.to}|${e.type}`;
        const existing = edgeMap.get(key);
        if (!existing) edgeMap.set(key, e);
        else if (e.last > existing.last) existing.last = e.last;
      }
    }
    const values = [...edgeMap.values()]
      .map((e) => `('${e.from}','${e.to}','${e.type}','${e.pid}','${e.fips}','${e.first}','${e.last}',1.0)`)
      .join(",");
    if (!values) continue;

    await pg(`
      INSERT INTO entity_relationships (from_entity, to_entity, relationship_type, property_id, county_fips, first_observed_at, last_observed_at, strength)
      VALUES ${values}
      ON CONFLICT (from_entity, to_entity, relationship_type) DO UPDATE SET
        last_observed_at = GREATEST(entity_relationships.last_observed_at, EXCLUDED.last_observed_at),
        strength = LEAST(1.0, entity_relationships.strength + 0.05);
    `).catch((e) => console.log(`\n  ! batch ${idx + 1} skipped: ${e.message.slice(0, 80)}`));

    process.stdout.write(`\r  batch ${idx + 1}/${batches.length}`);
  }
  console.log("\n  ✓ relationships upserted");
}

// ── Summary ─────────────────────────────────────────────────────────────────

async function summary() {
  const [counts] = await pg(`
    SELECT
      (SELECT count(*) FROM entities)::int AS entity_count,
      (SELECT count(*) FROM entity_relationships)::int AS rel_count,
      (SELECT count(DISTINCT county_fips) FROM entity_relationships)::int AS counties;
  `);
  console.log(`\n━━━ Knowledge graph summary ━━━`);
  console.log(`  ${counts.entity_count.toLocaleString()} entities`);
  console.log(`  ${counts.rel_count.toLocaleString()} relationships`);
  console.log(`  ${counts.counties} counties covered`);
}

async function main() {
  console.log("MXRE Knowledge Graph Builder");
  console.log(`  source: ${MXRE_PG.replace("/pg/query", "")}`);
  if (countyFilter) console.log(`  county: ${countyFilter}`);
  if (limit) console.log(`  limit: ${limit}`);
  console.log();

  await ingestEntities();
  await ingestRelationships();
  await summary();
}

main().catch((e) => { console.error(e); process.exit(1); });
