#!/usr/bin/env tsx
/**
 * Fix interest_rate_type tagging on mortgage_records.
 *
 * Problem:
 *   1. Records with source_url LIKE 'assessor-sale-%' have interest_rate_type = 'actual'
 *      but the rate was computed from PMMS (estimated), not from actual documents.
 *      → Set these to 'estimated'.
 *
 *   2. Records with interest_rate_type IS NULL but estimated_monthly_payment IS NOT NULL
 *      have a rate that was estimated at some point but never tagged.
 *      → Set these to 'estimated'.
 *
 *   3. Records with interest_rate_type IS NULL and estimated_monthly_payment IS NULL
 *      → Leave as NULL (no rate was set at all).
 *
 * After updates, refresh materialized views county_lien_counts and county_stats_mv.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const BATCH_SIZE = 200;

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function countWhere(filters: (q: ReturnType<typeof db.from>) => ReturnType<typeof db.from>): Promise<number> {
  const q = db.from("mortgage_records").select("*", { count: "exact", head: true });
  const { count, error } = await filters(q as any);
  if (error) throw new Error(`Count query failed: ${error.message}`);
  return count ?? 0;
}

async function fetchBatchIds(
  filters: (q: ReturnType<typeof db.from>) => ReturnType<typeof db.from>,
): Promise<number[]> {
  const q = db.from("mortgage_records").select("id").limit(BATCH_SIZE);
  const { data, error } = await filters(q as any);
  if (error) throw new Error(`Fetch IDs failed: ${error.message}`);
  return (data ?? []).map((r: { id: number }) => r.id);
}

async function updateBatch(ids: number[], value: string): Promise<void> {
  const { error } = await db
    .from("mortgage_records")
    .update({ interest_rate_type: value })
    .in("id", ids);
  if (error) throw new Error(`Update failed: ${error.message}`);
}

// ---------------------------------------------------------------------------
// Phase 1 — assessor-sale records: 'actual' → 'estimated'
// ---------------------------------------------------------------------------

async function fixAssessorSaleRecords(): Promise<void> {
  console.log("─────────────────────────────────────────────────────────");
  console.log("Phase 1: assessor-sale records with interest_rate_type = 'actual'");
  console.log("─────────────────────────────────────────────────────────\n");

  // Count first (dry-run check)
  const total = await countWhere(q =>
    (q as any)
      .like("source_url", "assessor-sale-%")
      .eq("interest_rate_type", "actual"),
  );
  console.log(`  Records to fix: ${total.toLocaleString()}`);

  if (total === 0) {
    console.log("  Nothing to do for Phase 1.\n");
    return;
  }

  let updated = 0;

  while (updated < total) {
    const ids = await fetchBatchIds(q =>
      (q as any)
        .like("source_url", "assessor-sale-%")
        .eq("interest_rate_type", "actual"),
    );

    if (ids.length === 0) break;

    await updateBatch(ids, "estimated");
    updated += ids.length;
    process.stdout.write(`\r  Updated: ${updated.toLocaleString()} / ${total.toLocaleString()}`);
  }

  console.log(`\n  Phase 1 complete: ${updated.toLocaleString()} records updated 'actual' → 'estimated'.\n`);
}

// ---------------------------------------------------------------------------
// Phase 2 — NULL interest_rate_type with estimated_monthly_payment IS NOT NULL
// ---------------------------------------------------------------------------

async function fixNullWithPayment(): Promise<void> {
  console.log("─────────────────────────────────────────────────────────");
  console.log("Phase 2: NULL interest_rate_type with estimated_monthly_payment IS NOT NULL");
  console.log("─────────────────────────────────────────────────────────\n");

  const total = await countWhere(q =>
    (q as any)
      .is("interest_rate_type", null)
      .not("estimated_monthly_payment", "is", null),
  );
  console.log(`  Records to fix: ${total.toLocaleString()}`);

  if (total === 0) {
    console.log("  Nothing to do for Phase 2.\n");
    return;
  }

  let updated = 0;

  while (updated < total) {
    const ids = await fetchBatchIds(q =>
      (q as any)
        .is("interest_rate_type", null)
        .not("estimated_monthly_payment", "is", null),
    );

    if (ids.length === 0) break;

    await updateBatch(ids, "estimated");
    updated += ids.length;
    process.stdout.write(`\r  Updated: ${updated.toLocaleString()} / ${total.toLocaleString()}`);
  }

  console.log(`\n  Phase 2 complete: ${updated.toLocaleString()} records tagged NULL → 'estimated'.\n`);
}

// ---------------------------------------------------------------------------
// Refresh materialized views
// ---------------------------------------------------------------------------

async function refreshMaterializedViews(): Promise<void> {
  console.log("─────────────────────────────────────────────────────────");
  console.log("Refreshing materialized views");
  console.log("─────────────────────────────────────────────────────────\n");

  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_KEY!;
  const views = ["county_lien_counts", "county_stats_mv"];

  for (const view of views) {
    process.stdout.write(`  REFRESH MATERIALIZED VIEW ${view} ... `);
    const res = await fetch(`${url}/pg/query`, {
      method: "POST",
      headers: {
        apikey: key,
        Authorization: `Bearer ${key}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW ${view}` }),
    });
    if (res.ok) {
      console.log("OK");
    } else {
      const text = await res.text();
      console.log(`FAILED (${res.status}): ${text}`);
    }
  }

  console.log();
}

// ---------------------------------------------------------------------------
// Summary counts before/after
// ---------------------------------------------------------------------------

async function printSummary(label: string): Promise<void> {
  const [actual, estimated, nullCount] = await Promise.all([
    countWhere(q => (q as any).eq("interest_rate_type", "actual")),
    countWhere(q => (q as any).eq("interest_rate_type", "estimated")),
    countWhere(q => (q as any).is("interest_rate_type", null)),
  ]);
  console.log(`  [${label}]`);
  console.log(`    interest_rate_type = 'actual'   : ${actual.toLocaleString()}`);
  console.log(`    interest_rate_type = 'estimated': ${estimated.toLocaleString()}`);
  console.log(`    interest_rate_type IS NULL       : ${nullCount.toLocaleString()}`);
  console.log();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("MXRE — Fix interest_rate_type on mortgage_records");
  console.log("═══════════════════════════════════════════════════\n");

  console.log("Summary before fixes:");
  await printSummary("before");

  await fixAssessorSaleRecords();
  await fixNullWithPayment();

  console.log("Summary after fixes:");
  await printSummary("after");

  await refreshMaterializedViews();

  console.log("Done.");
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
