#!/usr/bin/env tsx
/**
 * Backfill estimated mortgage fields for every record that has enough data
 * but no rate_source yet.
 *
 * Source rule:
 *   - Need: loan_amount > 0 OR original_amount > 0
 *   - Need: recording_date IS NOT NULL
 *   - Skip: rate_source already set (already done)
 *
 * Writes via direct UPDATE statements (no upsert) so existing NOT NULL
 * columns aren't disturbed.
 *
 * Usage:
 *   npx tsx scripts/backfill-rate-estimates.ts             # full
 *   npx tsx scripts/backfill-rate-estimates.ts --limit 5000  # capped
 *   npx tsx scripts/backfill-rate-estimates.ts --dry-run     # no writes
 */

import "dotenv/config";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const LIMIT_ARG = args.find((a) => a.startsWith("--limit"));
const LIMIT = LIMIT_ARG
  ? parseInt(LIMIT_ARG.split("=")[1] || args[args.indexOf(LIMIT_ARG) + 1] || "0", 10)
  : 0;
const BATCH_SIZE = 500;

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

async function pg<T = any>(query: string): Promise<T> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 60_000);
  try {
    const r = await fetch(`${SUPABASE_URL}/pg/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SERVICE_KEY,
        Authorization: `Bearer ${SERVICE_KEY}`,
      },
      body: JSON.stringify({ query }),
      signal: ctrl.signal,
    });
    return (await r.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

interface Row {
  id: number;
  loan_amount: number | null;
  original_amount: number | null;
  recording_date: string | null;
  term_months: number | null;
}

function escapeSql(s: string): string {
  return s.replace(/'/g, "''");
}

async function main() {
  console.log("=".repeat(60));
  console.log("MORTGAGE RATE ESTIMATE BACKFILL");
  console.log("=".repeat(60));
  console.log(`Mode:   ${DRY_RUN ? "DRY RUN (no writes)" : "LIVE"}`);
  console.log(`Limit:  ${LIMIT || "no limit (full backfill)"}`);
  console.log(`Batch:  ${BATCH_SIZE}`);
  console.log("");

  // Eligible count
  const elig: any = await pg(
    `SELECT count(*)::int AS n FROM mortgage_records
       WHERE rate_source IS NULL
         AND recording_date IS NOT NULL
         AND (loan_amount > 0 OR original_amount > 0)`,
  );
  const eligible = Array.isArray(elig) && elig[0]?.n ? elig[0].n : 0;
  console.log(`Eligible: ${eligible.toLocaleString()}\n`);
  if (eligible === 0) {
    console.log("Nothing to backfill.");
    return;
  }

  let lastId = 0;
  let processed = 0;
  let written = 0;
  let errors = 0;
  const startTime = Date.now();

  while (true) {
    if (LIMIT && processed >= LIMIT) {
      console.log(`Reached --limit ${LIMIT}, stopping.`);
      break;
    }

    // Fetch a cursor-paginated batch
    const sel = `SELECT id, loan_amount, original_amount, recording_date, term_months
                   FROM mortgage_records
                  WHERE rate_source IS NULL
                    AND recording_date IS NOT NULL
                    AND (loan_amount > 0 OR original_amount > 0)
                    AND id > ${lastId}
                  ORDER BY id
                  LIMIT ${BATCH_SIZE}`;
    const rows: any = await pg(sel);
    if (!Array.isArray(rows) || rows.length === 0) {
      console.log("No more records.");
      break;
    }

    // Build CASE WHEN ... END statements for batch update
    const updates: Array<{
      id: number;
      interest_rate: number;
      term_months: number;
      monthly: number;
      balance: number;
      asOf: string;
      maturity: string;
    }> = [];

    for (const r of rows as Row[]) {
      lastId = r.id;
      processed++;

      const amt = r.original_amount || r.loan_amount || 0;
      if (!amt || amt <= 0 || !r.recording_date) continue;

      const f = computeMortgageFields({
        originalAmount: amt,
        recordingDate: r.recording_date,
        termMonths: r.term_months || undefined,
      });

      if (f.interest_rate == null || !f.maturity_date) continue;

      updates.push({
        id: r.id,
        interest_rate: f.interest_rate,
        term_months: f.term_months,
        monthly: f.estimated_monthly_payment ?? 0,
        balance: f.estimated_current_balance ?? 0,
        asOf: f.balance_as_of,
        maturity: f.maturity_date,
      });
    }

    if (updates.length === 0) continue;

    if (!DRY_RUN) {
      // One UPDATE ... FROM (VALUES (...), ...) statement for the whole batch.
      const values = updates
        .map(
          (u) =>
            `(${u.id},${u.interest_rate},${u.term_months},${u.monthly},${u.balance},'${u.asOf}'::date,'${u.maturity}'::date)`,
        )
        .join(",");

      const sql = `
        UPDATE mortgage_records m
           SET interest_rate              = v.rate,
               term_months                = COALESCE(m.term_months, v.term),
               estimated_monthly_payment  = v.payment,
               estimated_current_balance  = v.balance,
               balance_as_of              = v.asof,
               maturity_date              = v.maturity,
               rate_source                = 'estimated'
          FROM (VALUES ${values})
            AS v(id, rate, term, payment, balance, asof, maturity)
         WHERE m.id = v.id
      `;

      try {
        await pg(sql);
        written += updates.length;
      } catch (e) {
        errors += updates.length;
        console.error(`  batch err at id=${lastId}: ${e instanceof Error ? e.message : e}`);
      }
    } else {
      written += updates.length;
    }

    if (processed % 5000 === 0 || rows.length < BATCH_SIZE) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = processed / elapsed;
      const etaMin = eligible ? ((eligible - processed) / rate / 60).toFixed(1) : "?";
      console.log(
        `  proc=${processed.toLocaleString()}  written=${written.toLocaleString()}  err=${errors}  rate=${rate.toFixed(0)}/s  eta=${etaMin}min`,
      );
    }
  }

  const elapsed = (Date.now() - startTime) / 1000;
  console.log("");
  console.log("=".repeat(60));
  console.log(`Processed: ${processed.toLocaleString()}`);
  console.log(`Written:   ${written.toLocaleString()}  (rate_source='estimated')`);
  console.log(`Errors:    ${errors}`);
  console.log(`Elapsed:   ${(elapsed / 60).toFixed(1)} min`);
  if (DRY_RUN) console.log("(no writes — DRY RUN)");
}

main().catch((e) => {
  console.error("Backfill failed:", e);
  process.exit(1);
});
