#!/usr/bin/env node
/**
 * BULK HMDA RATE MATCHER (set-based)
 *
 * Replaces the per-row backfill-rates-from-hmda.mjs with a single SQL UPDATE
 * that joins mortgage_records to hmda_lar via the strategy 2 / 3 / 4 logic.
 *
 * Strategies (highest confidence first):
 *   S2: year + state + county_fips + loan_amount EXACT          -> conf 80
 *   S3: year + state + county_fips + loan_amount within $5k     -> conf 70
 *   S4: year ±1 + state + loan_amount within $5k                -> conf 55
 *
 * Set-based approach:
 *   1. UPDATE mortgage_records m
 *      SET interest_rate = h.interest_rate, ...
 *      FROM hmda_lar h
 *      JOIN properties p ON p.id = m.property_id
 *      WHERE <s2 conditions>
 *
 *   2. Repeat with s3 conditions for unmatched rows
 *   3. Repeat with s4 conditions for unmatched rows
 *
 * Each pass writes to mortgage_rate_matches audit table and updates
 * mortgage_records.{interest_rate, term_months, rate_source, rate_match_*}.
 *
 * Should run in minutes, not days.
 */
import pkg from "pg";
const { Pool } = pkg;

const DRY = !process.argv.includes("--apply");

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 1,
  statement_timeout: 0,
  connectionTimeoutMillis: 60000,
});

async function coverage(c) {
  const r = await c.query(
    `SELECT
       COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly')  AS pmms,
       COUNT(*) FILTER (WHERE rate_source = 'hmda_match')   AS hmda,
       COUNT(*) FILTER (WHERE rate_source = 'agency_match') AS agency,
       COUNT(*) FILTER (WHERE rate_source IS NULL)          AS none,
       COUNT(*)                                             AS total
     FROM mortgage_records`
  );
  return r.rows[0];
}

async function runStrategy(c, label, joinSQL, confidence) {
  console.log(`\n[${label}] starting (confidence=${confidence})...`);
  const t0 = Date.now();
  const sql = `
    WITH candidates AS (
      SELECT m.id AS mortgage_id,
             h.id AS hmda_id,
             h.interest_rate,
             h.loan_term,
             ROW_NUMBER() OVER (PARTITION BY m.id ORDER BY h.id) AS rn
        FROM mortgage_records m
        JOIN hmda_lar h ON ${joinSQL}
       WHERE m.recording_date IS NOT NULL
         AND m.loan_amount IS NOT NULL
         AND m.county_fips IS NOT NULL
         AND h.action_taken = 1
         AND h.interest_rate IS NOT NULL
         AND m.rate_source != 'hmda_match'
    ),
    chosen AS (
      SELECT mortgage_id, hmda_id, interest_rate, loan_term
        FROM candidates
       WHERE rn = 1
    )
    ${DRY ? "SELECT COUNT(*) AS would_update FROM chosen" :
    `, audit AS (
      INSERT INTO mortgage_rate_matches
        (mortgage_record_id, rate_source, source_row_id, match_confidence, match_strategy, interest_rate, loan_term)
      SELECT mortgage_id, 'hmda_match', hmda_id, ${confidence}, '${label}', interest_rate, loan_term
        FROM chosen
      RETURNING mortgage_record_id
    )
    UPDATE mortgage_records m
       SET interest_rate = c.interest_rate,
           term_months = COALESCE(c.loan_term, m.term_months),
           rate_source = 'hmda_match',
           rate_match_confidence = ${confidence},
           rate_match_source_id = c.hmda_id,
           rate_matched_at = now()
      FROM chosen c
     WHERE m.id = c.mortgage_id`}
  `;
  const r = await c.query(sql);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
  if (DRY) {
    console.log(`[${label}] would update ${r.rows[0].would_update.toLocaleString()} rows in ${elapsed}s`);
  } else {
    console.log(`[${label}] updated ${r.rowCount.toLocaleString()} rows in ${elapsed}s`);
  }
}

async function main() {
  const c = await pool.connect();
  try {
    console.log(`=== BULK HMDA MATCHER (${DRY ? "DRY RUN" : "APPLY"}) ===\n`);
    console.log("BEFORE:", await coverage(c));

    // Strategy 2: exact county + amount (county_fips encodes state in first 2 digits)
    await runStrategy(
      c,
      "s2-exact",
      `EXTRACT(YEAR FROM m.recording_date) = h.activity_year
       AND m.county_fips = h.county_code
       AND (FLOOR(m.loan_amount / 10000) * 10000 + 5000) = h.loan_amount`,
      80
    );

    // Strategy 3: county + amount within ±$5k
    await runStrategy(
      c,
      "s3-fuzzy-amount",
      `EXTRACT(YEAR FROM m.recording_date) = h.activity_year
       AND m.county_fips = h.county_code
       AND h.loan_amount BETWEEN (FLOOR(m.loan_amount / 10000) * 10000 - 5000) AND (FLOOR(m.loan_amount / 10000) * 10000 + 15000)`,
      70
    );

    // Strategy 4 SKIPPED: state-level cross-joins are too expensive without a county filter,
    // and only added a marginal % of matches in the per-row test (4%). Not worth the table-scan.

    console.log("\nAFTER:", await coverage(c));
  } finally {
    c.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  pool.end().catch(() => {});
  process.exit(1);
});
