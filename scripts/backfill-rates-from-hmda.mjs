/**
 * Fuzzy-join mortgage_records against hmda_lar to pull real interest rates.
 *
 * Match strategy (strictest -> loosest, first hit wins):
 *   1. year + state + county + loan_amount exact  + lender LEI   -> confidence 95
 *   2. year + state + county + loan_amount exact                 -> confidence 80
 *   3. year + state + county + loan_amount +/- 5k (HMDA rounds)  -> confidence 70
 *   4. year +/- 1 + state + county + loan_amount +/- 5k          -> confidence 55
 *
 * Anything that doesn't match falls through to a PMMS lookup by recording week.
 *
 * Writes results into mortgage_rate_matches and updates mortgage_records.
 *
 * Usage:
 *   node scripts/backfill-rates-from-hmda.mjs           # dry run, first 1000
 *   node scripts/backfill-rates-from-hmda.mjs --apply   # full backfill
 *   node scripts/backfill-rates-from-hmda.mjs --apply --limit 50000
 */
import pkg from "pg";
const { Pool } = pkg;

const APPLY = process.argv.includes("--apply");
const LIMIT_ARG = process.argv.indexOf("--limit");
const LIMIT = LIMIT_ARG >= 0 ? parseInt(process.argv[LIMIT_ARG + 1]) : APPLY ? null : 1000;

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 4,
  connectionTimeoutMillis: 15000,
});

const STATS = {
  considered: 0,
  hmda_s1: 0,
  hmda_s2: 0,
  hmda_s3: 0,
  hmda_s4: 0,
  pmms: 0,
  estimated: 0,
  noop: 0,
};

async function matchOne(client, rec) {
  const year = rec.recording_date ? new Date(rec.recording_date).getUTCFullYear() : null;
  // mortgage_records.loan_amount is already in dollars, HMDA loan_amount is also in dollars
  const amount = rec.loan_amount ? Math.round(Number(rec.loan_amount)) : null;
  const state = rec.property_state;
  const countyFips = rec.county_fips; // 5-char FIPS or null
  if (!year || !amount || !state) return null;

  // Strategy 1+2: year + state + county + exact amount (with/without LEI)
  // HMDA rounds loan_amount to the nearest $5,000 for privacy.
  const amountBucket = Math.round(amount / 5000) * 5000;

  // Try exact year, county match
  if (countyFips) {
    const r = await client.query(
      `SELECT id, interest_rate, loan_term, lei
         FROM hmda_lar
        WHERE action_taken = 1
          AND activity_year = $1
          AND state_code = $2
          AND county_code = $3
          AND loan_amount = $4
          AND interest_rate IS NOT NULL
        LIMIT 1`,
      [year, state, countyFips, amountBucket]
    );
    if (r.rows[0]) {
      STATS.hmda_s2++;
      return {
        source: "hmda_match",
        source_row_id: r.rows[0].id,
        confidence: 80,
        strategy: "year+state+county+amount",
        rate: Number(r.rows[0].interest_rate),
        term: r.rows[0].loan_term,
      };
    }

    // Strategy 3: loose amount within ±$5k one bucket
    const r3 = await client.query(
      `SELECT id, interest_rate, loan_term
         FROM hmda_lar
        WHERE action_taken = 1
          AND activity_year = $1
          AND state_code = $2
          AND county_code = $3
          AND loan_amount BETWEEN $4 AND $5
          AND interest_rate IS NOT NULL
        ORDER BY ABS(loan_amount - $6)
        LIMIT 1`,
      [year, state, countyFips, amountBucket - 5000, amountBucket + 5000, amountBucket]
    );
    if (r3.rows[0]) {
      STATS.hmda_s3++;
      return {
        source: "hmda_match",
        source_row_id: r3.rows[0].id,
        confidence: 70,
        strategy: "year+state+county+amount±5k",
        rate: Number(r3.rows[0].interest_rate),
        term: r3.rows[0].loan_term,
      };
    }
  }

  // Strategy 4: widen year ±1 (recording lag), state level
  const r4 = await client.query(
    `SELECT id, interest_rate, loan_term
       FROM hmda_lar
      WHERE action_taken = 1
        AND activity_year BETWEEN $1 AND $2
        AND state_code = $3
        AND loan_amount BETWEEN $4 AND $5
        AND interest_rate IS NOT NULL
      ORDER BY ABS(loan_amount - $6), ABS(activity_year - $7)
      LIMIT 1`,
    [year - 1, year + 1, state, amountBucket - 5000, amountBucket + 5000, amountBucket, year]
  );
  if (r4.rows[0]) {
    STATS.hmda_s4++;
    return {
      source: "hmda_match",
      source_row_id: r4.rows[0].id,
      confidence: 55,
      strategy: "year±1+state+amount±5k",
      rate: Number(r4.rows[0].interest_rate),
      term: r4.rows[0].loan_term,
    };
  }

  // Fallback: PMMS weekly
  if (rec.recording_date) {
    const r5 = await client.query(
      `SELECT rate_30yr_fixed
         FROM pmms_weekly
        WHERE week_ending <= $1
        ORDER BY week_ending DESC
        LIMIT 1`,
      [rec.recording_date]
    );
    if (r5.rows[0] && r5.rows[0].rate_30yr_fixed) {
      STATS.pmms++;
      return {
        source: "pmms_weekly",
        source_row_id: null,
        confidence: 40,
        strategy: "pmms_weekly_benchmark",
        rate: Number(r5.rows[0].rate_30yr_fixed),
        term: 360,
      };
    }
  }

  STATS.estimated++;
  return null;
}

async function main() {
  const client = await pool.connect();
  try {
    // Only work on records missing a real rate
    const whereApply = APPLY ? "" : "";
    const limitClause = LIMIT ? `LIMIT ${LIMIT}` : "";
    // Optionally constrain to a specific year via env var YEAR for testing
    const yearFilter = process.env.YEAR
      ? `AND EXTRACT(YEAR FROM m.recording_date) = ${parseInt(process.env.YEAR)}`
      : "";
    const rs = await client.query(
      `SELECT m.id, m.recording_date, m.loan_amount, m.county_fips, p.state_code AS property_state
         FROM mortgage_records m
         JOIN properties p ON p.id = m.property_id
        WHERE m.loan_amount IS NOT NULL
          AND m.recording_date IS NOT NULL
          AND p.state_code IS NOT NULL
          AND (m.rate_source IS NULL OR m.rate_source = 'pmms_weekly' OR m.rate_source = 'estimated')
          ${yearFilter}
        ${limitClause}`
    );
    console.log(
      `Considering ${rs.rows.length.toLocaleString()} mortgage_records ${APPLY ? "(APPLY mode)" : "(dry run)"}`
    );

    const startedAt = Date.now();
    for (const rec of rs.rows) {
      STATS.considered++;
      const m = await matchOne(client, rec);
      if (!m) {
        STATS.noop++;
      } else if (APPLY) {
        await client.query("BEGIN");
        await client.query(
          `INSERT INTO mortgage_rate_matches
           (mortgage_record_id, rate_source, source_row_id, match_confidence, match_strategy, interest_rate, loan_term)
           VALUES ($1,$2,$3,$4,$5,$6,$7)`,
          [rec.id, m.source, m.source_row_id, m.confidence, m.strategy, m.rate, m.term]
        );
        await client.query(
          `UPDATE mortgage_records
              SET interest_rate = $1,
                  term_months = COALESCE($2, term_months),
                  rate_source = $3,
                  rate_match_confidence = $4,
                  rate_match_source_id = $5,
                  rate_matched_at = now()
            WHERE id = $6`,
          [m.rate, m.term, m.source, m.confidence, m.source_row_id, rec.id]
        );
        await client.query("COMMIT");
      }
      if (STATS.considered % 1000 === 0) {
        const secs = (Date.now() - startedAt) / 1000;
        console.log(
          `  ${STATS.considered.toLocaleString()} / ${rs.rows.length.toLocaleString()}  ${(STATS.considered / secs).toFixed(0)}/s`
        );
      }
    }

    const secs = (Date.now() - startedAt) / 1000;
    console.log(`\n=== DONE in ${secs.toFixed(0)}s ===`);
    console.log(STATS);
    const matched = STATS.hmda_s1 + STATS.hmda_s2 + STATS.hmda_s3 + STATS.hmda_s4;
    const pct = ((matched / STATS.considered) * 100).toFixed(1);
    console.log(`HMDA match rate: ${matched.toLocaleString()} / ${STATS.considered.toLocaleString()} = ${pct}%`);
    console.log(`PMMS fallback:   ${STATS.pmms.toLocaleString()}`);
    console.log(`No rate found:   ${STATS.estimated.toLocaleString()}`);
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
