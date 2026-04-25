/**
 * PMMS BASELINE BACKFILL
 *
 * For every mortgage_records row that has no interest_rate (or rate_source='estimated'),
 * assign the Freddie Mac PMMS 30-year weekly rate for the week of its recording_date.
 * Tag rate_source='pmms_weekly' confidence 40.
 *
 * This is a one-shot UPDATE ... FROM that runs as pure SQL on the Contabo VPS.
 * Zero Claude tokens, zero GPU, runs in minutes against millions of rows.
 *
 * Later the HMDA matcher will UPGRADE these rows to higher confidence.
 *
 * Usage:
 *   node scripts/backfill-pmms-baseline.mjs              # dry run (report counts)
 *   node scripts/backfill-pmms-baseline.mjs --apply      # execute
 */
import pkg from "pg";
const { Pool } = pkg;

const APPLY = process.argv.includes("--apply");

const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 2,
  connectionTimeoutMillis: 15000,
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("=== PMMS baseline backfill ===");
    const before = await client.query(
      `SELECT
         COUNT(*) FILTER (WHERE interest_rate IS NULL) AS no_rate,
         COUNT(*) FILTER (WHERE rate_source = 'estimated') AS estimated,
         COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
         COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
         COUNT(*) AS total
       FROM mortgage_records`
    );
    console.log("BEFORE:", before.rows[0]);

    const candidates = await client.query(
      `SELECT COUNT(*) AS n
         FROM mortgage_records m
        WHERE m.recording_date IS NOT NULL
          AND (m.rate_source IS NULL OR m.rate_source = 'estimated' OR m.interest_rate IS NULL)
          AND EXISTS (
            SELECT 1 FROM pmms_weekly p
             WHERE p.week_ending <= m.recording_date
          )`
    );
    console.log(`Candidates for PMMS assignment: ${candidates.rows[0].n.toLocaleString()}`);

    if (!APPLY) {
      console.log("\n(dry run — pass --apply to execute)");
      return;
    }

    console.log("\nRunning UPDATE ... this may take several minutes...");
    const startedAt = Date.now();

    // Find the most recent PMMS week for each candidate via LATERAL JOIN
    const result = await client.query(
      `WITH candidates AS (
         SELECT m.id, m.recording_date
           FROM mortgage_records m
          WHERE m.recording_date IS NOT NULL
            AND (m.rate_source IS NULL OR m.rate_source = 'estimated' OR m.interest_rate IS NULL)
       ),
       matched AS (
         SELECT c.id, p.week_ending, p.rate_30yr_fixed
           FROM candidates c
           CROSS JOIN LATERAL (
             SELECT week_ending, rate_30yr_fixed
               FROM pmms_weekly
              WHERE week_ending <= c.recording_date
                AND rate_30yr_fixed IS NOT NULL
              ORDER BY week_ending DESC
              LIMIT 1
           ) p
       )
       UPDATE mortgage_records m
          SET interest_rate = matched.rate_30yr_fixed,
              rate_source = 'pmms_weekly',
              rate_match_confidence = 40,
              rate_matched_at = now()
         FROM matched
        WHERE m.id = matched.id`
    );
    const secs = (Date.now() - startedAt) / 1000;
    console.log(`Updated ${result.rowCount.toLocaleString()} rows in ${secs.toFixed(0)}s`);

    const after = await client.query(
      `SELECT
         COUNT(*) FILTER (WHERE interest_rate IS NULL) AS no_rate,
         COUNT(*) FILTER (WHERE rate_source = 'estimated') AS estimated,
         COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
         COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
         COUNT(*) AS total
       FROM mortgage_records`
    );
    console.log("AFTER:", after.rows[0]);
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
