/**
 * Populate mortgage_lender_lei_map from mortgage_records distinct lender_names.
 * Phase 1: SQL exact-match against hmda_lender_crosswalk.
 *
 * Phase 2 (separate script: lender-lei-mundox.mjs) handles fuzzy residuals via MundoX.
 */
import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 1,
  statement_timeout: 0,
});

const c = await pool.connect();
try {
  console.log("=== Lender LEI map population ===");

  // Step 1: ensure crosswalk has a normalized column. The sub-agent loaded
  // respondent_name_normalized already; just confirm.
  const colCheck = await c.query(
    `SELECT column_name FROM information_schema.columns
      WHERE table_name='hmda_lender_crosswalk' AND column_name='respondent_name_normalized'`
  );
  if (!colCheck.rows.length) throw new Error("hmda_lender_crosswalk.respondent_name_normalized missing");

  // Step 2: insert distinct lender_names (already-known ones get skipped)
  console.log("Inserting distinct lender_name strings...");
  const t0 = Date.now();
  const insertRes = await c.query(
    `INSERT INTO mortgage_lender_lei_map (lender_name, lender_name_normalized)
     SELECT DISTINCT lender_name,
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(lender_name), '[^A-Z0-9]+', ' ', 'g'), '\\s+', ' ', 'g')) AS norm
       FROM mortgage_records
      WHERE lender_name IS NOT NULL AND lender_name <> ''
     ON CONFLICT (lender_name) DO NOTHING`
  );
  console.log(`Inserted ${insertRes.rowCount.toLocaleString()} new rows in ${((Date.now() - t0) / 1000).toFixed(0)}s`);

  // Step 3: exact-match against crosswalk
  console.log("\nRunning exact match against hmda_lender_crosswalk...");
  const t1 = Date.now();
  const matchRes = await c.query(
    `UPDATE mortgage_lender_lei_map m
        SET lei = sub.lei,
            match_type = 'exact',
            match_confidence = 95,
            matched_at = now()
       FROM (
         SELECT DISTINCT ON (respondent_name_normalized) respondent_name_normalized, lei
           FROM hmda_lender_crosswalk
          ORDER BY respondent_name_normalized, activity_year DESC
       ) sub
      WHERE m.lender_name_normalized = sub.respondent_name_normalized
        AND m.lei IS NULL`
  );
  console.log(`Exact-matched ${matchRes.rowCount.toLocaleString()} rows in ${((Date.now() - t1) / 1000).toFixed(0)}s`);

  // Step 4: stats
  const stats = await c.query(
    `SELECT
       COUNT(*) AS total_distinct_lenders,
       COUNT(*) FILTER (WHERE lei IS NOT NULL) AS matched,
       COUNT(*) FILTER (WHERE lei IS NULL) AS unmatched
     FROM mortgage_lender_lei_map`
  );
  console.log("\nMap stats:", stats.rows[0]);

  // Step 5: how many mortgage_records would be covered if we propagate?
  const cov = await c.query(
    `SELECT
       COUNT(*) AS total_mortgages,
       COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM mortgage_lender_lei_map m WHERE m.lender_name = mr.lender_name AND m.lei IS NOT NULL)) AS would_have_lei
     FROM mortgage_records mr
     WHERE lender_name IS NOT NULL AND lender_name <> ''`
  );
  console.log("Mortgage coverage if propagated:", cov.rows[0]);
  const pct = ((Number(cov.rows[0].would_have_lei) / Number(cov.rows[0].total_mortgages)) * 100).toFixed(1);
  console.log(`Coverage: ${pct}%`);
} finally {
  c.release();
  await pool.end();
}
