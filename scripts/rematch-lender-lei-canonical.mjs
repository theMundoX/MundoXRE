/**
 * Aggressive canonical normalization + re-match.
 *
 * Canon strategy:
 *   1. UPPER, replace non-alphanum with space
 *   2. Strip filler tokens: NA, NATIONAL ASSOCIATION, FSB, F S B, INC, LLC, LP, LTD, CORP, COMPANY, CO, BANK
 *   3. Remove standalone single-letter tokens
 *   4. Collapse whitespace
 *
 * "Wells Fargo Bank, National Association" -> "WELLS FARGO"
 * "WELLS FARGO BANK NA"                    -> "WELLS FARGO"
 * "U.S. Bank, N.A."                        -> "US"
 * "U S BANK NATIONAL ASSOCIATION"          -> "US"
 *
 * Caveat: collisions on common short forms (e.g. "First National Bank" -> "FIRST")
 * are acceptable because we'll combine with state/county/amount in the matcher.
 */
import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
  statement_timeout: 0,
});

const CANON_SQL = `
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            UPPER(<INPUT>),
            '[^A-Z0-9 ]+', ' ', 'g'
          ),
          '\\\\b(NA|FSB|F S B|N A|NATIONAL ASSOCIATION|INC|LLC|LP|LTD|CORP|CORPORATION|COMPANY|CO|BANK|FEDERAL|SAVINGS|TRUST|FINANCIAL|MORTGAGE|LENDING|LOAN|LOANS|CREDIT|UNION|HOME|LOANS|GROUP|SERVICES|HOLDINGS|N|NA)\\\\b', ' ', 'g'
        ),
        '\\\\b[A-Z]\\\\b', ' ', 'g'
      ),
      '\\\\s+', ' ', 'g'
    )
  )
`;

const c = await pool.connect();
try {
  console.log("=== Canonical re-match ===");

  // 1. Backfill canonical on crosswalk
  console.log("Computing canonical for hmda_lender_crosswalk...");
  const t0 = Date.now();
  await c.query(
    `UPDATE hmda_lender_crosswalk
        SET respondent_name_canon = ${CANON_SQL.replace("<INPUT>", "respondent_name")}`
  );
  console.log(`Done in ${((Date.now() - t0) / 1000).toFixed(0)}s`);

  // 2. Backfill canonical on map
  console.log("Computing canonical for mortgage_lender_lei_map...");
  const t1 = Date.now();
  await c.query(
    `UPDATE mortgage_lender_lei_map
        SET lender_name_canon = ${CANON_SQL.replace("<INPUT>", "lender_name")}`
  );
  console.log(`Done in ${((Date.now() - t1) / 1000).toFixed(0)}s`);

  // 3. Re-run match using canonical name (only for rows still unmatched)
  console.log("Running canonical exact match (only rows still unmatched)...");
  const t2 = Date.now();
  const r = await c.query(
    `UPDATE mortgage_lender_lei_map m
        SET lei = sub.lei,
            match_type = 'canon_exact',
            match_confidence = 85,
            matched_at = now()
       FROM (
         SELECT DISTINCT ON (respondent_name_canon) respondent_name_canon, lei
           FROM hmda_lender_crosswalk
          WHERE respondent_name_canon <> ''
          ORDER BY respondent_name_canon, activity_year DESC
       ) sub
      WHERE m.lender_name_canon = sub.respondent_name_canon
        AND m.lender_name_canon <> ''
        AND m.lei IS NULL`
  );
  console.log(`Canon-matched ${r.rowCount.toLocaleString()} more rows in ${((Date.now() - t2) / 1000).toFixed(0)}s`);

  // 4. Stats
  const stats = await c.query(
    `SELECT
       COUNT(*) AS total_distinct_lenders,
       COUNT(*) FILTER (WHERE lei IS NOT NULL) AS matched_total,
       COUNT(*) FILTER (WHERE match_type = 'exact') AS matched_exact,
       COUNT(*) FILTER (WHERE match_type = 'canon_exact') AS matched_canon,
       COUNT(*) FILTER (WHERE lei IS NULL) AS unmatched
     FROM mortgage_lender_lei_map`
  );
  console.log("Map stats:", stats.rows[0]);

  // 5. Volume coverage
  const cov = await c.query(
    `SELECT
       COUNT(*) AS total_mortgages,
       COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM mortgage_lender_lei_map m WHERE m.lender_name = mr.lender_name AND m.lei IS NOT NULL)) AS would_have_lei
     FROM mortgage_records mr
     WHERE lender_name IS NOT NULL AND lender_name <> ''`
  );
  const pct = ((Number(cov.rows[0].would_have_lei) / Number(cov.rows[0].total_mortgages)) * 100).toFixed(1);
  console.log(`Mortgage volume coverage: ${cov.rows[0].would_have_lei.toLocaleString()} / ${cov.rows[0].total_mortgages.toLocaleString()} = ${pct}%`);
} finally {
  c.release();
  await pool.end();
}
