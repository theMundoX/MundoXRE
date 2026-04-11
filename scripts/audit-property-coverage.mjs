/**
 * Audit MXRE's current data footprint:
 *   - properties table size, by state
 *   - mortgage_records linked vs unlinked
 *   - county scraper coverage
 *   - HMDA / agency_lld / pmms_weekly / hmda_lender_crosswalk row counts
 *   - rate_source breakdown
 *
 * Writes a JSON snapshot to data/mxre-audit-{timestamp}.json
 */
import pkg from "pg";
import { writeFileSync } from "fs";
const { Pool } = pkg;

const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
  statement_timeout: 120000,
});

async function q(client, sql, params = []) {
  const r = await client.query(sql, params);
  return r.rows;
}

async function main() {
  const c = await pool.connect();
  try {
    const audit = { generated_at: new Date().toISOString() };

    // 1. Top-level table sizes
    audit.tables = await q(
      c,
      `SELECT relname AS table, n_live_tup AS rows
         FROM pg_stat_user_tables
        WHERE relname IN ('properties','mortgage_records','hmda_lar','agency_lld',
                          'pmms_weekly','hmda_lender_crosswalk','counties',
                          'mortgage_rate_matches','tax_records','sales_history',
                          'liens','assessments','owners')
        ORDER BY n_live_tup DESC`
    );

    // 2. mortgage_records breakdown
    audit.mortgage_records = (
      await q(
        c,
        `SELECT
           COUNT(*) AS total,
           COUNT(*) FILTER (WHERE property_id IS NOT NULL) AS linked,
           COUNT(*) FILTER (WHERE property_id IS NULL) AS unlinked,
           COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
           COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
           COUNT(*) FILTER (WHERE rate_source = 'agency_match') AS agency,
           COUNT(*) FILTER (WHERE interest_rate IS NULL) AS no_rate,
           COUNT(*) FILTER (WHERE county_fips IS NOT NULL) AS has_county_fips,
           COUNT(*) FILTER (WHERE recording_date IS NOT NULL) AS has_recording_date,
           MIN(recording_date) AS min_date,
           MAX(recording_date) AS max_date
         FROM mortgage_records`
      )
    )[0];

    // 3. mortgage_records by source_url host
    audit.mortgage_records_by_source = await q(
      c,
      `SELECT
         CASE
           WHEN source_url LIKE '%fidlar%' THEN 'fidlar'
           WHEN source_url LIKE '%granicus%' THEN 'granicus'
           WHEN source_url LIKE '%landmark%' THEN 'landmark'
           WHEN source_url LIKE '%tylertech%' OR source_url LIKE '%eaglesoftware%' THEN 'tyler-eagle'
           WHEN source_url LIKE '%recordsfinder%' THEN 'recordsfinder'
           WHEN source_url IS NULL THEN 'NULL'
           ELSE 'other'
         END AS platform,
         COUNT(*) AS n
       FROM mortgage_records
       GROUP BY 1
       ORDER BY n DESC`
    );

    // 4. properties table
    try {
      audit.properties = (
        await q(
          c,
          `SELECT COUNT(*) AS total,
                  COUNT(DISTINCT county_id) AS counties_covered
             FROM properties`
        )
      )[0];

      audit.properties_by_state = await q(
        c,
        `SELECT c.state_code, COUNT(p.id) AS n
           FROM properties p
           LEFT JOIN counties c ON c.id = p.county_id
          GROUP BY c.state_code
          ORDER BY n DESC
          LIMIT 30`
      );
    } catch (e) {
      audit.properties_error = e.message;
    }

    // 5. counties table - coverage
    try {
      audit.counties = (
        await q(c, `SELECT COUNT(*) AS total, COUNT(DISTINCT state_code) AS states FROM counties`)
      )[0];
    } catch (e) {
      audit.counties_error = e.message;
    }

    // 6. agency_lld
    try {
      audit.agency_lld = await q(
        c,
        `SELECT source, vintage_year, COUNT(*) AS n,
                ROUND(AVG(origination_rate)::numeric, 3) AS avg_rate,
                ROUND(AVG(original_upb)::numeric, 0) AS avg_upb
           FROM agency_lld
          GROUP BY source, vintage_year
          ORDER BY vintage_year DESC, source`
      );
    } catch (e) {
      audit.agency_lld_error = e.message;
    }

    // 7. hmda_lar progress
    try {
      audit.hmda_lar = await q(
        c,
        `SELECT activity_year, COUNT(*) AS n,
                ROUND(AVG(interest_rate)::numeric, 3) AS avg_rate
           FROM hmda_lar
          GROUP BY activity_year
          ORDER BY activity_year`
      );
    } catch (e) {
      audit.hmda_lar_error = e.message;
    }

    // 8. By state - mortgage volume
    audit.mortgage_by_state = await q(
      c,
      `SELECT c.state_code, COUNT(m.id) AS n
         FROM mortgage_records m
         LEFT JOIN properties p ON p.id = m.property_id
         LEFT JOIN counties c ON c.id = p.county_id
        GROUP BY c.state_code
        ORDER BY n DESC NULLS LAST
        LIMIT 30`
    );

    // 9. By county - mortgage volume top 30
    audit.mortgage_by_county = await q(
      c,
      `SELECT c.state_code || '/' || c.county_name AS county, COUNT(m.id) AS n
         FROM mortgage_records m
         JOIN properties p ON p.id = m.property_id
         JOIN counties c ON c.id = p.county_id
        GROUP BY c.state_code, c.county_name
        ORDER BY n DESC
        LIMIT 30`
    );

    const ts = Date.now();
    const path = `C:/Users/msanc/mxre/data/mxre-audit-${ts}.json`;
    writeFileSync(path, JSON.stringify(audit, null, 2));
    console.log("---SUMMARY---");
    console.log("Tables:", audit.tables);
    console.log("Mortgage records:", audit.mortgage_records);
    console.log("Mortgage by platform:", audit.mortgage_records_by_source);
    if (audit.properties) console.log("Properties:", audit.properties);
    if (audit.counties) console.log("Counties:", audit.counties);
    if (audit.agency_lld) console.log("Agency LLD:", audit.agency_lld);
    if (audit.hmda_lar) console.log("HMDA loaded:", audit.hmda_lar);
    console.log(`\nFull JSON: ${path}`);
  } finally {
    c.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
