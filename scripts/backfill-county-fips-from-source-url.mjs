/**
 * Backfill county_fips on mortgage_records BY PARSING source_url.
 *
 * Many records have NULL county_fips because they're not linked to a property,
 * but their source_url encodes the county directly (e.g. /OHFairfield/, /TXGalveston/).
 * This script extracts state+county from the URL and looks up the FIPS code.
 *
 * Patterns handled:
 *   - https://ava.fidlar.com/{ST}{County}/AvaWeb/        (Fidlar AVA, ~4.13M records)
 *   - https://officialrecords.{county}clerk.com/...      (Florida LandmarkWeb)
 *   - https://{county}.{state}.publicsearch.us/          (PublicSearch)
 *
 * Effect: enables HMDA strategy-2/3 matching on millions of previously
 * un-county-tagged records.
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

const c = await pool.connect();
try {
  console.log("=== source_url -> county_fips backfill ===");

  // 1. Counts before
  const before = await c.query(
    `SELECT
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE county_fips IS NOT NULL) AS has_fips,
       COUNT(*) FILTER (WHERE county_fips IS NULL) AS missing_fips
     FROM mortgage_records`
  );
  console.log("BEFORE:", before.rows[0]);

  // 2. Build a temp lookup of (source_url_pattern, state_code, county_name) → fips
  // For Fidlar AVA: source_url like '%/STCountyName/AvaWeb/'
  // The token is the part right before /AvaWeb/. e.g. OHFairfield = OH + Fairfield
  console.log("\nBuilding source_url -> county map for Fidlar AVA pattern...");
  const fidlarPatterns = await c.query(
    `SELECT DISTINCT
       source_url,
       SUBSTRING(source_url FROM 'fidlar\\.com/([^/]+)/AvaWeb') AS slug
     FROM mortgage_records
     WHERE source_url LIKE '%fidlar.com/%/AvaWeb%'
       AND county_fips IS NULL`
  );
  console.log(`Found ${fidlarPatterns.rows.length} distinct Fidlar source_urls`);

  const updates = [];
  for (const row of fidlarPatterns.rows) {
    const slug = row.slug;
    if (!slug || slug.length < 3) continue;
    // First 2 chars = state
    const state = slug.slice(0, 2).toUpperCase();
    // Rest = county name in CamelCase, e.g. "BlackHawk", "Fairfield"
    const camel = slug.slice(2);
    // Convert "BlackHawk" -> "Black Hawk" by inserting space before each uppercase except first
    const countyName = camel.replace(/([a-z])([A-Z])/g, "$1 $2");
    // Look up in counties table
    const countyRes = await c.query(
      `SELECT state_fips || county_fips AS fips
         FROM counties
        WHERE state_code = $1
          AND county_name ILIKE $2
        LIMIT 1`,
      [state, countyName]
    );
    if (countyRes.rows.length) {
      updates.push({ url: row.source_url, fips: countyRes.rows[0].fips, label: `${state}/${countyName}` });
    } else {
      // Try without space normalization
      const alt = await c.query(
        `SELECT state_fips || county_fips AS fips, county_name
           FROM counties
          WHERE state_code = $1
            AND REPLACE(county_name, ' ', '') ILIKE $2
          LIMIT 1`,
        [state, camel]
      );
      if (alt.rows.length) {
        updates.push({ url: row.source_url, fips: alt.rows[0].fips, label: `${state}/${alt.rows[0].county_name} (alt)` });
      }
    }
  }
  console.log(`Mapped ${updates.length} / ${fidlarPatterns.rows.length} Fidlar source_urls to FIPS`);

  // 3. Apply updates
  let totalUpdated = 0;
  for (const u of updates) {
    const r = await c.query(
      `UPDATE mortgage_records SET county_fips = $1
        WHERE source_url = $2 AND county_fips IS NULL`,
      [u.fips, u.url]
    );
    totalUpdated += r.rowCount;
    if (r.rowCount > 1000) console.log(`  ${u.label.padEnd(30)} ${r.rowCount.toLocaleString()} rows`);
  }
  console.log(`\nTotal mortgage_records updated: ${totalUpdated.toLocaleString()}`);

  // 4. Counts after
  const after = await c.query(
    `SELECT
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE county_fips IS NOT NULL) AS has_fips,
       COUNT(*) FILTER (WHERE county_fips IS NULL) AS missing_fips
     FROM mortgage_records`
  );
  console.log("AFTER:", after.rows[0]);
} finally {
  c.release();
  await pool.end();
}
