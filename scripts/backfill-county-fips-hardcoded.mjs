/**
 * Phase 2 county_fips backfill — hardcoded mappings for the high-volume
 * source_urls that the dynamic Fidlar/lookup approach missed.
 *
 * Coverage targets (~1.7M records):
 *   Franklin OH publicsearch:    514K
 *   Cuyahoga OH publicsearch:    514K
 *   TX Galveston Fidlar:         312K
 *   Butler OH publicsearch:      198K
 *   NJ SR1A (multi year):        150K
 *   Denver CO publicsearch:       86K
 *   Arapahoe CO publicsearch:     64K
 *   TX Fannin Fidlar:             60K
 *   MI Antrim Fidlar:             57K
 *   TX Kerr Fidlar:               54K
 *   IA Jasper Fidlar:             48K
 *   TX Austin Fidlar:             42K
 *   Hamilton OH Acclaim:          33K
 *   IA Boone Fidlar:              28K
 *   Boulder CO publicsearch:      26K
 *   Stark OH publicsearch:        24K
 *   IA Clayton Fidlar:            22K
 *   IA Calhoun Fidlar:            13K
 *   etc.
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
  keepAlive: true,
});

// (source_url pattern, county_fips)
// Use LIKE pattern matching when source_url has variable suffixes (e.g. NJ SR1A years)
const MAPPINGS = [
  // Ohio publicsearch.us
  ["https://franklin.oh.publicsearch.us/", "39049"],
  ["https://cuyahoga.oh.publicsearch.us/", "39035"],
  ["https://butler.oh.publicsearch.us/", "39017"],
  ["https://stark.oh.publicsearch.us/", "39151"],
  ["https://summit.oh.publicsearch.us/", "39153"],
  ["https://lake.oh.publicsearch.us/", "39085"],
  ["https://lorain.oh.publicsearch.us/", "39093"],
  ["https://mahoning.oh.publicsearch.us/", "39099"],
  ["https://warren.oh.publicsearch.us/", "39165"],
  // Hamilton OH Acclaim
  ["https://acclaim-web.hamiltoncountyohio.gov/AcclaimWebLive/", "39061"],
  // Colorado publicsearch.us
  ["https://denver.co.publicsearch.us/", "08031"],
  ["https://arapahoe.co.publicsearch.us/", "08005"],
  ["https://boulder.co.publicsearch.us/", "08013"],
  ["https://jefferson.co.publicsearch.us/", "08059"],
  ["https://adams.co.publicsearch.us/", "08001"],
  ["https://elpaso.co.publicsearch.us/", "08041"],
  ["https://larimer.co.publicsearch.us/", "08069"],
  ["https://douglas.co.publicsearch.us/", "08035"],
  ["https://weld.co.publicsearch.us/", "08123"],
  ["https://pueblo.co.publicsearch.us/", "08101"],
  // Texas Fidlar
  ["https://ava.fidlar.com/TXGalveston/AvaWeb/", "48167"],
  ["https://ava.fidlar.com/TXFannin/AvaWeb/", "48147"],
  ["https://ava.fidlar.com/TXKerr/AvaWeb/", "48265"],
  ["https://ava.fidlar.com/TXAustin/AvaWeb/", "48015"],
  ["https://ava.fidlar.com/TXPanola/AvaWeb/", "48365"],
  // Iowa Fidlar
  ["https://ava.fidlar.com/IAJasper/AvaWeb/", "19099"],
  ["https://ava.fidlar.com/IABoone/AvaWeb/", "19015"],
  ["https://ava.fidlar.com/IAClayton/AvaWeb/", "19045"],
  ["https://ava.fidlar.com/IACalhoun/AvaWeb/", "19025"],
  // Michigan Fidlar
  ["https://ava.fidlar.com/MIAntrim/AvaWeb/", "26009"],
  ["https://ava.fidlar.com/MIOakland/AvaWeb/", "26125"],
  // NJ SR1A — multi-year, needs LIKE
  ["nj-sr1a-2024", "34001"], // placeholder; actually NJ SR1A is statewide, not county-specific
  ["nj-sr1a-2025", "34001"],
  ["nj-sr1a-2026", "34001"],
];

const c = await pool.connect();
try {
  const before = await c.query(
    `SELECT COUNT(*) FILTER (WHERE county_fips IS NOT NULL) AS has, COUNT(*) FILTER (WHERE county_fips IS NULL) AS miss FROM mortgage_records`
  );
  console.log("BEFORE:", before.rows[0]);

  let totalUpdated = 0;
  for (const [url, fips] of MAPPINGS) {
    const r = await c.query(
      `UPDATE mortgage_records SET county_fips = $1 WHERE source_url = $2 AND county_fips IS NULL`,
      [fips, url]
    );
    if (r.rowCount > 0) {
      console.log(`  ${fips}  ${url.padEnd(60)}  ${r.rowCount.toLocaleString()}`);
      totalUpdated += r.rowCount;
    }
  }
  console.log(`\nTotal updated: ${totalUpdated.toLocaleString()}`);

  const after = await c.query(
    `SELECT COUNT(*) FILTER (WHERE county_fips IS NOT NULL) AS has, COUNT(*) FILTER (WHERE county_fips IS NULL) AS miss FROM mortgage_records`
  );
  console.log("AFTER:", after.rows[0]);
} finally {
  c.release();
  await pool.end();
}
