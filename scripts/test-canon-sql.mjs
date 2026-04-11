import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
});
const c = await pool.connect();
const TEST = [
  "Wells Fargo Bank, National Association",
  "WELLS FARGO BANK NA",
  "JPMorgan Chase Bank, N.A.",
  "JPMORGAN CHASE BANK NA",
  "U.S. Bank National Association",
  "US BANK NA",
  "FIFTH THIRD BANK",
  "Fifth Third Bank, N.A.",
  "ROCKET MORTGAGE LLC",
  "Rocket Mortgage, LLC",
];
// Approach: pad with spaces, lowercase to upper, strip non-alnum, strip filler tokens via space-bounded match
const SQL = `SELECT
  RTRIM(LTRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        ' ' || UPPER($1) || ' ',
        '[^A-Z0-9]+', ' ', 'g'
      ),
      ' (BANK|NA|N A|NATIONAL|ASSOCIATION|ASSN|INC|LLC|LP|LTD|CORP|CORPORATION|FSB|F S B|COMPANY|CO|FEDERAL|SAVINGS|TRUST|FINANCIAL|FINANCE|MORTGAGE|LENDING|LOAN|LOANS|HOME|GROUP|SERVICES|SERVICING|HOLDINGS|N|TR|ATTY|FA) ', ' ', 'g'
    ),
    '\\s+', ' ', 'g'
  ))) AS canon`;
for (const s of TEST) {
  const r = await c.query(SQL, [s]);
  console.log(`${s.padEnd(45)} -> "${r.rows[0].canon}"`);
}
c.release();
await pool.end();
