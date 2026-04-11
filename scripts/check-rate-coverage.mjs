import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
  statement_timeout: 30000,
});
const client = await pool.connect();
try {
  const r = await client.query(
    `SELECT
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
       COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
       COUNT(*) FILTER (WHERE rate_source = 'estimated') AS estimated,
       COUNT(*) FILTER (WHERE rate_source IS NULL) AS no_source
     FROM mortgage_records`
  );
  console.log(r.rows[0]);
} finally {
  client.release();
  await pool.end();
}
