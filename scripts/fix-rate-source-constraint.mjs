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
const client = await pool.connect();
try {
  await client.query(`ALTER TABLE mortgage_records DROP CONSTRAINT IF EXISTS mortgage_records_rate_source_check`);
  await client.query(`ALTER TABLE mortgage_records ADD CONSTRAINT mortgage_records_rate_source_check
    CHECK (rate_source IS NULL OR rate_source IN ('recorded','estimated','pmms_weekly','hmda_match','agency_match'))`);
  console.log("constraint updated");
} finally {
  client.release();
  await pool.end();
}
