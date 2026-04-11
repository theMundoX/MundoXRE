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
const r = await c.query(
  `SELECT id, loan_amount, document_type, FLOOR(loan_amount/10000)*10000+5000 AS bucket, rate_source
     FROM mortgage_records
    WHERE county_fips='39045'
      AND EXTRACT(YEAR FROM recording_date)=2018
      AND rate_source != 'hmda_match'
    LIMIT 10`
);
console.log(r.rows);
c.release();
await pool.end();
