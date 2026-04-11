import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({ host: "207.244.225.239", port: 5432, database: "postgres", user: "postgres.your-tenant-id", password: "d6168ff6e8d9559d62642418bafb3d17", max: 1 });
const c = await pool.connect();
const r = await c.query(`
  SELECT document_type, COUNT(*) AS total, COUNT(loan_amount) AS has_amt
  FROM mortgage_records
  GROUP BY document_type
  ORDER BY total DESC
  LIMIT 20`);
for (const row of r.rows) console.log(`${row.document_type?.padEnd(30) ?? "(null)"}  total=${row.total.toLocaleString().padStart(10)}  has_amt=${row.has_amt.toLocaleString().padStart(10)}`);
const tot = await c.query(`SELECT COUNT(*) AS total, COUNT(loan_amount) AS has_amt, COUNT(*) FILTER (WHERE rate_source='hmda_match') AS hmda FROM mortgage_records`);
console.log("\nGRAND TOTAL:", tot.rows[0]);
const eligible = await c.query(`SELECT COUNT(*) FROM mortgage_records WHERE loan_amount IS NOT NULL AND recording_date IS NOT NULL AND county_fips IS NOT NULL AND EXTRACT(YEAR FROM recording_date) BETWEEN 2018 AND 2023`);
console.log("Eligible for HMDA matching (2018-2023):", eligible.rows[0]);
c.release();
await pool.end();
