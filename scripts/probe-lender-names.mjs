import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
  statement_timeout: 60000,
});
const c = await pool.connect();
try {
  console.log("--- mortgage_records columns matching lender/lender_name ---");
  const cols = await c.query(
    `SELECT column_name, data_type FROM information_schema.columns
      WHERE table_name='mortgage_records'
        AND (column_name ILIKE '%lender%' OR column_name ILIKE '%mortgagee%' OR column_name ILIKE '%beneficiary%')`
  );
  console.log(cols.rows);

  console.log("\n--- sample lender name values ---");
  // Try common column names
  for (const col of ["lender_name", "lender", "mortgagee", "mortgagee_name", "beneficiary", "beneficiary_name"]) {
    try {
      const r = await c.query(
        `SELECT ${col}, COUNT(*) AS n FROM mortgage_records WHERE ${col} IS NOT NULL AND ${col} <> '' GROUP BY ${col} ORDER BY n DESC LIMIT 15`
      );
      if (r.rows.length) {
        console.log(`\n[${col}] top 15:`);
        for (const row of r.rows) console.log(`  ${row.n.toString().padStart(8)}  ${row[col]}`);
        const d = await c.query(`SELECT COUNT(DISTINCT ${col}) AS dist FROM mortgage_records WHERE ${col} IS NOT NULL AND ${col} <> ''`);
        console.log(`  DISTINCT: ${d.rows[0].dist}`);
      }
    } catch (e) {
      // column doesn't exist
    }
  }
} finally {
  c.release();
  await pool.end();
}
