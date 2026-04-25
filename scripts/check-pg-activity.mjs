import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 1,
  statement_timeout: 15000,
});
const c = await pool.connect();
try {
  const r = await c.query(
    `SELECT pid, state, wait_event_type, wait_event, now()-query_start AS dur, substring(query,1,80) AS q
       FROM pg_stat_activity
      WHERE query ILIKE '%mortgage_records%' OR query ILIKE '%pmms_weekly%'
      ORDER BY query_start`
  );
  for (const row of r.rows) {
    console.log(row.pid, row.state, row.wait_event_type || "-", row.dur, "|", row.q);
  }
} finally {
  c.release();
  await pool.end();
}
