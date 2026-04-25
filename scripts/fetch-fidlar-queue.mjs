// Pull N random Fidlar document numbers from mortgage_records for bulk scraping.
import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 1,
});
const n = parseInt(process.argv[2] || "30");
const client = await pool.connect();
try {
  const r = await client.query(
    `SELECT document_number, source_url
       FROM mortgage_records
      WHERE source_url LIKE '%fidlar%'
        AND document_number IS NOT NULL
        AND document_number <> ''
        AND document_number ~ '^[0-9]+$'
      ORDER BY random() LIMIT $1`,
    [n]
  );
  console.log(JSON.stringify(r.rows, null, 2));
} finally {
  client.release();
  await pool.end();
}
