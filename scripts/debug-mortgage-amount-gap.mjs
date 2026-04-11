import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({ host: "207.244.225.239", port: 5432, database: "postgres", user: "postgres.your-tenant-id", password: "d6168ff6e8d9559d62642418bafb3d17", max: 1 });
const c = await pool.connect();

console.log("=== Missing loan_amount investigation ===\n");

// 1. By document_type, which "mortgage" records are missing amount?
console.log("Mortgage-type records missing loan_amount, by source platform:");
const r1 = await c.query(`
  SELECT
    CASE
      WHEN source_url LIKE '%fidlar%' THEN 'fidlar'
      WHEN source_url LIKE '%publicsearch%' THEN 'publicsearch'
      WHEN source_url LIKE '%hamiltoncountyohio%' THEN 'acclaim-hamilton'
      WHEN source_url LIKE '%nj-sr1a%' THEN 'nj-sr1a'
      WHEN source_url LIKE '%landmark%' THEN 'landmark'
      ELSE 'other'
    END AS platform,
    COUNT(*) AS total,
    COUNT(loan_amount) AS has_amount,
    ROUND(100.0 * COUNT(loan_amount) / COUNT(*), 1) AS pct
  FROM mortgage_records
  WHERE document_type = 'mortgage'
  GROUP BY platform
  ORDER BY total DESC`);
for (const row of r1.rows) console.log(`  ${row.platform.padEnd(20)} total=${String(row.total).padStart(10)}  has_amount=${String(row.has_amount).padStart(10)}  ${row.pct}%`);

// 2. Are there OTHER amount-bearing fields populated?
console.log("\n\nOriginal_amount vs loan_amount overlap (mortgage type only):");
const r2 = await c.query(`
  SELECT
    COUNT(*) AS total,
    COUNT(loan_amount) AS has_loan_amount,
    COUNT(original_amount) AS has_original_amount,
    COUNT(*) FILTER (WHERE loan_amount IS NULL AND original_amount IS NOT NULL) AS only_original,
    COUNT(*) FILTER (WHERE loan_amount IS NOT NULL AND original_amount IS NULL) AS only_loan,
    COUNT(*) FILTER (WHERE loan_amount IS NULL AND original_amount IS NULL) AS neither
  FROM mortgage_records
  WHERE document_type = 'mortgage'`);
console.log(r2.rows[0]);

// 3. Can we find the amount in a different column or in raw_text?
console.log("\n\nTotal columns on mortgage_records (in case of more amount fields):");
const cols = await c.query(`SELECT column_name FROM information_schema.columns WHERE table_name='mortgage_records' AND (column_name ILIKE '%amount%' OR column_name ILIKE '%principal%' OR column_name ILIKE '%loan%' OR column_name ILIKE '%balance%' OR column_name ILIKE '%credit%')`);
for (const r of cols.rows) console.log(`  ${r.column_name}`);

c.release();
await pool.end();
