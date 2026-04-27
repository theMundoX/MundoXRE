#!/usr/bin/env tsx
/**
 * Apply mortgage_records migration via pg-meta HTTP API.
 * Uses the same MXRE_PG_URL + MXRE_SUPABASE_SERVICE_KEY that all
 * production code uses — no direct Postgres password needed.
 */
import "dotenv/config";

const PG_URL = process.env.MXRE_PG_URL || process.env.SUPABASE_URL && `${process.env.SUPABASE_URL}/pg/query` || "";
const SVC_KEY = process.env.MXRE_SUPABASE_SERVICE_KEY || process.env.SUPABASE_SERVICE_KEY || "";

if (!PG_URL) throw new Error("MXRE_PG_URL (or SUPABASE_URL) not set in .env");
if (!SVC_KEY) throw new Error("MXRE_SUPABASE_SERVICE_KEY (or SUPABASE_SERVICE_KEY) not set in .env");

async function pg(query: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: {
      "apikey": SVC_KEY,
      "Authorization": `Bearer ${SVC_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`pg-meta ${res.status}: ${text}`);
  }
  return res.json();
}

const STATEMENTS = [
  {
    label: "ADD COLUMN legal_description",
    sql: `ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS legal_description TEXT`,
  },
  {
    label: "ADD COLUMN raw",
    sql: `ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS raw JSONB`,
  },
  {
    label: "ADD COLUMN rate_source",
    sql: `ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS rate_source TEXT CHECK (rate_source IN ('estimated', 'recorded'))`,
  },
  {
    label: "CREATE INDEX idx_mort_docnum_county",
    sql: `CREATE UNIQUE INDEX IF NOT EXISTS idx_mort_docnum_county ON mortgage_records (document_number, county_fips) WHERE document_number IS NOT NULL AND county_fips IS NOT NULL`,
  },
  {
    label: "CREATE INDEX idx_mort_county_fips",
    sql: `CREATE INDEX IF NOT EXISTS idx_mort_county_fips ON mortgage_records (county_fips) WHERE property_id IS NULL`,
  },
  {
    label: "CREATE INDEX idx_mort_rate_source",
    sql: `CREATE INDEX IF NOT EXISTS idx_mort_rate_source ON mortgage_records (rate_source, recording_date) WHERE original_amount IS NOT NULL`,
  },
  {
    label: "CREATE INDEX idx_mort_amount_type",
    sql: `CREATE INDEX IF NOT EXISTS idx_mort_amount_type ON mortgage_records (document_type, original_amount) WHERE original_amount IS NOT NULL`,
  },
];

async function main() {
  console.log("Applying mortgage_records migration...");
  console.log(`Endpoint: ${PG_URL}`);

  // Verify connectivity first
  const ver = await pg("SELECT version()");
  console.log(`Connected: ${ver[0]?.version?.split(" ").slice(0, 2).join(" ")}\n`);

  for (const { label, sql } of STATEMENTS) {
    process.stdout.write(`  ${label}... `);
    try {
      await pg(sql);
      console.log("✅");
    } catch (err: any) {
      if (err.message.includes("already exists")) {
        console.log("(already exists)");
      } else {
        console.log(`❌ ${err.message.slice(0, 120)}`);
      }
    }
  }

  // Verify columns
  const cols = await pg(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'mortgage_records'
      AND column_name IN ('legal_description','raw','rate_source','county_fips')
    ORDER BY column_name
  `);
  console.log("\nVerified columns:");
  cols.forEach((r: any) => console.log(`  ✅ ${r.column_name} (${r.data_type})`));

  // Verify indexes
  const idxs = await pg(`
    SELECT indexname FROM pg_indexes
    WHERE tablename = 'mortgage_records'
    ORDER BY indexname
  `);
  console.log("\nIndexes:");
  idxs.forEach((r: any) => console.log(`  ${r.indexname}`));

  console.log("\nMigration complete.");
}

main().catch(e => { console.error(e); process.exit(1); });
