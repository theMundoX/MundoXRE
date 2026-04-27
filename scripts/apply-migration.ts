#!/usr/bin/env tsx
/**
 * Apply mortgage_records migration directly via pg client.
 * Adds:
 *   - legal_description TEXT
 *   - raw JSONB
 *   - rate_source TEXT CHECK (estimated|recorded)
 *   - Unique index on (document_number, county_fips) for upsert dedup
 *   - Index on county_fips for linking queries
 */
import "dotenv/config";
import pg from "pg";

const { Client } = pg;

// Self-hosted Supabase uses Supavisor on port 5432 which requires "user.tenant" format.
// Try direct Postgres on 5432 first, then fallback to Supavisor format.
const DB_HOST = process.env.DB_HOST || (() => { throw new Error("DB_HOST not set"); })();

const client = new Client({
  host: DB_HOST,
  port: parseInt(process.env.DB_PORT ?? "5432", 10),
  database: "postgres",
  user: "postgres.postgres",
  password: process.env.POSTGRES_PASSWORD || (() => { throw new Error("POSTGRES_PASSWORD not set"); })(),
  connectionTimeoutMillis: 10000,
});

const MIGRATION = `
-- Add legal description from recorder instrument (actual text from deed/mortgage)
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS legal_description TEXT;

-- Raw JSON response from the recorder platform (Fidlar, LandmarkWeb, etc.)
-- Used for future OCR parsing, rate extraction, lien chain analysis
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS raw JSONB;

-- Rate source tag: 'estimated' = PMMS historical average, 'recorded' = actual from document
-- CRITICAL: never mix these without the tag. Estimated rates look like real data.
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS rate_source TEXT
  CHECK (rate_source IN ('estimated', 'recorded'));

-- Unique constraint for idempotent upserts from Fidlar/PublicSearch
-- document_number is the instrument/CFN number; county_fips scopes it to one county
CREATE UNIQUE INDEX IF NOT EXISTS idx_mort_docnum_county
  ON mortgage_records (document_number, county_fips)
  WHERE document_number IS NOT NULL AND county_fips IS NOT NULL;

-- Index for linking queries (match unlinked records to properties by county)
CREATE INDEX IF NOT EXISTS idx_mort_county_fips
  ON mortgage_records (county_fips)
  WHERE property_id IS NULL;

-- Index for rate matching queries
CREATE INDEX IF NOT EXISTS idx_mort_rate_source
  ON mortgage_records (rate_source, recording_date)
  WHERE original_amount IS NOT NULL;

-- Index for amount analytics
CREATE INDEX IF NOT EXISTS idx_mort_amount_type
  ON mortgage_records (document_type, original_amount)
  WHERE original_amount IS NOT NULL;
`;

async function main() {
  console.log("Applying mortgage_records migration...");
  console.log(`Host: ${DB_HOST}:${process.env.DB_PORT ?? "5432"}`);

  await client.connect();
  console.log("Connected to Postgres.");

  const statements = MIGRATION
    .split(";")
    .map(s => s.trim())
    .filter(s => s.length > 10);

  for (const stmt of statements) {
    const preview = stmt.replace(/\s+/g, " ").slice(0, 80);
    process.stdout.write(`  ${preview}... `);
    try {
      await client.query(stmt);
      console.log("✅");
    } catch (err: any) {
      if (err.message.includes("already exists")) {
        console.log("(already exists)");
      } else {
        console.log(`❌ ${err.message.slice(0, 100)}`);
      }
    }
  }

  // Verify columns exist
  const { rows } = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'mortgage_records'
      AND column_name IN ('legal_description','raw','rate_source','county_fips')
    ORDER BY column_name;
  `);
  console.log("\nVerified columns:");
  rows.forEach(r => console.log(`  ✅ ${r.column_name} (${r.data_type})`));

  // Check index
  const { rows: idxRows } = await client.query(`
    SELECT indexname FROM pg_indexes
    WHERE tablename = 'mortgage_records'
    ORDER BY indexname;
  `);
  console.log("\nIndexes:");
  idxRows.forEach(r => console.log(`  ${r.indexname}`));

  await client.end();
  console.log("\nMigration complete.");
}

main().catch(e => { console.error(e); process.exit(1); });
