#!/usr/bin/env tsx
/**
 * Finish migration 005 by issuing each missing statement individually.
 * pg-meta times out responses but the queries succeed in the background.
 * Strategy: fire each statement, sleep, verify, move on.
 */
import "dotenv/config";

const URL = process.env.SUPABASE_URL!;
const KEY = process.env.SUPABASE_SERVICE_KEY!;

async function fire(query: string): Promise<void> {
  // 8-second client timeout. Server may have already completed the work.
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 8000);
  try {
    await fetch(`${URL}/pg/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: KEY,
        Authorization: `Bearer ${KEY}`,
      },
      body: JSON.stringify({ query }),
      signal: ctrl.signal,
    });
  } catch {
    // Ignore — pg-meta times out but the work happens server-side
  } finally {
    clearTimeout(t);
  }
}

async function query<T = any>(sql: string): Promise<T> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 10000);
  try {
    const r = await fetch(`${URL}/pg/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: KEY,
        Authorization: `Bearer ${KEY}`,
      },
      body: JSON.stringify({ query: sql }),
      signal: ctrl.signal,
    });
    return (await r.json()) as T;
  } finally {
    clearTimeout(t);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function ensureColumn(table: string, column: string, ddl: string) {
  process.stdout.write(`  ${table}.${column} ... `);
  const before = await query<any[]>(
    `SELECT 1 FROM information_schema.columns WHERE table_name='${table}' AND column_name='${column}'`,
  );
  if (Array.isArray(before) && before.length > 0) {
    console.log("✓ exists");
    return;
  }
  await fire(ddl);
  await sleep(2000);
  const after = await query<any[]>(
    `SELECT 1 FROM information_schema.columns WHERE table_name='${table}' AND column_name='${column}'`,
  );
  if (Array.isArray(after) && after.length > 0) console.log("✅ added");
  else console.log("⏳ not yet visible (maybe slower) — continuing");
}

async function ensureTable(table: string, ddl: string) {
  process.stdout.write(`  table ${table} ... `);
  const before = await query<any[]>(
    `SELECT 1 FROM information_schema.tables WHERE table_name='${table}'`,
  );
  if (Array.isArray(before) && before.length > 0) {
    console.log("✓ exists");
    return;
  }
  await fire(ddl);
  await sleep(2500);
  const after = await query<any[]>(
    `SELECT 1 FROM information_schema.tables WHERE table_name='${table}'`,
  );
  if (Array.isArray(after) && after.length > 0) console.log("✅ created");
  else console.log("⏳ not yet visible — continuing");
}

async function main() {
  console.log("=== FINISH MIGRATION 005 ===\n");

  console.log("[A] properties CDC columns");
  await ensureColumn("properties", "current_hash", `ALTER TABLE properties ADD COLUMN IF NOT EXISTS current_hash BYTEA`);
  await ensureColumn("properties", "last_seen_at", `ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ`);
  await ensureColumn("properties", "last_changed_at", `ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_changed_at TIMESTAMPTZ`);
  await ensureColumn("properties", "record_status", `ALTER TABLE properties ADD COLUMN IF NOT EXISTS record_status TEXT NOT NULL DEFAULT 'active'`);

  console.log("\n[B] property_history table");
  await ensureTable("property_history", `
    CREATE TABLE IF NOT EXISTS property_history (
      id BIGSERIAL PRIMARY KEY,
      property_id INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      hash BYTEA NOT NULL,
      changed_fields TEXT[],
      snapshot JSONB NOT NULL
    )
  `);

  console.log("\n[C] property_sales_history table");
  await ensureTable("property_sales_history", `
    CREATE TABLE IF NOT EXISTS property_sales_history (
      id BIGSERIAL PRIMARY KEY,
      property_id INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
      sale_date DATE NOT NULL,
      sale_price BIGINT,
      sale_type TEXT,
      buyer_name TEXT,
      seller_name TEXT,
      document_number TEXT,
      document_type TEXT,
      source TEXT NOT NULL DEFAULT 'recorder',
      source_url TEXT,
      raw JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  console.log("\n[D] mls_listings table");
  await ensureTable("mls_listings", `
    CREATE TABLE IF NOT EXISTS mls_listings (
      id BIGSERIAL PRIMARY KEY,
      property_id INTEGER REFERENCES properties(id) ON DELETE SET NULL,
      mls_id TEXT, mls_number TEXT,
      source TEXT NOT NULL, source_url TEXT,
      status TEXT NOT NULL,
      list_date DATE, status_date DATE, off_market_date DATE,
      days_on_market INTEGER,
      list_price BIGINT, original_list_price BIGINT, current_price BIGINT, sold_price BIGINT, price_per_sqft BIGINT,
      beds INTEGER, baths NUMERIC(4,1), sqft INTEGER, lot_sqft INTEGER, year_built INTEGER, property_type TEXT,
      list_agent_name TEXT, list_agent_email TEXT, list_agent_phone TEXT, list_brokerage TEXT,
      buyer_agent_name TEXT, buyer_brokerage TEXT,
      address TEXT, city TEXT, state_code CHAR(2), zip TEXT, lat NUMERIC(9,6), lng NUMERIC(9,6),
      public_remarks TEXT, private_remarks TEXT,
      photo_urls TEXT[], primary_photo_url TEXT,
      raw JSONB,
      current_hash BYTEA, last_seen_at TIMESTAMPTZ, last_changed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (source, mls_id, mls_number)
    )
  `);

  console.log("\n[E] mortgage_records.rate_source");
  await ensureColumn(
    "mortgage_records",
    "rate_source",
    `ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS rate_source TEXT`,
  );

  console.log("\n[F] indexes (best-effort, non-blocking)");
  const indexes = [
    `CREATE INDEX IF NOT EXISTS idx_mort_rate_source ON mortgage_records(rate_source) WHERE rate_source IS NOT NULL`,
    `CREATE INDEX IF NOT EXISTS idx_prop_last_seen ON properties(last_seen_at)`,
    `CREATE INDEX IF NOT EXISTS idx_prop_last_changed ON properties(last_changed_at)`,
    `CREATE INDEX IF NOT EXISTS idx_sales_prop ON property_sales_history(property_id, sale_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_mls_status ON mls_listings(status, status_date DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_mls_active ON mls_listings(status, state_code, zip) WHERE status IN ('active','pending','coming_soon')`,
  ];
  for (const idx of indexes) {
    await fire(idx);
    await sleep(800);
  }
  console.log("  fired all index DDLs");

  console.log("\n[G] flagging existing populated rates as 'estimated' (batches of 50K)");
  let totalFlagged = 0;
  const startTime = Date.now();
  for (let i = 0; i < 30; i++) {
    // 30 batches × 50K = 1.5M ceiling, plenty for the 464K target
    await fire(`
      WITH batch AS (
        SELECT id FROM mortgage_records
         WHERE interest_rate IS NOT NULL
           AND rate_source IS NULL
         LIMIT 50000
      )
      UPDATE mortgage_records m SET rate_source = 'estimated'
        FROM batch WHERE m.id = batch.id
    `);
    await sleep(3000);
    const r = await query<any[]>(
      `SELECT COUNT(*)::int AS n FROM mortgage_records WHERE rate_source = 'estimated'`,
    );
    const n = (Array.isArray(r) && r[0]?.n) || 0;
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`  batch ${i + 1}: total flagged so far = ${n.toLocaleString()}  (${elapsed}s)`);
    if (n === totalFlagged && i > 0) {
      console.log("  no progress on last batch — assuming done");
      break;
    }
    totalFlagged = n;

    // Stop if we've hit the expected count
    const remaining = await query<any[]>(
      `SELECT COUNT(*)::int AS n FROM mortgage_records WHERE interest_rate IS NOT NULL AND rate_source IS NULL`,
    );
    const r2 = (Array.isArray(remaining) && remaining[0]?.n) || 0;
    if (r2 === 0) {
      console.log("  ✅ no more unflagged populated rates");
      break;
    }
  }

  console.log("\n=== FINAL VERIFY ===");
  const final = await query(`
    SELECT
      (SELECT count(*) FROM mortgage_records WHERE rate_source='estimated') AS estimated,
      (SELECT count(*) FROM mortgage_records WHERE rate_source='recorded')  AS recorded,
      (SELECT count(*) FROM mortgage_records WHERE rate_source IS NULL AND interest_rate IS NOT NULL) AS leaked,
      (SELECT count(*) FROM information_schema.tables WHERE table_name IN ('mls_listings','property_sales_history','property_history','change_events','mls_history')) AS new_tables
  `);
  console.log(JSON.stringify(final, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
