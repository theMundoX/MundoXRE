#!/usr/bin/env tsx
/**
 * Apply migration 005 in pieces to avoid pg-meta timeouts.
 *   1. DDL only (fast)
 *   2. UPDATE mortgage_records.rate_source in batches (avoids long lock)
 */
import "dotenv/config";

const URL = process.env.SUPABASE_URL!;
const KEY = process.env.SUPABASE_SERVICE_KEY!;

async function pg(query: string): Promise<{ ok: boolean; data: any; status: number }> {
  const resp = await fetch(`${URL}/pg/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: KEY,
      Authorization: `Bearer ${KEY}`,
    },
    body: JSON.stringify({ query }),
  });
  const text = await resp.text();
  let data: any;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  return { ok: resp.ok, data, status: resp.status };
}

async function step(label: string, query: string) {
  process.stdout.write(`  ${label.padEnd(60)} `);
  const r = await pg(query);
  if (r.ok) {
    console.log("✅");
    return true;
  }
  const err = typeof r.data === "object" ? JSON.stringify(r.data) : String(r.data);
  console.log(`❌ HTTP ${r.status}`);
  console.log(`     ${err.slice(0, 300)}`);
  return false;
}

async function main() {
  console.log("=== APPLYING MIGRATION 005 (split) ===\n");

  console.log("[1/6] Properties table additions");
  await step("ADD mailing_address etc", `
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_address    TEXT;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_city       TEXT;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_state      CHAR(2);
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_zip        TEXT;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS owner_occupied     BOOLEAN;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS absentee_owner     BOOLEAN;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS current_hash       BYTEA;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_seen_at       TIMESTAMPTZ;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_changed_at    TIMESTAMPTZ;
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS record_status      TEXT NOT NULL DEFAULT 'active' CHECK (record_status IN ('active','archived','pending_review'));
  `);
  await step("indexes on properties", `
    CREATE INDEX IF NOT EXISTS idx_prop_mailing_state ON properties(mailing_state) WHERE mailing_state IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_prop_absentee      ON properties(absentee_owner) WHERE absentee_owner = true;
    CREATE INDEX IF NOT EXISTS idx_prop_last_seen     ON properties(last_seen_at);
    CREATE INDEX IF NOT EXISTS idx_prop_last_changed  ON properties(last_changed_at);
    CREATE INDEX IF NOT EXISTS idx_prop_status        ON properties(record_status) WHERE record_status != 'active';
  `);

  console.log("\n[2/6] property_history table");
  await step("CREATE property_history", `
    CREATE TABLE IF NOT EXISTS property_history (
      id              BIGSERIAL PRIMARY KEY,
      property_id     INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
      captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      hash            BYTEA NOT NULL,
      changed_fields  TEXT[],
      snapshot        JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_prop_hist_prop ON property_history(property_id, captured_at DESC);
    CREATE INDEX IF NOT EXISTS idx_prop_hist_date ON property_history(captured_at DESC);
  `);

  console.log("\n[3/6] property_sales_history table");
  await step("CREATE property_sales_history", `
    CREATE TABLE IF NOT EXISTS property_sales_history (
      id                BIGSERIAL PRIMARY KEY,
      property_id       INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
      sale_date         DATE NOT NULL,
      sale_price        BIGINT,
      sale_type         TEXT,
      buyer_name        TEXT,
      seller_name       TEXT,
      document_number   TEXT,
      document_type     TEXT,
      source            TEXT NOT NULL DEFAULT 'recorder',
      source_url        TEXT,
      raw               JSONB,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_sales_prop  ON property_sales_history(property_id, sale_date DESC);
    CREATE INDEX IF NOT EXISTS idx_sales_date  ON property_sales_history(sale_date DESC);
    CREATE INDEX IF NOT EXISTS idx_sales_price ON property_sales_history(sale_price) WHERE sale_price IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_sales_doc   ON property_sales_history(document_number);
  `);

  console.log("\n[4/6] mls_listings + mls_history tables");
  await step("CREATE mls_listings", `
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
    );
    CREATE INDEX IF NOT EXISTS idx_mls_property   ON mls_listings(property_id) WHERE property_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_mls_status     ON mls_listings(status, status_date DESC);
    CREATE INDEX IF NOT EXISTS idx_mls_active     ON mls_listings(status, state_code, zip) WHERE status IN ('active','pending','coming_soon');
    CREATE INDEX IF NOT EXISTS idx_mls_zip_date   ON mls_listings(zip, status_date DESC);
    CREATE INDEX IF NOT EXISTS idx_mls_addr_match ON mls_listings(state_code, zip, address) WHERE property_id IS NULL;
    CREATE INDEX IF NOT EXISTS idx_mls_last_seen  ON mls_listings(last_seen_at);
  `);
  await step("CREATE mls_history", `
    CREATE TABLE IF NOT EXISTS mls_history (
      id BIGSERIAL PRIMARY KEY,
      listing_id BIGINT NOT NULL REFERENCES mls_listings(id) ON DELETE CASCADE,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      hash BYTEA NOT NULL,
      changed_fields TEXT[],
      snapshot JSONB NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_mls_hist_listing ON mls_history(listing_id, captured_at DESC);
  `);

  console.log("\n[5/6] change_events queue");
  await step("CREATE change_events", `
    CREATE TABLE IF NOT EXISTS change_events (
      id BIGSERIAL PRIMARY KEY,
      entity_type TEXT NOT NULL,
      entity_id BIGINT NOT NULL,
      event_type TEXT NOT NULL,
      changed_fields TEXT[],
      occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      processed_at TIMESTAMPTZ,
      processor TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_change_unprocessed ON change_events(occurred_at) WHERE processed_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_change_entity      ON change_events(entity_type, entity_id, occurred_at DESC);
  `);

  console.log("\n[6/6] mortgage rate_source flag column");
  await step("ADD rate_source column + index", `
    ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS rate_source TEXT CHECK (rate_source IN ('estimated','recorded'));
    CREATE INDEX IF NOT EXISTS idx_mort_rate_source ON mortgage_records(rate_source) WHERE rate_source IS NOT NULL;
  `);

  console.log("\n[7/7] Marking existing populated rates as 'estimated' (BATCHED)");
  // Use a cursor by id to avoid one giant lock
  const BATCH = 100_000;
  let lastId = 0;
  let totalUpdated = 0;
  const startTime = Date.now();
  while (true) {
    const r = await pg(`
      WITH batch AS (
        SELECT id FROM mortgage_records
         WHERE interest_rate IS NOT NULL
           AND rate_source IS NULL
           AND id > ${lastId}
         ORDER BY id
         LIMIT ${BATCH}
      ),
      upd AS (
        UPDATE mortgage_records m
           SET rate_source = 'estimated'
          FROM batch
         WHERE m.id = batch.id
        RETURNING m.id
      )
      SELECT COALESCE(MAX(id),0) AS max_id, COUNT(*) AS n FROM upd;
    `);
    if (!r.ok) {
      console.error(`  ❌ batch failed: ${JSON.stringify(r.data).slice(0,300)}`);
      break;
    }
    const row = Array.isArray(r.data) && r.data[0] ? r.data[0] : null;
    if (!row || Number(row.n) === 0) {
      console.log(`  done — no more rows above id=${lastId}`);
      break;
    }
    lastId = Number(row.max_id);
    totalUpdated += Number(row.n);
    const elapsed = ((Date.now() - startTime)/1000).toFixed(0);
    console.log(`  +${row.n} updated  (cursor id=${lastId})  total=${totalUpdated.toLocaleString()}  ${elapsed}s`);
  }

  console.log("\n=== VERIFY ===");
  const v = await pg(`
    SELECT
      (SELECT count(*) FROM mortgage_records WHERE rate_source = 'estimated') AS estimated_count,
      (SELECT count(*) FROM mortgage_records WHERE rate_source = 'recorded')  AS recorded_count,
      (SELECT count(*) FROM mortgage_records WHERE rate_source IS NULL AND interest_rate IS NOT NULL) AS leaked_unflagged,
      (SELECT count(*) FROM information_schema.tables WHERE table_name IN ('mls_listings','property_sales_history','property_history','change_events','mls_history')) AS new_tables_present
  `);
  console.log(JSON.stringify(v.data, null, 2));
  console.log("\nMigration 005 applied. ✅");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
