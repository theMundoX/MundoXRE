-- Migration 005 — MLS listings, sales history, mailing address, change-data-capture
--
-- This migration is the foundation of:
--   1) MLS data ingestion (Zillow / Redfin / RESO Web API)
--   2) Multi-transaction sales history (not just last_sale_*)
--   3) Separate mailing address vs property address
--   4) Change-data-capture: hash + last_seen / last_changed timestamps for freshness tracking
--
-- Apply via Supabase SQL editor.
-- Safe to re-run: every statement uses IF NOT EXISTS.

-- ─── 1. Mailing address on properties (separate from property address) ───────

ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_address    TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_city       TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_state      CHAR(2);
ALTER TABLE properties ADD COLUMN IF NOT EXISTS mailing_zip        TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS owner_occupied     BOOLEAN;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS absentee_owner     BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_prop_mailing_state    ON properties(mailing_state)    WHERE mailing_state IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prop_absentee         ON properties(absentee_owner)   WHERE absentee_owner = true;

-- ─── 2. Change-data-capture columns on properties ───────────────────────────

ALTER TABLE properties ADD COLUMN IF NOT EXISTS current_hash       BYTEA;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_seen_at       TIMESTAMPTZ;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS last_changed_at    TIMESTAMPTZ;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS record_status      TEXT NOT NULL DEFAULT 'active'
    CHECK (record_status IN ('active', 'archived', 'pending_review'));

CREATE INDEX IF NOT EXISTS idx_prop_last_seen     ON properties(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_prop_last_changed  ON properties(last_changed_at);
CREATE INDEX IF NOT EXISTS idx_prop_status        ON properties(record_status) WHERE record_status != 'active';

-- ─── 3. Property history (every change snapshot) ────────────────────────────

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

-- ─── 4. Sales history (multiple transactions per property) ──────────────────

CREATE TABLE IF NOT EXISTS property_sales_history (
  id                BIGSERIAL PRIMARY KEY,
  property_id       INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
  sale_date         DATE NOT NULL,
  sale_price        BIGINT,
  sale_type         TEXT,           -- 'cash', 'financed', 'inherited', 'foreclosure', 'short_sale', 'reo'
  buyer_name        TEXT,
  seller_name       TEXT,
  document_number   TEXT,
  document_type     TEXT,           -- 'warranty_deed', 'quitclaim_deed', 'trustee_deed', etc.
  source            TEXT NOT NULL DEFAULT 'recorder',
  source_url        TEXT,
  raw               JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sales_prop          ON property_sales_history(property_id, sale_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_date          ON property_sales_history(sale_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_price         ON property_sales_history(sale_price) WHERE sale_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sales_doc           ON property_sales_history(document_number);

-- ─── 5. MLS listings (ALL listing activity, not just current) ──────────────

CREATE TABLE IF NOT EXISTS mls_listings (
  id                  BIGSERIAL PRIMARY KEY,
  property_id         INTEGER REFERENCES properties(id) ON DELETE SET NULL,

  -- MLS identification
  mls_id              TEXT,                    -- e.g. NTREIS, GLAR, etc.
  mls_number          TEXT,                    -- listing number within that MLS
  source              TEXT NOT NULL,           -- 'zillow', 'redfin', 'realtor', 'reso', 'manual'
  source_url          TEXT,

  -- Status & dates
  status              TEXT NOT NULL,           -- 'active', 'pending', 'sold', 'withdrawn', 'expired', 'cancelled', 'coming_soon'
  list_date           DATE,
  status_date         DATE,
  off_market_date     DATE,
  days_on_market      INTEGER,

  -- Pricing
  list_price          BIGINT,
  original_list_price BIGINT,
  current_price       BIGINT,
  sold_price          BIGINT,
  price_per_sqft      BIGINT,

  -- Listing details (denormalized for speed)
  beds                INTEGER,
  baths               NUMERIC(4,1),
  sqft                INTEGER,
  lot_sqft            INTEGER,
  year_built          INTEGER,
  property_type       TEXT,

  -- Agent / brokerage
  list_agent_name     TEXT,
  list_agent_email    TEXT,
  list_agent_phone    TEXT,
  list_brokerage      TEXT,
  buyer_agent_name    TEXT,
  buyer_brokerage     TEXT,

  -- Address fallback (when property_id is null because we haven't matched yet)
  address             TEXT,
  city                TEXT,
  state_code          CHAR(2),
  zip                 TEXT,
  lat                 NUMERIC(9,6),
  lng                 NUMERIC(9,6),

  -- Description / remarks
  public_remarks      TEXT,
  private_remarks     TEXT,

  -- Photos (URLs only — actual image storage TBD)
  photo_urls          TEXT[],
  primary_photo_url   TEXT,

  -- Raw payload for downstream re-parsing
  raw                 JSONB,

  -- CDC
  current_hash        BYTEA,
  last_seen_at        TIMESTAMPTZ,
  last_changed_at     TIMESTAMPTZ,

  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- A given (source, mls_id, mls_number) is unique per listing event
  UNIQUE (source, mls_id, mls_number)
);

CREATE INDEX IF NOT EXISTS idx_mls_property      ON mls_listings(property_id) WHERE property_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mls_status        ON mls_listings(status, status_date DESC);
CREATE INDEX IF NOT EXISTS idx_mls_active        ON mls_listings(status, state_code, zip)
    WHERE status IN ('active', 'pending', 'coming_soon');
CREATE INDEX IF NOT EXISTS idx_mls_zip_date      ON mls_listings(zip, status_date DESC);
CREATE INDEX IF NOT EXISTS idx_mls_addr_match    ON mls_listings(state_code, zip, address) WHERE property_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_mls_last_seen     ON mls_listings(last_seen_at);

-- ─── 6. MLS history snapshots (every status / price change) ────────────────

CREATE TABLE IF NOT EXISTS mls_history (
  id              BIGSERIAL PRIMARY KEY,
  listing_id      BIGINT NOT NULL REFERENCES mls_listings(id) ON DELETE CASCADE,
  captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  hash            BYTEA NOT NULL,
  changed_fields  TEXT[],
  snapshot        JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mls_hist_listing ON mls_history(listing_id, captured_at DESC);

-- ─── 7. Change events (consumed by downstream enrichment) ──────────────────

CREATE TABLE IF NOT EXISTS change_events (
  id              BIGSERIAL PRIMARY KEY,
  entity_type     TEXT NOT NULL,    -- 'property', 'mls_listing', 'mortgage_record'
  entity_id       BIGINT NOT NULL,
  event_type      TEXT NOT NULL,    -- 'inserted', 'updated', 'archived'
  changed_fields  TEXT[],
  occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at    TIMESTAMPTZ,
  processor       TEXT
);

CREATE INDEX IF NOT EXISTS idx_change_unprocessed ON change_events(occurred_at) WHERE processed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_change_entity      ON change_events(entity_type, entity_id, occurred_at DESC);

-- ─── 8. Mortgage rate source flag (estimated vs recorded) ─────────────
--
-- Per the rule: estimates are OK as long as they're flagged.
-- When real OCR data arrives, we overwrite the estimate AND flip the flag.
--   'estimated' → from src/utils/mortgage-calc.ts (Freddie Mac PMMS yearly avg)
--   'recorded'  → from actual document OCR or recorder API field
--   NULL        → no rate at all

ALTER TABLE mortgage_records
  ADD COLUMN IF NOT EXISTS rate_source TEXT
    CHECK (rate_source IN ('estimated', 'recorded'));

CREATE INDEX IF NOT EXISTS idx_mort_rate_source
  ON mortgage_records(rate_source) WHERE rate_source IS NOT NULL;

-- Mark all existing populated rates as 'estimated' (they came from
-- the Freddie Mac PMMS yearly average lookup table — verified by audit
-- on 2026-04-06 which found only 3 distinct rates across 200 sample records).
-- Real OCR-derived rates will overwrite these and set rate_source = 'recorded'.

UPDATE mortgage_records
   SET rate_source = 'estimated'
 WHERE interest_rate IS NOT NULL
   AND rate_source IS NULL;

-- Same for related estimated fields — they were all computed from the same
-- estimated rate via mortgage-calc.computeMortgageFields().
-- estimated_current_balance, estimated_monthly_payment, maturity_date,
-- balance_as_of are kept (they're useful for analytics) but the rate_source
-- column tells consumers they're derived from an estimate.

-- ─── 9. Row-level security ─────────────────────────────────────────────────

ALTER TABLE property_history       ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_sales_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE mls_listings           ENABLE ROW LEVEL SECURITY;
ALTER TABLE mls_history            ENABLE ROW LEVEL SECURITY;
ALTER TABLE change_events          ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  CREATE POLICY "anon_read" ON property_history       FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE POLICY "anon_read" ON property_sales_history FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE POLICY "anon_read" ON mls_listings           FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE POLICY "anon_read" ON mls_history            FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
-- change_events: no anon access
