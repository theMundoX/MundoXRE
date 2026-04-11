-- MXRE Database Schema
-- Run in Supabase SQL Editor to initialize

-- ─── Markets & Geography ─────────────────────────────────────────────

CREATE TABLE counties (
  id SERIAL PRIMARY KEY,
  state_fips CHAR(2) NOT NULL,
  county_fips CHAR(3) NOT NULL,
  state_code CHAR(2) NOT NULL,
  county_name TEXT NOT NULL,
  msa TEXT,
  assessor_url TEXT,
  recorder_url TEXT,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (state_fips, county_fips)
);

-- ─── Properties (All Asset Types) ───────────────────────────────────

CREATE TABLE properties (
  id SERIAL PRIMARY KEY,
  county_id INTEGER NOT NULL REFERENCES counties(id),
  parcel_id TEXT,

  -- Location
  address TEXT NOT NULL,
  address2 TEXT,
  city TEXT NOT NULL,
  state_code CHAR(2) NOT NULL,
  zip TEXT NOT NULL,
  lat NUMERIC(9,6),
  lng NUMERIC(9,6),
  msa TEXT,

  -- Physical
  property_type TEXT,
  total_units INTEGER,
  stories INTEGER,
  year_built INTEGER,
  total_sqft INTEGER,

  -- Classification
  is_apartment BOOLEAN DEFAULT false,
  is_sfr BOOLEAN DEFAULT false,
  is_condo BOOLEAN DEFAULT false,
  is_btr BOOLEAN DEFAULT false,
  is_senior BOOLEAN DEFAULT false,
  is_student BOOLEAN DEFAULT false,
  is_affordable BOOLEAN DEFAULT false,

  -- Ownership
  owner_name TEXT,
  mgmt_company TEXT,
  website TEXT,

  -- Valuation (from assessor)
  assessed_value INTEGER,
  market_value INTEGER,
  taxable_value INTEGER,
  land_value INTEGER,
  last_sale_price INTEGER,
  last_sale_date DATE,
  assessor_url TEXT,

  -- Construction details
  construction_class TEXT,
  improvement_quality TEXT,
  total_buildings INTEGER,
  land_sqft INTEGER,

  source TEXT NOT NULL DEFAULT 'assessor',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (county_id, parcel_id)
);

CREATE INDEX idx_prop_county ON properties(county_id);
CREATE INDEX idx_prop_zip ON properties(zip);
CREATE INDEX idx_prop_city ON properties(city);
CREATE INDEX idx_prop_type ON properties(property_type);
CREATE INDEX idx_prop_units ON properties(total_units) WHERE total_units >= 5;
CREATE INDEX idx_prop_mgmt ON properties(mgmt_company) WHERE mgmt_company IS NOT NULL;

-- ─── Property Websites ──────────────────────────────────────────────

CREATE TABLE property_websites (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  url TEXT NOT NULL,
  platform TEXT,
  discovery_method TEXT,
  last_scraped_at TIMESTAMPTZ,
  scrape_success BOOLEAN,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (property_id, url)
);

CREATE INDEX idx_web_platform ON property_websites(platform);
CREATE INDEX idx_web_active ON property_websites(active) WHERE active = true;

-- ─── Floorplans (unit configurations at a property) ─────────────────

CREATE TABLE floorplans (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  name TEXT,
  beds INTEGER NOT NULL DEFAULT 0,
  baths INTEGER NOT NULL DEFAULT 0,
  half_baths INTEGER NOT NULL DEFAULT 0,
  sqft INTEGER,
  estimated_count INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (property_id, name)
);

CREATE INDEX idx_fp_property ON floorplans(property_id);
CREATE INDEX idx_fp_beds ON floorplans(beds);

-- ─── Rent Snapshots (time-series — the core value) ──────────────────

CREATE TABLE rent_snapshots (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  floorplan_id INTEGER REFERENCES floorplans(id),
  website_id INTEGER REFERENCES property_websites(id),
  observed_at DATE NOT NULL,

  -- Unit config (denormalized for fast queries)
  beds INTEGER,
  baths INTEGER,
  sqft INTEGER,

  -- Pricing (all in cents)
  asking_rent INTEGER,
  effective_rent INTEGER,
  concession_value INTEGER,
  asking_psf INTEGER,
  effective_psf INTEGER,
  deposit INTEGER,
  concession_text TEXT,

  -- Availability
  available_count INTEGER,
  days_on_market INTEGER,

  -- Occupancy signals
  leased_pct NUMERIC(5,2),
  exposure_pct NUMERIC(5,2),
  renewal_pct NUMERIC(5,2),

  raw JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_snap_prop_date ON rent_snapshots(property_id, observed_at DESC);
CREATE INDEX idx_snap_date ON rent_snapshots(observed_at DESC);
CREATE INDEX idx_snap_beds ON rent_snapshots(beds);
CREATE INDEX idx_snap_zip ON rent_snapshots(observed_at DESC, beds);

-- ─── Lease Transactions (individual lease events) ───────────────────

CREATE TABLE lease_events (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  floorplan_id INTEGER REFERENCES floorplans(id),

  event_type TEXT NOT NULL,  -- 'new_lease', 'renewal', 'notice_to_vacate'
  event_date DATE NOT NULL,
  lease_start DATE,
  lease_end DATE,
  term_months INTEGER,

  -- Pricing at signing (cents)
  signed_rent INTEGER,
  signed_psf INTEGER,
  signed_concession INTEGER,
  signed_effective INTEGER,

  beds INTEGER,
  baths INTEGER,
  sqft INTEGER,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_lease_prop ON lease_events(property_id, event_date DESC);
CREATE INDEX idx_lease_date ON lease_events(event_date DESC);

-- ─── Fee Schedules ──────────────────────────────────────────────────

CREATE TABLE fee_schedules (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  observed_at DATE NOT NULL,

  -- Application & admin (cents)
  app_fee INTEGER,
  admin_fee INTEGER,
  amenity_fee INTEGER,
  storage_fee INTEGER,

  -- Pet fees (cents)
  pet_deposit INTEGER,
  pet_monthly INTEGER,
  pet_onetime INTEGER,

  -- Parking (cents/month)
  parking_covered INTEGER,
  parking_garage INTEGER,
  parking_surface INTEGER,

  raw JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fees_prop ON fee_schedules(property_id, observed_at DESC);

-- ─── Property Amenities ─────────────────────────────────────────────

CREATE TABLE amenities (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  scope TEXT NOT NULL,  -- 'building' or 'unit'
  amenity TEXT NOT NULL,
  present BOOLEAN NOT NULL DEFAULT true,
  observed_at DATE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (property_id, scope, amenity)
);

CREATE INDEX idx_amen_prop ON amenities(property_id);

-- ─── Reputation & Reviews ───────────────────────────────────────────

CREATE TABLE reputation (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  observed_at DATE NOT NULL,
  platform TEXT,  -- 'google', 'apartments_com', 'yelp', etc.
  avg_rating NUMERIC(3,2),
  review_count INTEGER,

  -- Sentiment breakdown (count of positive/negative mentions)
  pos_amenities INTEGER DEFAULT 0,
  pos_cleanliness INTEGER DEFAULT 0,
  pos_location INTEGER DEFAULT 0,
  pos_staff INTEGER DEFAULT 0,
  pos_value INTEGER DEFAULT 0,
  neg_amenities INTEGER DEFAULT 0,
  neg_cleanliness INTEGER DEFAULT 0,
  neg_location INTEGER DEFAULT 0,
  neg_staff INTEGER DEFAULT 0,
  neg_value INTEGER DEFAULT 0,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rep_prop ON reputation(property_id, observed_at DESC);

-- ─── Condition & Finish Level ───────────────────────────────────────

CREATE TABLE condition_scores (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  observed_at DATE NOT NULL,

  overall NUMERIC(5,2),
  kitchen NUMERIC(5,2),
  bathroom NUMERIC(5,2),
  bedroom NUMERIC(5,2),
  living_area NUMERIC(5,2),
  common_areas NUMERIC(5,2),
  exterior NUMERIC(5,2),
  fitness NUMERIC(5,2),
  pool NUMERIC(5,2),

  source TEXT,  -- 'photo_analysis', 'manual', 'listing_data'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_cond_prop ON condition_scores(property_id, observed_at DESC);

-- ─── Pricing Strategy Signals ───────────────────────────────────────

CREATE TABLE pricing_signals (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  observed_at DATE NOT NULL,

  uses_rev_mgmt BOOLEAN,
  avg_price_change_pct NUMERIC(5,2),
  avg_price_duration_days INTEGER,
  avg_time_on_market_days INTEGER,
  price_update_count INTEGER,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pricing_prop ON pricing_signals(property_id, observed_at DESC);

-- ─── Mortgage & Deed Records ────────────────────────────────────────

CREATE TABLE mortgage_records (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  document_type TEXT NOT NULL,
  recording_date DATE,
  loan_amount INTEGER,
  lender_name TEXT,
  borrower_name TEXT,
  document_number TEXT,
  book_page TEXT,
  source_url TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mort_prop ON mortgage_records(property_id);
CREATE INDEX idx_mort_date ON mortgage_records(recording_date DESC);

-- ─── Scrape Jobs ────────────────────────────────────────────────────

CREATE TABLE scrape_jobs (
  id SERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
  county_id INTEGER REFERENCES counties(id),
  property_id INTEGER REFERENCES properties(id),
  website_id INTEGER REFERENCES property_websites(id),
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_pending ON scrape_jobs(status, priority DESC) WHERE status = 'pending';

-- ─── Row Level Security ─────────────────────────────────────────────

ALTER TABLE counties ENABLE ROW LEVEL SECURITY;
ALTER TABLE properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_websites ENABLE ROW LEVEL SECURITY;
ALTER TABLE floorplans ENABLE ROW LEVEL SECURITY;
ALTER TABLE rent_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE lease_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE fee_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE amenities ENABLE ROW LEVEL SECURITY;
ALTER TABLE reputation ENABLE ROW LEVEL SECURITY;
ALTER TABLE condition_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE pricing_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE mortgage_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_jobs ENABLE ROW LEVEL SECURITY;

-- Anon role: read-only on data tables
CREATE POLICY "anon_read" ON counties FOR SELECT USING (true);
CREATE POLICY "anon_read" ON properties FOR SELECT USING (true);
CREATE POLICY "anon_read" ON property_websites FOR SELECT USING (true);
CREATE POLICY "anon_read" ON floorplans FOR SELECT USING (true);
CREATE POLICY "anon_read" ON rent_snapshots FOR SELECT USING (true);
CREATE POLICY "anon_read" ON lease_events FOR SELECT USING (true);
CREATE POLICY "anon_read" ON fee_schedules FOR SELECT USING (true);
CREATE POLICY "anon_read" ON amenities FOR SELECT USING (true);
CREATE POLICY "anon_read" ON reputation FOR SELECT USING (true);
CREATE POLICY "anon_read" ON condition_scores FOR SELECT USING (true);
CREATE POLICY "anon_read" ON pricing_signals FOR SELECT USING (true);
CREATE POLICY "anon_read" ON mortgage_records FOR SELECT USING (true);
-- No anon access to scrape_jobs
