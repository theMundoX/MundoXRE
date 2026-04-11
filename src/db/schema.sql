-- MXRE Database Schema
-- Run this in the Supabase SQL Editor to initialize the database

-- Counties being tracked
CREATE TABLE counties (
  id SERIAL PRIMARY KEY,
  state_fips CHAR(2) NOT NULL,
  county_fips CHAR(3) NOT NULL,
  state_code CHAR(2) NOT NULL,
  county_name TEXT NOT NULL,
  assessor_url TEXT,
  recorder_url TEXT,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (state_fips, county_fips)
);

-- Master property list (ALL property types)
CREATE TABLE properties (
  id SERIAL PRIMARY KEY,
  county_id INTEGER NOT NULL REFERENCES counties(id),
  parcel_id TEXT,
  address_line1 TEXT NOT NULL,
  address_line2 TEXT,
  city TEXT NOT NULL,
  state_code CHAR(2) NOT NULL,
  zip TEXT NOT NULL,
  lat NUMERIC(9,6),
  lng NUMERIC(9,6),
  property_type TEXT,
  unit_count INTEGER,
  year_built INTEGER,
  total_sqft INTEGER,
  owner_name TEXT,
  assessed_value INTEGER,
  last_sale_price INTEGER,
  last_sale_date DATE,
  assessor_url TEXT,
  source TEXT NOT NULL DEFAULT 'assessor',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (county_id, parcel_id)
);

CREATE INDEX idx_properties_county ON properties(county_id);
CREATE INDEX idx_properties_zip ON properties(zip);
CREATE INDEX idx_properties_city ON properties(city);
CREATE INDEX idx_properties_type ON properties(property_type);
CREATE INDEX idx_properties_units ON properties(unit_count) WHERE unit_count >= 5;

-- Discovered website URLs for properties
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

CREATE INDEX idx_websites_platform ON property_websites(platform);
CREATE INDEX idx_websites_active ON property_websites(active) WHERE active = true;

-- Time-series rent observations
CREATE TABLE rent_observations (
  id SERIAL PRIMARY KEY,
  property_id INTEGER NOT NULL REFERENCES properties(id),
  website_id INTEGER REFERENCES property_websites(id),
  observed_at DATE NOT NULL,
  unit_type TEXT NOT NULL,
  unit_name TEXT,
  sqft INTEGER,
  rent_min INTEGER,
  rent_max INTEGER,
  rent_avg INTEGER,
  available_units INTEGER,
  deposit INTEGER,
  specials TEXT,
  raw_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rents_property_date ON rent_observations(property_id, observed_at DESC);
CREATE INDEX idx_rents_observed ON rent_observations(observed_at DESC);
CREATE INDEX idx_rents_unit_type ON rent_observations(unit_type);

-- Mortgage/deed records
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

CREATE INDEX idx_mortgages_property ON mortgage_records(property_id);
CREATE INDEX idx_mortgages_date ON mortgage_records(recording_date DESC);

-- Job queue for scraping
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
CREATE INDEX idx_jobs_type ON scrape_jobs(job_type, status);

-- Row Level Security (service role bypasses, but required by Supabase)
ALTER TABLE counties ENABLE ROW LEVEL SECURITY;
ALTER TABLE properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_websites ENABLE ROW LEVEL SECURITY;
ALTER TABLE rent_observations ENABLE ROW LEVEL SECURITY;
ALTER TABLE mortgage_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_jobs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_access" ON counties FOR ALL USING (true);
CREATE POLICY "service_access" ON properties FOR ALL USING (true);
CREATE POLICY "service_access" ON property_websites FOR ALL USING (true);
CREATE POLICY "service_access" ON rent_observations FOR ALL USING (true);
CREATE POLICY "service_access" ON mortgage_records FOR ALL USING (true);
CREATE POLICY "service_access" ON scrape_jobs FOR ALL USING (true);
