-- Enriched apartment / multifamily complex profiles.
-- This stores names and operating details that assessor records usually do not carry.

CREATE TABLE IF NOT EXISTS property_complex_profiles (
  property_id BIGINT PRIMARY KEY REFERENCES properties(id) ON DELETE CASCADE,
  complex_name TEXT,
  management_company TEXT,
  website TEXT,
  phone TEXT,
  email TEXT,
  source TEXT NOT NULL,
  source_url TEXT,
  unit_count INTEGER,
  year_built INTEGER,
  amenities JSONB DEFAULT '[]'::jsonb,
  description TEXT,
  confidence TEXT DEFAULT 'medium',
  first_seen_at TIMESTAMPTZ DEFAULT now(),
  last_seen_at TIMESTAMPTZ DEFAULT now(),
  raw JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_property_complex_profiles_name
  ON property_complex_profiles USING gin (complex_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_property_complex_profiles_source
  ON property_complex_profiles(source);
