-- External commercial/multifamily listing observations.
-- Used for CRE-style sources that are not linked to a parcel/property yet.

CREATE TABLE IF NOT EXISTS external_market_listings (
  id BIGSERIAL PRIMARY KEY,
  market TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  source TEXT NOT NULL,
  source_url TEXT,
  title TEXT,
  address TEXT,
  city TEXT,
  state_code TEXT,
  zip TEXT,
  units INTEGER,
  list_price INTEGER,
  price_per_unit INTEGER,
  cap_rate NUMERIC(8,4),
  noi INTEGER,
  status TEXT DEFAULT 'active',
  confidence TEXT DEFAULT 'low',
  observed_at TIMESTAMPTZ DEFAULT now(),
  first_seen_at TIMESTAMPTZ DEFAULT now(),
  last_seen_at TIMESTAMPTZ DEFAULT now(),
  raw JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_external_market_listings_market_asset
  ON external_market_listings(market, asset_class, status);

CREATE INDEX IF NOT EXISTS idx_external_market_listings_units
  ON external_market_listings(units);
