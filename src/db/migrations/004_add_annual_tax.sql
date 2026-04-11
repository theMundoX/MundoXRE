-- Add annual_tax column for actual tax bills (not assessed/appraised values).
-- property_tax currently stores appraised values incorrectly.
-- annual_tax stores the real annual tax amount in cents (e.g., 324700 = $3,247.00).

ALTER TABLE properties ADD COLUMN IF NOT EXISTS annual_tax INTEGER;

-- Index for tax queries
CREATE INDEX IF NOT EXISTS idx_prop_annual_tax ON properties(annual_tax) WHERE annual_tax IS NOT NULL;

-- Optional: track when tax was last scraped
ALTER TABLE properties ADD COLUMN IF NOT EXISTS tax_year INTEGER;
