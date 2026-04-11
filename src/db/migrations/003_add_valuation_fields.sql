-- Add detailed valuation and construction fields to properties table.
-- These fields support Florida NAL bulk data and richer assessor data.

-- Valuation
ALTER TABLE properties ADD COLUMN IF NOT EXISTS market_value INTEGER;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS taxable_value INTEGER;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS land_value INTEGER;

-- Construction details
ALTER TABLE properties ADD COLUMN IF NOT EXISTS construction_class TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS improvement_quality TEXT;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS total_buildings INTEGER;
ALTER TABLE properties ADD COLUMN IF NOT EXISTS land_sqft INTEGER;

-- Indexes for valuation queries
CREATE INDEX IF NOT EXISTS idx_prop_market_value ON properties(market_value) WHERE market_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prop_taxable_value ON properties(taxable_value) WHERE taxable_value IS NOT NULL;
