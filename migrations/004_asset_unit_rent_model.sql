-- Canonical asset classification + unit-level rent economics.

ALTER TABLE properties
  ADD COLUMN IF NOT EXISTS asset_type TEXT,
  ADD COLUMN IF NOT EXISTS asset_subtype TEXT,
  ADD COLUMN IF NOT EXISTS unit_count_source TEXT,
  ADD COLUMN IF NOT EXISTS asset_confidence TEXT;

ALTER TABLE rent_snapshots
  ADD COLUMN IF NOT EXISTS rent_unit_basis TEXT DEFAULT 'per_unit',
  ADD COLUMN IF NOT EXISTS rent_per_door INTEGER,
  ADD COLUMN IF NOT EXISTS estimated_unit_count INTEGER,
  ADD COLUMN IF NOT EXISTS total_monthly_rent INTEGER;

UPDATE rent_snapshots
SET rent_unit_basis = 'per_unit'
WHERE rent_unit_basis IS NULL;

CREATE INDEX IF NOT EXISTS idx_floorplans_property_beds
  ON floorplans(property_id, beds, baths);

-- Large-table backfills/indexes are intentionally handled by targeted scripts.
-- Full-table rent_snapshots updates and global property indexes can exceed the
-- pg-meta read timeout on the current Supabase deployment.
