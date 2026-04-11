-- Maps each distinct lender_name string from mortgage_records to its best LEI.
-- Used by the HMDA fuzzy matcher's high-confidence strategy.

CREATE TABLE IF NOT EXISTS mortgage_lender_lei_map (
  lender_name TEXT PRIMARY KEY,
  lender_name_normalized TEXT NOT NULL,
  lei TEXT,                              -- NULL = no match yet
  match_type TEXT,                       -- 'exact' | 'mundox' | 'manual' | 'unmatched'
  match_confidence SMALLINT,             -- 0-100
  candidate_count INT,                   -- how many crosswalk candidates considered
  matched_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lender_lei_map_normalized
  ON mortgage_lender_lei_map (lender_name_normalized);
CREATE INDEX IF NOT EXISTS idx_lender_lei_map_lei
  ON mortgage_lender_lei_map (lei);
