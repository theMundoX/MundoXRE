-- HMDA LAR ingest + PMMS weekly + rate source tracking
-- Public data from CFPB (HMDA) + Freddie Mac (PMMS)

CREATE TABLE IF NOT EXISTS hmda_lar (
  id BIGSERIAL PRIMARY KEY,
  activity_year SMALLINT NOT NULL,
  lei TEXT NOT NULL,
  derived_msa_md INT,
  state_code CHAR(2),
  county_code CHAR(5),         -- FIPS state+county
  census_tract TEXT,
  conforming_loan_limit TEXT,
  action_taken SMALLINT,       -- 1 = originated
  loan_type SMALLINT,          -- 1 conv 2 FHA 3 VA 4 USDA
  loan_purpose SMALLINT,       -- 1 purchase 2 home improv 31 refi 32 cashout 4 other 5 N/A
  lien_status SMALLINT,        -- 1 first 2 subordinate
  occupancy_type SMALLINT,     -- 1 principal 2 second 3 investment
  loan_amount NUMERIC(14,2),   -- reported in dollars, rounded to $5k
  interest_rate NUMERIC(6,3),  -- THE MONEY COLUMN
  rate_spread NUMERIC(6,3),
  loan_term SMALLINT,          -- in months
  property_value NUMERIC(14,2),
  total_loan_costs NUMERIC(12,2),
  origination_charges NUMERIC(12,2),
  discount_points NUMERIC(12,2),
  debt_to_income_ratio TEXT,
  combined_loan_to_value NUMERIC(6,3),
  applicant_credit_score_type SMALLINT,
  hoepa_status SMALLINT,
  preapproval SMALLINT,
  construction_method SMALLINT,
  manufactured_home_secured_property_type SMALLINT,
  total_units SMALLINT,
  reverse_mortgage SMALLINT,
  open_end_line_of_credit SMALLINT,
  business_or_commercial_purpose SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_hmda_match ON hmda_lar
  (activity_year, state_code, county_code, loan_amount, lei)
  WHERE action_taken = 1;

CREATE INDEX IF NOT EXISTS idx_hmda_tract ON hmda_lar
  (activity_year, census_tract, loan_amount)
  WHERE action_taken = 1;

CREATE INDEX IF NOT EXISTS idx_hmda_lei ON hmda_lar (lei);

-- Lender name <-> LEI crosswalk (HMDA Panel / Transmittal Sheet)
CREATE TABLE IF NOT EXISTS hmda_lender_crosswalk (
  lei TEXT PRIMARY KEY,
  respondent_name TEXT NOT NULL,
  respondent_name_normalized TEXT NOT NULL,
  activity_year SMALLINT,
  agency_code SMALLINT,
  assets NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS idx_hmda_lender_norm
  ON hmda_lender_crosswalk (respondent_name_normalized);

-- Freddie Mac Primary Mortgage Market Survey weekly (free, goes back to 1971)
CREATE TABLE IF NOT EXISTS pmms_weekly (
  week_ending DATE PRIMARY KEY,
  rate_30yr_fixed NUMERIC(6,3),
  rate_15yr_fixed NUMERIC(6,3),
  rate_5_1_arm NUMERIC(6,3),
  points_30yr NUMERIC(4,2),
  points_15yr NUMERIC(4,2)
);

-- Rate match audit trail — every rate_source change is logged here
CREATE TABLE IF NOT EXISTS mortgage_rate_matches (
  id BIGSERIAL PRIMARY KEY,
  mortgage_record_id BIGINT NOT NULL,
  rate_source TEXT NOT NULL,       -- hmda_match | agency_match | pmms_weekly | estimated
  source_row_id BIGINT,            -- pk in hmda_lar / agency_lld / pmms_weekly
  match_confidence SMALLINT,       -- 0-100
  match_strategy TEXT,             -- e.g. "county+amount+lei+year"
  interest_rate NUMERIC(6,3),
  loan_term SMALLINT,
  matched_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rate_matches_mortgage
  ON mortgage_rate_matches (mortgage_record_id);

-- Add rate_match columns onto mortgage_records if not present
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='mortgage_records' AND column_name='rate_match_confidence') THEN
    ALTER TABLE mortgage_records ADD COLUMN rate_match_confidence SMALLINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='mortgage_records' AND column_name='rate_match_source_id') THEN
    ALTER TABLE mortgage_records ADD COLUMN rate_match_source_id BIGINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='mortgage_records' AND column_name='rate_matched_at') THEN
    ALTER TABLE mortgage_records ADD COLUMN rate_matched_at TIMESTAMPTZ;
  END IF;
END $$;
