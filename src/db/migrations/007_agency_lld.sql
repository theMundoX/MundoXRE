-- Agency (Fannie/Freddie) single-family loan-level data ingest
-- Source: FHFA Public Use Database (PUDB) Single-Family Census Tract File
-- https://www.fhfa.gov/data/pudb  (free, no registration)
--
-- The PUDB is annual (vintage_month is NULL for PUDB-sourced rows). We keep
-- vintage_month in the schema so later quarterly sources (SFLLD / LPD) can use it.
-- loan_id is the FHFA record number (record_num_sf_ctf) — unique within file.

CREATE TABLE IF NOT EXISTS agency_lld (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,                  -- 'fannie' | 'freddie'
  vintage_year SMALLINT NOT NULL,
  vintage_month SMALLINT,                -- NULL for annual PUDB data
  loan_id TEXT,                          -- FHFA record_num_sf_ctf (not a real loan id)
  channel TEXT,                          -- channel_apply raw code
  seller_name TEXT,                      -- not provided by PUDB
  servicer_name TEXT,                    -- not provided by PUDB
  origination_rate NUMERIC(6,3),         -- rate_orig (note rate at origination)
  original_upb NUMERIC(14,2),            -- upb_orig
  original_loan_term SMALLINT,           -- term_orig (months)
  original_ltv NUMERIC(6,3),             -- ltv
  original_cltv NUMERIC(6,3),            -- not in PUDB, NULL
  number_of_borrowers SMALLINT,          -- borr_num
  dti NUMERIC(6,3),                      -- PUDB gives dti_cat only; stored raw as numeric bucket
  credit_score SMALLINT,                 -- score_borr_model raw code (1-9 bucket in PUDB)
  first_time_buyer TEXT,                 -- fthb raw code
  loan_purpose TEXT,                     -- purpose_ctf raw code
  property_type TEXT,                    -- property_type raw code
  number_of_units SMALLINT,              -- units_num
  occupancy_status TEXT,                 -- occupancy_sf_ctf raw code
  property_state CHAR(2),                -- derived from state_fips
  property_zip3 TEXT,                    -- not in PUDB (tract-level only)
  msa TEXT                               -- cbsa_metro_code
);

CREATE INDEX IF NOT EXISTS idx_agency_lld_match ON agency_lld
  (vintage_year, vintage_month, property_state, original_upb);

CREATE INDEX IF NOT EXISTS idx_agency_lld_rate ON agency_lld
  (vintage_year, origination_rate)
  WHERE origination_rate IS NOT NULL;
