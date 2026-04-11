#!/bin/bash
# Bulk link mortgage_records to properties via direct SQL on VPS.
# Requires 2-word name match. Skips if multiple properties match same name.
# FIPS join: mr.county_fips = c.state_fips || c.county_fips

export PGPASSWORD="d6168ff6e8d9559d62642418bafb3d17"
PSQL="psql -h 127.0.0.1 -p 5432 -U postgres.your-tenant-id -d postgres"

echo "=== MXRE Bulk SQL Property Linker ==="
echo "$(date)"
echo ""

# Step 1: Status before
$PSQL -c "
SELECT
  COUNT(*) as total_unlinked,
  COUNT(*) FILTER (WHERE borrower_name IS NOT NULL AND borrower_name != '') as unlinked_with_name
FROM mortgage_records
WHERE property_id IS NULL;
"

echo ""
echo "Running bulk link in single session (build lookup + update)..."

# All SQL in ONE psql session so the temp table persists
$PSQL <<'SQL'
\timing on

-- Step 1: Build lookup table from properties (full 5-digit FIPS + 2 name words)
\echo 'Building prop_name_lookup table...'

DROP TABLE IF EXISTS prop_name_lookup;
CREATE TABLE prop_name_lookup AS
SELECT
  (c.state_fips || c.county_fips) AS full_fips,
  UPPER(TRIM(SPLIT_PART(TRIM(p.owner_name), ' ', 1))) AS w1,
  UPPER(TRIM(SPLIT_PART(TRIM(p.owner_name), ' ', 2))) AS w2,
  p.id AS prop_id
FROM properties p
JOIN counties c ON c.id = p.county_id
WHERE p.owner_name IS NOT NULL
  AND c.state_fips IS NOT NULL
  AND c.county_fips IS NOT NULL
  AND LENGTH(TRIM(p.owner_name)) > 3;

\echo 'Creating index...'
CREATE INDEX ON prop_name_lookup (full_fips, w1, w2);
ANALYZE prop_name_lookup;

SELECT COUNT(*) as lookup_rows FROM prop_name_lookup;

-- Step 2: Bulk link — 2-word match, unique only
\echo 'Running bulk UPDATE...'

WITH match_counts AS (
  -- For each unlinked mortgage record, count distinct matching properties
  SELECT
    mr.id AS mr_id,
    COUNT(DISTINCT l.prop_id) AS match_cnt,
    MIN(l.prop_id) AS prop_id  -- take the single match
  FROM mortgage_records mr
  JOIN prop_name_lookup l ON
    l.full_fips = mr.county_fips
    AND l.w1 = UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 1)))
    AND l.w2 = UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 2)))
    AND l.w2 != ''
    AND l.w1 != ''
  WHERE mr.property_id IS NULL
    AND mr.county_fips IS NOT NULL
    AND mr.borrower_name IS NOT NULL
    AND mr.borrower_name != ''
    AND LENGTH(TRIM(mr.borrower_name)) > 4
    AND UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 1))) NOT IN
      ('LLC', 'INC', 'CORP', 'BANK', 'TRUST', 'MORTGAGE', 'FEDERAL', 'WELLS',
       'ESTATE', 'NATIONAL', 'CREDIT', 'LENDING', 'FINANCIAL', 'SAVINGS',
       'THE', 'CITY', 'STATE', 'COUNTY', 'REALTY', 'PROPERTIES', 'COMMUNITY',
       'FIRST', 'SECOND', 'THIRD', 'AMERICAN', 'UNITED', 'HOME', 'LAND')
  GROUP BY mr.id
),
unique_matches AS (
  SELECT mr_id, prop_id
  FROM match_counts
  WHERE match_cnt = 1
)
UPDATE mortgage_records mr
SET property_id = um.prop_id
FROM unique_matches um
WHERE mr.id = um.mr_id
  AND mr.property_id IS NULL;

\echo 'Cleanup...'
DROP TABLE IF EXISTS prop_name_lookup;
SQL

# Step 3: Status after
echo ""
echo "Status after linking:"
$PSQL -c "
SELECT
  COUNT(*) as total_unlinked,
  COUNT(*) FILTER (WHERE borrower_name IS NOT NULL AND borrower_name != '') as unlinked_with_name
FROM mortgage_records
WHERE property_id IS NULL;
"

echo ""
echo "Done: $(date)"
