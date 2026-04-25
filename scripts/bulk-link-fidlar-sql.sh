#!/bin/bash
# Bulk link Fidlar mortgage records to properties via SQL on the VPS.
# Runs directly in PostgreSQL for speed — no JS overhead or API rate limits.
# Safety: requires 2-word name match AND unique match per record.

set -e

PSQL="PGPASSWORD=${process.env.MXRE_PG_PASSWORD} psql -h 127.0.0.1 -p 5432 -U postgres.your-tenant-id -d postgres"

echo "=== MXRE Bulk Fidlar Linker (SQL) ==="
echo "$(date)"

# Step 1: Count before
$PSQL -c "SELECT COUNT(*) as unlinked_fidlar FROM mortgage_records WHERE property_id IS NULL AND source_url LIKE '%ava.fidlar.com%';"

# Step 2: Build temp lookup table (county_fips + owner name words → property_id)
$PSQL <<'SQL'
BEGIN;

CREATE TEMP TABLE fidlar_lookup ON COMMIT DROP AS
SELECT
  c.county_fips,
  UPPER(TRIM(SPLIT_PART(TRIM(p.owner_name), ' ', 1))) AS w1,
  UPPER(TRIM(SPLIT_PART(TRIM(p.owner_name), ' ', 2))) AS w2,
  p.id AS prop_id
FROM properties p
JOIN counties c ON c.id = p.county_id
WHERE p.owner_name IS NOT NULL
  AND LENGTH(TRIM(p.owner_name)) > 3
  AND c.county_fips IS NOT NULL;

CREATE INDEX ON fidlar_lookup (county_fips, w1, w2);
ANALYZE fidlar_lookup;

-- Find mortgage records where exactly 1 property matches on 2 name words
-- This is the high-confidence tier
WITH candidates AS (
  SELECT
    mr.id AS mr_id,
    fl.prop_id,
    COUNT(*) OVER (PARTITION BY mr.id) AS match_count,
    ROW_NUMBER() OVER (
      PARTITION BY mr.id
      ORDER BY
        -- prefer second-word match
        CASE WHEN fl.w2 = UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 2)))
             AND fl.w2 != '' THEN 1 ELSE 0 END DESC,
        fl.prop_id
    ) AS rn
  FROM mortgage_records mr
  JOIN fidlar_lookup fl ON
    fl.county_fips = mr.county_fips
    AND fl.w1 = UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 1)))
    AND fl.w2 = UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 2)))
    AND fl.w2 != ''
  WHERE mr.property_id IS NULL
    AND mr.source_url LIKE '%ava.fidlar.com%'
    AND mr.county_fips IS NOT NULL
    AND mr.borrower_name IS NOT NULL
    AND LENGTH(TRIM(mr.borrower_name)) > 3
    AND UPPER(TRIM(SPLIT_PART(TRIM(mr.borrower_name), ' ', 1))) NOT IN
      ('LLC', 'INC', 'CORP', 'BANK', 'TRUST', 'MORTGAGE', 'FEDERAL', 'WELLS',
       'ESTATE', 'NATIONAL', 'CREDIT', 'LENDING', 'FINANCIAL', 'SAVINGS')
),
best_match AS (
  SELECT mr_id, prop_id
  FROM candidates
  WHERE rn = 1
  GROUP BY mr_id, prop_id
  HAVING COUNT(*) = 1  -- unique match only
)
UPDATE mortgage_records mr
SET property_id = bm.prop_id
FROM best_match bm
WHERE mr.id = bm.mr_id
  AND mr.property_id IS NULL;

GET DIAGNOSTICS;

COMMIT;
SQL

# Step 3: Count after
$PSQL -c "SELECT COUNT(*) as unlinked_fidlar_after FROM mortgage_records WHERE property_id IS NULL AND source_url LIKE '%ava.fidlar.com%';"

echo ""
echo "Done: $(date)"
