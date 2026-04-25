#!/bin/bash
# Fast NJ SR1A linker — loads raw files via COPY, links via SQL in one shot.
# Replaces the slow JS version (which did sequential API calls per update).
#
# NJ SR1A line format (fixed-width):
#   cols 0-1:   county code (01-21)
#   cols 2-3:   district code
#   cols 328-332: deed book
#   cols 333-337: deed page
#   cols 350-354: block (zero-padded)
#   cols 359-363: lot (zero-padded)
#
# NJ parcel_id in properties: "CCDD_BLOCK_LOT" (no leading zeros on block/lot)
# mortgage_records document_number: "BOOK-PAGE"

set -e
export PGPASSWORD="${process.env.MXRE_PG_PASSWORD}"
PSQL="psql -h 127.0.0.1 -p 5432 -U postgres.your-tenant-id -d postgres"

echo "=== MXRE Fast NJ SR1A Linker (SQL) ==="
echo "$(date)"

# Files to process (uploaded to VPS)
FILES=(/tmp/nj-sr1a-2024.txt /tmp/nj-sr1a-2025.txt /tmp/nj-sr1a-2026.txt)

$PSQL <<'SQL'
\timing on

-- Create staging table for SR1A data
DROP TABLE IF EXISTS nj_sr1a_staging;
CREATE TABLE nj_sr1a_staging (
  line TEXT
);
SQL

# Load each file into staging
# Use delimiter E'\x01' (SOH) to avoid splitting fixed-width records on tabs
for f in "${FILES[@]}"; do
  if [ -f "$f" ]; then
    echo "Loading $f ($(wc -l < "$f") lines)..."
    $PSQL -c "\copy nj_sr1a_staging (line) FROM STDIN WITH (FORMAT text, DELIMITER E'\x01');" < "$f"
  else
    echo "Skipping $f (not found)"
  fi
done

$PSQL <<'SQL'
\timing on
\echo 'Parsing SR1A lines into structured table...'

-- Parse fixed-width SR1A records
DROP TABLE IF EXISTS nj_sr1a_parsed;
CREATE TABLE nj_sr1a_parsed AS
SELECT
  TRIM(SUBSTRING(line, 1, 2)) AS cc,
  TRIM(SUBSTRING(line, 3, 2)) AS dd,
  TRIM(TRIM(LEADING '0' FROM TRIM(SUBSTRING(line, 351, 5)))) AS block,
  TRIM(TRIM(LEADING '0' FROM TRIM(SUBSTRING(line, 360, 5)))) AS lot,
  TRIM(SUBSTRING(line, 329, 5)) AS deed_book,
  TRIM(SUBSTRING(line, 334, 5)) AS deed_page,
  LENGTH(line) AS line_len
FROM nj_sr1a_staging
WHERE LENGTH(line) >= 370
  AND TRIM(SUBSTRING(line, 1, 2)) ~ '^[0-9]{2}$';

-- Fix empty block/lot to '0'
UPDATE nj_sr1a_parsed SET block = '0' WHERE block = '' OR block IS NULL;
UPDATE nj_sr1a_parsed SET lot = '0' WHERE lot = '' OR lot IS NULL;

-- Build parcel_id: "CCDD_BLOCK_LOT"
ALTER TABLE nj_sr1a_parsed ADD COLUMN parcel_id TEXT;
UPDATE nj_sr1a_parsed SET parcel_id = cc || dd || '_' || block || '_' || lot;

-- Build document_number: "BOOK-PAGE"
ALTER TABLE nj_sr1a_parsed ADD COLUMN doc_num TEXT;
UPDATE nj_sr1a_parsed SET doc_num = deed_book || '-' || deed_page;

CREATE INDEX ON nj_sr1a_parsed (parcel_id);
CREATE INDEX ON nj_sr1a_parsed (doc_num);
ANALYZE nj_sr1a_parsed;

SELECT COUNT(*) as parsed_rows, COUNT(DISTINCT parcel_id) as unique_parcels FROM nj_sr1a_parsed;

\echo 'Linking mortgage_records via parcel_id...'

-- Link: match parsed SR1A doc_num → mortgage_records, via parcel_id → property_id
UPDATE mortgage_records mr
SET property_id = p.id
FROM nj_sr1a_parsed sr
JOIN properties p ON p.parcel_id = sr.parcel_id
WHERE mr.document_number = sr.doc_num
  AND mr.source_url ILIKE '%sr1a%'
  AND mr.property_id IS NULL;

\echo 'Cleanup...'
DROP TABLE IF EXISTS nj_sr1a_staging;
DROP TABLE IF EXISTS nj_sr1a_parsed;

SELECT COUNT(*) as remaining_unlinked_nj FROM mortgage_records
WHERE property_id IS NULL AND source_url ILIKE '%nj-sr1a%';
SQL

echo ""
echo "Done: $(date)"
