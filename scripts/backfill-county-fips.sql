-- Backfill mortgage_records.county_fips (CHAR(5) = state_fips || county_fips)
-- Source of truth: existing `counties` table, joined via properties.county_id.
\timing on

\echo [1] ALTER TABLE + CREATE INDEX
ALTER TABLE public.mortgage_records ADD COLUMN IF NOT EXISTS county_fips CHAR(5);
CREATE INDEX IF NOT EXISTS idx_mortgage_records_county_fips ON public.mortgage_records(county_fips);

\echo [2] before counts
SELECT COUNT(*) AS total, COUNT(county_fips) AS with_fips FROM public.mortgage_records;

\echo [3] running backfill UPDATE
BEGIN;
UPDATE public.mortgage_records m
SET county_fips = (c.state_fips || c.county_fips)
FROM public.properties p, public.counties c
WHERE m.property_id = p.id
  AND p.county_id = c.id
  AND m.county_fips IS NULL;
COMMIT;

\echo [4] after counts
SELECT COUNT(*) AS total, COUNT(county_fips) AS with_fips, COUNT(*) - COUNT(county_fips) AS null_fips FROM public.mortgage_records;

\echo [5] residual null breakdown by property_id presence
SELECT (property_id IS NULL) AS no_property_id, COUNT(*)
FROM public.mortgage_records
WHERE county_fips IS NULL
GROUP BY 1;

\echo [6] residual nulls with property_id, by properties.state_code
SELECT p.state_code, COUNT(*)
FROM public.mortgage_records m
JOIN public.properties p ON p.id = m.property_id
WHERE m.county_fips IS NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
