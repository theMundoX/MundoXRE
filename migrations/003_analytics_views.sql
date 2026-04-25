-- 003_analytics_views.sql
-- Analytics foundation for the "deep analytical firm" goal.
--
-- Pattern: materialized views WITH NO DATA so they build instantly. The
-- refresh_analytics_views() function populates them in the background.
-- Re-run that function nightly via pg_cron or after a county ingest.

-- ───────────────────────────────────────────────────────────────────────
-- 0. Code violations / code enforcement (per-property + per-county counts)
--    Source: city/county code enforcement portals (Indianapolis: maps.indy.gov)
--    Top distress signal — 3+ violations on a property strongly correlates
--    with motivated seller.
-- ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS code_violations (
  id              bigserial PRIMARY KEY,
  property_id     integer REFERENCES properties(id),
  parcel_id       text,
  county_fips     char(5),
  case_number     text,
  case_status     text,            -- open, closed, in_compliance, etc.
  violation_type  text,            -- trash, unsafe structure, junk vehicle, weeds, etc.
  description     text,
  filed_date      date,
  closed_date     date,
  inspector       text,
  source_url      text,
  source_system   text,            -- 'indy_request' / 'indy_code_enf' / etc.
  raw             jsonb,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_code_violations_property ON code_violations (property_id);
CREATE INDEX IF NOT EXISTS idx_code_violations_parcel   ON code_violations (parcel_id);
CREATE INDEX IF NOT EXISTS idx_code_violations_county   ON code_violations (county_fips);
CREATE INDEX IF NOT EXISTS idx_code_violations_status   ON code_violations (case_status) WHERE case_status IN ('open','active','in_violation');
CREATE INDEX IF NOT EXISTS idx_code_violations_filed    ON code_violations (filed_date DESC);

-- ───────────────────────────────────────────────────────────────────────
-- 1. Distressed property flags (derived from mortgage_records + violations)
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_property_distress AS
SELECT
  p.id AS property_id,
  p.county_id,
  c.state_fips || c.county_fips AS county_fips_full,
  bool_or(mr.document_type = 'foreclosure'  AND mr.recording_date >= CURRENT_DATE - INTERVAL '24 months') AS active_foreclosure,
  bool_or(mr.document_type = 'lis_pendens'  AND mr.recording_date >= CURRENT_DATE - INTERVAL '12 months') AS pre_foreclosure,
  bool_or(mr.document_type IN ('tax_lien', 'tax_deed') AND mr.recording_date >= CURRENT_DATE - INTERVAL '24 months') AS tax_delinquent,
  bool_or(mr.document_type = 'judgment_lien' AND mr.recording_date >= CURRENT_DATE - INTERVAL '24 months') AS has_judgment,
  max(mr.recording_date) FILTER (WHERE mr.document_type = 'foreclosure')   AS last_foreclosure_date,
  max(mr.recording_date) FILTER (WHERE mr.document_type = 'lis_pendens')   AS last_lis_pendens_date,
  count(DISTINCT mr.id) FILTER (WHERE mr.document_type IN ('foreclosure','lis_pendens','tax_lien','judgment_lien')) AS distress_event_count,
  -- code violations join
  (SELECT count(*) FROM code_violations cv WHERE cv.property_id = p.id) AS code_violation_count,
  (SELECT count(*) FROM code_violations cv WHERE cv.property_id = p.id AND cv.case_status IN ('open','active','in_violation')) AS open_code_violations,
  (SELECT max(filed_date) FROM code_violations cv WHERE cv.property_id = p.id) AS last_code_violation_date
FROM properties p
JOIN counties c       ON p.county_id = c.id
LEFT JOIN mortgage_records mr ON mr.property_id = p.id
GROUP BY p.id, p.county_id, c.state_fips, c.county_fips
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_property_distress_pk ON mv_property_distress (property_id);
CREATE INDEX IF NOT EXISTS mv_property_distress_county    ON mv_property_distress (county_fips_full);

-- ───────────────────────────────────────────────────────────────────────
-- 2. Per-property equity estimate
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_property_equity AS
SELECT
  p.id AS property_id,
  p.county_id,
  p.market_value,
  COALESCE(SUM(mr.estimated_current_balance) FILTER (WHERE mr.open IS TRUE), 0) AS open_mortgage_balance,
  count(*) FILTER (WHERE mr.open IS TRUE) AS open_mortgage_count,
  CASE
    WHEN p.market_value IS NULL OR p.market_value = 0 THEN NULL
    ELSE p.market_value - COALESCE(SUM(mr.estimated_current_balance) FILTER (WHERE mr.open IS TRUE), 0)
  END AS estimated_equity,
  CASE
    WHEN p.market_value IS NULL OR p.market_value = 0 THEN NULL
    WHEN COALESCE(SUM(mr.estimated_current_balance) FILTER (WHERE mr.open IS TRUE), 0) = 0 THEN 1.0
    ELSE 1.0 - (COALESCE(SUM(mr.estimated_current_balance) FILTER (WHERE mr.open IS TRUE), 0)::numeric / p.market_value)
  END AS equity_percent,
  count(*) FILTER (WHERE mr.open IS TRUE) = 0 AS free_clear
FROM properties p
LEFT JOIN mortgage_records mr ON mr.property_id = p.id
GROUP BY p.id, p.county_id, p.market_value
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_property_equity_pk    ON mv_property_equity (property_id);

-- ───────────────────────────────────────────────────────────────────────
-- 3. Owner portfolio aggregates
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_owner_portfolio AS
SELECT
  upper(trim(p.owner_name)) AS owner_key,
  count(*) AS properties_owned,
  count(*) FILTER (WHERE p.last_sale_date >= CURRENT_DATE - INTERVAL '6 months')  AS purchased_last6,
  count(*) FILTER (WHERE p.last_sale_date >= CURRENT_DATE - INTERVAL '12 months') AS purchased_last12,
  sum(p.market_value)                AS portfolio_value,
  bool_or(p.corporate_owned IS TRUE) AS has_corporate_holdings,
  array_agg(DISTINCT (c.state_fips || c.county_fips))                           AS county_fips_list,
  array_agg(DISTINCT c.state_code)                                              AS state_list
FROM properties p
JOIN counties c           ON p.county_id   = c.id
WHERE p.owner_name IS NOT NULL AND length(trim(p.owner_name)) >= 3
GROUP BY upper(trim(p.owner_name))
HAVING count(*) >= 1
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_owner_portfolio_pk ON mv_owner_portfolio (owner_key);

-- ───────────────────────────────────────────────────────────────────────
-- 4. Sales activity rollup (county × month × asset_class)
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_monthly AS
SELECT
  mr.county_fips,
  date_trunc('month', mr.recording_date)::date AS month,
  CASE
    WHEN p.is_apartment THEN 'apartment'
    WHEN p.is_condo     THEN 'condo'
    WHEN p.is_sfr       THEN 'sfr'
    WHEN p.total_units >= 2 AND p.total_units <= 4 THEN 'mfh_2to4'
    WHEN p.total_units >= 5 THEN 'mfh_5plus'
    ELSE 'other'
  END AS asset_class,
  count(*) AS sale_count,
  sum(mr.original_amount)                AS dollar_volume,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY mr.original_amount) AS median_price,
  avg(mr.original_amount)                AS avg_price,
  min(mr.original_amount)                AS min_price,
  max(mr.original_amount)                AS max_price
FROM mortgage_records mr
LEFT JOIN properties p ON mr.property_id = p.id
WHERE mr.document_type IN ('deed', 'warranty_deed', 'special_warranty_deed', 'grant_deed', 'sale')
  AND mr.original_amount IS NOT NULL
  AND mr.original_amount > 1000
  AND mr.recording_date IS NOT NULL
GROUP BY mr.county_fips, date_trunc('month', mr.recording_date), asset_class
WITH NO DATA;

CREATE INDEX IF NOT EXISTS mv_sales_monthly_county_month ON mv_sales_monthly (county_fips, month DESC);

-- ───────────────────────────────────────────────────────────────────────
-- 5. Rent activity rollup (county × month × asset_class × beds)
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rent_monthly AS
SELECT
  c.state_fips || c.county_fips AS county_fips,
  date_trunc('month', rs.observed_at)::date AS month,
  CASE
    WHEN p.is_apartment THEN 'apartment'
    WHEN p.is_condo     THEN 'condo'
    WHEN p.is_sfr       THEN 'sfr'
    ELSE 'other'
  END AS asset_class,
  COALESCE(rs.beds, 0) AS beds,
  count(*) AS snapshot_count,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY rs.asking_rent)   AS median_asking_rent,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY rs.effective_rent) AS median_effective_rent,
  avg(rs.asking_rent)    AS avg_asking_rent,
  avg(rs.effective_rent) AS avg_effective_rent,
  avg(rs.days_on_market) AS avg_dom
FROM rent_snapshots rs
JOIN properties p ON rs.property_id = p.id
JOIN counties c   ON p.county_id   = c.id
WHERE rs.asking_rent IS NOT NULL AND rs.asking_rent > 0
  AND rs.observed_at IS NOT NULL
GROUP BY c.state_fips, c.county_fips, date_trunc('month', rs.observed_at), asset_class, COALESCE(rs.beds, 0)
WITH NO DATA;

CREATE INDEX IF NOT EXISTS mv_rent_monthly_county_month ON mv_rent_monthly (county_fips, month DESC);

-- ───────────────────────────────────────────────────────────────────────
-- 6. Appreciation index (county × year × asset_class)
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_appreciation_yearly AS
WITH yearly AS (
  SELECT
    county_fips,
    EXTRACT(year FROM month)::int AS year,
    asset_class,
    sum(sale_count)             AS sale_count,
    sum(dollar_volume)          AS dollar_volume,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY median_price) AS median_price
  FROM mv_sales_monthly
  GROUP BY county_fips, EXTRACT(year FROM month)::int, asset_class
)
SELECT
  y.county_fips,
  y.year,
  y.asset_class,
  y.sale_count,
  y.dollar_volume,
  y.median_price,
  prev.median_price AS prev_year_median,
  CASE
    WHEN prev.median_price IS NULL OR prev.median_price = 0 THEN NULL
    ELSE round(((y.median_price - prev.median_price) / prev.median_price * 100)::numeric, 2)
  END AS yoy_appreciation_pct
FROM yearly y
LEFT JOIN yearly prev
  ON prev.county_fips  = y.county_fips
 AND prev.asset_class  = y.asset_class
 AND prev.year         = y.year - 1
WITH NO DATA;

CREATE INDEX IF NOT EXISTS mv_appreciation_yearly_county ON mv_appreciation_yearly (county_fips, year DESC);

-- ───────────────────────────────────────────────────────────────────────
-- 7. County coverage / freshness scorecard
-- ───────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_county_coverage AS
SELECT
  c.id AS county_id,
  c.state_fips || c.county_fips AS county_fips_full,
  c.county_name,
  c.state_code,
  count(p.id)                                            AS properties_total,
  count(p.id) FILTER (WHERE p.owner_name IS NOT NULL)    AS with_owner,
  count(p.id) FILTER (WHERE p.mailing_address IS NOT NULL) AS with_mailing,
  count(p.id) FILTER (WHERE p.market_value IS NOT NULL)  AS with_value,
  count(p.id) FILTER (WHERE p.year_built IS NOT NULL)    AS with_year_built,
  count(p.id) FILTER (WHERE p.bedrooms IS NOT NULL)      AS with_beds,
  count(p.id) FILTER (WHERE p.last_sale_date IS NOT NULL) AS with_last_sale,
  count(p.id) FILTER (WHERE p.absentee_owner IS TRUE)    AS absentee_count,
  count(p.id) FILTER (WHERE p.corporate_owned IS TRUE)   AS corporate_count,
  (SELECT count(*) FROM mortgage_records mr WHERE mr.county_fips = c.state_fips || c.county_fips) AS recorder_docs,
  (SELECT max(recording_date) FROM mortgage_records mr WHERE mr.county_fips = c.state_fips || c.county_fips) AS last_filing_date,
  (SELECT count(*) FROM listing_signals ls JOIN properties pp ON ls.property_id = pp.id WHERE pp.county_id = c.id) AS listings,
  (SELECT count(DISTINCT rs.property_id) FROM rent_snapshots rs JOIN properties pp ON rs.property_id = pp.id WHERE pp.county_id = c.id) AS rented_props,
  (SELECT count(*) FROM code_violations cv WHERE cv.county_fips = c.state_fips || c.county_fips) AS code_violations,
  max(p.updated_at) AS last_property_update
FROM counties c
LEFT JOIN properties p ON p.county_id = c.id
GROUP BY c.id, c.state_fips, c.county_fips, c.county_name, c.state_code
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS mv_county_coverage_pk ON mv_county_coverage (county_id);

-- ───────────────────────────────────────────────────────────────────────
-- Refresh function — call from pg_cron nightly, or after any large ingest.
-- ───────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_property_distress;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_property_equity;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_owner_portfolio;
  REFRESH MATERIALIZED VIEW mv_sales_monthly;
  REFRESH MATERIALIZED VIEW mv_rent_monthly;
  REFRESH MATERIALIZED VIEW mv_appreciation_yearly;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_county_coverage;
END;
$$ LANGUAGE plpgsql;

-- ───────────────────────────────────────────────────────────────────────
-- HOTFIX 003a — broaden mv_sales_monthly doc_type filter.
-- The original filter looked for 'warranty_deed' / 'grant_deed' / 'sale' but
-- the actual mortgage_records corpus uses just 'deed' (2.08M rows).
-- Without 'deed' in the filter, mv_sales_monthly was empty even though sales
-- data was in the DB. Adding 'deed' brings 2M+ historical sales into scope
-- and makes mv_appreciation_yearly compute correctly.
-- (Already applied to live MXRE pg via direct re-create.)
-- ───────────────────────────────────────────────────────────────────────
