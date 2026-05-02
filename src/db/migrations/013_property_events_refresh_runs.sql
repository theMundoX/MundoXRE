-- Migration 013 - MXRE property events, refresh runs, market metrics, and API sync cursors
--
-- Foundation for Buy Box Club delta search, safe re-sync, and nationwide daily refreshes.
-- Safe to re-run: every statement uses IF NOT EXISTS where PostgreSQL supports it.

create table if not exists source_refresh_runs (
  id bigserial primary key,
  run_id text not null unique,
  market_key text not null,
  source_key text not null,
  job_name text not null,
  status text not null default 'running'
    check (status in ('running', 'success', 'partial_success', 'failed', 'cancelled')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  records_seen integer not null default 0,
  records_inserted integer not null default 0,
  records_updated integer not null default 0,
  records_unchanged integer not null default 0,
  records_failed integer not null default 0,
  error_message text,
  log_path text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_source_refresh_runs_market
  on source_refresh_runs(market_key, started_at desc);

create index if not exists idx_source_refresh_runs_source
  on source_refresh_runs(source_key, started_at desc);

create index if not exists idx_source_refresh_runs_status
  on source_refresh_runs(status, started_at desc)
  where status in ('running', 'failed', 'partial_success');

create table if not exists property_events (
  id bigserial primary key,
  property_id bigint references properties(id) on delete set null,
  listing_signal_id bigint references listing_signals(id) on delete set null,
  source_refresh_run_id bigint references source_refresh_runs(id) on delete set null,
  market_key text not null,
  event_type text not null,
  event_at timestamptz not null default now(),
  changed_fields text[] not null default array[]::text[],
  previous_values jsonb,
  current_values jsonb,
  source text not null,
  source_category text not null default 'public_record',
  confidence text not null default 'medium',
  underwriting_relevant boolean not null default true,
  record_version text,
  raw jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_property_events_property
  on property_events(property_id, event_at desc);

create index if not exists idx_property_events_market
  on property_events(market_key, event_type, event_at desc);

create index if not exists idx_property_events_underwriting
  on property_events(market_key, event_at desc)
  where underwriting_relevant = true;

create unique index if not exists idx_property_events_record_version
  on property_events(record_version)
  where record_version is not null;

create table if not exists property_snapshots (
  id bigserial primary key,
  property_id bigint not null references properties(id) on delete cascade,
  captured_at timestamptz not null default now(),
  schema_version text not null default 'mxre.property_snapshot.v1',
  record_version text not null,
  snapshot jsonb not null,
  source_refresh_run_id bigint references source_refresh_runs(id) on delete set null,
  created_at timestamptz not null default now()
);

create unique index if not exists idx_property_snapshots_version
  on property_snapshots(property_id, record_version);

create index if not exists idx_property_snapshots_property
  on property_snapshots(property_id, captured_at desc);

create table if not exists market_daily_metrics (
  id bigserial primary key,
  market_key text not null,
  metric_date date not null,
  asset_group text not null default 'all',
  geography_type text not null default 'market',
  geography_id text not null default 'all',
  active_listings integer not null default 0,
  new_listings integer not null default 0,
  delisted_listings integer not null default 0,
  price_changes integer not null default 0,
  price_drops integer not null default 0,
  sold_listings integer not null default 0,
  deed_sales integer not null default 0,
  rent_observations integer not null default 0,
  creative_finance_positive integer not null default 0,
  preforeclosure_active integer not null default 0,
  median_list_price numeric,
  median_rent numeric,
  median_days_on_market numeric,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (market_key, metric_date, asset_group, geography_type, geography_id)
);

create index if not exists idx_market_daily_metrics_market
  on market_daily_metrics(market_key, metric_date desc);

create table if not exists api_sync_cursors (
  id bigserial primary key,
  client_id text not null,
  cursor_key text not null,
  market_key text,
  last_sync_at timestamptz,
  last_event_id bigint,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (client_id, cursor_key)
);

create index if not exists idx_api_sync_cursors_client_market
  on api_sync_cursors(client_id, market_key);

-- Backfill property_events from listing_signal_events so BBC changes endpoint has a unified source.
insert into property_events (
  property_id,
  listing_signal_id,
  market_key,
  event_type,
  event_at,
  changed_fields,
  previous_values,
  current_values,
  source,
  source_category,
  confidence,
  underwriting_relevant,
  record_version,
  raw
)
select
  e.property_id,
  e.listing_signal_id,
  case
    when upper(coalesce(e.city,'')) = 'INDIANAPOLIS' and e.state_code = 'IN' then 'indianapolis'
    when upper(coalesce(e.city,'')) = 'COLUMBUS' and e.state_code = 'OH' then 'columbus'
    when upper(coalesce(e.city,'')) = 'WEST CHESTER' and e.state_code = 'PA' then 'west-chester'
    else lower(coalesce(e.city, 'unknown'))
  end as market_key,
  e.event_type,
  e.event_at,
  case
    when e.event_type in ('price_changed', 'listing_price_changed') then array['market.listPrice']
    when e.event_type in ('status_changed', 'listing_status_changed') then array['market.status']
    else array[e.event_type]
  end as changed_fields,
  jsonb_strip_nulls(jsonb_build_object(
    'listPrice', e.previous_list_price,
    'status', e.previous_mls_status
  )) as previous_values,
  jsonb_strip_nulls(jsonb_build_object(
    'listPrice', e.list_price,
    'status', e.mls_status,
    'listingUrl', e.listing_url
  )) as current_values,
  coalesce(e.listing_source, 'public_listing') as source,
  'public_listing' as source_category,
  'medium' as confidence,
  true as underwriting_relevant,
  md5(coalesce(e.property_id::text,'') || '|' || coalesce(e.listing_signal_id::text,'') || '|' || e.event_type || '|' || e.event_at::text) as record_version,
  e.raw
from listing_signal_events e
where e.event_at is not null
on conflict (record_version) where record_version is not null do nothing;
