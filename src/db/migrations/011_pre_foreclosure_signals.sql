create table if not exists pre_foreclosure_signals (
  id bigserial primary key,
  property_id bigint references properties(id) on delete set null,
  parcel_id text,
  address text not null,
  city text,
  state_code text not null,
  zip text,
  county_id bigint,
  county_name text,
  owner_name text,
  borrower_name text,
  lender_name text,
  case_number text,
  filing_date date,
  sale_date date,
  auction_date date,
  notice_type text,
  status text not null default 'active',
  source text not null,
  source_url text,
  confidence text not null default 'single_source',
  raw jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_pre_foreclosure_signals_case_source
  on pre_foreclosure_signals(state_code, county_name, source, case_number)
  where case_number is not null;

create index if not exists idx_pre_foreclosure_signals_property
  on pre_foreclosure_signals(property_id, last_seen_at desc);

create index if not exists idx_pre_foreclosure_signals_market
  on pre_foreclosure_signals(state_code, city, status, last_seen_at desc);

create index if not exists idx_pre_foreclosure_signals_county
  on pre_foreclosure_signals(state_code, county_name, status, filing_date desc);
