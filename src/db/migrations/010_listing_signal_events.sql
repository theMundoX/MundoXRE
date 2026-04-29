create table if not exists listing_signal_events (
  id bigserial primary key,
  listing_signal_id bigint references listing_signals(id) on delete set null,
  property_id bigint references properties(id) on delete set null,
  address text not null,
  city text not null,
  state_code text not null,
  zip text,
  listing_source text not null,
  listing_url text,
  event_type text not null,
  event_at timestamptz not null default now(),
  list_price numeric,
  previous_list_price numeric,
  mls_status text,
  previous_mls_status text,
  days_on_market integer,
  listing_agent_name text,
  listing_brokerage text,
  raw jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_listing_signal_events_signal
  on listing_signal_events(listing_signal_id, event_at desc);

create index if not exists idx_listing_signal_events_property
  on listing_signal_events(property_id, event_at desc);

create index if not exists idx_listing_signal_events_market
  on listing_signal_events(state_code, city, event_type, event_at desc);

create index if not exists idx_listing_signal_events_source
  on listing_signal_events(listing_source, event_at desc);
