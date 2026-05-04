-- Fallback enrichment cache/queue for paid or licensed providers.
-- The external API response stays internal; public/BBC responses use normalized
-- MXRE tables plus meta.sourceMix/dataQuality provenance.

create table if not exists realestateapi_property_details (
  id bigserial primary key,
  property_id bigint not null references properties(id) on delete cascade,
  realestateapi_id text,
  request_body jsonb not null default '{}'::jsonb,
  response_body jsonb not null,
  normalized_summary jsonb not null default '{}'::jsonb,
  status text not null default 'ok',
  fetched_at timestamptz not null default now(),
  expires_at timestamptz not null default now() + interval '30 days',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(property_id)
);

create index if not exists idx_reapi_property_details_expires
  on realestateapi_property_details(expires_at);

create index if not exists idx_reapi_property_details_reapi_id
  on realestateapi_property_details(realestateapi_id)
  where realestateapi_id is not null;

create table if not exists property_enrichment_queue (
  id bigserial primary key,
  property_id bigint not null references properties(id) on delete cascade,
  provider text not null,
  reason text not null,
  status text not null default 'queued',
  priority int not null default 100,
  attempts int not null default 0,
  next_run_at timestamptz not null default now(),
  locked_at timestamptz,
  completed_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(property_id, provider, reason)
);

create index if not exists idx_property_enrichment_queue_ready
  on property_enrichment_queue(provider, status, priority, next_run_at);

create index if not exists idx_property_enrichment_queue_property
  on property_enrichment_queue(property_id);
