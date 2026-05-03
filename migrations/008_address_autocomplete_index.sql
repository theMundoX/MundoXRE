create table if not exists address_autocomplete_entries (
  id bigserial primary key,
  source text not null,
  external_id text,
  type text not null default 'address',
  label text not null,
  street text,
  city text,
  state_code text not null,
  zip text,
  county text,
  lat numeric,
  lng numeric,
  confidence text not null default 'medium',
  mxre_property_id bigint,
  market_key text,
  normalized_label text generated always as (
    regexp_replace(upper(coalesce(label, '')), '[^A-Z0-9 ]', ' ', 'g')
  ) stored,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(source, external_id)
);

create index if not exists idx_address_autocomplete_label_prefix
  on address_autocomplete_entries (normalized_label text_pattern_ops);

create index if not exists idx_address_autocomplete_state_city
  on address_autocomplete_entries (state_code, city);

create index if not exists idx_address_autocomplete_mxre_property
  on address_autocomplete_entries (mxre_property_id)
  where mxre_property_id is not null;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_name = 'address_autocomplete_entries'
      and column_name = 'normalized_label'
  ) then
    alter table address_autocomplete_entries drop column normalized_label;
    alter table address_autocomplete_entries add column normalized_label text generated always as (
      regexp_replace(upper(coalesce(label, '')), '[^A-Z0-9 ]', ' ', 'g')
    ) stored;
  end if;
end $$;

create index if not exists idx_address_autocomplete_label_prefix
  on address_autocomplete_entries (normalized_label text_pattern_ops);
