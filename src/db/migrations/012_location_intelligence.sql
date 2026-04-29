create table if not exists transit_stops (
  id bigserial primary key,
  source text not null,
  stop_id text not null,
  stop_code text,
  stop_name text,
  lat numeric not null,
  lon numeric not null,
  routes text[] not null default array[]::text[],
  raw jsonb,
  observed_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(source, stop_id)
);

create index if not exists idx_transit_stops_source on transit_stops(source);
create index if not exists idx_transit_stops_lat_lon on transit_stops(lat, lon);

create table if not exists crime_incidents (
  id bigserial primary key,
  source text not null,
  source_object_id text not null,
  case_number text,
  occurred_at timestamptz,
  incident_year integer,
  incident_type text,
  nibrs_class text,
  nibrs_class_desc text,
  class_type text,
  disposition text,
  block_address text,
  city text,
  zip text,
  district text,
  beat text,
  lat numeric,
  lon numeric,
  raw jsonb,
  observed_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(source, source_object_id)
);

create index if not exists idx_crime_incidents_source_object on crime_incidents(source, source_object_id);
create index if not exists idx_crime_incidents_zip_date on crime_incidents(zip, occurred_at desc);
create index if not exists idx_crime_incidents_lat_lon on crime_incidents(lat, lon);
create index if not exists idx_crime_incidents_class_date on crime_incidents(class_type, occurred_at desc);

create table if not exists property_location_scores (
  property_id bigint primary key references properties(id) on delete cascade,
  nearest_bus_stop_id text,
  nearest_bus_stop_name text,
  nearest_bus_distance_miles numeric,
  bus_routes text[] not null default array[]::text[],
  crime_incidents_025mi_365d integer,
  crime_incidents_05mi_365d integer,
  crime_incidents_1mi_365d integer,
  violent_crime_05mi_365d integer,
  property_crime_05mi_365d integer,
  drug_crime_05mi_365d integer,
  crime_score numeric,
  crime_score_basis text,
  scored_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_property_location_scores_bus_distance
  on property_location_scores(nearest_bus_distance_miles);

create index if not exists idx_property_location_scores_crime_score
  on property_location_scores(crime_score);
