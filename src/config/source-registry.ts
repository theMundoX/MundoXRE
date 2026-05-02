export type SourceCost = 'free' | 'open_source' | 'paid_fallback';
export type SourceRole =
  | 'system_of_record'
  | 'identity_resolution'
  | 'geospatial'
  | 'orchestration'
  | 'cdc'
  | 'listing_discovery'
  | 'rent_discovery'
  | 'fallback_validation'
  | 'analytics';

export type SourceRegistryEntry = {
  key: string;
  name: string;
  cost: SourceCost;
  roles: SourceRole[];
  priority: number;
  status: 'active' | 'planned' | 'evaluate' | 'fallback';
  useFor: string[];
  avoidFor?: string[];
  notes: string;
};

export const SOURCE_REGISTRY: SourceRegistryEntry[] = [
  {
    key: 'county_assessor_cama',
    name: 'County assessor / CAMA / GIS parcels',
    cost: 'free',
    roles: ['system_of_record', 'identity_resolution', 'geospatial'],
    priority: 1,
    status: 'active',
    useFor: ['parcel identity', 'owner facts', 'valuation', 'physical facts', 'asset classification', 'parcel geometry'],
    notes: 'Primary source for parcel universe and property facts. Coverage is county-specific and must be tracked by county capability.',
  },
  {
    key: 'county_recorder',
    name: 'County recorder / state deed transfer data',
    cost: 'free',
    roles: ['system_of_record', 'cdc'],
    priority: 1,
    status: 'active',
    useFor: ['sales events', 'deed transfers', 'mortgages', 'liens', 'lien releases', 'ownership change signals'],
    notes: 'Primary source for recorded transaction activity. Raw URLs stay internal; API exposes normalized source labels.',
  },
  {
    key: 'public_listing_pages',
    name: 'Public listing pages and search result feeds',
    cost: 'free',
    roles: ['listing_discovery', 'cdc'],
    priority: 2,
    status: 'active',
    useFor: ['active listings', 'delistings', 'price changes', 'public remarks', 'agent contact fields when public'],
    notes: 'Use respectful rate limits, residential proxy where configured, and event snapshots. Public listing data is volatile and needs fallback.',
  },
  {
    key: 'property_websites',
    name: 'Property and apartment websites',
    cost: 'free',
    roles: ['rent_discovery', 'cdc'],
    priority: 2,
    status: 'active',
    useFor: ['complex names', 'management company', 'floorplans', 'rent snapshots', 'concessions', 'availability changes'],
    notes: 'Best free path for HelloData-style rent availability, but every parser needs source drift monitoring.',
  },
  {
    key: 'openaddresses',
    name: 'OpenAddresses',
    cost: 'open_source',
    roles: ['identity_resolution', 'geospatial'],
    priority: 3,
    status: 'evaluate',
    useFor: ['address normalization', 'address universe validation', 'geocode fallback', 'source discovery hints'],
    notes: 'Useful for national address backbone and matching QA. Not a replacement for assessor parcel ownership.',
  },
  {
    key: 'pelias_or_nominatim',
    name: 'Pelias / Nominatim',
    cost: 'open_source',
    roles: ['identity_resolution', 'geospatial'],
    priority: 3,
    status: 'evaluate',
    useFor: ['geocoding', 'reverse geocoding', 'address search UX', 'matching weak listing addresses to parcels'],
    notes: 'Self-hosting avoids expensive Google geocoding at scale. Needs tuning with OpenAddresses and local parcel data.',
  },
  {
    key: 'census_tiger_acs_hud',
    name: 'Census TIGER/Line, ACS, HUD FMR/SAFMR',
    cost: 'free',
    roles: ['geospatial', 'analytics'],
    priority: 2,
    status: 'active',
    useFor: ['boundaries', 'tract/block groups', 'demographics', 'rent baseline estimates', 'market rollups'],
    notes: 'Core analytical layer for neighborhoods, affordability, and baseline rent estimates.',
  },
  {
    key: 'postgis',
    name: 'PostGIS',
    cost: 'open_source',
    roles: ['geospatial', 'analytics', 'identity_resolution'],
    priority: 1,
    status: 'planned',
    useFor: ['parcel geometry', 'distance to transit', 'crime radius', 'zip/tract rollups', 'spatial joins', 'market boundaries'],
    notes: 'Required for nationwide analytical dashboards and spatial matching performance.',
  },
  {
    key: 'dagster',
    name: 'Dagster',
    cost: 'open_source',
    roles: ['orchestration'],
    priority: 2,
    status: 'evaluate',
    useFor: ['market partitions', 'asset materialization', 'daily refresh observability', 'data quality checks'],
    notes: 'Best fit if we want a data-platform view of every market/source as assets and partitions.',
  },
  {
    key: 'temporal',
    name: 'Temporal',
    cost: 'open_source',
    roles: ['orchestration'],
    priority: 2,
    status: 'evaluate',
    useFor: ['long-running resilient county workflows', 'retries', 'resume after failure', 'rate-limited scrapes'],
    notes: 'Best fit for durable workflows. More operational overhead than simple cron, but stronger for nationwide refresh.',
  },
  {
    key: 'debezium_or_pg_cdc',
    name: 'Debezium / Postgres logical CDC',
    cost: 'open_source',
    roles: ['cdc'],
    priority: 3,
    status: 'evaluate',
    useFor: ['change streams', 'BBC update queues', 'cache invalidation', 'event fanout'],
    notes: 'Useful once property_events and snapshots are stable. Start with DB-trigger events before Kafka-scale CDC.',
  },
  {
    key: 'realestateapi',
    name: 'RealEstateAPI',
    cost: 'paid_fallback',
    roles: ['fallback_validation'],
    priority: 4,
    status: 'fallback',
    useFor: ['BBC fallback when MXRE misses required underwriting fields', 'bootstrap validation', 'gap analysis by market'],
    avoidFor: ['MXRE primary system of record', 'unbounded bulk refreshes without cost controls'],
    notes: 'Best paid fallback because BBC already has integration familiarity. Use behind policy, quotas, and diagnostics only.',
  },
];

export function getSourceRegistry() {
  return {
    version: 'mxre.source-registry.v1',
    primaryStrategy: 'MXRE-owned public-record system of record with RealEstateAPI as paid fallback.',
    entries: SOURCE_REGISTRY,
  };
}
