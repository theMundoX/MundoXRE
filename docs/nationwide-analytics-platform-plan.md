# MXRE Nationwide Analytics Platform Plan

MXRE should be the primary real estate data system of record. RealEstateAPI should be a paid fallback and validation layer, not the foundation.

## North Star

MXRE powers two products:

1. Property API: exact property facts for underwriting.
2. Market intelligence: change tracking, dashboards, opportunity discovery, and analytics for Buy Box Club.

The platform should answer:

- What is this property?
- What changed since the last refresh?
- How confident are we?
- Which market/zip/asset class is creating opportunity right now?

## Fallback Policy

RealEstateAPI is the preferred paid fallback because Buy Box Club already has familiarity with it.

Use RealEstateAPI only when:

- MXRE cannot match a property.
- MXRE is missing required underwriting fields.
- BBC explicitly requests fallback enrichment.
- We need a controlled market-gap validation sample.

Do not use RealEstateAPI for:

- unbounded nationwide bulk refreshes,
- replacing public-record ingestion,
- hiding MXRE coverage gaps,
- source fields in MXRE-owned records.

Every fallback response should record:

- fallback provider,
- fallback reason,
- MXRE completeness score at fallback time,
- fields filled by fallback,
- cost/accounting metadata where available.

## Open-Source Stack To Evaluate

The registry lives in `src/config/source-registry.ts` and is exposed privately at:

```text
GET /v1/platform/source-registry
```

Initial stack:

- County assessor/CAMA/GIS: primary parcel and property facts.
- County recorder/state deed transfers: primary sales, mortgage, lien, and ownership-change signals.
- Public listing pages: active listings, delistings, price changes, public remarks, public agent fields.
- Property websites: apartment complex names, floorplans, rents, concessions, availability.
- OpenAddresses: national address backbone and QA.
- Pelias/Nominatim: self-hosted geocoding/search instead of high-cost Google geocoding.
- Census TIGER/Line, ACS, HUD FMR/SAFMR: boundaries, demographics, baseline rent estimates.
- PostGIS: spatial joins, distance, neighborhoods, crime/transit/parcel analytics.
- Dagster: data asset orchestration by market/source/date partition.
- Temporal: durable long-running workflows for fragile county/source adapters.
- Debezium/Postgres CDC: later-stage change stream once event tables are stable.
- RealEstateAPI: paid fallback and validation.

## Open-Source Implementation Order

Do not install every platform tool at once. The order should match the bottleneck we are solving.

### 1. PostGIS

First priority because MXRE is becoming an analytical platform. PostGIS unlocks:

- parcel geometry,
- market boundaries,
- zip/tract/neighborhood rollups,
- distance to transit/crime/schools/jobs,
- parcel-to-listing spatial matching,
- apartment complex clustering.

### 2. OpenAddresses

Second priority because BBC needs address autocomplete/search parity without depending on RealEstateAPI. OpenAddresses helps:

- validate address universe coverage,
- normalize addresses,
- detect missing parcel addresses,
- power autocomplete/search fallback,
- QA county coverage.

### 3. Census TIGER/Line + ACS + HUD Formal Layer

Third priority because market analytics need official boundaries and context:

- tract/block/zip/county geometry,
- demographics,
- income,
- rent baselines,
- affordability metrics.

### 4. DuckDB Bulk Processing

Fourth priority for local processing of large county/state files before loading Postgres:

- shapefiles,
- CSV,
- parquet,
- dedupe,
- transform,
- fast QA counts.

### 5. Dagster

Fifth priority after the event/snapshot model is stable. Use it for:

- market/source partitions,
- refresh observability,
- data quality checks,
- materialized market metrics.

### 6. Pelias or Photon/Nominatim

Sixth priority when internal property autocomplete plus Geoapify is not enough. Prefer self-hosting once volume or cost justifies it.

### 7. Temporal

Seventh priority when long-running source workflows need durable retries/resume. Add it only after we know which workflows actually need it.

### 8. Debezium/Postgres CDC

Last priority. Start with explicit `property_events` first. CDC becomes valuable once event volume and downstream consumers justify it.

## Data Model Foundation

Nationwide daily refresh requires these core concepts:

- canonical property identity,
- source capability registry,
- refresh job runs,
- append-only property events,
- daily property snapshots,
- market daily metrics,
- API sync/update cursors.

Existing migrations already contain partial foundations:

- `property_history`
- `property_sales_history`
- `mls_listings`
- `mls_history`
- `change_events`
- `listing_signal_events`

Next schema work should unify these into a stable API-facing event model:

```text
property_events
property_snapshots
market_daily_metrics
api_sync_cursors
source_refresh_runs
market_source_capabilities
```

## Refresh Event Types

Every completed market should repeatedly detect:

- `parcel_created`
- `parcel_updated`
- `new_construction_detected`
- `lot_created_or_split`
- `ownership_changed`
- `deed_recorded`
- `mortgage_recorded`
- `lien_recorded`
- `lien_released`
- `listing_created`
- `listing_price_changed`
- `listing_pending`
- `listing_sold`
- `listing_delisted`
- `listing_relisted`
- `agent_contact_updated`
- `rent_observed`
- `rent_changed`
- `floorplan_changed`
- `creative_finance_detected`
- `preforeclosure_detected`
- `tax_changed`
- `assessment_changed`

## Nationwide Bottlenecks

1. County fragmentation: every county has different formats and access rules.
2. Identity matching: address, parcel, listing, deed, rent, and complex records must resolve to one canonical property.
3. Incremental refresh: re-scraping all records daily is not scalable.
4. Listing volatility: public listing sources drift and can rate-limit.
5. Rent availability: apartment websites vary heavily.
6. Agent contact coverage: public email/phone data is inconsistent.
7. Database performance: BBC should hit precomputed summaries, not raw joins.
8. Compliance: raw source URLs and sensitive provider details stay internal.

## Implementation Phases

### Phase 1: Stabilize BBC Integration

- Add BBC-normalized property endpoint.
- Add BBC-normalized market dashboard endpoint.
- Add update/since endpoint for properties BBC already pulled.
- Keep RealEstateAPI fallback behind explicit diagnostics.

### Phase 2: Make Daily Refresh Reusable

- Convert Indianapolis runner into a parameterized market runner.
- Add market configs for Dallas, Columbus, West Chester, and future metros.
- Add source refresh run logs to DB, not just filesystem logs.
- Emit property/listing/rent events from every refresh.

### Phase 3: Add Analytics Store

- Build daily market metrics by market, zip, asset type, and date.
- Store trend series for listings, sales, rents, price drops, and creative finance.
- API reads from aggregates for dashboards.

### Phase 4: Orchestration Decision

Evaluate Dagster vs Temporal after the source/event model is stable:

- Dagster if we primarily want data asset observability and partitions.
- Temporal if we primarily need durable long-running source workflows.
- Hybrid later if needed, but do not introduce both first.

### Phase 5: Nationwide Expansion

- Build county capability registry.
- Prioritize states/counties with open assessor/recorder feeds.
- Use RealEstateAPI sampling to quantify gaps and validate market completeness.
- Add PostGIS-backed geospatial analytics.

## Immediate Next Build Items

1. Implement `/v1/bbc/property`.
2. Implement `/v1/bbc/markets/{market}/changes`.
3. Implement `/v1/bbc/search-runs`.
4. Add `property_events` migration.
5. Add `source_refresh_runs` migration.
6. Modify daily runners to write refresh run summaries.
7. Add PostGIS planning migration.
8. Add OpenAddresses ingest prototype for one market.

## Buy Box Club Daily Search Flow

BBC owns user underwriting rules and exclusions. MXRE owns market truth and change detection.

1. BBC user defines a market search.
2. BBC sends filters and excluded MXRE ids to `/v1/bbc/search-runs`.
3. MXRE returns new or changed listing candidates only.
4. BBC underwrites returned candidates.
5. BBC stores pass/fail/user override state.
6. If a failed deal later has a price/status/rent/agent/creative-finance change, MXRE returns it again with a new `recordVersion`.

This avoids re-underwriting unchanged deals while still allowing previously failed deals to become eligible after a meaningful market change.
