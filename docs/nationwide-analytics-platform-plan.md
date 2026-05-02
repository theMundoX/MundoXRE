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
2. Implement `/v1/platform/source-registry`.
3. Add `property_events` migration.
4. Add `source_refresh_runs` migration.
5. Modify daily runners to write refresh run summaries.
6. Add `/v1/updates/properties?market=...&since=...`.
