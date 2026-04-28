# Indianapolis Market Refresh

MXRE market coverage should be reproducible. One-off fixes are allowed while developing a source adapter, but every repeatable task should become a script and then be added to the daily refresh orchestrator.

## Daily Command

```powershell
npm run market:indy:refresh
```

Dry run:

```powershell
npm run market:indy:refresh:dry
```

The orchestrator writes a JSON run record to:

```text
logs/market-refresh/
```

## Current Refresh Order

1. Classify Indianapolis parcel asset types.
2. Ingest Indianapolis public parcel signals.
3. Seed external CRE observations.
4. Upsert multifamily complex profiles.
5. Refresh apartment floorplan rent availability.
6. Refresh on-market listing signals.

Each step stays as a separate source-specific script so it can be rerun, debugged, or replaced without changing the whole pipeline.

## Coverage Tiers

Use these tiers when deciding whether a county or asset class is done:

- Parcel Coverage: every official parcel exists once in `properties`.
- Core Coverage: identity, asset classification, owner, and value are present.
- Underwriting Coverage: core coverage plus physical facts and transaction fields.
- Market Coverage: listing, sale comp, rent comp, mortgage, lien, and public-signal refreshes are current.

## Daily Operating Rule

If a data source matters to Buy Box Club, it should have:

- a reusable script in `scripts/`,
- a dry-run mode when practical,
- deterministic upsert behavior,
- a log or JSON summary,
- dashboard completion metrics that expose remaining gaps.

## Near-Term Gaps

The current `Indianapolis Core` scope is Marion County. The `Indianapolis Metro` scope covers the 11-county Indianapolis-Carmel-Greenwood MSA.

Metro parcel shells exist, but surrounding counties still need assessor enrichment for owner, valuation, physical facts, and transaction fields. The completion dashboard is the source of truth for which county should be worked next.

## HelloData-Style Rent Availability Layer

The HelloData sample is two products in one workbook:

- `Property Data`: stable property profile fields.
- `Availability`: daily/periodic floorplan and unit availability snapshots.

MXRE should reproduce this from public sources by storing both the stable facts and the time-series observations.

### Property Data Mapping

| HelloData field | MXRE target | Source strategy |
| --- | --- | --- |
| Building Name | `property_complex_profiles.complex_name` | property website, Google/business profile, apartment directory, manager site |
| Street Address / City / State / Zip | `properties` | assessor parcel, property website |
| Latitude / Longitude | `properties.lat/lng` | assessor/GIS/geocoder |
| MSA | market scope lookup | county-to-market mapping |
| Building Phone # | `property_complex_profiles.phone` | property website / public business profile |
| Building Website | `properties.website` or profile website | property website discovery |
| Property Type | `properties.asset_type/subtype` | assessor use code + classifier |
| # Units | `properties.total_units` | assessor, public listing, property website |
| Year Built / Stories | `properties.year_built/stories` | assessor + property website |
| Management Company | `property_complex_profiles.management_company` | property website / public business profile |
| Pet, admin, amenity, application, storage, parking fees | rent snapshot raw + future fee table | property website floorplan/fees page |
| Amenities | `property_complex_profiles.amenities` | property website |
| Deposit | rent snapshot raw + future fee table | property website |
| As Of | `last_seen_at` / `observed_at` | scraper run timestamp |

### Availability Mapping

| HelloData field | MXRE target | Source strategy |
| --- | --- | --- |
| Is Floorplan | floorplan vs unit snapshot flag | parsed from source page |
| Floorplan | `floorplans.name` | property website |
| Unit | future `unit_availability_snapshots.unit` | property website when exposed |
| Floor | future unit snapshot field | property website when exposed |
| Bed / Bath / Partial Bath | `floorplans.beds/baths` + snapshot raw | property website |
| First Listed | derived from first observed active date | daily snapshots |
| Listing Removed | derived from first missing date after active | daily snapshots |
| Days on Market | derived from first/last observation | daily snapshots |
| Min/Sqft/Max Sqft | `floorplans.sqft_min/sqft_max` | property website |
| Min/Mkt/Max Price | `rent_snapshots.rent_min/rent_max` | property website |
| Effective Price | future concession-adjusted rent | rent + concessions |
| Availability | snapshot status/count | property website |
| As Of | `rent_snapshots.observed_at` | scraper run date |

The daily job now includes `scripts/scrape-rents-bulk.ts` for Indianapolis so floorplan rent availability can be updated repeatedly rather than manually.
