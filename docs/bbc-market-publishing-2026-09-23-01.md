# BBC market publishing — 2026-09-23 01 UTC

Continued from d53693e, newer than the prior automation memory. Used the ranked CSV in C:/Users/msanc/mxre and the established clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Prior holds and unrelated primary worktree changes were preserved. Public inventory exclusions remain provisional: hydrated public API requests return HTTP 401 without an available credential.

| Market | Active listings | Unlinked before -> after | Historical overall completion | Result |
| --- | ---: | ---: | ---: | --- |
| Southborough MA | 23 | 23 -> 0 | 0% | Live configuration; local production_allowed, readiness 32.3/28 |
| Rockbridge OH | 23 | 23 -> 12 | 0% | Held: unresolved addresses and enrichment gaps |
| Green OH | 77 | 77 -> 77 | 0% (CSV 22 active) | Next county/parcel reconciliation target |

Completion is the historical ranking snapshot, not recalculated enrichment. Stored active flags were not refreshed.

## Southborough

No initial linked county evidence. Census identifies Southborough as Worcester County, FIPS 25027; created missing county_id 2338958. Official MassGIS town parcels support the assignment. Ran scripts/link-market-listings-fast.ts with exact-unique and SSH transport before imports: zero links.

Retrieved all 4,032 town parcel features with ordered pagination and transfer-limit checks. Thirteen unique normalized situs-street and town matches had nonblank PROP_ID, TOWN_ID 277, NO_MATCH N and blank LOCATION. Guarded transactions imported and linked 13 identities, owners, use descriptions, assessed totals, years built and positive building areas. Source FY2027 tax assessments are not market prices. Situs ZIP is null: ZIP is listing-derived and not independently verified; owner mailing ZIP was not used. Full attributes, source and limitations are retained in raw.massgisParcelEvidence. No property coordinates, debt, rents, or verified contacts inferred.

After linked county corroboration, created 10 labeled listing-backed shells for residual address/ZIP 01772 listings. County assignment uses the 13 linked town parcels; unresolved individual parcel identity, unit/lot text and possible duplicate marketing addresses are disclosed in raw.listingCityCountyEvidence. Shells have null parcel/value and unknown/listing_only classification. Transactions guard active/unlinked listing state, unchanged address/ZIP, expected counts and existing property collisions; listing timestamps were preserved.

Final audit: 23 active/linked properties, zero unlinked; 13 identities/owners/values/classifications/years and 10 shells; 23 brokerage rows, zero coordinates/debt/rents/verified contacts. Added MARKET_CONFIGS with audited fallbackCoverageMetrics and limitations. Display location is the Census town representative point, not property coordinates. Latest stored listing timestamp remains 2026-05-07T15:32:31.784976+00:00.

Sources:
- https://services1.arcgis.com/hGdibHYSPO59RG1h/ArcGIS/rest/services/Massachusetts_Property_Tax_Parcels/FeatureServer/0
- https://www.mass.gov/info-details/massgis-data-property-tax-parcels
- https://tigerweb.geo.census.gov/tigerwebmain/Files/acs26/tigerweb_acs26_cousub_ma.html

## Rockbridge

Official Ohio address query returned 33 candidates and 11 unique exact street/ZIP/state matches in Hocking county_id 2338926. Exact-unique linker first returned zero. Created 11 labeled shells only after unit/range/building/floor safety checks; durable raw.ohioAddressCountyEvidence, null identity/value, unknown classification, unchanged listing timestamps. Remaining 12 addresses lack exact county evidence and remain unlinked. All audited substantive enrichment is absent; market held.

Source: https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0

## Validation

npm run typecheck, npm run build and git diff --check passed. Restarted verified node origin on port 3101 with the existing hidden detached helper and disposable local credential. Local HTTP 200 returned 642 markets and southborough-ma production_allowed at readiness 32.3/28. Public repeated HTTP 401, credentialAvailable=false after hydration; public market count and public production visibility remain UNKNOWN. No RapidAPI or secrets requested/printed.

Detailed evidence is in tmp/*-sep23-01-* and durable database provenance. Only src/api/server.ts and this report are intentional commit files. Green OH is next, followed by Coventry Township OH; earlier holds remain.
