# BBC public enrichment — 2026-09-22 02 UTC

Continued from origin/main 0631d94 using the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. The primary C:/Users/msanc/mxre checkout has unrelated changes and was preserved. Ranked the primary historical completion CSV against current configs, retaining earlier aliases and county/readiness holds. Public inventory exclusion remains provisional because authentication failed.

| Market | Active | Unlinked before / after | Historical overall | Result this run |
| --- | ---: | --- | ---: | --- |
| Elk Grove CA | 174 | 0 / 0 | 13.31% | Fresh audit confirms prior run's 139 coordinates; 35 shells; readiness hold |
| Fair Oaks CA | 95 | 0 / 0 | 13.16% | Fresh audit confirms prior run's 75 coordinates; 20 shells; readiness hold |
| Antelope CA | 66 | 0 / 0 | 12.88% | Added 25 parcel centroids; coordinates 26 to 51; 15 shells |
| Orangevale CA | 48 | 0 / 0 | 12.85% | Added six parcel centroids; coordinates 31 to 37; 11 shells |
| Folsom CA | 185 | 0 / 0 | 11.98% | Added 51 parcel centroids; coordinates 82 to 133; 52 shells |
| Citrus Heights CA | 98 | 0 / 0 | 9.01% | Exact linker no-op; 53 identities/coordinates; 45 shells |
| Wilton CA | 17 | 0 / 0 | 7.84% | Exact linker no-op; eight identities/coordinates; nine shells |
| Enumclaw WA | 13 | 12 / 12 | 1.28% | Exact linker no-op; one linked King County parcel; 12 unresolved |

Elk Grove/Fair Oaks enrichment was already applied by the previous run (retained tmp results show 86 and five updates respectively). Those 91 updates are not counted as this run's work. This fresh verification closes the incomplete checkpoint without replaying mutations. Historical percentages were not recalculated.

All California linked properties above have Sacramento county_id 773106 / FIPS 06067. Ran scripts/link-market-listings-fast.ts with exact-unique and SSH transport for Antelope, Orangevale, Folsom, Citrus Heights, Wilton, and Enumclaw; each changed zero links. No unlinked California listings required new shells. Enumclaw's single linked property has King county_id 748414 / FIPS 53033, but that alone does not establish county identity for all 12 remaining addresses. No speculative shells created.

## Verified coordinate enrichment

The public [Sacramento parcel layer](https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer/0) returned 82 unique APN matches with exact street/ZIP agreement and valid WGS84 centroids in bounded, non-truncated queries. Guarded updates required unchanged ID/APN/address/ZIP, correct county, sacramento-gis source, both coordinates null, and an active listing in the intended market. Exactly 82 properties and 82 listing provenance records were updated. Existing nonnull fields and listing timestamps were preserved. raw.sacramentoParcelCentroid records source, observation time, method, APN, property ID and coordinates; these are parcel centroids, not rooftops. No ownership, valuations or other unsupported facts were inferred.

All seven California markets lack audited ownership, year built, verified agent contacts, debt and rents. Their parcel identity/classification counts equal the coordinate counts in the table; remaining properties are shells. Even counting all stored values (shell asking prices), readiness is below the existing target of 28: Elk Grove 25.7, Fair Oaks 25.6, Antelope 25.3, Orangevale 25.3, Folsom 24.6, Citrus Heights 22.0, Wilton 21.0. Excluding unsupported valuations makes readiness lower. Coordinates do not affect the seven-component readiness score. No configs or readiness thresholds changed; no new market published.

## Validation and continuation

npm run typecheck and npm run build passed. Initial and final hydrated public probes returned HTTP 401, credentialAvailable=false. Public endpoint count and production_allowed status remain UNKNOWN. Local port 3101 returned HTTP 401 with the historical local smoke credential; local count also UNKNOWN. No API code changed and no restart was needed. No secrets printed/requested; no RapidAPI used. All protected access called hydrateWindowsUserEnv first.

Evidence is retained under publishing tmp/{antelope,orangevale,folsom,citrus-heights,wilton,enumclaw}-sep22-02-* and fresh Elk Grove/Fair Oaks audits. Database provenance is durable. Only this report is intended for commit.

Next: establish county/address evidence for Enumclaw's 12 remaining listings, then Wilmington OH (historical completion 0%, 80 CSV active). Preserve all prior holds, including Franklin's 77 unresolved links. Public deployment verification remains credential-blocked.
