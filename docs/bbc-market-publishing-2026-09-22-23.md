# BBC market publishing — 2026-09-22 23 UTC

Resumed from 2dd30fe after reading automation memory and recovering newer committed reports. Ranking used C:/Users/msanc/mxre/tmp/market-full-data-completion-report-best-first.csv; intentional publication changes used the established clean C:/Users/msanc/mxre-overnight-sep09 checkout. Earlier holds and unrelated primary-worktree changes were preserved. Fetched origin/main before work. Public exclusions remain provisional because authenticated public inventory was unavailable.

| Market | Active listings | Unlinked before -> after | Historical overall completion | Result |
| --- | ---: | ---: | ---: | --- |
| Gothenburg NE | 26 | 26 -> 0 | 0% | Held: readiness 15.4/28, incomplete enrichment |
| Bellbrook OH | 80 | 80 -> 0 | 0% | Live configuration; local production_allowed, readiness 41.1/28 |
| Peebles OH | 53 | 53 -> 19 | 0% | Held: 19 unresolved, county and parcel reconciliation |
| Morrisville PA | 26 | 26 -> 26 | 0% | Held: no exact match or linked-property county evidence |
| Thousand Oaks CA | 25 | 25 -> 25 | 0% | Read-only next-target audit; no linked county evidence |

Completion percentages are the historical CSV ranking snapshot, not freshly calculated enrichment percentages. Bellbrook and Peebles each had 26 active rows in that snapshot; current audits have 80 and 53 respectively.

## Gothenburg

Ran scripts/link-market-listings-fast.ts --exact-unique --ssh-psql for Dawson county_id 2338944 first: zero links. Nebraska Department of Revenue identifies https://www.nebraskaassessorsonline.us as a Dawson parcel-search provider. Queried the public Dawson search for all 26 listings and accepted 14 unique exact situs address/city/state/ZIP matches with conservative street-suffix normalization. Guarded transactions imported those 14 county parcel IDs and owners and linked the listings with raw.dawsonParcelEvidence. No valuation, classification or other enrichment was inferred.

Re-audited linked county evidence, then created 12 explicitly labeled listing-backed shells for remaining addresses in ZIP 69138. Their county assignment uses the city cohort supported by the 14 linked Dawson parcels, with pending property-level reconciliation recorded in raw.listingCityCountyEvidence. Ambiguous 1021 16TH ST has two source candidates: neither parcel identity was selected. Lot/road address text is preserved. Shell identities/values are null, classification unknown, confidence listing_only. Final: 26 linked properties, 14 identities/owners, 12 shells, 26 brokerage rows, all other audited enrichment zero. Readiness 15.4 is below 28; no config added.

Source directory: https://revenue.nebraska.gov/property-assessment-county-contact-information-parcel-search

## Bellbrook

Exact-unique linker first searched Greene county_id 2338903 and found zero. Public county parcels https://gis.greenecountyohio.gov/webgis2/rest/services/DynamicLayers/PublicNotifications/MapServer/1 returned 1199 house-number candidates across bounded batches, with transfer-limit checks. Accepted 57 single-feature exact situs-address/ZIP/state matches, requiring consistent Parcel_Id and normalized Parcel_Number and no source unit. Guarded transactions inserted and linked 57 parcels, preserving source attributes in raw.greeneParcelEvidence: 57 owners/use classifications, 55 positive Assessed_Total values, 52 years built. Assessed_Total is tax assessment, not market price; Tax_Year 2025 is retained in source evidence.

Created 22 clearly labeled shells in ZIP 45305 using the linked Greene cohort. Original unit, lot and address-range text remains unchanged; potential duplicate marketing addresses remain unresolved. No shell identity/value/classification was inferred. Excluded 1940 ALDA CT / 45459 from this county assignment.

For that last listing, searched Ohio address points and Montgomery's official address/CAMA services. Alda Court address points are Montgomery County; the CAMA table has one exact 1940 ALDA CT / O67 28912 0006 record. Ran Montgomery exact-unique linker first (zero). Located existing county parcel property_id 79252416 / O67289120006 with identical street address. Reused that parcel under guards; refreshed its older stored owner and null assessment from the public CAMA record, preserving previous owner/value and full source evidence in raw.montgomeryParcelEvidence. Current Assessed_Total is 26070. No duplicate parcel was inserted. Listing Bellbrook/45459 and stored parcel Miamisburg/45342 labels conflict and were preserved; identity reconciliation relies on exact county situs and parcel ID, not a claimed ZIP match. CAMA mailing ZIP was not treated as situs ZIP. Source: https://gis.mcohio.org/server/rest/services/VantagePoints/AUDGIS_B1/MapServer/15

Final audit: 80 active/linked properties, 58 identities/owners/classifications, 56 values, 52 years, 22 shells, three brokerage rows; no coordinates/debt/rents/verified contacts/positive creative-finance evidence. Counties: Greene 79, Montgomery one. All linking/import transactions preserve listing timestamps. Latest stored timestamp remains 2026-09-21T13:41:26.682+00:00; assessor transfers can postdate individual listing observations, and availability was not refreshed.

Added Bellbrook to MARKET_CONFIGS with audited field-level metrics, primary county 2338903, explicit cross-county scope and address/ZIP conflicts, shell/enrichment restrictions and readiness target 28. Display location is the mean of 58 exact matched Ohio address points; it is not stored as parcel coordinates. Local score 41.1/28.

## Peebles and next targets

Official Ohio address service https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 154 candidates and 34 unique exact address/ZIP/state matches with blank unit/range/building/floor fields: Adams 31, Pike three. Created Adams county_id 2338957, FIPS 39001, verified with Census: https://tigerweb.geo.census.gov/tigerwebmain/Files/acs26/tigerweb_acs26_county_oh.html

Ran exact-unique linker for Adams and existing Pike 2338942 before shell creation: both zero. Guarded transaction inserted 34 county-evidenced shells and linked 34, with raw.ohioAddressCountyEvidence, null identity/value and unknown/listing_only classification. Final 19 unlinked remain; no blanket county assignment or config added.

Morrisville exact-unique Bucks 1498719 search returned zero; re-audit remains 26 unlinked. Thousand Oaks next-target read-only audit has 25 active/25 unlinked and no county evidence. Continue parcel/county reconciliation there, then Jamestown OH. Preserve earlier holds and Morrisville's unresolved county evidence.

## Validation

npm run typecheck, npm run build and git diff --check passed. Restarted the verified node.exe API origin on port 3101 through the existing hidden detached helper with a disposable local credential. Local smoke returned HTTP 200, 640 markets and Bellbrook production_allowed with readiness 41.1/28. Public probes returned HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv; public endpoint count and production visibility remain UNKNOWN. No verified public publication is claimed.

A broad Bellbrook existing-property preflight exceeded its 45-second statement timeout; the subsequent guarded import transaction succeeded with its own exact collision checks. All DB/API access scripts hydrate Windows user environment before access. No RapidAPI, secrets requested, or secrets printed. Detailed evidence remains in tmp/*-sep22-23-*; imported provenance is durable in the database. Only src/api/server.ts and this report are intentional commit files.
