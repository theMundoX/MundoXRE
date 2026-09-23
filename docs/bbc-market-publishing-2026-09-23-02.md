# BBC market publishing — 2026-09-23 02 UTC

Ranked the supplied best-first completion CSV by completion and active volume, excluding configured live markets and preserving prior holds. Public inventory exclusions remain provisional: hydrated credentials unavailable and deployed /v1/bbc/markets HTTP 401. No RapidAPI or secrets requested/printed. Primary checkout dirty changes preserved; intentional code publishing uses established clean mxre-overnight-sep09 checkout, aligned with origin/main.

## Coventry Township, OH

Historical CSV: 21 active / 21 unlinked / 0% overall completion. Current database: 89 active, unlinked 89 -> 0. Correct linked county is Summit 1698989 / FIPS39153. Exact-unique linker ran first and linked six existing properties, advancing their listing timestamps; this was not an availability refresh.

Ohio address service supplied 442 candidates and 74 unique street+ZIP+state matches, all Summit. Summit parcel service supplied 5,677 candidates with transfer-limit checks. Accepted 67 unique normalized situs streets corroborated by Ohio addresses, blank parcel unit, consistent parcelid/lowparcelid and COVENTRY TOWNSHIP tax district. All existed under unique county/parcel keys. Reconciled those existing properties to public owner, cntassdval, usecd and year built; preserved prior fields and full source attributes in raw.summitParcelEvidence. Four already linked, 63 newly linked. Mailing ZIP was not used as situs evidence; assessed value vintage unspecified. Custom transactions preserved listing timestamps.

Twenty residual active address/ZIP listings had no existing exact address/ZIP property collision. Created 20 labeled listing-backed shells with null parcel/value, unknown/listing_only classification and raw.listingCityCountyEvidence, using the 69 linked parcel cohort for county assignment. No arbitrary selection among multi-parcel source addresses; individual identities, address ranges and duplicate marketing addresses remain unresolved.

Final audit: 89 linked properties, 69 identities/owners/values/classifications/years, 20 shells, 32 existing coordinates, zero debt/rent/verified contacts/brokerage. Two exact-linked properties retain legacy enrichment rather than newly verified values. Live config includes fallback metrics and these limitations. Overall CSV completion 0% is historical, distinct from current readiness44.3/28.

Sources:
- https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0
- https://scgis.summitoh.net/hosted/rest/services/parcels_web_GEODATA_Tax_Parcels/FeatureServer/0
- https://tigerweb.geo.census.gov/tigerwebmain/Files/acs26/tigerweb_acs26_cousub_oh.html (county verification and representative display point)

## Validation

npm run typecheck, npm run build and git diff --check passed. Restarted verified node origin3101 via hidden detached helper and disposable local credential. Local HTTP200, count643, coventry-township-oh production_allowed readiness44.3/28. Public repeated HTTP401, credentialAvailable=false; public count and production visibility UNKNOWN. Evidence tmp/*-sep23-02-* and durable database provenance.

Next ranked target South Bloomfield OH, then West Lake Hills TX; retain earlier holds.
