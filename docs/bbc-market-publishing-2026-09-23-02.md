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

## South Bloomfield, OH — held

Historical CSV and current inventory: 21 active, overall completion0%. Exact-unique Pickaway2338949 first linked zero. Ohio address service supplied64 candidates, three exact street+ZIP+state/county matches with unit/range/building guards. Created three labeled shells with raw.ohioAddressCountyEvidence. Repeated parcel query by positive house number after broad query hit transfer limit:156 candidates, two unique normalized street matches corroborated by Ohio address ZIP and consistent hyphenated PARCELID / PARCELCAMA. No existing parcel collision. Promoted two shells to public parcel rows with owner/identity and raw.pickawayParcelEvidence; no value/classification inferred. Final21 active, unlinked21->18, two identities/owners, one shell,21 brokerage names, other enrichment zero. Listing timestamps preserved. Residual new-construction/lot addresses unresolved, market held; no live config.

Parcel source: https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/Parcels_Search/FeatureServer/1

## West Lake Hills, TX

Historical CSV21 active/21 unlinked/0% overall. Current32 active, unlinked32->0. Exact-unique Travis1973351 first linked zero. Queried official Travis parcels by house number and situs ZIP78746 after rejecting a broader truncated response;335 candidates. Twenty-three unique normalized situs street+state+ZIP matches, unique PROP_ID, no existing PROP_ID/geo_id collisions. Imported23 identities/owners/use classifications,21 county market_value fields and21 years. County market estimate is not assessed value or transaction price; unspecified vintage and py_owner_name freshness disclosed. Stored full raw.travisParcelEvidence. Nine residual active address/ZIP listings gained labeled shells with null identity/value, unknown/listing_only and raw.listingCityCountyEvidence based on23 linked county-cohort parcels. Preserved original unit/street text and all listing timestamps; availability was not refreshed.

Final32 properties,23 identities/owners/classifications,21 values/years,nine shells,six brokerage names,zero coords/debt/rents/verified contacts. Live config readiness40.2/28 with explicit limitations and fallback metrics. Historical overall0% is not current readiness.

Sources:
- https://taxmaps.traviscountytx.gov/arcgis/rest/services/Parcels/FeatureServer/0
- https://westlakehills.gov/ (Travis County confirmation)
- https://tigerweb.geo.census.gov/tigerwebmain/Files/acs26/tigerweb_acs26_incplace_tx.html (representative display point)

## Final validation and next target

Coventry Township config pushed in6c54aba. Typecheck/build/diff check passed again for West Lake Hills. Restarted verified node3101 with hidden detached helper/disposable local credential. Local HTTP200/count644; Coventry Township production_allowed44.3/28 and West Lake Hills production_allowed40.2/28. Hydrated public smoke HTTP401 credentialAvailable=false; public count and deployed production visibility remain UNKNOWN.

Next ranked target Autryville NC: preliminary read-only audit20 active/20 unlinked, no linked county,20 brokerage names, other enrichment zero, historical completion0%. Then Beavercreek Township OH and Fairlawn OH. Prior holds retained. Evidence in tmp/*-sep23-02-* and durable DB provenance; only src/api/server.ts and this report committed.
