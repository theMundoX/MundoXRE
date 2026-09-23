# BBC market publishing — 2026-09-23 00 UTC

Continued from e4eb873 using the ranked CSV in C:/Users/msanc/mxre and the established clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Fetched origin/main; unrelated primary checkout changes and earlier holds remain untouched. Ranking is overall completion descending then active listing count; completed/held prior candidates were retained as checkpoints. Public inventory exclusions remain provisional because the authenticated endpoint is unavailable.

| Market | Active listings | Unlinked before -> after | Historical overall completion | Result |
| --- | ---: | ---: | ---: | --- |
| Thousand Oaks CA | 25 | 25 -> 1 | 0% | Held: remaining address/county reconciliation |
| Jamestown OH | 24 | 24 -> 0 | 0% | Live configuration; local production_allowed, readiness 31.0/28 |
| Southborough MA | 23 | 23 -> 23 | 0% | Next target: county and parcel evidence required |

Completion is the historical CSV ranking snapshot, not a recalculated enrichment percentage. Stored active flags were not refreshed.

## Thousand Oaks

No initially linked properties. Existing Ventura county_id 774899 / FIPS 06111 was corroborated by county public parcel and address records. Ran scripts/link-market-listings-fast.ts --exact-unique --ssh-psql before imports: zero links.

Queried 1484 public parcel candidates and 1889 county address references in bounded batches with transfer-limit checks. Accepted 12 unique exact normalized situs-address/ZIP/state matches with blank suites, consistent parcel APN/APN10/suffix, and corroborating address APN. Guarded transactions imported and linked 12 identities and 12 tax values (L_V plus I_V). No owners, classification or coordinates inferred. Values have unspecified vintage and are not market prices. Source master address points explicitly describe reference addresses rather than official verified address points; that limitation is retained in raw.venturaParcelEvidence.

Sources:
- https://maps.venturacounty.gov/arcgis/rest/services/SDs/Parcels/MapServer/0
- https://maps.ventura.org/arcgis/rest/services/DataDownloads/Address/FeatureServer/0

Using the 12 linked Ventura parcels, created 12 labeled listing-backed shells for remaining ZIP 91361 rows with addresses. County assignment uses the linked city/ZIP cohort, not verified individual parcels. Null identity/value, unknown/listing_only classification, and original lot/zero-number/incomplete street text are preserved. Multiple parcels for 900 W STAFFORD RD remain unresolved. raw.listingCityCountyEvidence records limitations. Excluded 622 E CARLISLE RD / 91360 because this ZIP lacked the same corroboration; no blanket assignment was made. Final 25 active, 24 linked, one unlinked, 12 identities/values, 12 shells, 25 brokerage rows and no ownership/classification/debt/rent/verified-contact/property-coordinate support. Market remains held. Listing timestamps unchanged.

## Jamestown

No initially linked properties. Public Ohio address references corroborated Greene county_id 2338903 / FIPS 39057. Exact-unique linker first returned zero. Ohio address query yielded 42 candidates and 14 unique exact address/ZIP/state matches in Greene. Public Greene parcels yielded 277 candidates and 13 unique exact situs-address/ZIP/state matches with blank unit and consistent Parcel_Id/Parcel_Number. Imported 13 parcels with owners, assessed totals, source land-use descriptions and 12 years built; full evidence retained in raw.greeneParcelEvidence. Assessed totals are tax-year 2025 values, not market prices. No property coordinates, debt or contact verification inferred.

Sources:
- https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0
- https://gis.greenecountyohio.gov/webgis2/rest/services/DynamicLayers/PublicNotifications/MapServer/1

After linked county corroboration, created 11 labeled shells for residual address/ZIP 45335 rows using the linked city/ZIP cohort. Original lot, zero-number and incomplete street text retained, null identity/value, unknown/listing_only classification, with unresolved property-level reconciliation disclosed in raw.listingCityCountyEvidence. All import/shell transactions guard listing identity, active status, unchanged address/ZIP, unlinked state and existing property collisions; listing timestamps were preserved.

Final 24 active/linked properties, zero unlinked; 13 identities/owners/tax values/source land-use classifications, 12 years, 11 shells, no property coordinates/brokerage/verified contacts/debt/rent/positive creative-finance coverage. Audit classification counts exclude unknown and listing_backed_property placeholders. Added MARKET_CONFIGS entry with these field-level metrics and restrictions; readiness target 28, actual 31.0. Display location is the mean of matched Ohio address points, not property coordinates. Latest stored listing timestamp remains 2026-05-18T07:23:15.893+00:00.

## Validation and next target

npm run typecheck, npm run build and git diff --check passed. Restarted verified node origin on port 3101 using the existing hidden detached helper and disposable local credential. Local smoke HTTP 200: 641 markets, jamestown-oh production_allowed, readiness 31.0/28. Public probes after environment hydration returned HTTP 401 with credentialAvailable=false; public endpoint count and public production visibility remain UNKNOWN.

Southborough MA is next: 23 active, 23 unlinked, 23 with address/ZIP, historical overall 0%. No linked county evidence, including historical listing links; no Worcester MA county row currently found. County/public-parcel reconciliation is required before exact linking or shells. Then Rockbridge OH. Preserve earlier holds, including Thousand Oaks.

No RapidAPI, secrets requested or secrets printed. All DB/API calls hydrated Windows user environment through scripts/lib/env.ts. Detailed evidence is in tmp/*-sep23-00-* and durable listing provenance. Only src/api/server.ts and this report are intentional commit files.
