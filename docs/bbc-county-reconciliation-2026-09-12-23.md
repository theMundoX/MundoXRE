# Grapevine and Box Elder parcel reconciliation — September 12, 2026, 23 UTC

Continued from origin/main `0e7feb1` in the clean detached publishing checkout at C:/Users/msanc/mxre-overnight-sep09. Read automation memory, fetched origin, and ranked the original best-first CSV against current configs as provisional exclusions. Preserved previous aliases/holds and unrelated dirty primary checkout changes. Public exclusion remains unverified: hydrated public probes returned HTTP 401 with credentialAvailable=false.

## Grapevine TX

Historical CSV overall completion **33.33%**; **95 active listings**; unlinked **0 -> 0 exact -> 0 after reconciliation**. Linked county_id **8**, Tarrant/FIPS 48439. Exact-unique linker used the existing SSH transport and made no changes; no shells were needed.

The [official Tarrant parcel service](https://mapit.tarrantcounty.com/arcgis/rest/services/Dynamic/TADParcels/MapServer/0) returned a fully paginated **17,263-row** Grapevine city query. Selected **75 unique exact situs-street matches**, excluding units, addenda, duplicate streets and duplicate accounts. All 75 primary ACCOUNT identifiers already existed in Tarrant properties with the same address. Public situs ZIP is blank; no owner mailing fields were used for matching.

A guarded statement rechecked active listing IDs, previous shell IDs/source/county/address/ZIP and existing parcel county/primary APN/address before relinking **75/75**. No property insertion, overwrite or deletion. `raw.tarrantParcelReconciliation` preserves full public attributes, matching method, source, observation time, prior IDs and prior listing timestamps. Listing ZIP remains listing-derived; existing parcel ZIP was not overwritten. APPRAISAL_ is January 1, 2026, not evidence of fresh listing availability.

After: **20 shells**, **75 parcel identities/owners/classifications**, **95 valuations**, **11 coordinates**, **69 year-built values**, and **75 properties with 150 existing rent snapshots** (latest March 26). Rent counts indicate stored coverage, not newly verified market rents. No verified contacts/debt/positive creative evidence. Before substantive classification was zero; the initial generic audit incorrectly counted shell labels, so the final audit used the existing corrected classifier. Original listing observations were May 25, 2026; reconciliation updates are not fresh observations. **Publication held for 20 residual county/parcel assignments.**

## Box Elder SD

Historical CSV overall completion **33.33%**; **88 active listings**; unlinked **0 -> 0 exact -> 0 after reconciliation**. All initial links were shells assigned Pennington county_id **2338914**, FIPS 46103. Exact-unique linker made no changes; no shells were created.

The [city GIS page](https://www.boxeldersd.gov/GISDivision) identifies both Pennington and Meade county GIS partners. Queried the official [Pennington tax parcels](https://gis.rcgov.org/server/rest/services/OpenData/TaxParcels/MapServer/0) and [address points](https://gis.rcgov.org/server/rest/services/OpenData/Addressing/MapServer/0) for listing street addresses. Complete responses returned 53 parcels and 51 address points. Selected **47 unique exact parcel-street matches** corroborated by unique address points with exact street, ZIP, Box Elder city, SD and explicit Pennington County; excluded units/half addresses. A second countywide PIN query confirmed 47 unique identifiers. Neither PIN nor TaxID existed in the county property inventory.

Guarded import returned **47 eligible, 47 inserted, 47 linked**. Imported parcel identity, official situs address/ZIP and WGS84 address-point coordinates. Grantee names, ambiguous valuation semantics/year, coded classifications and physical fields remain in full provenance and were not promoted. `raw.penningtonParcelReconciliation` retains both public source records, matching method, prior IDs, original timestamps and limitations. No overwrites/deletions or availability refresh claim. Coordinates are address points, not parcel centroids.

After: **41 shells**, **47 parcel identities/coordinates**, **41 shell asking valuations**, zero owners/classifications/year-built/contacts/debt/rents, and 88 brokerages. Valuation coverage falls because newly verified parcels do not inherit shell asking prices as assessed values. Listing prices remain on listing records. **Publication held for 41 residual assignments, including possible Meade scope.** Original listing observations were May 25, 2026.

## Validation and next target

Net **122 reconciled links**, including **47 new public parcel records**; zero new shells/live configs. No readiness changes. Historical CSV percentages were not recomputed. `npm run typecheck` and `npm run build` passed. Local API HTTP 200 with **632 markets**, no restart needed. Final public probe HTTP 401 with credentialAvailable=false: public count and deployed production_allowed remain unknown. Hydrated Windows environment before protected access; no RapidAPI or secrets requested/printed.

Next distinct ranked target: **Baltimore MD**, historical **33.32%**, **2,362 active / zero unlinked**, all shells assigned Baltimore City **1973418** (FIPS 24510), 2,360 valuations and 2,362 brokerages, other audited enrichment absent. Fresh audit only; verify city-versus-county scope before publication. Retain earlier holds and Grapevine 20 / Box Elder 41 residuals.

Evidence and guarded statements remain in publishing tmp/grapevine-23-* and tmp/box-elder-23-*; next audit tmp/baltimore-23-before.json. Only this report is committed.
