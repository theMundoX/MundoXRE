# BBC publishing and county reconciliation — 2026-09-22 18 UTC

Resumed from dfae03c. Rankings came from C:\Users\msanc\mxre\tmp\market-full-data-completion-report-best-first.csv; intentional publishing changes use the existing clean checkout C:\Users\msanc\mxre-overnight-sep09. Prior holds and unrelated dirty work remain untouched. Public inventory exclusions remain provisional: authenticated public inventory is unavailable.

| Market | Active | Unlinked before → after | Historical overall completion | Result |
| --- | ---: | ---: | ---: | --- |
| New Lexington OH | 32 | 32 → 21 | 0% | Held for residuals and incomplete enrichment |
| York SC | 32 | 32 → 7 | 0% | Held for residuals |
| North Chesterfield VA | 32 | 32 → 0 | 0% | Live config; local production_allowed, 54.0/28 readiness |

The completion column is the historical ranking CSV, not a recalculated post-enrichment percentage. Readiness is a separate metric.

## New Lexington OH

No linked county or existing Perry OH county metadata initially. Created county_id 2338954 with Census FIPS 39127. Ran scripts/link-market-listings-fast.ts with --exact-unique --ssh-psql first: zero links. Downloaded the official [Perry engineer parcel archive](https://www.perrycountyengineer.com/downloads/), parsed all 30,452 DBF records, and imported three unique exact situs-address/city matches. Retained parcel Name and Deeded_Nam owner text; ZIP is listing-derived, no value or classification inferred. Five parcel candidates at 524 SHAWNEE ST were not collapsed into a selected identity.

The [Ohio address service](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) returned 143 house-number/ZIP candidates and 12 unique exact address/ZIP matches in Perry County. After excluding imported parcels and unit ambiguity, created eight labeled shells with null parcel identity/value and unknown/listing_only classification. Final: 11 linked, three parcel identities/owners, eight shells, zero values/classifications/coordinates/debt/rents/verified contacts. Guarded transactions retain raw.perryParcelEvidence or raw.ohioAddressCountyEvidence and preserve listing timestamps.

## York SC

Created county_id 2338955 with Census FIPS 45091; exact-unique linker first returned zero. Followed the [official GIS download page](https://www.yorkcountysc.gov/239/GIS-Data-Download) to the York SC ArcGIS organization. Excluded similarly named Virginia/Pennsylvania search results. Current [parcel service](https://services1.arcgis.com/2AGLxyiJoNiVHKwq/arcgis/rest/services/Parcels/FeatureServer/0) returned 1,428 candidates; the [address service](https://services1.arcgis.com/2AGLxyiJoNiVHKwq/arcgis/rest/services/Addresses/FeatureServer/0) returned 2,004 fully paginated candidates.

Imported 24 unique exact parcel situs matches corroborated by unique county street-address/ZIP/SC matches with no units. Stored consistent TAXMAPID/ParcelID, owner text, AsdTotVal assessed totals, LandUseDesc, and valid year/area fields. Assessment vintage unspecified; no appraisal/market-price substitution. A single address-backed shell at 2874 CARMEL CT has unresolved multiple parcel candidates: no identity, owner or value selected. Four model-plan listings and three other residual addresses remain unlinked. Final: 25 linked, 24 identities/owners/assessed totals/use descriptions, 22 years, one shell; zero coordinates/debt/rents/verified contacts. Durable raw.yorkParcelEvidence/raw.yorkAddressCountyEvidence; listing timestamps preserved.

## North Chesterfield VA

Created Chesterfield county_id 2338956 (51041); exact-unique linker first returned zero. Current county [parcel service](https://services3.arcgis.com/TsynfzBSE6sXfoLq/ArcGIS/rest/services/Cadastral_ProdA/FeatureServer/3) and [address service](https://services3.arcgis.com/TsynfzBSE6sXfoLq/ArcGIS/rest/services/Address_ProdA/FeatureServer/0) returned 930 and 1,123 candidates. Imported 31 unique exact address/ZIP parcel matches corroborated by address records with consistent TaxID/GPIN and explicit Chesterfield/VA. One source Name was null; its TaxID matched the address TaxId and was used consistently as parcel identity. Stored owner, TotalAssessment, source UseCode, valid year and area. Retained complete parcel/address evidence in raw.chesterfieldParcelEvidence. No coordinates, debt or refreshed listing availability inferred.

Created one labeled shell at 2300 SPRINGS RD with county assignment based on the 31 linked county parcels in the listing-city cohort; no exact public parcel/address match. raw.listingCityCountyEvidence explicitly discloses pending property-level reconciliation. Final: 32 linked, 31 identities/owners/values, 28 source use codes/years, one shell; zero coordinates/debt/rents/verified contacts. Source UseCode is untranslated county classification, not detailed asset type. Listing timestamps preserved; latest stored May 7. County transfers can postdate listings, so current availability must be reconfirmed.

Added MARKET_CONFIGS live entry with audited fallback counts, unchanged readiness target/formula, and field-level restrictions disclosing the shell, source limitations and stale availability.

## Verification and continuation

- npm run typecheck, npm run build and git diff --check passed.
- Restarted only the verified node API origin on port 3101 with a hidden detached helper and disposable local credential.
- Local authenticated smoke HTTP 200: 636 markets, north-chesterfield-va production_allowed, readiness 54.0/28.
- Public authenticated smoke unavailable: repeated HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv(). Public endpoint count and production_allowed remain UNKNOWN; local success does not verify public deployment.
- No RapidAPI, secret requests or secret output. API/DB helpers hydrate Windows user environment first.
- Next ranked target: Charleston WV. Preliminary audit: 32 active / 32 unlinked / 31 with address+ZIP, zero linked county evidence. No linking or enrichment attempted yet. Then Johns Creek GA and Basking Ridge NJ. Retain all earlier holds.
- Evidence and replay SQL: tmp/*-sep22-18-* in the clean checkout, plus durable DB provenance. Only src/api/server.ts and this report are intentional commit files.
