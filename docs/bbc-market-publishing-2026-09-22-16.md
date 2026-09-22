# BBC market publishing — 2026-09-22 16 UTC

Continued from 4cc2be7 using the ranked CSV in C:\Users\msanc\mxre and the existing clean publishing checkout. Earlier holds and unrelated dirty work preserved. Public market exclusions remain provisional because authenticated public inventory is unavailable.

| Market | Active | Unlinked before → after | Historical CSV overall | Result |
| --- | ---: | ---: | ---: | --- |
| Catonsville MD | 34 | 34 → 0 | 0% | Live configuration; local production_allowed, readiness 48.3/28 |
| Fostoria OH | 116 | 116 → 114 | 0% | Two Hancock-supported shells; held |
| Tubac AZ | 33 | 33 → 33 | 0% | Exact-unique Santa Cruz 2338855 zero; held pending public reconciliation |
| Montgomery OH | 79 | 79 → 30 | 0% | 39 exact Hamilton parcel links and 10 county-supported shells; held |

Overall percentages are historical CSV completion, not fresh readiness. CSV active counts were 34 Fostoria and 33 Montgomery.

## Catonsville

Exact linking first returned zero for Baltimore county_id 2338913. Queried the [county parcel service](https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1), paginating 17,030 house-number candidates. Validated 29 unique normalized exact situs-address/ZIP/TAXPIN matches, no condo units, distinct accounts, and consistent imported fields across duplicate parcel geometries. Guarded transaction imported and linked 29 identities/owners/use codes/years, 28 positive assessed totals, with raw.baltimoreParcelEvidence. Assessment vintage unspecified; city listing-derived; no market valuation inferred.

One residual matched an ACTIVE county [Accela address point](https://bcgisapps.baltimorecountymd.gov/arcgis/rest/services/Accela/MapServer/0) exactly by address/ZIP/MD. Created one labeled shell with raw.baltimoreAddressCountyEvidence; did not promote TAXACCTID to parcel identity. The four remaining unit-level addresses received separate listing-backed shells preserving full unit text and ZIP. County assignments derive from 29 linked parcels and county community information ([District 1 office in Catonsville](https://countycouncil.baltimorecountymd.gov/district-1/)); property-level parcel/county reconciliation remains pending and explicitly disclosed in raw.listingCityCountyEvidence and config restrictions.

All five shells have null parcel identity/value, unknown classification, listing_only confidence. All guarded mutations preserved listing timestamps. Final audit: 34 active and linked; 29 identities/owners/use classifications/years, 28 assessed values, five shells, 34 brokerage rows. Zero coordinates, verified contacts, debt, rents, positive creative evidence. Latest listing timestamp remains May 7; availability not refreshed. Map center is approximate display positioning, not property enrichment.

Live MARKET_CONFIGS entry includes honest fallback counts and restrictions covering unit-level shells, county assignment, assessed values, source classifications, contacts/debt/rents, and stale source availability. Readiness formula/target unchanged.

## Other markets

Fostoria has no initially linked county. Fresh Ohio address service query returned 990 candidates and 48 unique address/ZIP matches: 45 Seneca, two Hancock, one Wood. Only Hancock has matching county metadata (2338877). Exact-unique Hancock first zero. Created two guarded labeled shells with exact OH/county evidence and blank unit/range fields, durable raw.ohioAddressCountyEvidence, null identities/values, unknown classification and unchanged listing timestamps. Missing county metadata and 114 residuals prevent publication.

Tubac exact-unique Santa Cruz zero; no shells or config. Montgomery exact-unique Hamilton 1698987 linked 39 existing identities/owners/values/classifications, 30 coordinates, one mortgage record without amount. Exact linker timestamps are not availability refreshes. Ohio service returned 409 candidates and 11 exact county-supported residual addresses. Initial shell transaction safely rolled back because existing properties require reconciliation; no shells created in that transaction. A bounded follow-up query found two distinct Hamilton parcel IDs at 10019 ZIG ZAG RD; left that listing unlinked. Excluded it and created ten guarded county-supported shells with unchanged timestamps and durable Ohio evidence. Final audit: 79 active, 49 linked, 30 unlinked, 39 identities/owners/values/classifications, 30 coordinates, ten shells, one mortgage record with no amount. No rent/verified contacts. Held for residual reconciliation.

## Validation

- npm run typecheck and npm run build passed; git diff --check passed.
- Restarted verified node API origin on port 3101 using the hidden detached helper and disposable local credential.
- Local smoke HTTP 200: 634 markets; Catonsville production_allowed, readiness 48.3/28.
- Public smoke twice HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public count and production_allowed UNKNOWN.
- No RapidAPI or secret requests/output. Only this report and src/api/server.ts are intentional commit files.
- Evidence retained under tmp/*-sep22-16-*; database provenance retained with the linked listing rows.
- Next ranked target: Richland Hills TX. Preserve Montgomery and all earlier holds, including Tubac.

