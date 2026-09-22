# BBC market publishing — 2026-09-22 17 UTC

Resumed from 6654ca0 (automation memory was stale at 7a295c6). Used ranked CSV in C:\Users\msanc\mxre and existing clean publishing checkout. Earlier holds and unrelated edits preserved. Public exclusions remain provisional because authenticated inventory is unavailable.

## Richland Hills TX

33 active listings; unlinked 33 → 0; historical CSV overall completion 0%. Exact-unique linker first returned zero for Tarrant county_id 8. Queried 7,457 house-number candidates from the public [Tarrant parcel service](https://mapit.tarrantcounty.com/arcgis/rest/services/Dynamic/TADParcels/FeatureServer/0). Imported 28 unique exact situs-address/city/state matches with single accounts and no confidentiality flag. ZIP is listing-derived because county situs ZIP fields are blank. Guarded transaction rejected changed listings or existing conflicting properties and retained source attributes in raw.tarrantParcelEvidence. Stored TAXPIN identities, owners, APPRAISEDV appraisal values, 25 years, and positive living areas where present. No market prices or substantive classification inferred.

The five remaining address/ZIP listings received clearly labeled shells with null identities/values, unknown classification and listing_only confidence. Tarrant assignment is supported by 28 linked parcels and the [city comprehensive plan](https://www.richlandhills.com/DocumentCenter/View/2069/2025-Richland-Hills-Comprehensive-Plan); property-level reconciliation remains pending. 2600 ROSEBUD LN and 3720 GRANADA DR have multiple county accounts: no account, owner or value was selected for these shells. Durable raw.listingCityCountyEvidence records limitations. All listing timestamps preserved.

Final audit: 33 linked properties, 28 identities/owners/appraisal values, 25 years, five shells, 33 brokerage rows. Zero substantive classifications, coordinates, verified contacts, debt, rents or positive creative evidence. Latest stored listing timestamp May 7; availability not refreshed.

Added live MARKET_CONFIGS entry with audited fallback counts and explicit field-level restrictions. Readiness formula and target unchanged. Approximate map center is display positioning only.

## Validation

- npm run typecheck, npm run build and git diff --check passed.
- Restarted verified node origin on port 3101 with hidden detached helper and disposable local credential.
- Local smoke HTTP 200: 635 markets; richland-hills-tx production_allowed, readiness 36.3/28.
- Public smoke twice HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production_allowed UNKNOWN.
- No RapidAPI or secret requests/output. Only this report and src/api/server.ts are intentional files.
- Evidence retained in tmp/richland-hills-sep22-17-* and DB provenance.
- Next ranked target: Rolling Hills Estates CA (32 active, 32 unlinked, historical overall 0%), then Wilmington CA.

## Continued ranked reconciliation

- Rolling Hills Estates CA: 32 active, unlinked 32 → 0, historical CSV overall 0%. Exact-unique Los Angeles county_id 401712 linked five existing properties. Guarded transaction created 27 labeled shells for residual address/ZIP listings. Final audit: five identities, stored values, classifications, coordinates and years; zero owners/debt/rents/verified contacts. Calculated readiness 6.7/28; held, no live config.
- Wilmington CA: 32 active, unlinked 32 → 1, historical CSV overall 0%. Exact-unique Los Angeles linked ten existing properties. Created 21 guarded labeled shells; excluded the ZIP 90001 residual pending city/ZIP reconciliation. Final audit: 31 linked properties, ten identities/stored values/classifications/coordinates, nine years, zero owners/debt/rents/verified contacts. Held for residual and inadequate enrichment.
- Shells preserve full listing address/unit text and ZIP with null parcel/value, unknown classification, listing_only confidence and durable raw.listingCityCountyEvidence. County assignment follows linked properties in each listing-city market; property-level county/parcel reconciliation remains pending. Exact-linker timestamps changed without refreshing availability; shell transactions preserved timestamps.
- Current public [LA parcel service](https://public.gis.lacounty.gov/public/rest/services/LACounty_Cache/LACounty_Parcel/MapServer/0) metadata retrieved. Broad query cancelled; narrower paginated query timed out after its first page. No incomplete public data was imported. Resume with smaller address batches if revisiting.
- Next ranked target: New Lexington OH, then York SC. Retain both California holds and all earlier holds.
