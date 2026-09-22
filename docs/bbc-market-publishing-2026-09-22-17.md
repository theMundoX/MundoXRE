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
