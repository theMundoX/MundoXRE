# BBC market publishing — 2026-09-22 15 UTC

Continued from 94e5f5e in the existing clean publishing checkout, using the primary ranked CSV. Preserved unrelated primary worktree edits and prior holds. Public live-market exclusion remains provisional: hydrated public endpoint returned HTTP 401 without an available API credential.

| Market | Active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Langhorne PA | 36 | 36 → 36 | 0% | Bucks 1498719 exact-unique pass zero; held for address/ZIP reconciliation |
| Windcrest TX | 91 | 91 → 0 | 0% | 55 existing assessor links, 36 labeled shells; live config added |
| Norfolk VA | 35 | 35 → 35 | 0% | No linked county; matching county metadata absent; exact pending |
| Seaside CA | 34 | 34 → 34 | 0% | No linked county; Monterey metadata absent; exact pending |

Overall percentages are historical CSV values, not freshly recomputed completion or readiness. Windcrest had 36 active listings in that CSV and 91 in the fresh audit.

## Windcrest evidence and quality

Exact linking first connected 54 existing Bexar assessor parcels. All linked properties confirmed county_id 1741238, FIPS 48029. The [official city description](https://www.windcrest-tx.gov/ArchiveCenter/ViewFile/Item/1899) places Windcrest in Bexar County. Shell county assignments are based on the listing city and that county evidence, not independent parcel-level verification.

A guarded shell transaction initially rolled back because one address already had numerous duplicate shells and one assessor parcel. It made no changes. Excluded that address and created 36 distinct listing_signal_shell:redfin properties for unchanged active/unlinked listings with address and ZIP and no existing matching property. Shells have null parcel identity/value, unknown asset type, listing_only confidence and active_listing_shell status. Durable raw.listingCityCountyEvidence records the source, method and limits. Listing timestamps were preserved.

The final residual address had exactly one existing bexar_tx_bcad parcel with nonempty parcel identity at the same address, ZIP and county. A guarded unique-assessor transaction linked it, preserving all duplicate shell rows. Durable raw.exactAssessorLinkEvidence records the method; listing timestamps unchanged.

Fresh substantive audit: 91 active/linked properties, zero unlinked, 36 shells; 55 parcel identities, owners, stored assessed values and substantive property classifications; 54 year-built values. No coordinates, verified contacts, debt, rents, brokerage or positive creative evidence. Unknown shell markers are excluded from substantive classification. Exact linker timestamp changes do not refresh availability.

Added Windcrest to MARKET_CONFIGS with honest fallback counts and explicit shell, county-assignment, valuation, physical-data, contact, debt, rent and stale-availability limitations. Readiness target remains 28; formula unchanged. Display coordinates from [Texas Almanac](https://www.texasalmanac.com/places/windcrest) are not property enrichment.

## Other markets and validation

Langhorne: queried the [Bucks parcel service](https://services3.arcgis.com/SP47Tddf7RK32lBU/arcgis/rest/services/Bucks_County_Parcels/FeatureServer/0) for exact street addresses. Eighteen unique matches in Middletown Township/Langhorne Borough, but no source ZIP field. No mutation or parcel enrichment promoted from these incomplete matches.

Norfolk/Seaside: fresh audits all unlinked, no county evidence in linked properties. Existing county metadata query found neither Norfolk VA nor Monterey CA. No speculative county creation or shells.

- npm run typecheck and npm run build passed; diff whitespace check passed.
- Restarted verified node API origin on port 3101 with existing hidden detached helper and disposable local smoke credential.
- Local endpoint HTTP 200: 633 markets; windcrest-tx production_allowed, readiness 34.5/28.
- Public endpoint initial/final HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public count and production_allowed UNKNOWN.
- No RapidAPI or secret requests/output. Only server configuration and this report are intentional commit files.
- Net 91 links: 55 existing parcels and 36 new shells. Evidence retained in tmp/*-sep22-15-* and durable database provenance.
- Next Catonsville MD: fresh audit only, 34 active/all 34 unlinked, no linked county. Exact/public county reconciliation pending; then Fostoria OH. Preserve all prior holds.
