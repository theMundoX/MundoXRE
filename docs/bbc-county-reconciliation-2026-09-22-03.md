# BBC county reconciliation — 2026-09-22 03 UTC

Continued from c451d8c in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09, using the ranked CSV in C:/Users/msanc/mxre. Recovered the newer repository checkpoint because automation memory ended at 0631d94. Preserved all unrelated primary-checkout changes and prior holds. Config-based live exclusions remain provisional: deployed inventory authentication is unavailable.

| Market | Active listings | Unlinked before / after | Historical overall completion | Result |
| --- | ---: | --- | ---: | --- |
| Enumclaw WA | 13 | 12 / 12 | 1.28% | King county 748414 exact-unique linker changed zero; county evidence still needed for unresolved addresses |
| Wilmington OH | 180 | 180 / 77 | 0% | Created and linked 103 explicitly labeled shells with unique public address/ZIP county evidence |
| Ferris TX | 78 | 78 / 78 | 0% | Next ranked candidate audited; no linked property county evidence, no mutation |

Historical CSV percentages are not recalculated current completion. Wilmington's historical CSV had 80 listings; the fresh audit has 180.

## Wilmington evidence and guarded linking

The [Ohio public address layer](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) returned 1,240 deduplicated candidate address points in bounded non-truncated ZIP/house-number queries. Exactly 103 unlinked listings each matched one point by exact street and ZIP, all in Clinton County. The other 77 were excluded. County metadata was absent; verified state/county FIPS 39/027 against the [Census Clinton County report](https://tigerweb.geo.census.gov/tigerwebmain/Files/acs26/tigerweb_acs26_tabblock_2020_oh_027.html) and inserted county_id 2338953 with conflict protection.

Ran scripts/link-market-listings-fast.ts with exact-unique and SSH transport for Clinton before shell creation; zero existing parcels linked. A guarded transaction created 103 distinct listing_signal_shell:redfin properties with active_listing_shell status, listing_only quality, unknown classification, and null parcel IDs and values. It required active/unlinked listings with unchanged address/ZIP, no existing exact state/address/ZIP property, and exact expected candidate/link counts. Saved durable raw.ohioAddressCountyEvidence on each linked listing. No coordinates, ownership, valuation or parcel identities were inferred, and listing timestamps were preserved.

Re-audit: 180 active, 103 linked distinct properties, 77 unlinked, 103 shells; zero audited parcel identity, classification, ownership, valuation, year-built, coordinates, verified agent contacts, mortgage or rent coverage. Brokerage count remains 51. Wilmington fails zero-unlinked and enrichment readiness requirements and remains HELD. Enumclaw and Ferris also remain HELD. No market configs or thresholds changed; no new production market enabled.

## Validation and continuation

npm run typecheck and npm run build passed. Initial and final public smoke probes hydrated scripts/lib/env.ts first and returned HTTP 401 with credentialAvailable=false. Public endpoint count and production_allowed status remain UNKNOWN. No API change or restart needed. No RapidAPI used, no secrets requested or printed.

Evidence: publishing tmp/wilmington-sep22-03-* (input, public features, SQL, result, before/after audit), tmp/enumclaw-sep22-03-* and tmp/ferris-sep22-03-before.json. Only this report is intended for commit. Next target: Ferris TX property-level county reconciliation, then Santa Maria CA; retain Wilmington's 77 unresolved and all prior holds.
