# BBC enrichment and publication holds — September 12, afternoon

No new market was enabled. This pass retained prior ranking holds and aliases, then continued the Falls Church / Laguna Hills / Murrysville queue recorded in automation memory. Rankings use the historical primary completion CSV and remote configurations; authoritative public market exclusion is still credential-blocked.

| Market | Active listings | Unlinked before | Unlinked after | Historical CSV completion |
| --- | ---: | ---: | ---: | ---: |
| Falls Church VA | 40 | 40 | 40 | 0% |
| Laguna Hills CA | 39 | 39 | 0 | 0% |
| Murrysville PA | 39 | 39 | 21 | 0% |

CSV percentages are historical baselines, not refreshed completion measurements.

## Laguna Hills

Exact-unique linking ran first and linked zero rows. Orange County GIS returned 24 candidate features without truncation; 15 unique exact situs-address-and-city matches were imported without overwriting existing properties and then linked. Linked parcels establish Orange county_id 2338869 / FIPS 06059. Eleven have county year-built facts. ZIPs are explicitly listing-derived. The source does not supply ownership or assessor values, and no asking price was written into those assessor rows.

After confirming the linked county and all 24 residual listing ZIPs as 92653, the existing linker created 24 labeled listing-backed shells. They include unit-level and abbreviated-address records; no parcel identity is inferred for them. Final audit: 39 properties, 15 parcel identities, 24 shells with listing-price valuation support, 11 years, 39 brokerage records, and zero substantive classifications, ownership, coordinates, verified contacts, debt, or rent coverage. Unknown shell types are excluded from classifications. The unchanged seven-input readiness formula yields 14.3 against target 28, so publication remains on hold.

Pre-link latest stored listing update: 2026-05-07T15:32:12.538308+00:00. Linking advances updated_at without proving current availability.

Sources: [Orange County parcel layer](https://www.ocgis.com/arcpub/rest/services/Map_Layers/Parcels/MapServer/0), [92653 county coverage](https://www.zip-codes.com/zip-code/92653/zip-code-92653.asp).

## Murrysville

Exact-unique linking initially linked zero rows. The official Westmoreland county service returned 2,105 fully paginated house-number candidates. Eighteen unique matches used SITUS or a single FULL_ADDRESS, normalizing whitespace and standard street-type abbreviations while preserving house numbers. Matches require active county tax district 49, independently documented as Murrysville. Multiple-address matches, lot labels, units and ambiguities were excluded. Owner mailing addresses and mailing ZIPs were not used for matching.

Imported 18 county parcel IDs, owner names, assessed ATOTL totals, and nine valid year-built facts with explicit provenance; no overwrites. Each total was checked against ALAND + AIMP + AMINL. Assessment vintage is unspecified; these are tax assessments, not current market values. Source land-use codes remain undecoded and were not counted as classification. Linked properties confirm Westmoreland county_id 1096045 / FIPS 42129. Final: 39 active listings, 18 linked properties, 21 unlinked, no shells created.

Hold: ZIP 15668 has conflicting county evidence, including Allegheny segments. Individual residual property county checks are required before blanket shell assignment. Pre-link latest stored listing update: 2026-05-07T15:33:46.251189+00:00.

Sources: [Official GIS entry](https://www.westmorelandcountypa.gov/141/GIS), [county parcel service](https://gis.westmorelandcountypa.gov/arcgis/rest/services/Upgrade/Parcels_AS400_VIEW_Upgrade/MapServer/2), [tax district codes](https://www.westmorelandcountypa.gov/1459/Tax-Mapping-Districts), [15668 county segments](https://www.zip-codes.com/zip-code/15668/zip-code-15668.asp), [conflicting single-county coverage](https://www.zipdatamaps.com/15668).

## Falls Church and verification

Falls Church still has no linked county evidence. Its [official address finder](https://fallschurchva.gov/AddressFinder) explicitly warns that the mailing label includes Fairfax County addresses outside the independent city. No shells or county assignment were made.

Local smoke: HTTP 200, 630 markets. Public smoke: HTTP 401, no available key after environment hydration; public count and deployed production access remain unknown. No API configuration changes or origin restart. No RapidAPI; no secrets printed or requested. Original dirty checkout preserved; database helpers hydrate Windows user environment before access.

Before/after audits, source responses, unique match evidence and import SQL remain in the publishing checkout's untracked tmp/laguna-hills-* and tmp/murrysville-* files. Next queued investigation: Vienna VA, with a fresh baseline audit saved; prior holds remain active.

Validation: npm run typecheck, npm run build, and git diff --check passed.
