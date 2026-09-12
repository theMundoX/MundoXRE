# BBC county reconciliation — September 12, 2026, 18 UTC

No new market enabled. This run revisited the highest-ranked unresolved county hold, Plain City, then Ellenwood. Config inventory was used only as a ranking fallback: the authenticated public inventory remains unavailable. Known MT LAUREL TOWNSHIP / O'BRIEN aliases and prior holds remain unresolved.

## Plain City, Ohio

190 stored active listings, unlinked 0 before and 0 after the exact-unique linker. Original ranking CSV overall completion: 33.33% (historical; not recomputed readiness). Initially all 190 properties were listing-backed shells assigned to Franklin county_id 1698985.

The official Union County current-tax-year parcel service at https://www7.co.union.oh.us/unioncountyohio/rest/services/parcel/MapServer/0 returned 565 candidates in fully paginated house-number batches. Sixty-six unique matches required exact street address with whitespace/case normalization only, source city PLAIN CITY, source situs ZIP suffix OH 43064, and isVisible Yes. Unit identifiers were not removed. All 66 camano APNs were unique and absent from county_id 2338937, Union OH / FIPS 39159.

A single guarded statement imported 66 public parcels and reconciled 66 active listing links. Guards checked listing ID, original shell property ID, exact address/ZIP, state/city, original county, null APN, shell provenance and absence of the target APN. The first SQL attempt had a syntax error and made no changes; the corrected statement returned 66 eligible / 66 imported / 66 relinked. The public source and matching evidence are recorded in property source and listing raw.unionCountyParcelReconciliation, including old property/county IDs, APN, object ID, observation time, method and previous listing timestamp.

Ownership uses nonempty OwnerCurrent, falling back to Owner. Undecoded ParcelClass values were not mapped to asset types. No values, coordinates, building years or rents were manufactured or copied. No old properties or rent records were deleted; no shells were created.

| Coverage | Before | After |
| --- | ---: | ---: |
| Active listings / properties | 190 | 190 |
| Unlinked listings | 0 | 0 |
| Active listing-backed shells | 190 | 124 |
| Public parcel identities / ownership | 0 | 66 |
| Properties with values | 190 | 124 |
| Active properties with rent snapshots | 141 | 98 |
| Substantive classification / coordinates / year built | 0 | 0 |

Value and rent counts decreased because old shells' asking prices and rent snapshots were not transferred to the public parcels. The remaining rent provenance is not verified. Debt, agent contacts and positive creative-finance evidence remain absent. Public ownership evidence does not refresh listing availability: prior listing updated_at was May 25, 2026, preserved in reconciliation provenance.

Publication remains held for the remaining 124 unresolved county assignments and enrichment quality. Do not treat the remaining Franklin assignments as validated. Evidence and guarded SQL are retained under tmp/plain-city-18-* and tmp/plain-city-current18-* in the isolated publishing checkout.

## Ellenwood, Georgia

167 active listings; unlinked 0 -> 0 after exact-unique linking against the existing DeKalb county_id 1741141. All 167 remain shells, with asking prices and brokerage names but zero parcel identities, ownership, substantive classification, coordinates, year built, verified contacts, debt or rents. Historical CSV overall completion: 33.33%.

The existing county metadata pointed to https://dcgis.dekalbcountyga.gov/hosted/rest/services/Parcels/MapServer/0 . Fully paginated house-number queries returned 6,968 candidates and four unique exact street + situs city/ZIP matches excluding unit-bearing records. All four carry May 28, 2018 LASTUPDATE values and undecoded STATUS 7. No imports or property mutations were made from this stale evidence. Reconcile county scope and locate a current parcel source before enabling the market. Evidence: tmp/ellenwood-18-* and tmp/ellenwood-current18-*.

## Verification and continuation

Typecheck and build passed. Local endpoint HTTP 200, 632 markets; no config changes or origin restart required. Public endpoint retried twice with hydrated environment: HTTP 401, credentialAvailable false. Public count and deployed production_allowed remain unverified. No RapidAPI or secret output. The original dirty C:/Users/msanc/mxre checkout was untouched; this report is the only intentional Git change in C:/Users/msanc/mxre-overnight-sep09.

Next: resolve Plain City's remaining 124 county assignments using Madison/Union/Franklin property evidence; obtain current Ellenwood parcel evidence. Next ranked distinct county hold after Ellenwood: Acworth, GA (historical 164 active, 33.33%). Previous lower-ranked continuation was Mimbres, NM, per the 17 UTC report; preserve all prior holds.
