# BBC publication audit — September 12, 2026

This run reconciled Sacramento inventory and re-audited Kent. No market was newly enabled. Existing higher-ranked county-scope holds and the MT LAUREL TOWNSHIP / O'BRIEN aliases remain unresolved; they were not declared ineligible.

## Sacramento, California

County 773106 (Sacramento) is supported by linked county GIS parcels. The exact-unique linker ran first and made no changes. A subsequent public GIS check found 93 unique address-and-ZIP matches to 93 existing assessor parcels. Matching preserved street numbers and unit identifiers, normalizing only whitespace and spacing around `#`. Each target APN was unique in the queried county inventory and corroborated by the public service. The guarded update required the original shell, county, state, ZIP, APN, and normalized address to still match.

Only listing links changed. No properties were deleted, no shells were created, and no asking price was copied into an assessor valuation. Each affected listing's raw data records the previous property ID, matched APN, source URL, method, and observation time under `sacramentoParcelReconciliation`.

| Metric | Before | After |
| --- | ---: | ---: |
| Active listings | 943 | 943 |
| Unlinked listings | 0 | 0 |
| Active properties | 943 | 943 |
| Listing-backed shells | 264 | 171 |
| Parcel identities | 679 | 772 |
| Substantive classifications | 679 | 772 |
| Properties with coordinates | 474 | 560 |
| Properties with valuation support | 264 | 171 |

Valuation support decreased because reconciled assessor rows have no value; their former shells held asking prices. The original ranked CSV overall completion remains 21.33%, a historical report value, not a refreshed readiness score. Audited readiness increased from approximately 24.6 to 26.0 against the unchanged 28 target. Ownership, verified agent contacts, mortgage coverage, and rents remain absent.

**Publication remains on hold:** readiness is below target and 52 existing property labels contain `PLAN`, representing unresolved floor-plan inventory. Do not infer current availability from the reconciliation timestamp: the prior latest listing update was May 25, 2026, and linking advanced `updated_at` without refreshing the source observation.

Public evidence: [Sacramento county parcel service](https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer/0). Initial oversized queries were rejected on truncation; smaller complete batches were used for matching.

## Kent, Washington

County 748414 (King); 11 active listings; unlinked 0 before and after exact-unique linking; no new shells or links. Original CSV overall completion is 18.48%. The existing `2319 PLAN` shell remains unresolved, so publication remains on hold despite the prior readiness estimate exceeding 28.

## Verification and next work

The local BBC markets endpoint returned HTTP 200 and 630 markets. Public endpoint attempts returned HTTP 401 with no hydrated BBC/API client credential; public count and deployed production access are unverified. No origin restart was necessary because market configuration did not change. No RapidAPI was used. Environment hydration preceded DB/API access, and no secrets were printed.

Next: resolve Sacramento's floor-plan inventory and acquire real valuation/ownership evidence; reconcile Kent's floor-plan shell. The next lower-ranked county investigation is West Alexandria, Ohio (prior audit 35 active / 34 unlinked), whose sole linked Montgomery parcel is insufficient evidence for a blanket county assignment. Keep MC DONALD and Warminster on county holds as recorded in automation memory.

Before/after audits, public responses, approved match evidence, and the guarded SQL remain in the publishing checkout's untracked `tmp/sacramento-*` and `tmp/kent-sep12-*` artifacts. The original working tree's unrelated edits were untouched.

Validation: npm run typecheck, npm run build, and git diff --check passed.
