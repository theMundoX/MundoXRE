# Albemarle and Grandview parcel reconciliation — September 12, 2026, 22 UTC

Continued in the clean detached publishing checkout from origin/main `57e9337`. Read automation memory and the newer 21 UTC committed report, then ranked the original best-first CSV against current market configs as provisional exclusions. Preserved all earlier aliases and county holds and the unrelated dirty primary checkout. Public exclusion remains unverified because authenticated public access was unavailable.

## Charlottesville, Virginia

Historical CSV overall completion: **33.33%**; 100 stored active listings; unlinked **0 → 0 exact → 0 after reconciliation**. The HTTP database bridge failed before linking; the existing linker's `--ssh-psql --exact-unique` transport succeeded with no changes. No new shells were needed.

Continued the previous run's 21 unique exact matches in the [official Albemarle parcel export](https://www.albemarle.org/government/information-technology/geographic-information-system-gis-mapping/gis-data). All matched CURRENT county parcels, exact street number/name, situs Charlottesville and listing ZIP, with no unit. The complete prior downloaded export contains 51,032 rows. Confirmed unique APNs and checked fresh database inventory for existing accounts. [Census county records](https://www2.census.gov/geo/maps/DC2020/DC20BLK/st51_va/county/) identify Albemarle as FIPS 51003, distinct from Charlottesville city 51540.

Created verified Albemarle county_id **2338952** and imported/relinked **21/21** parcels in one guarded statement. Eligibility required unchanged active listing IDs, old shell property IDs, state, city, address, ZIP and shell county/source. The county insertion required all 21 eligible rows; conflicts do not overwrite existing records. No shells were deleted. Imported current owner and TotalValue as assessed_value after validating land plus improvement totals. The export does not identify an assessment year. No owner mailing, legal-description classification, ambiguous lot-size units or sale information was promoted into property fields.

`raw.albemarleParcelReconciliation` retains complete source attributes, source URLs, observation time, matching method, county FIPS evidence, old property ID, original listing timestamp and limitations. Linking does not refresh source availability: original listing observations were May 25, 2026.

| Audited metric | Before | After |
| --- | ---: | ---: |
| Active listings/properties | 100 | 100 |
| Unlinked | 0 | 0 |
| Shells | 63 | 42 |
| Parcel identities | 37 | 58 |
| Ownership | 0 | 21 |
| Valuation support | 100 | 100 |
| Substantive classification | 36 | 36 |
| Year built | 34 | 34 |

Coordinates, verified contacts, debt and rents remain absent. Values now combine 58 public assessments and 42 shell asking values. **Publication remains held:** 42 residual shells still carry unverified city-county assignment. Linked scope is 79 city-assigned records (37 verified parcels plus 42 unresolved shells) and 21 verified Albemarle parcels.

## Grandview, Texas

Historical CSV completion: **33.33%**; 99 active listings; unlinked **0 → 0 exact → 0 after reconciliation**, no new shells. All initial links were Johnson county_id **2338850**, FIPS 48251, listing shells.

Downloaded the [Johnson CAD 2026 certified appraisal roll](https://johnsoncad.com/downloads/), published on the official downloads page as updated August 10, 2026. The archive is `BATCH_896.all_.zip`; `externalnal.tab` contains 111,072 rows. Found **25 unique exact situs street-number/name/suffix and city matches**, excluding units, secondary situs addresses, floor-plan labels and duplicate accounts. The roll has no situs ZIP: ZIP remains explicitly listing-derived, and mailing ZIP was not used. Checked both account and geographic account identifiers against existing Johnson properties; none existed.

Guarded import/relink returned **25 eligible, 25 inserted, 25 linked**, requiring unchanged active listings and shell source/county/address/ZIP. Imported owner, explicit market and assessed values, four explicit single-family residential classifications and ten year-built values supported by positive building value. Other use descriptions were not guessed into classifications. Source and full roll attributes, certified tax year 2026, original listing timestamps, old IDs and field limitations are retained in `raw.johnsonCertifiedParcelReconciliation`. No overwrites, deletion or fresh listing availability claim.

After: **74 shells**, 25 parcel identities/owners, 99 valuations (25 public plus 74 shell asking values), four classifications, ten year-built values, 99 brokerage names, zero coordinates/verified contacts/debt/rents/positive creative evidence. **Publication held** for residual county/parcel reconciliation. Original listing observations remain May 25, 2026.

## Next target and validation

Grapevine TX: historical completion **33.33%**, **95 active**, **0 → 0 unlinked** after exact-unique linking with linked Tarrant county_id **8**. All 95 remain shells with asking values and brokerage names; other audited enrichment absent. No mutation. Next work is property-level county and public parcel verification for Grapevine, while preserving prior holds and Grandview's 74 residual shells, Charlottesville's 42 residual shells, and the lower-ranked Mimbres continuation.

Net **46 public parcels imported/relinked**, one verified county created, **zero new shells or live market configs**. No readiness thresholds changed. CSV percentages are historical ranking values and were not recomputed from these audits.

`npm run typecheck` and `npm run build` passed. Local API returned HTTP 200 and **632 markets**; no origin restart was needed. Public probes before and after returned **HTTP 401, credentialAvailable=false**. Public market count and deployed `production_allowed` remain **unverified**. Hydrated `scripts/lib/env.ts` before protected operations; no RapidAPI, secrets requested or secrets printed.

Evidence and guarded SQL remain in publishing `tmp/albemarle-22-*`, `tmp/charlottesville-22-*`, `tmp/grandview-22-*` and `tmp/grapevine-22-before.json`. Only this report is committed; primary worktree changes are untouched.
