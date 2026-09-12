# Charlottesville parcel enrichment and Grandview hold — September 12, 2026, 21 UTC

Continued from origin/main a3032aa in the clean publishing checkout. Read automation memory and newer repository reports before selecting the next unresolved ranked candidate. Original dirty worktree changes were untouched. Configured inventory supplied provisional exclusions because the public endpoint could not be authenticated. Higher-ranked aliases and county holds remain unresolved.

## Charlottesville, Virginia

Historical ranking CSV overall completion: 33.33%. Stored active listings: 100. Unlinked: 0 before exact-unique linking, 0 afterward, and 0 after public enrichment. No shells created.

The linked properties all carried Charlottesville city county_id 2338911. Queried the official [city active parcel, current assessment, and residential tables](https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer) in complete house-number batches. Downloaded the current parcel text export linked by the [Albemarle GIS data page](https://www.albemarle.org/government/information-technology/geographic-information-system-gis-mapping/gis-data). The export contains 51,032 rows. Its extracted file timestamp was September 12, 2026; that is not an assessment tax year.

Unique exact street-number/name matching found 37 city parcels and 21 Albemarle parcels. Excluded unit-bearing, floor-plan, zero-number, and ambiguous candidates. Only case and whitespace normalization were used. Albemarle matches additionally required situs city Charlottesville and listing ZIP equality. City data does not expose situs ZIP, so those matches are street-verified with listing-derived ZIP, not independently ZIP-verified. No owner mailing addresses were used for matching. The remaining 42 listings are unresolved, not proven outside either jurisdiction.

No matching APNs existed in the queried Virginia property inventory. Imported and linked the 37 city parcels in one guarded statement requiring unchanged active listing ID, old property ID, street, ZIP, state, city and shell source/county. APNs were unique in the approved batch; insert conflicts do not overwrite existing rows. raw.charlottesvilleParcelReconciliation records source, observation time, complete matched public attributes, old property ID, previous listing timestamp, matching method and limitations. No shells were deleted or overwritten.

| Metric | Before | After |
| --- | ---: | ---: |
| Active listings/properties | 100 | 100 |
| Unlinked listings | 0 | 0 |
| Listing-backed shells | 100 | 63 |
| Parcel identities | 0 | 37 |
| Substantive classifications | 0 | 36 |
| Valuation support | 100 | 100 |
| Year built | 0 | 34 |

Classifications comprise 32 residential, three multifamily and one commercial parcel, derived from explicit city StateCode descriptions. One tax-exempt code was left unclassified. The audit's separate multifamily_count only considers asset_type/units and remains zero; do not interpret that as an absence of the three property_type classifications. Year built was imported only for a single residential-detail row per APN. Valuation support now comprises 37 public current-assessment values plus 63 residual shell asking values. The current-assessment endpoint does not identify a tax year. No asking prices were copied into assessor values. Ownership, coordinates, verified contacts, debt and rents remain absent; brokerage names cover 100 listings.

**Publication held:** 21 exact Albemarle matches demonstrate incorrect residual city-county assignment. No Albemarle county row exists yet in this database. Its parcels and the other unmatched inventory need county-aware reconciliation. No readiness threshold or market config was changed. Listing source observations remain stale; the previous stored timestamp was May 25, 2026, and reconciliation did not refresh availability.

## Grandview, Texas

Next ranked distinct candidate: historical CSV completion 33.33%, 99 active listings, zero unlinked before and after exact-unique linking with linked Johnson county_id 2338850. All 99 remain shells with asking values and brokerage names; no audited identities, substantive classifications, owner, coordinates, year built, contacts, debt or rents. No changes made. Publication held pending property-level county and parcel evidence. Located the [official Johnson CAD downloads](https://johnsoncad.com/downloads/) as the next public enrichment source; its parcel data has not yet been matched.

## Validation and continuation

npm run typecheck and npm run build passed. Local API HTTP 200 returned 632 markets. Public endpoint attempts returned HTTP 401 with credentialAvailable=false; public count and deployed production_allowed remain unverified. No new market published, config change or origin restart. Hydrated scripts/lib/env.ts before protected access; no RapidAPI and no secrets requested or printed.

Next: reconcile Charlottesville's 21 verified Albemarle matches with a verified county record, then the other 42 unresolved listings; continue Grandview against Johnson CAD public data. Preserve earlier higher-ranked holds. Evidence and guarded SQL are in publishing tmp/charlottesville-21-* and tmp/grandview-21-*. Only this report is committed.
