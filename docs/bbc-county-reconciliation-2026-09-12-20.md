# Sanford parcel reconciliation and Gainesville hold — September 12, 2026, 20 UTC

Continued ranked completion input from the original workspace against fetched origin/main 0aaa7cc in the clean detached publishing checkout. Original dirty changes were untouched. Higher-ranked aliases and county holds remain unresolved. Public inventory could not be authenticated; configured inventory provided provisional exclusions only.

## Sanford, North Carolina

Historical CSV overall completion: 33.33%. Active listings: 158. Unlinked: 0 before, 0 after the exact-unique linker, and 0 after reconciliation. No new shells were needed or created.

The linked shells all carried Lee county_id 548787. The direct Lee county GIS returned an ArcGIS Web Adaptor server-unavailable error. The official [NC OneMap parcel point service](https://services.gis.nc.gov/secure/rest/services/NC1Map_Parcels/MapServer/0) was available. Queried exact street candidates across Lee, Harnett and Chatham by state-county FIPS in small complete batches, respecting pagination. An initial county-name query used uppercase against title-case data and returned no rows; it was corrected to FIPS before drawing conclusions.

The complete public response contained 96 candidates and 83 unique, unit-free, positive street-number matches, all explicitly in Lee. Matching used only case and whitespace normalization. Public situs city and ZIP were blank, so these are street matches with public county evidence, not independently ZIP-verified matches. No owner mailing fields were used as situs evidence. The three-county search does not establish county identity for residual listings.

80 matches also had unique existing nc-onemap-parcels rows with the same APN and street. A guarded SQL statement reconciled 80 listing links to those existing properties, requiring unchanged listing/shell address, ZIP and old property ID, active status, state, shell source, county and unique target APN. Three remaining public matches require alternate-APN-format review and were left untouched. An initial preparation validation stopped before any SQL mutation because those three lacked exact primary APNs; filtering to the 80 verified primary APNs resolved that safely.

Only listing links and their reconciliation provenance changed. No properties were inserted, overwritten or deleted. Existing parcel situs ZIPs remain blank. raw.ncOneMapParcelReconciliation preserves old and new property IDs, county, APN, public object ID/source, method, field limitations and previous listing updated_at. Public transformation date was May 5, 2026; that date is not a tax year or proof of current listing availability.

| Metric | Before | After |
| --- | ---: | ---: |
| Active listings/properties | 158 | 158 |
| Unlinked listings | 0 | 0 |
| Shells | 158 | 78 |
| Parcel identities | 0 | 80 |
| Ownership support | 0 | 80 |
| Valuation support | 158 | 158 |
| Coordinates | 0 | 68 |
| Year built | 0 | 51 |
| Substantive classifications | 0 | 0 |

After-link valuations comprise 80 existing parcel values and 78 residual shell asking values; no asking prices were copied to parcel values. Brokerages remain 158; verified contacts, mortgage coverage, rents and positive creative-finance evidence remain zero. Historical completion was not recalculated.

**Publication held:** residual county scope and shell addresses remain unresolved, including floor-plan inventory. The original latest stored listing timestamp was May 25, 2026. Link updates did not refresh source availability. No readiness threshold or config was changed.

## Gainesville, Georgia

Historical CSV completion 33.33%; 131 active listings; exact-unique linker against linked Hall county_id 2338896 made no changes; unlinked 0 before and after. Fresh after-audit still shows 131 shells, asking values and brokerages, but zero parcel identity, ownership, classification, coordinates, year built, verified contacts, debt and rents. All listing ZIPs are 30504; none contain PLAN. ZIP alone was not used to verify county.

The [Hall County official property-tax links](https://www.hallcounty.org/QuickLinks.aspx?CID=82) direct GIS users to gis1.hallcounty.org/Public/PublicRedirect/default.htm. That host failed DNS resolution locally; browser retrieval also failed. No Gainesville database or config mutation. Publication remains held pending parcel/county evidence.

## Verification and next target

npm run typecheck and npm run build passed. Local endpoint HTTP 200: 632 markets. Public endpoint before and after returned HTTP 401 with credentialAvailable=false; public count and deployed production_allowed status remain unverified. No config change or origin restart was required. Hydrated scripts/lib/env.ts before protected access. No RapidAPI, secrets requested, or secrets printed.

Next distinct ranked county hold: Charlottesville, VA, historical completion 33.33%. Fresh baseline is 100 active/0 unlinked, 100 shells assigned Charlottesville city county_id 2338911, values and brokerages but no other audited enrichment. No exact pass or mutation yet; establish property-level Charlottesville city versus surrounding county scope first. Continue Sanford residual APN/county reconciliation and Gainesville GIS recovery as evidence becomes available.

Evidence, guarded SQL, and before/after audits remain in publishing tmp/sanford-20-*, tmp/gainesville-20-*, and tmp/charlottesville-20-before.json. Only this report is committed for this run.
