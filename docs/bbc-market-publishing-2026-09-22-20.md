# BBC publishing and reconciliation — 2026-09-22 20 UTC

Continued from b361c6d using the ranked CSV in C:/Users/msanc/mxre and the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Preserved unrelated main-checkout edits and all earlier holds. Authenticated public inventory is unavailable, so live exclusions use the current configuration provisionally.

| Market | Active listings | Unlinked before → after | Historical CSV overall | Result |
| --- | ---: | ---: | ---: | --- |
| Scottdale GA | 30 | 30 → 0 | 0% | Local production_allowed, readiness 34.3/28 |
| Huntersville NC | 30 | 30 → 30 | 0% | Held: no safe exact parcel matches |
| Liberty Twp OH | 59 (CSV 30) | 59 → 59 | 0% | Butler exact-unique zero; county reconciliation pending |

Historical overall completion is the ranking CSV value, not recalculated enrichment or readiness.

## Scottdale

Ran scripts/link-market-listings-fast.ts with --exact-unique --ssh-psql for DeKalb county_id 1741141 first: zero links. Fully paginated the official [DeKalb parcel service](https://dcgis.dekalbcountyga.gov/hosted/rest/services/PropertyAppraisal/Parcels_IASWorld/FeatureServer/0), returning 2,211 ZIP 30079 records. Matched unique normalized situs street, city, state and ZIP; promoted 24 parcels with blank UNIT and UNIT_NO, owners and CNTASSDVAL assessed values. Retained full source attributes under raw.dekalbParcelEvidence. Did not interpret untranslated class codes as asset classification.

Re-audit confirmed 24 linked DeKalb properties. Created six labeled listing-backed shells using that county cohort with explicit property-level reconciliation limitations under raw.listingCityCountyEvidence. Five source records carry unit fields not present in listings: 642/646/650 MEMPHIS DR and 541/485 LANTERN WOOD DR. Those candidate identities and values were not promoted; full unresolved source evidence remains durable. 481 GLENDALE RD has no exact parcel match. All shells have null parcel identity and valuation and unknown/listing_only classification. Guarded transactions prevent linking changed or inactive listings and reject existing-property conflicts. Listing timestamps were preserved.

Final: 30 linked properties; 24 identities/owners/assessed values; six shells; zero substantive classifications, coordinates, years, debt, rents and verified contacts. Brokerage text on 30 rows. Latest listing observation remains May 7, 2026, and active availability is unrefreshed. Config discloses all limitations, unchanged readiness target 28. [Map center](https://mapcarta.com/20789142) is display positioning only.

## Held markets

Huntersville: exact-unique Mecklenburg 93352 zero. County boundary services lack situs fields. Queried the official [NC OneMap parcels](https://services.gis.nc.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0), fully paginating 1,843 Mecklenburg house-number candidates. No exact street/ZIP/state matches; some listings include home-site/unit text. No county assumptions, shells or config promotion.

Liberty Twp: current inventory is 59 active/unlinked rather than historical 30. Butler county_id 1741128 exact-unique zero. No linked county evidence; do not assign all Liberty Township city labels to Butler without reconciliation.

## Verification

- npm run typecheck, npm run build and git diff --check passed.
- Restarted only verified node API origin on port 3101 using hidden detached helper and disposable local credential.
- Local smoke HTTP 200: 638 markets; scottdale-ga production_allowed, readiness 34.3/28.
- Public smoke repeatedly HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public count and public production_allowed remain UNKNOWN.
- No RapidAPI, secret requests or secret output. Hydrated Windows environment before protected access.
- Evidence retained in tmp/*-sep22-20-* and durable database provenance. Intentional files: src/api/server.ts and this report.
- Continuing with Belmont NC, then Copley OH and Ottawa Hills OH; preserve earlier holds.

## Continuation: Belmont and Copley

| Market | Active listings | Unlinked before → after | Historical CSV overall | Result |
| --- | ---: | ---: | ---: | --- |
| Belmont NC | 29 | 29 → 1 | 0% | 28 public parcel links; held for COOPER 3 PLAN |
| Copley OH | 90 (CSV 29) | 89 → 3 | 0% | Four exact links, 82 new shells; held |

Belmont: Gaston county_id 634608 exact-unique first zero. Fully paginated 1,038 house-number candidates from the official [Gaston parcels](https://gis.gastoncountync.gov/publicgis/rest/services/PublicGIS/Parcels/MapServer/11). Imported 28 unique exact situs street/ZIP/NC/postal-city matches, consistent AKPAR/PID and WHOLE_ADDRESS/PHYSSTRADD, no subPIN. Final 28 identities/owners/valuations/county use descriptions, 26 years, zero coordinates/debt/rents/verified contacts. Stored FMV_TOTAL in market_value with explicit provenance: county fair-market tax valuation, not independent current sale-price estimate; source parcel_year is 2027. Current co-owner names retained. Many rows are Cramerton tax district despite Belmont postal city. Durable raw.gastonParcelEvidence includes full source attributes. Listing timestamps unchanged. COOPER 3 PLAN is not a specific street address and receives no shell or fabricated identity; market remains held.

Copley: Summit 1698989 exact-unique linked four, two of them pre-existing shells. County evidence after exact linking was five linked properties, including three parcel records and two shells. Created 82 labeled shells for remaining numeric street addresses/ZIP 44321 with full unit text, null identities/values, unknown/listing_only classification, and raw.listingCityCountyEvidence disclosing cohort-level county assignment pending property-level reconciliation. Initial guarded transaction rolled back completely on existing-property conflicts; inspected and excluded those records before successful retry. Unresolved: 712 S HAMETOWN RD has two existing parcel identities; 20 S HAMETOWN RD has four pre-existing shells; V/L APPROX 62 AC JACOBY RD is not a specific street address. No arbitrary identity chosen.

Final Copley: 90 active, 87 linked, three unlinked, 84 total shells (82 new). Three parcel identities/owners/classifications/coordinates/years, five stored valuations, three historical mortgage records on one property (latest 2020-09-24), one rent snapshot; no verified contacts or brokerage rows. New-shell creation preserved timestamps; exact linking changed timestamps without refreshing availability. Public Summit metadata retrieved for later parcel reconciliation; no broad ingestion run.

Post-push public smoke remains HTTP 401 with credentialAvailable=false. Local inventory remains 638 and only Scottdale was promoted during this run. Published config/report commit b192f05. This continuation changes the report only. Next ranked target: Ottawa Hills OH, then Southampton PA. Preserve all previous holds.
