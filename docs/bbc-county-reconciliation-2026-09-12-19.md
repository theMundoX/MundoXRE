# Acworth and Irmo county reconciliation — 2026-09-12 19 UTC

Continued the original ranked completion CSV against fetched origin/main at 62069f6 in the clean detached publishing checkout. Earlier alias skips and county holds remain in force. The public endpoint returned HTTP 401 before and after work with no available hydrated API credential, so deployed exclusions and public production access could not be verified. Current remote configs provided provisional exclusion only.

## Acworth, Georgia: 46 public parcel replacements; publication held

- Historical CSV overall completion: 33.33%. Fresh active listings: 164. Unlinked: 0 before, 0 after exact-unique linking, 0 after reconciliation. No new shells.
- Before: all 164 linked properties were shells assigned to Cobb county_id 2338842 (FIPS 13067), with asking values and brokerages but no parcel identity, ownership, substantive classification, coordinates, year built, verified contacts, debt, rents or positive creative evidence.
- Queried the official [Cobb daily assessor parcels](https://gis.cobbcounty.gov/gisserver/rest/services/tax/taxassessorsdaily/MapServer/0) by listing house numbers with complete pagination: 7,736 candidates, 46 unique exact situs street matches after case/whitespace normalization only. All 46 had matching PARID/PIN, distinct APNs, no units and HAS_MULTIUNIT=N. FMV and assessed totals equaled their respective land plus building components. Tax year is unspecified by this layer. No suffix, directional, fuzzy, lot or unit merging.
- The [Cherokee parcel service](https://gis.cherokeecountyga.gov/arcgis/rest/services/MainLayersPRO/MapServer/1) returned 6,022 fully paginated house-number candidates and no exact street matches. This does not establish counties for unmatched listings.
- All 46 Cobb APNs were absent from the database. A guarded single statement inserted 46 public parcels and relinked 46 listings from unchanged shell IDs. Imported identity, OWNER_NAM1, FMV_TOTAL and ASV_TOTAL. City/ZIP remain explicitly listing-derived because this layer does not publish situs city/ZIP. Owner mailing fields and undecoded class codes were excluded. No existing property was overwritten or deleted. The initial SQL syntax error made no changes; the corrected statement returned eligible/imported/relinked = 46/46/46.
- Provenance in listing raw.cobbCountyParcelReconciliation retains old property/county IDs, official source, object ID, APN, method and previous listing updated_at. Source strings disclose field limitations.
- After: 164 active properties, 46 parcel identities/owners, 118 shells, 164 values (46 assessor totals and 118 listing asking values), 164 brokerages. Other audited enrichment remains zero. County 2338842 is verified for the 46 parcels only; 118 residual shell county assignments remain unverified. Market stays unpublished.
- Pre-link latest stored listing timestamp was 2026-05-25T10:31:41.003183+00:00. Reconciliation timestamps are not refreshed availability.

## Irmo, South Carolina: 9 public parcel replacements; county conflict confirmed

- Historical CSV overall completion: 33.33%. Fresh active listings: 161. Unlinked: 0 before, 0 after exact-unique linking against Lexington county_id 2338890, 0 after reconciliation. No new shells.
- Before: 161 shells assigned to Lexington (FIPS 45063), values and brokerages on all, otherwise no audited enrichment.
- [Lexington parcel layer](https://maps.lex-co.com/agstserver/rest/services/Property/MapServer/4) returned 1,161 fully paginated IRMO situs-city records. Direct concatenated street comparison found zero matches because source street formats differ. No fuzzy matching was used.
- [Official address points](https://maps.lex-co.com/agstserver/rest/services/AddressPts/MapServer/0) returned 4,989 fully paginated IRMO postal-community records. Unique exact FullAddress plus ZIP matched 41 listings: explicit County labels show 32 Richland County and 9 Lexington County. The 32 Richland records demonstrate that the existing all-Lexington shell assignment is incorrect; these shells were not automatically reassigned without Richland parcel reconciliation.
- The nine Lexington addresses were EXIST, unit-free, SubAddress=No and joined uniquely by TMS to the parcel layer with matching situs city/ZIP. APNs were absent from existing Lexington properties in both dashed and undashed forms. A guarded statement imported and relinked all nine, preserving old shell IDs and previous listing timestamps in raw.lexingtonCountyParcelReconciliation.
- Imported parcel identity, 2026 market/assessed/taxable values and official address-point coordinates. Assessed and taxable totals were checked against their components. Coordinates are address points, not surveyed parcel locations. Owner/physical/classification attributes labeled TaxYearOwnerAttr=2027 were excluded from current enrichment. Owner mailing fields were excluded.
- After: 161 active properties, 9 identities/coordinates, 152 shells, 161 values (9 public 2026 values and 152 asking values), 161 brokerages; zero ownership/classification/year built/verified contacts/debt/rents/positive creative evidence. Remains unpublished pending residual county reconciliation.
- Pre-link latest stored listing timestamp was 2026-05-25T10:31:44.049998+00:00. Reconciliation does not refresh listing availability.

## Verification and continuation

Hydrated Windows user environment through scripts/lib/env.ts before protected access. No RapidAPI, secrets requested or secrets printed. Original dirty workspace changes preserved. Only this report is intended for commit; helpers, raw public records and audits remain in publishing tmp/acworth-19-* and tmp/irmo-19-*.

Typecheck and build passed. Local API HTTP 200, 632 markets; no server/config change or restart needed. Public HTTP 401 with credentialAvailable=false, count unknown; no public production_allowed claim. No readiness formula or market config changed. Historical overall completion percentages were not recomputed.

Next: reconcile the 32 officially identified Richland addresses and remaining Irmo shells; Acworth still needs 118 residual county assignments. Next distinct ranked hold is Sanford, NC: fresh tmp/sanford-19-before.json has 158 active/0 unlinked, all shells assigned Lee county_id 548787, 158 asking values/brokerages and no substantive enrichment, historical overall 33.33%. No Sanford exact pass or mutation yet. Prior Plain City/Ellenwood and lower-ranked holds, including pending Mimbres, remain in force.
