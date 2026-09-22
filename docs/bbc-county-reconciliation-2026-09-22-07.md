# BBC county reconciliation — 2026-09-22 07 UTC

Read automation memory and fetched origin/main at da0532b. Used the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09 to preserve unrelated dirty work in C:/Users/msanc/mxre. Re-read the primary best-first CSV and current server configs as provisional live exclusions. Retained prior holds and aliases; authenticated public inventory remains unavailable.

| Market | Active listings | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Marysville, OH | 57 | 47 → 34 | 0% | 13 county-supported listing shells |
| Nottingham, MD | 56 | 25 → 21 | 0% | 4 public parcels imported and linked |
| Athens, OH | 56 | 33 → 27 | 0% | 6 county-supported listing shells |
| Onancock, VA | 56 | 56 → 1 | 0% | 55 postal-county-supported listing shells |
| Lovejoy, GA | 54 | 54 → 54 | 0% | Fresh audit only; county/exact pass next |

Overall percentages are historical CSV values, not recomputed current completion. Stored active flags do not constitute refreshed listing availability. No market satisfies zero-unlinked; no live config changes or publication claims.

## Marysville and Athens

Existing linked properties verify Union county_id 2338937 (39/159) and Athens county_id 2338938 (39/009). Exact-unique SSH linker passes changed zero in each market before shell creation. Official Ohio address service https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 282 Marysville and 1,579 Athens features from house-number/ZIP queries; errors and transfer truncation rejected. Matched exact street+ZIP or exact full street, city, state and ZIP LSN text, with one feature per accepted listing. Marysville yielded 13 Union matches; Athens six Athens matches.

Guarded transactions created and linked 19 shells total, with null parcel IDs/values/coordinates, explicit listing_signal_shell:redfin source, active_listing_shell status and listing_only quality. raw.ohioAddressCountyEvidence retains source, observation time, object ID, source address/ZIP and county. Preserved original listing timestamps. No owner, classification, valuation or parcel identity inferred. After: Marysville 10 existing identities/owners/values plus 13 shells; Athens 23 identities/owners/values plus six shells. No substantive classification, coordinates, debt, rents or verified contacts. Audit classified_count includes the listing_backed_property marker; this is not substantive asset classification and was not counted as enrichment.

## Nottingham

Existing 31 linked properties establish Baltimore county_id 2338913 (24/005). First exact-unique pass changed zero. Official public service https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1 returned 10,296 fully paginated candidates. Four unique exact premise-address/ZIP matches had numeric unique TAXPIN, no condo unit, and duplicate geometry rows agreeing on every imported field. Imported four absent parcels with owners, positive TOTAL_VALUE as assessed_value, year built, available structure area and undecoded LU_CODE. No existing property overwrites. Source string discloses unspecified assessment vintage and listing-derived city. Second exact-unique pass linked all four.

Guarded four-row raw.baltimorePublicParcelEvidence update retains full public attributes, source, observation time, match method, geometry duplicate count and limitations. Linker changed listing updated_at to run time; this is explicitly NOT a refreshed availability observation. Before-audit latest stored listing timestamp was September 11. After: 35 identities/owners, 32 valuations, 33 years built; no shells/substantive classification/coordinates/debt/rents/verified contacts. Hold 21 residual, including unit-level cases; no parent-parcel guessing.

## Onancock

No initially linked properties; existing county metadata verifies Accomack2338939 (51/001). Exact-unique pass changed zero. Fifty-five active unlinked addresses have ZIP23417, with single-county Accomack evidence from https://www.zip-codes.com/zip-code/23417/zip-code-23417.asp and https://www.unitedstateszipcodes.org/23417/ . Postal evidence establishes county only, not legal parcel/address validity. Official GIS entrypoint for further parcel work: https://www.accomack.gov/354/GIS .

Guarded transaction inserted/linked 55 distinctly labeled listing shells with null parcel identity, no values/coordinates/owners/classification inferred. raw.accomackZipCountyEvidence retains sources, method and limitations, including listing lot/unit/zero-number labels. Listing timestamps preserved at May7. One active listing lacks ZIP and remains unlinked. Readiness remains inadequate even apart from the residual link.

## Validation and next work

- Net 74 labeled shells and four public parcel records, 78 links; zero new live markets.
- npm run typecheck, npm run build and git diff --check passed. API source unchanged; no origin restart required.
- Public initial/final HTTP401 with credentialAvailable=false after hydrateWindowsUserEnv. Public count and deployed production_allowed UNKNOWN. Local3101 also HTTP401 with prior smoke credential; count UNKNOWN.
- Hydrated scripts/lib/env.ts before all protected access; no RapidAPI, requested secrets or printed secrets.
- Evidence, audits and SQL retained in tmp/*-sep22-07-*; shell and Nottingham provenance stored durably in listing raw data.
- Next target Lovejoy GA: fresh audit54 active/all54unlinked/no linked county. County evidence and exact pass pending. Then Vestavia AL and Sandy Springs GA in CSV rank order. Preserve all previous holds.
