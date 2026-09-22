# BBC county reconciliation — 2026-09-22 13 UTC

Continued from 1b13981 in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09, using the original ranked CSV in C:/Users/msanc/mxre. Fetched origin/main and verified alignment. Preserved unrelated primary-worktree changes and earlier holds/aliases. Configured-market exclusions are provisional because public inventory authentication remains unavailable.

| Market | Active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Murrysville, PA | 39 | 21 → 21 | 0% | Exact and public parcel searches found no safe residual matches; held |
| Halethorpe, MD | 38 | 9 → 4 | 0% | Five county-supported listing shells; held |
| Jefferson, NC | 38 | 28 → 28 | 0% | Exact and public parcel searches found no safe residual matches; held |
| Newark, TX | 38 | 38 → 36 | 0% | Two certified assessor parcels imported/linked; held |

Overall percentages are historical CSV values, not current recomputed completion. None meets the zero-unlinked requirement. No market config, fallback coverage, target, production access or API code was changed.

## Murrysville

Linked properties confirm Westmoreland county_id 1096045. Exact-unique linker added zero. Fresh fully paginated [county parcel query](https://gis.westmorelandcountypa.gov/arcgis/rest/services/Upgrade/Parcels_AS400_VIEW_Upgrade/MapServer/2) returned 1,461 house-number candidates, zero unique normalized exact situs matches for residuals. Residuals include lot labels and zero-number addresses. No new shells or county inference. After: 18 identities/owners/stored values, nine years, no substantive classification or coordinates. Debt, rents and verified contacts remain zero.

## Halethorpe

Linked properties confirm Baltimore county_id 2338913. Exact-unique linker added zero. Fresh fully paginated [county parcel query](https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1) returned 2,377 candidates, zero unique exact premise-address/ZIP matches.

The [county address-point service](https://bcgisapps.baltimorecountymd.gov/arcgis/rest/services/Accela/MapServer/0) returned 2,501 candidates without truncation. Five residual listings matched one exact ADDRLABEL/ZIP/MD record each, with ACTIVE address status, Field Verification and no listing unit/lot label. COUNTY_PREF is a community label, not a county name; county support comes from the county-published service and linked county metadata.

A guarded transaction created five labeled listing_signal_shell:redfin properties and links. Null parcel identity/value, unknown asset type and listing_only confidence. TAXACCTID was not promoted into parcel identity; address-use labels and coordinates were not promoted into enrichment. Source attributes and limitations persist in listing raw.baltimoreAddressCountyEvidence. Listing timestamps unchanged; active address status does not prove refreshed listing availability.

After: 34 linked properties, 29 parcel identities/owners/substantive use classifications/years, 28 preexisting stored values, five shells, four unlinked. Remaining labels: 0 MONUMENTAL RD; 18 INGATE TER #4402; 25-W W END CT; 216 CLYDE. No speculative unit or street completion. No coordinates/debt/rents/verified contacts.

## Jefferson

Linked properties confirm Ashe county_id 35170. Exact-unique linker added zero. Fresh [official Ashe parcels](https://gis.ashecountygov.com/arcgis/rest/services/Parcels/MapServer/0) query returned 171 complete house-number candidates, zero exact residual situs matches. Residual inventory includes lots, unit-specific and multi-address labels. Existing ten identities/owners/stored values; other audited enrichment zero. No mutation.

## Newark

No linked county evidence initially. Exact-unique linker ran Wise county_id 2338888 and Tarrant county_id 8 separately; both added zero. County scope remains property-specific.

Downloaded the free [Wise CAD 2026 certified roll](https://wise-cad.com/data-downloads/) and its Appraisal Export Layout - 8.0.32.xlsx. Read the field-layout workbook without modifying it. Streamed all 262,952 APPRAISAL_INFO rows from the August 5 export; retained 3,018 house-number candidates. Numeric year and supplement fields are zero-padded and were parsed numerically. Owner mailing fields were never used as situs fields.

Two unique real-property, certified-2026, single-owner records matched exact assembled situs address and situs ZIP, with blank situs unit and false owner confidentiality flag: 203 POINT RIDER RD (R0294.I033.00) and 189 POINT RIDER RD (R0294.I030.00). A guarded transaction imported geo_id identities, owner names and assessed_val values 380835 / 358586, then linked the active listings. Source field values, layout, method and limitations persist in raw.wiseCertifiedParcelEvidence. Values are 2026 certified assessed values, not asking prices or current market estimates. R denotes real property, not residential classification. No classification, year, coordinates, debt, rents or verified contacts inferred. Listing timestamps unchanged.

Other exact street candidates often lack situs city/ZIP; not accepted by this matching pass. ZIP/county reconciliation remains necessary. After: two identities/owners/assessed values, 36 unlinked, zero shells and other audited enrichment. Roll and parser retained under tmp/wise-* and tmp/newark-sep22-* for subsequent work; avoid downloading again unnecessarily.

## Validation and checkpoint

- npm run typecheck and npm run build passed; git diff --check passed before commit.
- Public smoke repeated after hydrateWindowsUserEnv: HTTP 401, credentialAvailable=false. Public endpoint count and deployed production_allowed remain UNKNOWN.
- Existing local smoke credential also returned HTTP 401. Origin responds, but local market count is unverified. No API code changed; no restart needed.
- No RapidAPI or secrets requested/printed. Database/API helpers hydrate Windows user environment before access.
- Before/after audits, exact-link logs, public queries and guarded transactions remain in tmp/*-sep22-run-*; mutation evidence is also durable in the database.
- Next ranked candidate: Mimbres NM, then Richmond VA. Retain all higher-ranked holds. Newark certified-roll residual reconciliation remains an enrichment opportunity.
