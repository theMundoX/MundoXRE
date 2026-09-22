# BBC county reconciliation — 2026-09-22 10 UTC

Continued from b39083a in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Read automation memory, fetched origin/main, and checked the primary best-first CSV against configured markets provisionally, preserving earlier holds and aliases. Unrelated dirty primary checkout changes were preserved. Public endpoint authentication still fails, so authoritative live exclusions remain unverified.

| Market | Active listings | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Helena, MT | 50 | 30 → 30 | 0% | Exact pass and fresh parcel search; residual hold |
| Waverly, OH | 49 | 32 → 21 | 0% | Eleven county-supported listing shells |
| Lexington, NE | 47 | 12 → 12 | 0% | Exact pass and fresh assessor search; residual hold |
| Mount Vernon, OH | 47 | 11 → 10 | 0% | One county-supported listing shell |

Historical overall percentages are CSV values, not recomputed current completion. All four markets fail the zero-unlinked requirement. No live configs, fallback metrics or readiness restrictions were changed; no publication claims.

## Helena

Linked properties establish Lewis and Clark county_id 2338941, FIPS 30/049. Exact-unique linking changed zero. The existing public parcel query fetched 843 candidate records from https://helenamontanamaps.org/arcgisadp/rest/services/Parcels/MapServer/0 with truncation checks. Twenty unique exact street/ZIP/state matches correspond to the existing linked records. Thirty residuals include ZIP disagreements, units, ranges and unspecified lot addresses. No duplicate imports or blanket county shells. After audit: 20 identities/owners, 17 stored values, 16 years, no shells or other audited enrichment. Existing values were not refreshed.

## Waverly

Linked properties establish Pike county_id 2338942, FIPS 39/131. Exact-unique linking changed zero. The official Ohio address service at https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 353 candidate features with truncation checks. Eleven unique exact street/ZIP matches explicitly identify Pike County and OH, with blank source units.

A guarded atomic transaction created eleven labeled listing-backed property shells and links. Full source attributes and limitations are durable in raw.ohioAddressCountyEvidence. No parcel identity, owner, value, coordinates, substantive classification or refreshed availability inferred. Listing timestamps unchanged. After audit: 28 linked, 17 preexisting identities/owners/values, 11 shells; other audited enrichment zero. Twenty-one residual listings remain unresolved.

## Lexington

Linked properties establish Dawson county_id 2338944, FIPS 31/047. Exact-unique linking changed zero. Fresh searches for all twelve unlinked listings at https://www.nebraskaassessorsonline.us/search.aspx?county=Dawson produced no unique exact situs address/city/ZIP matches. Inspected returned table structure: one otherwise exact street has listing ZIP 68937 versus assessor ZIP 68850; other candidates have street suffix discrepancies, missing city/ZIP, multiple results or no results. No inference from owner mailing addresses. No mutation. After audit: 35 identities/owners, no stored values, shells or other audited enrichment. Twelve residuals held for reconciliation.

## Mount Vernon

Linked properties establish Knox county_id 2338945, FIPS 39/083. Exact-unique linking changed zero. Ohio address service returned 843 candidate features. Its LSN combined-address field is empty in Knox, so matched the explicit house number, street prefix/name/type/suffix components and ZIP, requiring OH/Knox and no units or house-number ranges. One unique exact match supports county only. A second street has two source address points and was held; no coordinate selection or parcel inference.

Guarded atomic transaction created one labeled shell and link with complete durable raw.ohioAddressCountyEvidence and the component-matching method. Listing timestamps unchanged. After audit: 37 linked, 36 preexisting identities/owners/coordinates, one shell; no values, years, debt, rents or verified contacts. Ten residuals remain unresolved.

## Validation and handoff

- Net twelve new shells and twelve links; zero live market additions.
- npm run typecheck and npm run build passed. No API code change or origin restart required.
- Initial/final public smoke after hydrateWindowsUserEnv: HTTP 401, credentialAvailable=false. Public endpoint count and deployed production_allowed UNKNOWN.
- No RapidAPI, secrets requested or secret output. Protected tools hydrate Windows user environment first.
- Before/after audits, source evidence and guarded SQL retained in tmp/*-sep22-10-*; shell evidence also durable in database listing raw fields.
- Next ranked target Millsap TX: freshly audited only, 47 active, 32 unlinked, 15 linked properties in Parker county_id 2338849, FIPS 48/367. Existing 15 identities/owners/values and nine years. Exact pass and county-supported reconciliation pending. Then Greenwich CT. Preserve all prior holds.
