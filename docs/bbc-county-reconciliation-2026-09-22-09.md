# BBC county reconciliation — 2026-09-22 09 UTC

Continued from da1360d in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Read automation memory, fetched origin/main, re-ranked the primary best-first CSV against configured markets provisionally, and retained previous county holds and aliases. Preserved unrelated dirty primary checkout work. Authoritative live endpoint exclusions remain unavailable because public authentication fails.

| Market | Active listings | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Gwynn Oak, MD | 52 | 29 → 23 | 0% | Six official parcel imports and exact links |
| Liberty Township, OH | 104 | 36 → 33 | 0% | Three county-supported listing shells |
| Queen Creek, AZ | 50 | 50 → 49 | 0% | One exact link to an existing Pinal shell |
| South Fulton, GA | 50 | 31 → 20 | 0% | Eleven county-supported listing shells |

Historical overall percentages are CSV values, not recomputed current completion. Liberty Township has grown from 51 historical active rows to 104. No market meets the zero-unlinked requirement. No live config additions, readiness adjustments, or publication claims.

## Gwynn Oak

Linked properties establish Baltimore County 2338913. Initial exact-unique linking changed zero. The official [Baltimore County property service](https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1) returned 6,926 fully paginated house-number candidates. Six previously unlinked listings matched exact normalized PREMISE_ADDRESS and ZIP_CODE, with unique TAXPIN and no condo unit. Duplicate geometry records agreed on all imported fields. Imported six new parcel rows without overwriting existing records, then exact-linked six and guarded six durable raw.baltimorePublicParcelEvidence writes containing complete source attributes.

Imported owner, positive assessed value, year built and available structure area; retained land use undecoded. Assessment vintage is unspecified; values are not current market estimates. After audit: 29 parcel identities/owners/values/years, no shells, property coordinates, substantive classification, debt, rents or verified contacts. Exact linking updates listing updated_at; September 22 timestamps do not establish newly observed availability. Residual 23 remain on hold.

## Liberty Township

Existing linked properties establish Butler county_id 1741128, FIPS 39/017. Initial exact-unique linking changed zero. The official [Ohio address service](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) returned 350 features for listing house-number/ZIP candidates. Three unique exact street/ZIP matches explicitly identify Butler County and OH. Guarded transaction created three clearly labeled shells and links, preserving listing timestamps and leaving parcel identity, owner, value and property coordinates absent.

Stored raw.ohioAddressCountyEvidence, including complete source attributes and limitations. One source address has UNITNUM 45 while its listing lacks a unit; this supports county only, not a verified listing unit or parcel identity. No source unit was promoted. Listing city remains an unverified listing label. After audit: 71 linked properties, 66 existing identities/owners, 67 preexisting positive values, 50 coordinates, five shells; 17 properties have mortgage records but no positive mortgage amount. Unknown shell asset markers are not substantive classification. Residual 33 remain on hold.

## Queen Creek

No linked county evidence initially. The [town's official description](https://www.queencreekaz.gov/residents/about-queen-creek) confirms Maricopa and Pinal county scope. Exact-unique passes ran for existing Maricopa 1741140 and Pinal 2338859. Pinal linked one existing shell; no new shells or inferred county assignments. The existing shell has a stored positive value but no legal parcel identity or newly verified valuation. Exact linking updated listing timestamp without refreshing availability. Residual 49 need individual county evidence.

## South Fulton

Existing linked properties establish Fulton county_id 1741139. Initial exact-unique pass changed zero. Official [Fulton address points](https://services1.arcgis.com/AQDHTHDrZzfsFsB5/ArcGIS/rest/services/Address_Points/FeatureServer/0), service item b30f7e20983943c1ae7dbd4571bd8c12, returned 562 fully paginated house-number candidates. Eleven unique normalized street/ZIP matches also identify City South Fulton, State GA, Status Active, with no units. Conventional suffix normalization only; no fuzzy matching.

Guarded transaction created eleven labeled shells and links with full raw.fultonAddressCountyEvidence and limitations. Source ParcelID is not promoted to verified parcel identity; no owner, value, coordinates, substantive classification or refreshed availability inferred. Listing timestamps unchanged. After audit: 30 linked properties, 18 existing identities/owners, 19 preexisting values, 12 shells, no years/coordinates/debt/rents/verified contacts. Residual 20 remain on hold.

## Validation and handoff

- Net 21 links: six new public parcels, fourteen new shells, one link to an existing shell.
- npm run typecheck and npm run build passed. No API code change or origin restart required.
- Initial/final public smoke: HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and deployed production_allowed UNKNOWN.
- No RapidAPI, secret requests or secret output. All protected access hydrates Windows user environment first.
- Evidence, guarded SQL and before/after audits retained in tmp/*-sep22-09-*; provenance durable in listing raw fields.
- Next ranked target: Helena MT, freshly audited only: 50 active, 30 unlinked, Lewis and Clark county_id 2338941 from 20 linked properties. Existing 20 identities/owners, 17 positive values, 16 years. Exact linking and county-supported residual reconciliation pending. Then Waverly OH. Retain earlier holds.
