# BBC county reconciliation — 2026-09-22 11 UTC

Continued from eb3fa9e using the ranked CSV in C:/Users/msanc/mxre and the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Fetched origin/main and verified alignment. Recovered the latest checkpoint because automation memory ended at the 04 UTC run. Unrelated primary-checkout changes preserved. Earlier holds and aliases retained; configured market exclusions remain provisional because public inventory authentication failed.

| Market | Active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Millsap, TX | 47 | 32 → 32 | 0% | Exact pass and fresh public assessor search; held |
| Greenwich, CT | 46 | 46 → 46 | 0% | No linked county evidence; public source investigation; held |
| Rosedale, MD | 45 | 11 → 11 | 0% | Exact pass and fresh county parcel query; held |
| Ashville, OH | 41 | 16 → 12 | 0% | Four county-supported listing shells; held |

Percentages are historical CSV values, not recomputed current completion. None meets the zero-unlinked requirement. No market config, readiness target, fallback metric or production access was changed.

## Millsap

Linked properties establish Parker county_id 2338849. Exact-unique linker changed zero. Fresh searches for residual listing streets through https://iswdataclient.azurewebsites.net/webSearchAddress.aspx?dbkey=PARKERCAD returned 1,187 parsed candidate records and zero unique exact street matches. No broad county assignment or speculative shells. After audit retains 15 parcel identities/owners/stored values and nine years. Other audited enrichment zero. The parsed result count does not establish complete county coverage.

## Greenwich

Fresh audit: 46 active listings, none linked; county cannot be determined from linked properties. Requeried the existing Connecticut CAMA/parcel source using Greenwich town and listing house-number filters: zero features, no truncation reported. Official https://www.greenwichct.gov/150/Assessor and https://www.greenwichct.gov/691/Geographic-Information-Systems describe record/data request routes; no usable automatic parcel evidence retrieved. No outreach, purchases, county inference or shells. Exact linking deferred without established county.

## Rosedale

Linked properties establish Baltimore county_id 2338913. Exact-unique linker changed zero. Fresh paginated public query at https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1 returned 298 candidates for residual house numbers. Zero unique exact normalized premise-address/ZIP matches with parcel identity and no condo unit. Existing 34 linked identities/owners/values/use codes/years retained; other audited enrichment zero. Raw use-code presence is not new classification validation.

## Ashville

Linked properties establish Pickaway county_id 2338949, FIPS 39/129. Exact-unique linker first changed zero. Ohio's https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 458 candidate address features with truncation checks. Four unique exact street/ZIP matches explicitly identify OH and Pickaway, with blank unit and house-number-range fields. Listing city remains listing-derived; source municipality is preserved separately.

Guarded atomic transaction created four labeled listing-backed shells and four links. Full source attributes, observation time and limitations are durable in listing raw.ohioAddressCountyEvidence. Null parcel identity/value, unknown classification and listing_only confidence. No ownership, coordinates, refreshed availability or assessor facts inferred. Listing updated_at was not changed. After audit: 29 linked properties, 25 preexisting identities/owners, four shells, 12 unlinked. Other audited enrichment zero.

## Validation and next target

- npm run typecheck and npm run build passed; no origin restart needed because API code was unchanged.
- Public smoke after hydrateWindowsUserEnv returned HTTP 401 with credentialAvailable=false. Public count and deployed production_allowed remain UNKNOWN.
- No RapidAPI, secrets requested or printed. Protected scripts hydrate Windows user environment before access.
- Evidence and before/after audits retained in tmp/*-sep22-11-*; shell provenance also stored in the database.
- Next ranked target: Todd NC (CSV 40 active, 0% historical completion), then Falls Church VA. Preserve all prior holds.
