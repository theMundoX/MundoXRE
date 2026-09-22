# BBC county reconciliation — 2026-09-22 12 UTC

Continued from c010d73 using the ranked CSV in C:/Users/msanc/mxre and the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Fetched origin/main and verified alignment. Earlier holds and aliases retained; live inventory exclusions remain provisional because the public endpoint could not be authenticated. Unrelated primary-checkout changes preserved.

| Market | Active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Todd, NC | 40 | 40 → 32 | 0% | Eight assessor-backed parcels imported and linked; held |
| Falls Church, VA | 40 | 40 → 35 | 0% | Five county-supported listing shells linked; held |
| Laguna Hills, CA | 39 | 0 → 0 | 0% | Exact pass, refreshed audit; readiness 14.3 below 28; held |

Overall percentages are historical CSV values, not recomputed current completion. No market config, fallback metric, readiness target or production access was changed.

## Todd

No linked county evidence existed initially. Official [Ashe County parcel service](https://gis.ashecountygov.com/arcgis/rest/services/Parcels/MapServer/0) returned 319 house-number candidates without truncation. Eight listings each matched one normalized ParcelPropertyAddress exactly; units and lot labels excluded. Statewide NC OneMap query for scity TODD returned zero features. Owner mailing addresses and ZIPs were not used to match situs addresses.

Ran exact-unique linker first for existing Ashe county_id 35170 and Watauga 224343; both linked zero. Eight uniquely matched Ashe parcel records were then imported and linked in one guarded transaction. Stored county parcel identities, Name1 ownership and TotalAssessedValue; assessment vintage unspecified, not current market value. City and ZIP explicitly remain listing-derived. No asking-price substitution, classification, coordinates, building year, debt or refreshed availability inferred. Source attributes and limitations persisted in listing raw.asheParcelEvidence. Existing records were not overwritten; duplicate parcel/address checks abort on conflicts.

After audit: 40 active, eight linked identities/owners/assessed values, 32 unlinked, zero shells. No other audited enrichment. Latest listing timestamp unchanged at 2026-05-07T15:33:01.953273+00:00. Linked properties now establish Ashe county for these eight records only; residual listings still require individual county evidence.

## Falls Church

No linked county evidence existed initially. The independent city public Parcel_with_Plans layer exposes identifiers but no situs address for safe address matching. Fresh fully paginated query of [Fairfax County address points](https://services1.arcgis.com/ioennV6PpG5Xodq0/ArcGIS/rest/services/Address_Points/FeatureServer/0) returned 905 house-number candidates. Five unique exact address/ZIP matches explicitly identify VA, FAIRFAX COUNTY, Current address status and blank unit. Current address status is not evidence of current listing availability.

Exact-unique linker first ran Fairfax county_id 2338951 and linked zero. Guarded atomic transaction created and linked five labeled listing-backed shells. Null parcel identity and valuation; unknown classification and listing_only confidence. No source parcel PIN promoted into property identity. Source attributes and limitations persisted in listing raw.fairfaxAddressCountyEvidence. Existing-property conflicts abort the transaction. Listing timestamps were not changed.

After audit: 40 active, five linked shells, 35 unlinked. Zero substantive classification, identity, ownership, valuation, coordinates, years, debt, rents or verified contacts. The initial generic audit counted unknown asset types; the final corrected audit excludes them. Latest listing timestamp unchanged at 2026-05-07T15:32:31.478972+00:00. Residual addresses cannot be assigned wholesale to Fairfax or the independent city.

## Laguna Hills

Linked properties confirm Orange county_id 2338869. Exact-unique pass linked zero. Final audit: 39 active/linked, 15 identities, 24 preexisting shells and stored listing-price values, 11 building years. Zero substantive classification, owners, coordinates, debt, rents or verified contacts. Unknown shell types are excluded. Seven-input readiness remains 14.3 even counting stored listing-price values; below existing target 28. No mutation or publication.

## Verification and next target

- npm run typecheck and npm run build passed. API code unchanged; no origin restart needed.
- Public smoke twice after hydrateWindowsUserEnv returned HTTP 401, credentialAvailable=false. Public endpoint count and deployed production_allowed remain UNKNOWN.
- No RapidAPI, secrets requested or printed. Database/API access helpers hydrate Windows user environment first.
- Evidence and before/after audits retained in tmp/todd-sep22-*, tmp/falls-sep22-*, tmp/todd-falls-sep22-*, tmp/laguna-hills-sep22-*, and tmp/public-sep22-current.json. Mutation provenance also persists in the database.
- Next ranked target: Murrysville PA, then Halethorpe MD. Preserve previous holds and aliases.
