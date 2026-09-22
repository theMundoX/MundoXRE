# BBC county reconciliation — 2026-09-22 05 UTC

Continued from 6c59b7d in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Read automation memory, recovered the newer committed 03/04 UTC checkpoints, fetched origin/main, and ranked the original best-first CSV against current configs. Existing aliases and higher-ranked holds remain. Config exclusions remain provisional because deployed inventory authentication is unavailable. Preserved all unrelated primary-checkout changes.

| Market | Active listings | Unlinked before → after | Historical CSV overall | Result |
| --- | ---: | ---: | ---: | --- |
| Oxford, GA | 72 | 72 → 72 | 0% | County evidence unresolved; held |
| League City, TX | 72 | 72 → 55 | 0% | 17 evidence-backed shells; held |
| San Jose, CA | 71 | 0 → 0 | 0% | Insufficient enrichment; held |
| Waynesville, OH | 137 | 132 → 78 | 0% | 54 evidence-backed shells; held |

Historical percentages are not recomputed current completeness. Waynesville had 69 listings in the historical report. This work does not refresh listing availability.

## Oxford

No linked properties exist from which to infer county. Existing Walton county_id 2338886 exact-unique pass changed zero. Newton County is absent from the matching county metadata query. A public Newton_Parcels service returned zero unique exact situs address/city/ZIP matches; its ArcGIS item 40743c9b9c87460a94aaff77c787a705 belongs to mhackman_UofMD and was last modified in 2022, so it was not accepted as current county-authoritative evidence. No county or shell was created. The official county GIS page remains https://www.newtoncountyga.gov/259/Geographic-Information-Systems-GIS . County evidence and a Newton exact pass remain pending.

## League City

Ran exact-unique linker first against Galveston county_id 2338920 (48/167) and Harris county_id 11 (48/201); both changed zero. The public Galveston County Engineering/GCAD service https://services5.arcgis.com/NAnnb4W7JLztFw9i/arcgis/rest/services/GCAD_Parcels/FeatureServer/0 returned 17 unique exact full SITUS matches including street, League City, TX and listing ZIP. Batches rejected source errors and truncated responses. Full returned attributes are retained in tmp/league-city-sep22-05-public.json. Thirty entries in the original 72-listing set contain PLAN labels.

A guarded transaction created and linked 17 distinct active_listing_shell properties with listing_signal_shell:redfin source, listing_only quality, unknown classification, null parcel IDs and no inferred values or coordinates. Guards required unchanged active/unlinked listings, matching address/ZIP and no existing exact state/address/ZIP property. Durable raw.galvestonSitusCountyEvidence records source, observation time, feature ID, exact situs, county and explicit county-evidence-only quality. Source parcel identifiers were not promoted to verified identity. Listing updated_at was preserved.

After audit: 72 active, 17 linked shells, 55 unresolved; 72 brokerage rows; zero verified parcel identities, owners, substantive classifications, valuations, coordinates, year-built, debt, rents or verified contacts. Latest stored listing timestamp remains May 7, 2026. Publication blocked.

## San Jose

All 71 active listings already link to Santa Clara county_id 2338929 (06/085). Exact-unique linker changed zero; re-audit confirms zero unlinked. Nine parcel identities, 62 shells and 62 positive stored values; zero ownership, substantive classification, coordinates, year-built, mortgage, rent or verified contact coverage. Stored values have not been revalidated as assessor values. Even counting all positive stored values, the existing seven-field readiness formula yields 14.3, below the established 28 pilot target. No config or database mutation.

## Waynesville

Five existing linked properties establish Warren county_id 1741129 (39/165) as one county. Exact-unique pass changed zero. Public Ohio address layer https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 286 deduplicated features from bounded ZIP/house-number queries. Fifty-four unlinked listings uniquely matched exact LSN street and ZIP: 51 Warren and 3 Greene. Verified Greene county_id 2338903 (39/057); its exact-unique pass also changed zero before shell creation.

A guarded transaction created and linked 54 distinct labeled shells with the same unchanged-listing/no-existing-property/count guards. raw.ohioAddressCountyEvidence retains source, observation time, feature ID, matching address/ZIP and county. No parcel identity, valuation, ownership or coordinates were inferred. Listing timestamps were preserved.

After audit: 137 active, 59 linked properties, 78 unlinked, 54 shells; five existing parcel identities/owners/classifications, three existing coordinates; no valuations, brokerage, verified agent contacts, mortgage or rent coverage. Latest listing timestamp remains September 21, 2026. Publication blocked by unresolved links and enrichment; postal market crosses county scope.

## Validation and continuation

- Net 71 new shell links, zero new live configs. API code did not change, so no origin restart was needed.
- npm run typecheck, npm run build and git diff --check passed.
- Initial/final public endpoint probes returned HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv(). Public market count and production_allowed remain UNKNOWN. Local port 3101 also returned HTTP 401 with the prior smoke credential; local count remains UNKNOWN.
- Protected helpers hydrated scripts/lib/env.ts before access. No RapidAPI, secrets requested, or secrets printed.
- Evidence retained under tmp/oxford-sep22-05-*, league-city-sep22-05-*, san-jose-sep22-05-* and waynesville-sep22-05-*. Only this report is intended for commit.
- Next ranked target: Placentia, CA (historical 68 active / 0%), then Santa Ana, CA (68 / 0%). Retain Oxford county hold, League City 55 unresolved, Waynesville 78 unresolved, San Jose enrichment hold and all earlier holds.
