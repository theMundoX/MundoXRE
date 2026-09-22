# BBC county reconciliation — 2026-09-22 04 UTC

Continued from 7675c1e using the ranked report in C:/Users/msanc/mxre and the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Existing higher-ranked aliases and holds remain. Source-config exclusions are provisional because authenticated public inventory remains unavailable. Unrelated primary-checkout changes were preserved.

| Market | Active listings | Unlinked before → after | Historical CSV overall | Outcome |
| --- | ---: | ---: | ---: | --- |
| Ferris, TX | 78 | 78 → 78 | 0% | Held for property-level county evidence |
| Santa Maria, CA | 77 | 77 → 46 | 0% | 31 labeled listing-backed shells; held |
| Modesto, CA | 72 | 0 → 0 | 0% | Held for incomplete enrichment |

Historical completion percentages are not recomputed current completeness. Linking does not refresh availability.

## Ferris

Exact-unique linker ran against existing Ellis county_id 2338867 and Dallas county_id 7: zero links, zero shells. No linked county evidence exists. Ellis County's [official parcel-data page](https://elliscountytx.gov/1090/Parcel-Data-from-the-Ellis-County-Apprai) points to the ECAD public portal for GIS downloads; its current client-rendered page did not expose a usable download in this run. The county portal REST search returned no public items. No blanket county assignment was made. A slow read-only cross-state property lookup was cancelled; the bounded listing/county query succeeded. Before and after remain 78 unlinked.

## Santa Maria

Existing Santa Barbara county_id 2338950 has state/county FIPS 06/083. Exact-unique linker ran first and linked zero. The county-published [Address Points dataset](https://www.arcgis.com/home/item.html?id=8f91c915bdcb46dcb9b606b4a8fce893) describes verified site/structure addresses and states a September 1, 2026 update. Queried its public FeatureServer layer 0 in batches of 15 listing addresses, rejecting errors, truncated responses, multiple matches, or ZIP/state/county mismatches. Thirty-one unique exact street-and-ZIP matches passed; all had no unitStart/unitEnd. Postal Santa Maria addresses may be labeled Orcutt by the source; county evidence remains Santa Barbara.

A guarded transaction created 31 distinct shells and linked 31 still-active/unlinked listings, checking unchanged address/ZIP and absence of an existing exact-address property. Durable raw.santaBarbaraAddressCountyEvidence includes the source URL, observation time, feature ID, match method, address, ZIP and county. Shell parcel IDs and values remain null, classification unknown, asset confidence listing_only. Source parcelIdentifier was not promoted to verified parcel identity. Listing updated_at was preserved.

After audit: 77 active, 31 linked properties/shells, 46 unlinked, 77 brokerage rows; zero verified parcel identities, owners, substantive classifications, valuations, coordinates, year-built, debt, rents or verified contacts. Latest listing timestamp remains May 7, 2026. Publication remains blocked by unresolved links and enrichment gaps.

## Modesto

Seventy-two active listings already link to Stanislaus county_id 2338928 (06/099). Exact-unique linker changed zero; re-audit confirmed zero unlinked. Audit: 52 parcel identities, 20 shells, 20 positive stored values, 72 brokerage rows; zero ownership, substantive classification, coordinates, year-built, debt, rents or verified contacts. Stored values have not been revalidated as assessor values. No mutation or publication.

## Validation and continuation

- npm run typecheck, npm run build and git diff --check passed.
- Public endpoint https://api.mxre.mundox.ai/v1/bbc/markets repeatedly returned HTTP 401; credentialAvailable=false after scripts/lib/env.ts hydrateWindowsUserEnv(). Public endpoint count and production_allowed remain UNKNOWN.
- No API config change or restart was needed; no new market was enabled. No RapidAPI used, secrets requested or secrets printed.
- Local evidence: tmp/ferris-sep22-run-*, tmp/santa-maria-sep22-* and tmp/modesto-sep22-*; durable provenance is on the 31 linked listings.
- Next ranked target: Oxford, GA (historical 72 active / 0%), then League City, TX; retain Ferris county hold, Santa Maria's 46 unresolved, Modesto enrichment hold and all earlier holds.
