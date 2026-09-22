# BBC county reconciliation — 2026-09-22 08 UTC

Recovered repository checkpoints 5e56fb2, da0532b and c0db156 newer than automation memory. Continued from fetched origin/main c0db156 in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09; preserved unrelated dirty work in C:/Users/msanc/mxre. Re-read the primary best-first CSV, used server configs only as provisional live exclusions, and retained prior holds and aliases. Public authenticated inventory remains unavailable.

| Market | Active listings | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Lovejoy, GA | 54 | 54 → 3 | 0% | 51 county-supported listing shells |
| Vestavia, AL | 82 | 82 → 82 | 0% | Both county exact passes found zero; county evidence hold |
| Sandy Springs, GA | 52 | 32 → 22 | 0% | 10 county-supported listing shells |
| Gwynn Oak, MD | 52 | 29 → 29 | 0% | Fresh audit and exact pass; residual reconciliation next |

Overall percentages are historical CSV values, not recomputed current completion. Stored active flags are not a new availability observation. No market meets zero-unlinked; no live config changes or publication claims.

## Lovejoy

No initially linked properties. Existing county metadata establishes Clayton county_id 1741142, FIPS 13/063. [Clayton County](https://www.claytoncountyga.gov/residents/about-clayton-county/) lists Lovejoy, but postal labels alone were not used to assign county. Ran scripts/link-market-listings-fast.ts with --exact-unique --ssh-psql first; zero links.

Official [Clayton Address Points](https://gis.claytoncountyga.gov/server/rest/services/Reference/AddressPoints/MapServer/0) returned 12,292 fully paginated features for ZIPs 30228/30253. Fifty-one listings uniquely matched normalized street, exact ZIP, GA state and CLAYTON county, with no units. Normalization only uppercases, removes periods/commas, collapses whitespace and maps conventional street suffixes. Source municipality is often HAMPTON; listing city LOVEJOY remains unchanged and is not asserted as verified municipal jurisdiction.

Guarded transaction created and linked 51 distinct shells. Stored full public attributes and matching limitations in raw.claytonAddressCountyEvidence. Source PIN was not promoted to legal parcel identity. Residual addresses: 11934 LOVEJOY CROSSING BLVD, 11520 KIMBERLY WAY and 2981 CLEBURNE TER. After audit: 51 shells, zero parcel identity/ownership/valuation/substantive classification/coordinates/year/debt/rents/verified contacts. Latest listing timestamp unchanged at May 7.

## Vestavia

No linked county evidence. [Shelby County official site](https://www.shelbyal.com/799/Vestavia-Hills) confirms Vestavia Hills spans Jefferson and Shelby. Existing metadata: Jefferson 1973348 (01/073), Shelby 2338841 (01/117). Exact-unique passes for both counties changed zero. No blanket county assignment or shells. Current 82 active exceeds historical CSV 52; all 82 remain unlinked.

## Sandy Springs

Existing 20 linked properties establish Fulton county_id 1741139 (13/121). Initial exact-unique pass changed zero. [Fulton Address Points](https://services1.arcgis.com/AQDHTHDrZzfsFsB5/ArcGIS/rest/services/Address_Points/FeatureServer/0), item b30f7e20983943c1ae7dbd4571bd8c12 owned by Fulton_County_GIS, returned 2,963 fully paginated house-number/ZIP candidates. Item description says Sandy Springs data comes from the city's portal/REST endpoint.

Ten unique normalized street+ZIP matches also explicitly identify City Sandy Springs, State GA, Status Active and no unit fields. Guarded transaction created and linked ten shells, recording complete source attributes and limitations in raw.fultonAddressCountyEvidence. Source ParcelID and point classification were not promoted to verified parcel identity or property asset classification. All shells have null parcel/value/coordinates, listing_signal_shell:redfin source, active_listing_shell status, unknown asset type and listing_only confidence. Listing timestamps preserved.

After: 30 linked properties, 13 parcel identities/owners, 17 shells, 20 preexisting positive stored values, zero years/coordinates/debt/rents/verified contacts. Stored values are not newly assessor-validated. Audit classified_count includes unknown asset markers and is not substantive enrichment. Hold 22 residual links and enrichment gaps.

## Gwynn Oak and validation

Fresh audit: 52 active, 29 unlinked, 23 existing Baltimore County 2338913 identities/owners/values/years; no shells/classification/coordinates/debt/rents/verified contacts. Exact-unique pass changed zero. Existing public-parcel work retained; next step is residual address and county reconciliation, not replaying prior imports. Then Liberty Township OH in CSV rank order, retaining earlier holds.

- Net 61 labeled shells and 61 links; zero new live markets.
- npm run typecheck and npm run build passed; git diff --check passed.
- Initial and final public smoke returned HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and deployed production_allowed UNKNOWN.
- No API config change or origin restart required. No RapidAPI, secret requests or secret output.
- Evidence/audits/guarded SQL retained in tmp/*-sep22-08-*; new listing provenance durable in raw fields.
