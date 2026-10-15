# BBC market publication: Ravenna, Ohio

## Outcome

- Added Ravenna, OH as a live BBC market scoped to Portage County (`county_id` 1741134).
- The active inventory audit found 48 active listings and 48 linked properties; unlinked active listings were `0 -> 0`, so no new listing-backed shells were created.
- The exact-link script was invoked first with the supported county, but its HTTP database bridge was unavailable in this runtime before the no-op exact pass could complete. The SSH-backed audit verified the same zero-unlinked state without mutating listing rows.

## Current coverage

| Field | Count |
| --- | ---: |
| Active listings / properties | 48 / 48 |
| Assessor-backed parcel identities / owners | 1 / 1 |
| Listing-backed shells | 47 |
| Stored listing-derived values / classifications | 48 / 48 |
| Year built / coordinates | 0 / 0 |
| Agent contacts / mortgage records / rent snapshots | 0 / 0 / 0 |

The listing-backed shell records are explicitly disclosed in the market restrictions. Their stored values are listing asking prices rather than assessor valuations; absent enrichment is not inferred.
