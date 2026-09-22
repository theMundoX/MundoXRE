# BBC market publishing — 2026-09-22 22 UTC

Resumed from f4b8dff after reading automation memory and recovering newer committed reports. Ranked CSV read from C:/Users/msanc/mxre; intentional publication changes made in the existing clean C:/Users/msanc/mxre-overnight-sep09 checkout. Earlier holds and unrelated dirty primary-checkout changes preserved. Origin/main was fetched before work.

| Market | Active listings | Unlinked before -> after | Historical overall completion | Result |
| --- | ---: | ---: | ---: | --- |
| Yardley PA | 27 | 27 -> 27 | 0% | Held: no exact matches or linked-property county evidence |
| River Oaks TX | 27 | 27 -> 0 | 0% | Live config; local production_allowed, readiness 39.7/28 |
| Westlake TX | 27 | 27 -> 11 | 0% | Held: remaining county/parcel reconciliation |
| East Point GA | 26 | 26 -> 26 | 0% | Held: no exact matches or linked-property county evidence |

Completion percentages are the historical ranking snapshot, not newly calculated enrichment percentages. Public inventory exclusions remain provisional: authenticated public inventory was unavailable.

## River Oaks

Ran scripts/link-market-listings-fast.ts with --exact-unique --ssh-psql against Tarrant county_id 8 first: zero links. Fresh paginated public queries to https://mapit.tarrantcounty.com/arcgis/rest/services/Dynamic/TADParcels/FeatureServer/0 returned 25 unique exact situs-address/city/state, single-account matches with nonconfidential flag and no addendum. Guarded transactions imported 25 county properties and linked them, retaining full source attributes in raw.tarrantParcelEvidence. Existing parcel IDs and exact address conflicts were checked before insert. County metadata confirms Tarrant TX, FIPS 48439.

Two remaining listings, 1310 YALE ST and 1611 NANCY, had address/ZIP and no existing exact-address property conflicts. Created two clearly labeled listing-backed shells, null parcel identity/valuation, unknown classification, listing_only confidence. Their county assignment is supported by the 25 linked county parcels; property-level reconciliation remains pending and is recorded in raw.listingCityCountyEvidence. No source unit text or listing timestamps changed.

Final audit: 27 active properties, 25 parcel identities, owners and APPRAISEDV appraisal values, 24 years built, 27 brokerage rows, two shells. No substantive classification, coordinates, debt, rents, verified agent contacts or positive creative-finance evidence. Source situs ZIPs were blank; ZIP is listing-derived. Values are appraisal values, not market prices; source vintage remains in provenance. Latest stored listing timestamp remains May 7, 2026; current availability was not refreshed.

Added only River Oaks to MARKET_CONFIGS with field-level fallback metrics and explicit restrictions. Approximate display coordinates from https://www.tshaonline.org/handbook/entries/river-oaks-tx are not property coordinates.

## Westlake and holds

Westlake exact linking against Tarrant 8 and Denton 10 each found zero matches. Fresh Tarrant public query yielded 16 unique exact situs-address/city/state matches under the same guards; imported and linked all 16. They have county identities, owners and appraisal values, with 12 years built; no other enrichment. Preserved timestamps and durable raw.tarrantParcelEvidence. Eleven residuals remain; three have multiple source candidates (1730 J T OTTINGER RD, 1815 QUAIL HOLLOW DR, 2203 KING FISHER DR). No shells were created because remaining county/parcel assignments require individual reconciliation across the search scopes. No live config added.

Yardley exact search in Bucks 1498719 and East Point exact search in Fulton 1741139 found no matches. Search scope alone is not property-level county evidence; both remain held without shells or config changes. Re-audits completed for all four markets.

## Validation and continuation

npm run typecheck and npm run build passed. Restarted the verified Node API origin on port 3101 with the existing hidden detached helper and disposable local credential. Initial immediate probe raced startup; subsequent probe returned HTTP 200, 639 markets and River Oaks production_allowed at readiness 39.7/28. Public probes before and after changes returned HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production visibility remain UNKNOWN; no verified public publication is claimed.

No RapidAPI, secrets requested, or secrets printed. Detailed evidence is retained in tmp/*-sep22-22-*; imported provenance is durable in the database. Only src/api/server.ts and this report are intended for commit. Next ranked target: Gothenburg NE, then Bellbrook OH and Peebles OH. Retain all prior holds.
