# BBC publishing and reconciliation — 2026-09-22 19 UTC

Resumed from d23a835 in the clean C:/Users/msanc/mxre-overnight-sep09 publishing checkout, using the ranked CSV from C:/Users/msanc/mxre. Preserved unrelated primary worktree edits and all earlier holds. Public exclusions remain provisional because authenticated public inventory is unavailable.

| Market | Active listings | Unlinked before → after | Historical CSV overall | Result |
| --- | ---: | ---: | ---: | --- |
| Charleston WV | 32 | 32 → 32 | 0% | Held: no linked county evidence or Kanawha metadata; one listing lacks ZIP |
| Johns Creek GA | 31 | 31 → 20 | 0% | One existing shell linked exactly; ten county-address-supported shells; held |
| Basking Ridge NJ | 31 | 31 → 0 | 0% | Live config; local production_allowed, readiness 32.7/28 |
| Feasterville Trevose PA | 31 | 31 → 31 | 0% | Bucks exact-unique zero; held for public county/parcel reconciliation |
| Vernon AZ | 30 | 30 → 30 | 0% | No linked county evidence or Apache metadata; held |
| Berkeley CA | 30 | 30 → 30 | 0% | Alameda exact-unique zero; held for public reconciliation |

Historical overall completion is the ranking CSV value, not recalculated enrichment or readiness.

## Basking Ridge

Ran scripts/link-market-listings-fast.ts with --exact-unique --ssh-psql for Somerset county_id 13629 first: 23 unique address/ZIP links to existing NJGIN parcels. Re-audit confirmed all 23 assigned to Somerset. Created eight clearly labeled shells in a guarded transaction, retaining full unit text, null identities/values, unknown/listing_only classification and unchanged listing timestamps. Durable raw.listingCityCountyEvidence discloses that county assignment follows the listing-city cohort and 23 linked parcels, supported by the [Bernards assessor page](https://bernards.org/departments/assessor), not property-level verification. One source ZIP is 08558 rather than predominant 07920; listing-city geography remains unverified and is disclosed.

Final substantive field audit: 31 linked properties, 23 identities/assessed values/coordinates/years/classifications (22 single_family, one vacant_land), eight shells, no ownership or verified contacts, 31 brokerage rows. Two properties have historical mortgages, only one positive amount, latest recording 2023-07-27. All 23 rent snapshots have estimated_v2_nj, low-confidence provenance; inspected method is value_fmr_blend, not actual leases. No positive creative-finance evidence. Original observations were May 7; exact linking advanced timestamps without refreshing availability.

Used tmp/audit-classified-run.ts to count substantive classifications and exclude generic shell types. The basic audit counts eight shell types while missing the 23 property_type classifications, so it must not supply this market's classification metric. Readiness target/formula unchanged. Live config includes honest fallback metrics and explicit scope, stale availability, shell, assessment, rent and debt limitations. [Display coordinates](https://mapcarta.com/Basking_Ridge) are not property enrichment.

## Johns Creek and holds

Fulton county_id 1741139: exact first linked one existing shell, not an assessor parcel. [Official city information](https://johnscreekga.gov/community/about/fulton-county/) confirms Fulton. Queried the county [address service](https://services1.arcgis.com/AQDHTHDrZzfsFsB5/ArcGIS/rest/services/Address_Points/FeatureServer/0), fully paginating 4,902 candidates. Eleven unique normalized street/ZIP matches; ten passed Active, Johns Creek, GA and blank unit/range guards. Created ten shells with raw.fultonAddressCountyEvidence, full source attributes, null identities/values and unknown classification. Source ParcelID was not promoted to legal identity. Final 11 shells, one pre-existing stored valuation, zero substantive classifications/owners/coordinates/years/debt/rents/contacts, 20 residuals. Held.

Feasterville Trevose: exact-unique Bucks 1498719 zero. Berkeley: exact-unique Alameda 773104 zero. Neither has linked county evidence, so no shells or configs. Charleston: no Kanawha metadata or linked county; public [WV assessment viewer](https://mapwv.gov/assessment/Default) identified for later reconciliation. Vernon: [Apache community plan](https://www.apachecountyaz.gov/Community-Development?offset=0) identified, but Apache county metadata absent; did not substitute neighboring Navajo. No mutations to these held markets.

## Verification and continuation

- npm run typecheck, npm run build and git diff --check passed.
- Restarted only the verified node API origin on port 3101 using the existing hidden detached helper and disposable local credential.
- Local smoke HTTP 200: 637 markets, basking-ridge-nj production_allowed, readiness 32.7/28.
- Public smoke twice HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production_allowed remain UNKNOWN.
- No RapidAPI, secret requests or secret output. Protected helpers hydrate Windows user environment before access.
- Net 42 links and 18 new shells. Evidence and guarded replay SQL retained in tmp/*-sep22-19-*; durable provenance on listing rows.
- Only src/api/server.ts and this report are intentional commit files. Next ranked target: Scottdale GA (30 historical active / 0% historical completion), then Huntersville NC. Preserve all holds above and earlier reports.
