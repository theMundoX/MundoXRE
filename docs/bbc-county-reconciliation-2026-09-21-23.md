# BBC county reconciliation — 2026-09-21 23 UTC

Continued from 83a2013 using the primary checkout ranking CSV and the existing clean publishing checkout. Ranked by historical overall completion then active count, minimum 10. Public inventory returned HTTP 401 after environment hydration, so configured-market exclusions remain provisional. Retained higher-ranked aliases and county/enrichment holds. Unrelated primary checkout changes were preserved.

| Market | Current active | Unlinked before / after | Historical CSV overall completion | Result |
| --- | ---: | --- | ---: | --- |
| West Alexandria OH | 38 | 37 / 7 | 17.86% | 30 listings linked to 27 new county-supported shells |
| MC DONALD PA | 37 | 36 / 36 | 17.12% | Exact-unique no-op; county hold retained |
| Warminster PA | 40 | 39 / 39 | 17.08% | Read-only audit; suspect Sullivan association still held |
| Franklin OH | 284 | 283 / 283 | 16.88% | Read-only next-target audit; county scope unresolved |

## West Alexandria

Ran scripts/link-market-listings-fast.ts with exact-unique and SSH transport for Montgomery county 1698991 and Preble county 2338934. Both linked zero. A statewide exact-address/ZIP database query found no existing candidates for the 37 unlinked listings.

The [Ohio public address layer](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) returned complete address-point results for ZIP 45381 and the candidate house numbers. Unique exact street/ZIP matches supported 21 Preble and one Montgomery listing. Created 22 listing-backed shells using county evidence only. The [parcel layer](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/1) had no nonnull Preble situs addresses, so it was not used to assert parcel identity.

A second pass normalized SECOND/2ND, THIRD/3RD, US route spelling, and house-letter spacing, retaining directions and unit letters. Eight listings matched unique address points in Preble. Reused the existing 40 E SECOND ST shell for 40 E 2ND ST. Three 2984 E US route variants share one new shell. Created five additional shells and linked eight listings. No duplicate shell was created for those route variants.

Both writes were guarded transactions with expected row counts and unchanged listing identity, active status, address and ZIP checks. Existing links were preserved. Listing timestamps were preserved; this was not an availability refresh. Each affected listing has raw.ohioAddressCountyEvidence with source URL, observation time, feature ID, situs address, county, method and explicit county-only/shell quality. Shells have null parcel identity and valuation, listing_backed_property type, active_listing_shell status, unknown asset type and listing_only confidence. No ownership, building coordinates, assessor valuation or substantive classification was inferred from address points or asking prices.

Final audit: 38 active listings, 31 linked, 7 unlinked, 28 distinct linked properties (27 shells plus one preexisting parcel). Linked county counts: 29 Preble, 2 Montgomery. One parcel identity, owner and substantive classification; zero audited valuations, coordinates, year-built fields, debt, rents or verified agent contacts. Latest stored listing timestamp remains 2026-09-21T13:40:08.217Z. Historical overall completion was not recomputed.

Remaining unlinked addresses: 999 N 503 STATE; 109 W SECOND ST; 109 W 2ND ST; 109 2ND; 00 ENGLE RD; 4515 STATE ROUTE 35; 0 STATE RT 35. The 109 W SECOND ST source contains multiple address points; route/address omissions and vacant-lot locations need further evidence. No blanket Preble assignment was made. Publication remains held because seven listings are unlinked and enrichment is incomplete.

Evidence/replay files in publishing tmp/: west-alexandria-current.json, west-alexandria-exact-{montgomery,preble}.json, west-alexandria-public-addresses.json, west-alexandria-county-matches.json, west-alexandria-shells.{mjs,sql}, west-alexandria-shells-result.json, west-alexandria-normalized{,-plan}.json, west-alexandria-normalized-apply.sql, west-alexandria-normalized-result.json, west-alexandria-final.json. County evidence is also durably stored on the 30 affected database listings.

## Next markets and validation

MC DONALD's sole linked property is in Allegheny county 1973350; exact linking there found no additional match. Warminster's sole linked property remains associated with Sullivan county 2228992; no speculative shell or county mutation. Franklin has grown to 284 active listings; its sole linked property is in Montgomery county 1698991, insufficient to assign all other 283 listings. Next: Franklin property-level county reconciliation, preserving West Alexandria's seven unresolved records and all prior holds.

npm run typecheck and npm run build passed. Public smoke twice returned HTTP 401, credentialAvailable=false, after hydrateWindowsUserEnv. Public endpoint count and production_allowed status remain unknown. No market config changed, no new market was published, and no API restart was required. No RapidAPI, secret requests or secret output. Intentional commit scope: this report only.
