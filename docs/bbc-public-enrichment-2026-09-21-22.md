# BBC public enrichment — 2026-09-21 22 UTC

Continued from 52797cb in the existing clean publishing checkout. Ranked the primary checkout CSV by overall completion and active count (minimum 10), using configured-market exclusions as a provisional fallback because the public inventory could not be authenticated. Earlier aliases and higher-ranked county holds remain unresolved. Unrelated primary checkout changes were preserved.

| Market | Current active | Unlinked before / after | Historical CSV completion | Result |
| --- | ---: | --- | ---: | --- |
| Sacramento CA | 943 | 0 / 0 | 21.33% | Exact-unique no-op; 211 public parcel centroids added |
| Kent WA | 11 | 0 / 0 | 18.48% | Exact-unique no-op; 2319 PLAN shell still unresolved |
| West Alexandria OH | 38 | 37 / 37 | 17.86% | Read-only next-target audit; county scope unresolved |

## Sacramento enrichment

County 773106 is Sacramento, FIPS 06067. Of 212 assessor properties lacking both coordinates, the [public parcel service](https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer/0) returned 211 records in complete APN batches. Each passed unique APN, exact street address and ZIP checks and a geographic bounds check. The unmatched property was left unchanged.

A guarded SQL statement populated 211 previously null coordinate pairs only when property ID, APN, original address/ZIP, source and county were unchanged and the property still had an active Sacramento listing. It added raw.sacramentoParcelCentroid provenance to 211 listings, identifying the source, observation time, method and parcel-centroid precision. Existing coordinates and listing timestamps were preserved. These points are parcel centroids, not verified building locations. No shells, parcel links, valuations, owners or classifications were added or overwritten.

Re-audit: coordinates 560 -> 771; 772 identities and substantive classifications, 171 unknown listing-backed shells/asking-price values, no audited ownership, year-built, debt, rents or verified agent contacts. Classification audit considers substantive property_type/property_use as well as asset_type and excludes unknown shell labels. Historical CSV completion was not recomputed. Latest stored listing timestamp is unchanged at 2026-09-12T14:33:28.883641Z; this is not an availability refresh. Existing floor-plan inventory and enrichment holds remain; no live config was added.

Evidence and replay material remain in publishing tmp/sacramento-sep21-input.json, sacramento-sep21-coordinates.{mjs,json,sql}, sacramento-sep21-after.json. Source evidence includes all returned features and accepted matches. Database listing provenance durably records each applied coordinate update.

## Kent and next target

Kent county 748414 is King, FIPS 53033. Fresh evidence confirms ten residential parcel identities and one unknown shell at 2319 PLAN. Two assessor valuations plus one shell ask, two coordinates, no audited owners/debt/rents/contacts. Hold until floor-plan inventory is reconciled. Evidence: tmp/kent-sep21-{before,after,evidence}.json.

West Alexandria inventory grew to 38 active / 37 unlinked (prior checkpoint 35 / 34). The single linked parcel remains in Montgomery county 1698991. This is insufficient to assign the remaining market to that county. No speculative shells or county assignments were made. Next: property-level county reconciliation for West Alexandria, retaining Sacramento, Kent and all earlier holds. Evidence: tmp/west-alexandria-sep21-before.json.

## Validation

npm run typecheck and npm run build passed. Public smoke twice returned HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production_allowed visibility remain unknown. Local origin also returned HTTP 401 with the historical smoke credential; no current local count is claimed. API code did not change, so no origin restart was required. No markets published. No RapidAPI, secret requests or secret output. Commit scope is this report only.
