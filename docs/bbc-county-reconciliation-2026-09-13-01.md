# BBC county reconciliation — 2026-09-13 01 UTC

Continued ranked public-source enrichment from the prior run in the clean detached publishing checkout. Fetched origin/main at 3b99106. Original ranked CSV and current server configs used for provisional exclusions; earlier alias skips and county holds retained. The unrelated primary worktree remains untouched.

| Market | Active listings | Unlinked before / exact / final | Historical CSV completion | Result |
| --- | ---: | --- | ---: | --- |
| Springfield OH | 464 | 196 / 196 / 57 | 32.91% | 302 official parcels inserted and linked; 292 owners supported by matching CAMA situs |
| Timnath CO | 101 | 0 / 0 / 0 | 30.69% | 85 existing parcel IDs revalidated; 2 shells replaced with official parcel links |
| Berthoud CO | 134 | 0 / 0 / 0 | 29.60% | 104 existing parcel IDs revalidated; eight candidate imports blocked by existing APNs with different addresses |

Historical CSV completion is not a fresh recomputation. Springfield inventory has increased from 199 CSV listings to 464 current active rows. Active means the stored listing flag, not newly verified availability.

## Springfield

Linked-property county metadata is Clark, county_id 2338868, FIPS 39023. The [county auditor GIS page](https://clarkcountyauditor.org/Posts) links the official GeoHub, whose organization catalog identifies the county parcel service. The old Parcels_View returned no records; the active [BHA parcel layer](https://ago.clarkcountyohio.gov/ccoarcgis/rest/services/BHA/AGS_Clark_Parcels/MapServer/39) returned 323 records from complete exact-street batches. Of these, 302 had unique exact situs street/city/ZIP, unique listing and parcel IDs, and no unit or plan labels. A second countywide PIN query returned 302 unique unchanged parcels. Existing normalized primary APNs and non-shell exact addresses were absent.

The [Pivot CAMA layer](https://ago.clarkcountyohio.gov/ccoarcgis/rest/services/Pivot/Parcel_CAMA_State_Plane/MapServer/14) returned one record for each PIN. Promoted 292 primary owner names only where its full situs address also matched street, city, state and ZIP. No value, coded use, coordinate or area units were inferred. The guarded transaction inserted/linked exactly 302: 139 previously unlinked listings and 163 previous shells. It required unchanged active listing links/addresses and checked old shells for mortgage/rent relationships. No shell or existing parcel was deleted or overwritten.

Final: 407 linked properties, 302 identities, 292 owners, 105 remaining shells/asking-price valuations; other audited enrichment zero. Original latest listing timestamp remains 2026-09-12T13:29:34.073Z. Listing raw.clarkOhioParcelReconciliation retains full source records, URLs, method, county FIPS, old property ID and original listing timestamp. HOLD: 57 unlinked and 105 unverified shells, including prior ZIP-scope concern. No new shells created based solely on ZIP.

## Timnath

Linked-property county is Larimer, county_id 75651, FIPS 08069. [Official town mill levy resolution](https://timnath.civicweb.net/document/114023/) addresses both Larimer and Weld counties. The [Larimer tax parcel service](https://maps1.larimer.org/arcgis/rest/services/MapServices/Parcels/MapServer/3) returned 87 unique exact situs street/city/ZIP matches. All 85 existing parcel IDs matched current records. Two shell candidates passed a second countywide parcel query and primary/alternate APN absence checks; guarded transaction inserted/linked exactly two parcels and primary owner names.

Final: 87 identities/owners, 14 shells/asking-price valuations, 85 existing classifications, 23 existing coordinates, 101 brokerages; year built/debt/rents/verified contacts/positive creative evidence zero. Existing classifications/coordinates were not newly verified. Original latest listing timestamp remains 2026-08-06T19:36:55.342536Z. raw.larimerParcelReconciliation contains full provenance and prior IDs/timestamps. HOLD: 14 remaining shells include floor-plan labels, units and unmatched streets; county scope not resolved for all inventory.

## Berthoud and next work

Fresh audit: 134 active properties in Larimer 75651, 104 identities/owners/classifications, 30 shells/asking values, 27 coordinates, 134 brokerages, other audited enrichment zero. Exact linking was a no-op. Official Larimer exact-street batches returned 112 unique street/city/ZIP matches, including all 104 existing parcel IDs. The eight shell candidates passed countywide parcel uniqueness but every APN already exists on an assessor property with a different address (e.g. official 179 BUCKWHEAT LN versus existing 413 BUCKWHEAT LN). The preparation guard rejected them before any SQL mutation. Preserve existing records and reconcile address history/attached enrichment before relinking; do not create duplicate parcels or overwrite addresses. Next target is this Berthoud APN/address conflict, then Wayne PA (historical 23.15%, 36 CSV active), retaining Springfield/Timnath and all prior holds.

## Validation and publication

- npm run typecheck and npm run build passed. No server changes or origin restart needed.
- Local origin port 3101 returned HTTP 200 and 632 markets.
- Public endpoint before and after returned HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv and existing BBC key fallbacks. Public market count and deployed production_allowed remain UNKNOWN.
- No markets published: unresolved listing/county scope prevents an honest readiness declaration. No readiness formula changes, RapidAPI, secret requests or secret output.
- Intentional commit scope: this report only. Evidence and guarded SQL retained in untracked tmp/clark-01-*, springfield-01-*, larimer-01-*, timnath-01-*, berthoud-01-* and berthoud-larimer-01-*.
