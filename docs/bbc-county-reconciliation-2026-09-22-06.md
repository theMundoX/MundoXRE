# BBC county reconciliation — 2026-09-22 06 UTC

Read automation memory and fetched origin/main at 5e56fb2 in the existing isolated publishing checkout C:/Users/msanc/mxre-overnight-sep09. Preserved all unrelated changes in C:/Users/msanc/mxre. Continued the primary best-first CSV, using current server configs for provisional live exclusions because authenticated deployed inventory remains unavailable. Retained earlier holds and aliases.

| Market | Current active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Placentia, CA | 68 | 0 → 0 | 0% | Enrichment hold |
| Santa Ana, CA | 68 | 0 → 0 | 0% | Enrichment hold |
| Murphys, CA | 66 | 22 → 1 | 0% | 21 labeled shells; county/enrichment hold |
| Westport, CT | 65 | 65 → 65 | 0% | County metadata/public parcel work pending |
| New Carlisle, OH | 159 | 85 → 53 | 0% | 32 county-supported shells; residual link hold |

Historical CSV completion is not a recomputed current metric. New Carlisle had 58 active rows in that report. Database listing activity flags are not a fresh availability verification.

## Placentia and Santa Ana

Linked properties establish Orange county_id 2338869 (06/059). Exact-unique linker passes changed zero, followed by fresh audits. Placentia has 16 identities, 52 shells/stored values, 13 years built; Santa Ana has 19 identities, 49 shells/stored values, 14 years built. Both lack ownership, substantive classification, coordinates, verified contacts, debt and rents. Even counting stored shell values, each scores 14.3 on the seven-field readiness formula, below 28. No shell or config changes. The public Orange GIS layer was reachable and exposes only assessment number, situs, year built and bedroom count besides geometry; no new ownership or valuation evidence was obtained.

## Murphys

Existing 44 linked parcels establish Calaveras county_id 2338931 (06/009). Exact-unique first pass changed zero. Twenty-one residual listing addresses have ZIP 95247, identified as entirely Calaveras by https://www.zipdatamaps.com/95247 and corroborated by https://www.unitedstateszipcodes.org/95247/ . Guarded transaction inserted and linked 21 active_listing_shell records, source listing_signal_shell:redfin, quality listing_only, unknown classification, null parcel identity and no inferred valuation/coordinates. raw.calaverasZipCountyEvidence records sources, observation time, county-only method and limitations. Preserved zero-number, abbreviated, space and possible duplicate labels as distinct listing shells; these are not verified legal parcels. Listing timestamps were preserved.

One listing remains unlinked: 1445 GALLOWS, ZIP 95223. That ZIP crosses Calaveras/Alpine boundaries (https://www.zipdatamaps.com/95223), so no county was inferred. Final 66 active, 65 linked, 21 shells, 44 existing identities/assessed values; zero ownership/classification/coordinates/years/contacts/debt/rents. Existing imported Calaveras assessments identify tax year 2023 in retained source evidence, not current valuations. Last stored listing timestamp remains September 11. Publication held for unresolved county and readiness.

## Westport

Fresh audit: 65 active, all unlinked, no linked county; query of counties for CT returned no rows. No guessed county ID, exact pass, shell or config. The town's official GIS page returned HTTP 403 to the read tool. Its public assessor vendor https://gis.vgsi.com/westportct/ is available and states October 1, 2025 assessment vintage. Individual parcel matching and compatible county metadata remain pending; no assessment data was imported.

## New Carlisle

Existing 74 parcels establish Clark county_id 2338868 (39/023). Exact-unique first pass changed zero. Official Ohio address service https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0 returned 875 deduplicated features from bounded house-number/ZIP queries, rejecting truncated/error responses. Thirty-two unlinked listings uniquely match exact LSN street and ZIP: 21 Clark, 10 Miami and 1 Champaign. County metadata verifies Miami2338898 (39/109) and Champaign2338930 (39/021); exact-unique passes for both also changed zero before shell creation.

Guarded transaction inserted/linked 32 distinctly labeled listing shells with null parcel IDs, no values or coordinates, and raw.ohioAddressCountyEvidence containing source, feature ID, observation time, exact address/ZIP and county. Unchanged active/unlinked listing and no-existing-property guards passed; listing timestamps preserved. After audit: 159 active, 106 linked, 53 unlinked; 74 existing identities/owners/values, 68 years built, 32 shells, 33 brokerage rows; no classification/coordinates/contacts/debt/rents. Last stored listing timestamp remains September 21. Publication held despite readiness among linked records: zero-unlinked gate fails.

## Validation and continuation

- Net 53 new labeled shell links; zero live config additions. API code unchanged; no origin restart needed.
- npm run typecheck, npm run build and git diff --check passed.
- Initial/final public probe: HTTP 401, credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and deployed production_allowed are UNKNOWN. Local port 3101 also returned HTTP 401 with the prior smoke credential; local count UNKNOWN.
- Protected scripts hydrate scripts/lib/env.ts before access. No RapidAPI, requested secrets or printed secrets.
- Evidence/SQL retained in tmp/*-sep22-06-*; database raw evidence is durable. Only this checkpoint is intended for commit.
- Next target: Marysville, OH (historical 57 active/0%); fresh before audit retained, exact linking and county reconciliation pending. Preserve Murphys one residual, New Carlisle 53 residuals, Westport hold and all earlier holds.
