# BBC market linking audit — 2026-09-22 21 UTC

Continued ranked market processing from commit 91ca348, preserving prior holds and unrelated primary checkout changes. Read the ranked CSV in C:/Users/msanc/mxre and used the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09. Fetched origin/main; no newer commit was present. Public live exclusions remain provisional because the hydrated public probe returned HTTP 401 without an available API key.

| Market | Active listings | Unlinked before -> after | Historical CSV completion | Result |
| --- | ---: | ---: | ---: | --- |
| Ottawa Hills OH | 58 | 58 -> 1 | 0% | Held: ambiguous parcel identity |
| Southampton PA | 29 | 29 -> 29 | 0% | Held: exact pass found no linked-property county evidence |
| Blue Ash OH | 83 | 83 -> 2 | 0% | Held: two ambiguous parcel identities |
| Forest Hill TX | 28 | 28 -> 28 | 0% | Held: exact pass found no linked-property county evidence |
| Hapeville GA | 27 | 27 -> 27 | 0% | Held: exact pass found no linked-property county evidence |
| Towson MD | 27 | 27 -> 27 | 0% | Held: exact pass found no linked-property county evidence |

Historical completion is the ranking snapshot, not a newly recomputed completion percentage. Ottawa Hills and Blue Ash now have more active inventory than their historical CSV rows. All exact passes used scripts/link-market-listings-fast.ts with --exact-unique and SSH transport, before shell creation.

## Ottawa Hills

Exact linking connected 43 listings to existing lucas-oh-tax-parcels-arcgis properties. Their county_id 2338836 was verified as Lucas OH in county metadata. A guarded transaction created 14 clearly labeled listing-backed shells for remaining address/ZIP records without existing exact-address conflicts. Shells retain unit text, null parcel identity and valuation, unknown classification and listing_only confidence. Durable listingCityCountyEvidence records the 43-parcel county cohort and property-level limitations; shell linking preserves listing timestamps.

3905 HILLANDALE RD (listing 1197400, ZIP 43606) remains unlinked: property IDs 457546276 and 457610575 carry distinct parcel IDs 8820358 and 8820357. No arbitrary choice or duplicate shell was made.

Final audit: 57 properties, including 43 parcel identities/owners/valuations/classifications, 42 years built, 14 shells, no coordinates or mortgages. Forty-three existing rent snapshots use estimated low/medium-confidence methods (value_fmr_blend, msa_model and msa_model_blended), not observed leases. No verified contacts or positive creative evidence. Pre-link latest listing timestamp was September 21; exact linking advances updated_at and does not refresh availability.

## Blue Ash

Exact linking connected 59 listings to existing hamilton-oh-cagis properties, all county_id 1698987 verified as Hamilton OH. A guarded transaction created 22 labeled shells with preserved address/unit text and timestamps, no inferred enrichment, and durable listingCityCountyEvidence referencing the 59-parcel cohort.

Two listings remain unlinked: 4824 FAIRVIEW AVE (2080485) matches parcel IDs 061200400056 and 061200400550; 4932 PROSPECT AVE (4114645) matches 061200400033 and 061200400547. Both were excluded from shell creation.

Final audit: 81 properties, 59 parcel identities/owners/substantive classifications, 58 valuations, 48 coordinates, 22 shells, no years built or rents. One historical mortgage record dated 2025-10-07 has no positive amount and does not establish current debt balance. No verified contacts, brokerage or positive creative evidence. The audit's multifamily counter does not count property_type-only classifications; do not interpret zero as absence of multifamily properties. Pre-link latest listing timestamp was September 21; exact linking is not an availability refresh.

## Remaining markets and verification

Southampton exact pass used Bucks 1498719; Forest Hill Tarrant 8; Hapeville Fulton 1741139; Towson Baltimore 2338913. These are search scopes only: no linked-property county evidence resulted, so no shells or config promotions were made for them. Public parcel/county reconciliation remains needed.

Net result: 138 links (102 existing parcel links plus 36 labeled shells). No live config was added because every candidate retained unlinked inventory. Readiness thresholds and src/api/server.ts were unchanged.

npm run typecheck, npm run build and git diff --check passed. Existing local origin on port 3101 responded HTTP 200 with 638 markets; no restart was needed because server source was unchanged. Public smoke at https://api.mxre.mundox.ai/v1/bbc/markets returned HTTP 401, credentialAvailable=false, before and after work. Public endpoint count and production visibility remain UNKNOWN; no production publication is claimed.

Protected helpers called hydrateWindowsUserEnv before access. No RapidAPI, secrets requested or secrets printed. Detailed audit, exact-link and transaction evidence is retained in tmp/*-sep22-21-*; shell county provenance is durable in the database. Only this report is committed. Next ranked target: Yardley PA, then River Oaks TX. Preserve all existing holds, including the new Ottawa Hills and Blue Ash ambiguities.
