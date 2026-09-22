# BBC county reconciliation — 2026-09-22 14 UTC

Recovered the checkpoint through 7a295c6, newer than automation memory. Fetched origin/main and continued in the existing clean publishing checkout C:/Users/msanc/mxre-overnight-sep09, using the ranked CSV from C:/Users/msanc/mxre. Preserved the dirty primary checkout and earlier holds/aliases. Config exclusions remain provisional because public authentication is unavailable.

| Market | Active | Unlinked before → after | Historical overall | Result |
| --- | ---: | ---: | ---: | --- |
| Mimbres NM | 37 | 37 → 37 | 0% | Held: no linked-property county evidence; Grant NM county metadata absent in query |
| Richmond VA | 37 | 37 → 37 | 0% | Henrico exact-unique pass zero; county scope unresolved |
| Seeley Lake MT | 36 | 36 → 36 | 0% | Held: no linked-property county evidence; Missoula metadata absent in query |
| Cuyahoga Falls OH | 36 | 36 → 11 | 0% | 25 county-supported shells and links |
| Fairfield Township OH | 62 | 62 → 18 | 0% | One existing parcel linked, then 43 county-supported shells |

Overall percentages are historical ranked-CSV values, not recomputed completion. Fairfield Township had 36 active rows in that CSV but 62 in the fresh audit. No market meets zero-unlinked; no live config or readiness target changed.

## Evidence and mutations

Cuyahoga Falls had no linked county initially. Queried the [Ohio address service](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) by ZIP and house number, receiving 203 unique features without truncation. Twenty-five residual listings each matched exactly one normalized LSN and ZIP with OH state, Summit county and blank source unit. Existing county metadata identifies Summit as 1698989, FIPS 39153. Exact-unique linking ran first and linked zero existing properties.

A guarded transaction created 25 listing_signal_shell:redfin properties and links. It required unchanged active/unlinked listing address and ZIP and no existing property at that address/ZIP. Shells have null parcel IDs, unknown asset type, listing_only confidence and active_listing_shell status. Full source attributes, retrieval time, matching method and limitations persist in listing raw.ohioAddressCountyEvidence. No owner, value, physical detail, coordinates, parcel identity or refreshed availability inferred. All listing timestamps were preserved. Fresh audit: 25 shells, 11 unlinked, zero audited substantive enrichment, debt, rents or contacts.

Fairfield Township used the same source and safeguards. Received 325 unique features without truncation. Forty-four initial unique address/ZIP matches had OH state, Butler county and blank source unit. Exact-unique linking with Butler 1741128 first linked one existing parcel. Re-read current listings and queried the source again, leaving 43 eligible unique residual matches. A guarded transaction created and linked all 43 labeled shells, preserving listing timestamps and durable source evidence. Fresh audit: 44 linked properties, 18 unlinked, 43 shells, one existing parcel with identity/owner/stored value/substantive classification/coordinates. That parcel has six mortgage records but no positive mortgage amount; this does not establish usable debt coverage. No rents, verified contacts or year-built coverage. The exact linker timestamp change is not refreshed listing availability.

Mimbres, Richmond and Seeley Lake have no linked-property county evidence. The existing county query found Henrico 2338947 but no Grant NM or Missoula MT record. Richmond exact linking in Henrico yielded zero. No speculative county creation or blanket assignment was performed. Mimbres/Seeley exact passes remain pending county resolution. Public web searches returned no results in this run, so no geographic claims are treated as newly verified.

## Validation and next target

- Typecheck and build completed successfully; staged diff whitespace check passed.
- Public smoke after hydrateWindowsUserEnv returned HTTP 401 with credentialAvailable=false. Public endpoint count and production_allowed remain UNKNOWN; zero new live markets.
- API configuration unchanged, so no origin restart needed.
- No RapidAPI; no secrets requested or printed. Protected helpers hydrate Windows user environment before access.
- Net 69 links: 68 new labeled shells and one existing parcel. Evidence and before/after audits remain in tmp/*-sep22-14-* and mutation provenance persists in the database.
- Next ranked target: Langhorne PA, then Windcrest TX. Fresh Langhorne audit only: 36 active, all 36 unlinked, no linked-property county evidence; exact/county reconciliation pending. Preserve all higher-ranked holds.
