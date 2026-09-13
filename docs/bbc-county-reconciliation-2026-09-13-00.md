# BBC county reconciliation — September 13, 2026, 00 UTC

Continued in the clean detached publishing checkout from origin/main ec8f686. Ranked the original best-first CSV against current configs as provisional exclusions; retained prior aliases and county holds. The authenticated public exclusion check remains unavailable: hydrated public probe returned HTTP 401 with credentialAvailable=false. Preserved the unrelated dirty primary checkout.

## Baltimore MD

Historical CSV overall completion **33.32%**; **2,362 active listings**. Unlinked **0 before -> 0 exact -> 0 final**. Exact-unique linker used the existing SSH transport and made no changes. No new shells required.

The [official Baltimore City parcel service](https://baltegis.baltimorecity.gov/mapping/rest/services/CityView/RealProperty_OB/MapServer/0) returned 1,712 parcel records from complete exact-street batches. Selected **1,694 unique exact FULLADDR and situs-ZIP matches**, excluding units, fractions, address spans, duplicate listings/addresses and duplicate PINs. A second citywide PIN query confirmed every selected record was unique and unchanged. Neither primary/alternate parcel identifiers nor other non-shell properties at the same addresses existed in the city inventory.

An atomic transaction locked the relevant tables, checked all expected original listing/property/source/county/address/ZIP values, inserted **1,694 public parcels** in Baltimore City county_id **1973418** (FIPS 24510), and relinked **1,694 listings**. Imported identity, official owner, positive TAXBASE as taxable_value (1,684 records), and valid year-built values (1,610). Tax year remains unspecified. Inconsistent FULLCASH/current component values, coded use, mailing, area/lot units, ground rent and sale fields were not promoted. Full public attributes, source, observation time, matching method, prior IDs and original timestamps are in raw.baltimoreCityParcelReconciliation. **Listing updated_at was preserved**, not refreshed.

The [official Baltimore County parcel service](https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1) returned 101 exact-street records for residual listings. The initial long GET exceeded the server's URL handling; complete 20-address batches succeeded. Selected **57 unique exact street, situs city BALTIMORE, and ZIP matches**, excluding units. A second countywide TAXPIN query confirmed each unique unchanged record; primary/alternate identifier and same-address database checks were empty. Other situs cities and ambiguous/unmatched addresses remain pending.

A second atomic guarded transaction inserted and linked **57 public parcels** in Baltimore County **2338913** (FIPS 24005), correcting their former city assignment. Imported identity, owners and 51 valid year-built values. County TOTAL_VALUE semantics/year were not inferred, and asking prices were not copied into assessments. Full evidence and previous county/property/timestamps are in raw.baltimoreCountyParcelReconciliation. Listing updated_at was preserved. No existing properties were overwritten or deleted.

Final audit: **1,751 parcel identities/owners**, **1,661 year-built values**, **2,295 valuations** (1,684 city tax bases plus 611 remaining shell asking prices), **611 shells**, and 2,362 brokerages. No coordinates, substantive classifications, verified contacts, debt, rents or positive creative evidence. City assignment now covers 2,305 listings, including 611 unverified shells; county covers 57 verified parcels. Latest original listing timestamp remains May 25, 2026. **Publication held for residual county/parcel scope reconciliation.** No historical completion percentage was recomputed.

## Austin TX

Next distinct ranked candidate: historical CSV **33.30% / 599 listings**, but fresh audit has **5,245 active listings**. Unlinked **0 -> 0 exact -> 0 re-audit**; no shell creation needed. All linked records have active_listing_shell status and lack parcel_id, assigned Travis county_id **1973351** / FIPS 48453. Of these, 2,321 already have travis-county-arcgis-parcels source and 2,924 retain listing_signal_shell:redfin source. Existing property enrichment, mortgage and rent relationships require preservation during parcel reconciliation.

Stored coverage: 4,499 owners, 5,184 valuations, 4,493 classifications, 5,244 coordinates, 3,904 year-built values; 3,100 properties with 7,160 mortgage records; 5,244 rent snapshots; 4,076 brokerages and 22 positive creative signals. These are audit counts, not newly verified facts. The [official Travis parcel endpoint](https://taxmaps.traviscountytx.gov/arcgis/rest/services/Parcels/FeatureServer/0) responded HTTP 200 and exposes PROP_ID, geo_id, situs address/ZIP and explicit valuation fields. No public parcel matches or property mutations were attempted for Austin. Hold publication pending exact APN and Travis/adjacent-county reconciliation; do not discard attached enrichment by naively replacing shell IDs.

## Springfield OH

Next ranked after Austin: historical CSV **32.91% / 199 listings**. Fresh audit shows **464 active listings**, 268 linked shells and **196 unlinked** with address/ZIP. Exact-unique linker matched zero; unlinked **196 -> 196**, confirmed by re-audit. Assigned linked-property county is Clark **2338868** / FIPS 39023. Unlinked ZIPs are 45502 (57), 45503 (84), 45504 (55); existing links include one 43078 ZIP, requiring geographic review. Only existing shell asking valuations are present; other audited enrichment is absent. County/address reconciliation precedes shell creation; no new shells/configs were created for this unresolved scope.

## Validation and continuation

Net **1,751 public parcels imported and listings reconciled**, including **57 county corrections**. No new shells or live configurations; readiness formula unchanged. npm run typecheck and npm run build passed. Local endpoint HTTP 200, **632 markets**; no restart needed because API code did not change. Final public probe again returned HTTP 401 with no hydrated API credential: **public count and deployed production_allowed remain unknown**. hydrateWindowsUserEnv was called before protected accesses. No RapidAPI, secrets requested or secrets printed.

Only this report is committed. Evidence and guarded SQL remain in publishing tmp/baltimore-00-*, tmp/austin-00-* and tmp/springfield-00-*. Next focus: Baltimore's 611 residuals; Austin APN reconciliation preserving existing relations; Springfield county checks and 196 unlinked rows. Preserve all earlier market holds.
