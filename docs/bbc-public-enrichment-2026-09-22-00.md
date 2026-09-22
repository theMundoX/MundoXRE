# BBC public enrichment — 2026-09-22 00 UTC

Continued from c5de6c1 with the ranked CSV in C:\Users\msanc\mxre and the existing clean publishing checkout C:\Users\msanc\mxre-overnight-sep09. Ranked by historical overall completion and active count (minimum 10); retained prior aliases and higher-ranked holds. Public inventory authentication still fails, so source-config exclusions remain provisional. Unrelated primary-checkout changes were preserved.

| Market | Active listings | Unlinked before / after | Historical CSV overall | Result |
| --- | ---: | --- | ---: | --- |
| Franklin OH | 284 | 283 / 77 | 16.88% | 206 county-supported listing-backed shells created and linked |
| Gold River CA | 26 | 0 / 0 | 16.03% | Exact linking no-op; readiness hold |
| Rancho Cordova CA | 281 | 0 / 0 | 14.41% | 184 verified parcel centroids added |

## Franklin

Ran scripts/link-market-listings-fast.ts with exact-unique and SSH transport for Montgomery 1698991, Warren 1741129 and Butler 1741128. All three linked zero. Verified county IDs against database county metadata. Ohio's [public address layer](https://maps.ohio.gov/arcgis/rest/services/Accela/Accela_Service/MapServer/0) supplied 1,585 distinct address features across ZIP/house-number batches; every response was checked for source errors and truncation. Exactly one street/ZIP feature supported each of 206 listings: 205 Warren and one Montgomery. The other 77 listings were left unresolved.

A guarded transaction checked active/unlinked state, unchanged address and ZIP, absence of an existing statewide exact-address/ZIP property, and expected counts. Created 206 distinct shells with null parcel identity and valuation, listing_backed_property type, active_listing_shell status, unknown asset type, listing_only confidence and listing_signal_shell:redfin source. Stored source URL, observation time, address feature ID, county, situs address and explicit county-only quality evidence in listing raw.ohioAddressCountyEvidence. No assessor identity, ownership or coordinates were inferred. Listing timestamps were preserved.

After audit: 284 active, 207 distinct linked properties, 77 unlinked, 206 shells. Linked county counts are 205 Warren and two Montgomery, including the preexisting parcel. Only one property has parcel identity, ownership, substantive classification and coordinates. No audited values, year built, debt, rents or agent contacts. Publication remains held for unresolved links and enrichment.

## Gold River and Rancho Cordova

Both markets link to Sacramento county 773106. Exact-unique linker runs found no additional matches and created no shells.

Gold River: 25 parcel identities/classifications/coordinates and one unknown shell among 26 properties; no ownership, year built, debt, rents or agent contacts. The only stored market_value (535000) belongs to the Redfin listing shell at 11285 STANFORD COURT LN #708 and is not verified assessor valuation. Seven-component API readiness is 28.0 if that value is counted, but only 27.5 excluding unsupported valuation. Held rather than lowering the existing 28-point target or overstating valuation quality.

Rancho Cordova: 243 parcel identities/substantive classifications and 38 shells among 281 properties. Querying the [Sacramento parcel service](https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer/0) found unique APN plus exact street/ZIP matches with valid centroids for 184 of 185 sacramento-gis properties missing coordinates. Guarded updates required unchanged identity/address/ZIP, correct county/source, null coordinates and a current active market listing. Coordinates rose from 58 to 242. Stored raw.sacramentoParcelCentroid provenance on all 184 listings, explicitly identifying parcel centroids rather than rooftops. No listing timestamps or existing nonnull coordinates changed. One candidate had no feature and remains unresolved. Ownership, year built, debt, rents and agent contacts remain absent. Even counting all 38 stored values, readiness is only 26.6, below 28; publication remains held.

Historical CSV completion percentages were not recomputed. County-supported shells and centroids must not be presented as complete underwriting data.

## Validation and checkpoint

npm run typecheck and npm run build passed. Public smoke twice returned HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production_allowed remain unknown. No API configs changed, no new market was published and no origin restart was required. No RapidAPI or secrets requested/printed.

Evidence in publishing tmp/: franklin-sep22-{current,before,public,after}.json, franklin-sep22-exact-{montgomery,warren,butler}.json, franklin-sep22-shells.{mjs,sql}, franklin-sep22-shells-result.json, gold-river-sep22-{before,exact,after}.json, rancho-cordova-sep22-{before,input,coordinates,result,after}.json and rancho-cordova-sep22-coordinates.{mjs,sql}. Mutation provenance is durable in database listing raw fields.

Next ranked target: Elk Grove CA (historical 13.31%, 174 CSV active), then Fair Oaks CA. Retain Franklin's 77 unresolved listings, Gold River/Rancho Cordova readiness holds and all prior holds. Intentional commit scope: this report only.
