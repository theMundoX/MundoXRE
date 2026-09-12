# BBC publishing and enrichment — September 12, 2026, 17 UTC

## Scranton, Pennsylvania

Enabled in commit 0463522. All 38 stored active Redfin listings are linked: 38 unlinked before exact linking, 38 after the initial exact pass, 13 after 25 public parcel imports/exact links, and zero after 13 explicitly labeled listing-backed shells. Lackawanna county_id 1652972 / FIPS 42069 is confirmed from the linked county parcels.

Official source: https://gis.lackawannacounty.org/arcgis/rest/services/GISViewer/LandRecords/MapServer/85 . The older GISViewer/Parcels/MapServer/0 exposes field definitions but returned no non-null LOCATION rows; use the LandRecords source. Fully paginated house-number queries returned 2,578 candidates. Matches require a unique exact street address followed only by an explicit city/state/postal suffix, Scranton or Dickson City municipality, and source ZIP agreement where supplied. No lot, unit, fuzzy or multi-address merges. Imported ParcelPIN, OwnerName and assessed TotalValue, validated against LandValue plus ImprovedValue. Vintage is unspecified. Existing records were not overwritten; undecoded dwelling codes were not classified.

Final coverage: 25 parcel identities/owners, 38 values (25 assessments and 13 asking prices), 38 brokerage names; no substantive classification, coordinates, years built, verified agent contacts, debt, rents or positive creative-finance evidence. Shell labels preserve ranges, SUMMIT POINTE numbers and duplicated directions. ZIPs 18508 and 18519 are entirely Lackawanna according to https://www.zipdatamaps.com/18508 and https://www.zipdatamaps.com/18519 . Scope follows listing mailing-city labels, including two Dickson City parcels. Display coordinates are approximate ZIP coordinates, not parcel points.

Local endpoint: HTTP 200, 632 markets, scranton-pa production_allowed, readiness 33.1 / target 28. Public endpoint remains HTTP 401 with no API credential available after hydration; public count and deployed visibility are unknown. Typecheck, build and diff whitespace checks passed. Restarted verified local node origin on port 3101 with the existing disposable local smoke credential helper.

## Jefferson, North Carolina — held

38 active listings; unlinked 38 -> 38 initial exact -> 28 after ten public imports/exact links. No shells or live config. Ashe county_id 35170 / FIPS 37009 confirmed from ten linked official parcels. Source https://gis.ashecountygov.com/arcgis/rest/services/Parcels/MapServer/0 returned 405 fully paginated house-number candidates. Ten unique exact ParcelPropertyAddress matches imported ParcelNumber, Name1, TotalMarketValue and TotalAssessedValue. City and ZIP are explicitly listing-derived; source City/State/ZipCode are owner mailing fields and were excluded. Assessment vintage unspecified; no overwrites, classification, coordinates or debt claims.

The 28 residuals include TBD/lot labels, multiple street numbers, unit labels and 233 LAKEVIEW PL UNIT D-2 with ZIP 27640 rather than the dominant 28640. Hold until individual identity/address/county evidence resolves these records. Latest pre-link listing update: 2026-05-07T15:33:02.437974+00:00.

The ranked CSV overall completion value is a historical 0% baseline for both markets and has not been recomputed. Live readiness is a separate measure. Evidence is retained in tmp/scranton-* and tmp/jefferson-nc-* in the isolated publishing worktree. Unrelated primary-worktree changes were preserved.

## Newark, Texas — held for county reconciliation

38 active listings, 38 unlinked before and after separate exact-link passes against Wise county_id 2338888 / FIPS 48497 and Tarrant county_id 8 / FIPS 48439. No linked county evidence, imports, shells or config. Inventory includes ZIPs 76071 and 76078. Newark is included among Tarrant municipalities by https://www.tarrantcountytx.gov/en/county/about-tarrant/incorporated-areas.html and among Wise taxing units by https://comptroller.texas.gov/taxes/property-tax/county-directory/wise.php . Do not assign all Newark shells to one county without individual evidence. Next source investigation can start at the official https://wise-cad.com/ . Baseline CSV overall completion is 0%; current linkage remains zero. Latest pre-link listing update is 2026-05-07T15:33:24.723323+00:00.

Next ranked candidate after these holds: Mimbres, NM (historical 37 active listings), pending fresh audit and county/source review. Earlier documented holds and aliases remain in effect.
