# Halethorpe public enrichment hold — September 12, 2026

Halethorpe, Maryland remains unpublished: 38 stored active Redfin listings, with unlinked rows reduced from 38 to 9. The initial exact-unique linker found no existing matches. Imported 29 unique public tax accounts and linked all 29 using scripts/link-market-listings-fast.ts with --exact-unique. No shells were created.

Source: https://bcgisdata.baltimorecountymd.gov/arcgis/rest/services/Property/Property/MapServer/1 . The official Baltimore County service supplied 5,078 fully paginated house-number candidates. Accepted exact PREMISE_ADDRESS and ZIP_CODE matches with a unique TAXPIN. Repeated geometry records were collapsed only when all imported fields agreed. Owner mailing addresses were excluded; HALETHORPE is explicitly a listing-city label, while the source often uses BALTIMORE as the premise city.

The 29 linked properties confirm Baltimore County county_id 2338913 (FIPS 24005), distinct from Baltimore City. Imported 29 parcel identities, owner names, substantive LU_CODE classifications, years built and structure sizes; 28 positive TOTAL_VALUE amounts were retained as assessed values with unspecified vintage, not current market appraisals. One zero value was omitted. No existing property rows were overwritten. No coordinates, verified agent contacts, mortgages, rents or creative-finance evidence were inferred.

Nine residual addresses need individual county and parcel evidence: 3930 BENSON AVE (source ZIP differs), 0 MONUMENTAL RD, 5713 1ST AVE, 18 INGATE TER #4402, 4101 OLD WASHINGTON BLVD, 418 1ST AVE, 302 4TH AVE, 25-W W END CT and 216 CLYDE. Preserve units, directional suffixes and zero-number/lot distinctions. Further ordinal-address matching may be possible after source inspection.

Do not assign all residuals to Baltimore County from ZIP 21227 alone. Maryland Planning's Baltimore City ZIP map includes 21227: https://planning.maryland.gov/MSDC/Documents/zipcode_map/2011/bacizc11.pdf . HometownLocator also reports Baltimore County and Baltimore City segments: https://maryland.hometownlocator.com/zip-codes/data%2Czipcode%2C21227.cfm . This conflicts with ZipDataMaps' single-county claim.

The ranked CSV's overall completion is a historical 0% baseline, not recalculated by this work. Latest pre-link listing update: 2026-05-07T15:33:36.480019+00:00. Active flags do not guarantee current availability; linking can advance updated_at without a source refresh.

Evidence and reproducible import/matching helpers are retained locally under tmp/halethorpe-*. Public BBC endpoint verification remains unavailable: HTTP 401 with no credential found after hydrateWindowsUserEnv(). Local market count is 631 following Vienna publication in commit 2f3a74a. No Halethorpe MARKET_CONFIGS entry was added. Next ranked candidate is Jefferson, NC (38 active listings).
