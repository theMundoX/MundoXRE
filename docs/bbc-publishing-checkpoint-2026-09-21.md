# BBC publishing checkpoint — 2026-09-21

The ranked source remains tmp/market-full-data-completion-report-best-first.csv in the primary checkout. Current origin/main is 44a927e. Earlier alias exclusions and county reconciliation holds remain in force. Unrelated primary checkout changes were preserved; work used the existing clean publishing checkout.

| Market | Active | Unlinked before → after | Historical CSV completion | Result |
| --- | ---: | ---: | ---: | --- |
| Berthoud CO | 134 | 0 → 0 | 29.60% | Exact-unique linker no-op; fresh database check confirms all eight previously conflicting APNs still exist on different-address assessor records. Hold 30 shells pending reconciliation. |
| Wayne PA | 36 | 0 → 0 | 23.15% | Exact-unique linker for Montgomery county no-op; 30 links in Montgomery, three Chester, three Delaware. Hold county scope and 22 shells. |
| Sacramento CA | 943 | 0 → not yet re-audited | 21.33% | Next candidate audited; county_id 773106 / FIPS 06067. 772 parcel identities, 171 shells, 560 coordinates, 772 classifications; no audited ownership, year-built, debt, rents or verified contacts. Exact pass remains next. |

Berthoud county_id 75651 / FIPS 08069 has 104 parcel identities/owners/classifications, 27 coordinates, 30 shell asking values. Wayne has 14 identities/owners, 11 classifications/coordinates, 22 shell asking values. These values are not verified assessor valuations. CSV percentages are historical and were not recomputed. No listing availability refresh was performed.

No shells or market configs were created. Exact linking changed zero records. No property overwrite, deletion or speculative APN reconciliation was attempted. Fresh evidence is retained in publishing tmp/berthoud-sep21-after.json, berthoud-sep21-conflicts.json, wayne-sep21-before.json, wayne-sep21-after.json and sacramento-sep21-before.json.

Validation: npm run typecheck and npm run build passed. Initial public probe timed out; retry returned HTTP 401 with credentialAvailable=false after hydrateWindowsUserEnv. Public endpoint count and production_allowed visibility are unknown. Local port 3101 responded HTTP 401 to the historical local smoke credential; current local count is also unknown. The prior count of 632 is not a current verification. No API code changed, so no origin restart was performed.

All protected helper access hydrated Windows user environment first. No RapidAPI used and no secrets printed or requested. Continue with Sacramento exact linking and public parcel enrichment, preserving Berthoud, Wayne and earlier county holds. Public production verification remains blocked by missing hydrated credentials.
