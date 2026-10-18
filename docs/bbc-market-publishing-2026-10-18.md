# BBC market publishing — 2026-10-18

The current ranked report contained several already-live markets under alternate city labels, including Mount Laurel Township, O'Brien, and Sylmar. The next eligible candidate was **Timnath, CO** (historical overall completion 30.69%, 101 active listings, and 16 historical unlinked rows).

The current audit confirmed all 101 active Redfin listings link to 101 properties assigned to Larimer county_id `75651`; the exact-unique linker added zero links and a guarded shell pass added zero shells. The re-audit found 101 parcel identities, 87 ownership records, 14 values, 99 classifications, 23 coordinates, 14 explicitly labeled listing-backed shells, and 101 brokerage rows. Year built, physical size, verified contacts, mortgage/debt, and rent coverage are absent. Listing timestamps were not refreshed.

The new `timnath-co` BBC configuration is live with a readiness target of 28 and explicit field-level restrictions. No RapidAPI was used.

Springfield, OH was the next eligible meaningful market after resolving alternate city labels in the ranked report. The report listed 199 active rows, 194 linked, 5 unlinked, and 32.91% historical overall completion. The current audit found 477 active Redfin rows: 407 linked to Clark county_id `2338868` and 70 unlinked. The exact-unique pass added zero links; a guarded address-and-ZIP shell pass created and linked 70 clearly labeled listing-backed shells, leaving 477 linked and zero unlinked.

The re-audit found 302 parcel identities, 292 ownership rows, 175 listing-derived valuations and classifications, and 175 listing-backed shells. Year built, physical size, coordinates, verified contacts, brokerage, debt, rents, and creative-finance evidence are absent. The `springfield-oh` BBC configuration is live with a readiness target of 28 and explicit field-level restrictions. No RapidAPI was used.
