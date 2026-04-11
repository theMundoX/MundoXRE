# Geocoder issue (parked)

`scripts/geocode-properties-batch.mjs` produces HTTP 400 from Census batch endpoint
even though manual `curl -F addressFile=@<exact-file>` of the SAME generated CSV
returns matches successfully.

Tried:
- spawn(curl) — fails
- Node native fetch + FormData — fails
- ASCII-stripping CSV cells — no change
- Forward-slash temp paths — no change
- Setting curl User-Agent — no change
- Field order swap (benchmark before addressFile) — no change

Suspect: Node FormData serializes the multipart boundary or content-disposition
filename in a way Census rejects, but ONLY when not in a manual curl invocation.
Possibly related to streaming behavior or how Blob filename gets encoded.

Workaround for next session: instead of multipart batch endpoint, use the
single-address Onelinelookup endpoint and run 6-12 parallel workers that submit
one address per HTTP call. Slower per-call but no multipart edge cases.

Endpoint: `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=URL_ENCODED&benchmark=Public_AR_Current&format=json`

This will be slower (~5-10 addr/sec per worker × 12 workers = ~60-120/sec)
but reliable. Total time for 66.8M properties: ~150-300 hours, so it should
run as a permanent cron not a one-shot.

Or — use a free third-party batch geocoder like Nominatim (OpenStreetMap) which
has different multipart handling. Has rate limit (1 req/sec for the public API)
but you can self-host.

Status: PARKED. Pipeline does not depend on this for tonight's rate work.
