# MXRE Market Refresh Runbook

This is the repeatable local workflow for running market coverage jobs without relying on a chat session or exposing the database publicly.

## One-Time Per Computer Boot

Start the private database tunnel:

```powershell
cd C:\Users\msanc\mxre
powershell -ExecutionPolicy Bypass -File .\scripts\start-mxre-db-tunnel.ps1
```

The repo `.env` should use:

```text
SUPABASE_URL=http://127.0.0.1:8000
MXRE_PG_URL=http://127.0.0.1:8000/pg/query
```

The VPS keeps Supabase/pg-query bound to `127.0.0.1` for safety. Local scripts reach it through SSH port forwarding.

## Run One City

```powershell
npm run market:indy:refresh
npm run market:dallas:refresh
npm run market:west-chester:refresh
```

## Dallas Public-Source Quality Notes

Dallas refresh is intended to run public/free sources first. Do not add
RealEstateAPI, RapidAPI, or other paid fallback commands to
`scripts/refresh-dallas-market.ts` unless the runbook and command flags make
that explicit.

Paid fallback is now explicit and capped:

```powershell
npm run market:dallas:refresh -- --include-paid --paid-max-calls=100
```

For Dallas active listings that are not yet linked to a parcel/property, use the
one-to-one listing address resolver before property-detail normalization:

```powershell
$env:REALESTATEAPI_KEY=[Environment]::GetEnvironmentVariable('REALESTATEAPI_KEY','User')
npx tsx scripts/resolve-dallas-unlinked-listings-realestateapi.ts --limit=300 --max-calls=300 --concurrency=5
npx tsx scripts/enrich-on-market-realestateapi.ts --state=TX --city=Dallas --limit=500 --max-calls=500 --skip-ensure-queue
npx tsx scripts/generate-dallas-coverage-dashboard.ts
```

The resolver is intentionally capped and exact-address only. It stores the
provider response on the listing and links only when the returned APN/address
uniquely resolves to an MXRE property.

This runs the normal public refresh first, then calls paid enrichment only for
active linked properties selected by remaining gaps. RealEstateAPI Property
Detail is queried one property at a time from the MXRE property address and
writes the cached full response, mortgages, sales history, MLS history, and
agent fields back to that same `property_id`. Zillow/RapidAPI fallback is also
property-scoped and writes listing/contact/detail data back to active listings
for the same `property_id`.

Never run paid fallback without a daily `--paid-max-calls` cap. If a listing is
not linked to a `property_id`, link or resolve it first; do not spend paid calls
on ambiguous property identity.

Known Dallas gaps to work down before paid fallback:

- Agent email: public search only counts emails that appear on a public page
  tied to the same agent identity. Do not fabricate or pattern-guess emails.
- Recorder/debt: `scripts/ingest-recorder-tx.ts --county=Dallas` targets the
  Dallas County Clerk publicsearch.us portal, but should stay bounded because
  browser scraping can be slow or stale.
- Recorder alternate public sources to evaluate if publicsearch.us breaks:
  Dallas County Official Public Records at `https://dallas.tx.publicsearch.us/`
  and the GovOS/search.govos mirror at `https://dallas.tx.ds.search.govos.com/`.
- Multifamily depth: Dallas has rent snapshots but no complex website or
  floorplan coverage yet.

Dry runs:

```powershell
npm run market:indy:refresh:dry
npm run market:dallas:refresh:dry
npm run market:west-chester:refresh:dry
```

## Run All Enabled Markets

```powershell
npm run market:refresh:all
```

The enabled market list lives in:

```text
config/market-refresh-jobs.json
```

Add a city there after its refresh script exists.

## Running Multiple Cities At Once

Open separate PowerShell windows after the tunnel is running:

```powershell
cd C:\Users\msanc\mxre
npm run market:indy:refresh
```

```powershell
cd C:\Users\msanc\mxre
npm run market:dallas:refresh
```

Use this carefully. Parallel city jobs are fine when they touch different market rows, but paid API fallback scripts should keep daily call caps so RealEstateAPI/RapidAPI credits cannot spiral.

## Logs

Refresh logs write under:

```text
logs/market-refresh
```

Use the newest JSON log to see which step failed or completed.
