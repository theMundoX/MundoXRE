# National Address Autocomplete

MXRE autocomplete is not complete unless it is backed by a national address
index. The MXRE property table is not enough because MXRE market coverage is
still expanding.

## Runtime Flow

```text
BBC typing UI
  -> GET /v1/addresses/autocomplete
  -> address_autocomplete_entries / MXRE covered properties
  -> selected address
  -> GET /v1/bbc/property after user clicks Search
```

The endpoint must stay fast. It should not call live external geocoders on each
keystroke.

## Free Source Strategy

Primary free source:

- OpenAddresses regional U.S. downloads

OpenAddresses states that address data is open data and openly licensed, though
individual sources may require attribution. Keep the license/provenance files
from downloads when storing national autocomplete data.

## Setup

Apply the table/index migration:

```powershell
npx tsx scripts/apply-address-autocomplete-migration.ts
```

Load one state for testing:

```powershell
npx tsx scripts/ingest-openaddresses-national-autocomplete.ts --state=IN
```

Load one region:

```powershell
npx tsx scripts/ingest-openaddresses-national-autocomplete.ts --region=us_midwest
```

Load all configured U.S. regions:

```powershell
npx tsx scripts/ingest-openaddresses-national-autocomplete.ts
```

Set current OpenAddresses batch URLs through environment variables when
available:

```text
OPENADDRESSES_US_MIDWEST_URL=
OPENADDRESSES_US_SOUTH_URL=
OPENADDRESSES_US_NORTHEAST_URL=
OPENADDRESSES_US_WEST_URL=
```

The config fallback URLs are only a seed path. Fresh batch.openaddresses.io
downloads are preferred.

## Enabling In API

After the national index is populated and tested, enable:

```text
MXRE_ENABLE_NATIONAL_AUTOCOMPLETE=true
```

Then deploy/restart the API.

## BBC Contract

BBC should debounce keystrokes and call:

```http
GET /v1/addresses/autocomplete?q=429%20N%20Tibbs&state=IN&limit=5
```

When the user selects an `address` result and clicks Search, BBC calls:

```http
GET /v1/bbc/property?address={street}&city={city}&state={state}&zip={zip}
```

When the user selects a `city` result, BBC calls market/search endpoints such as:

```http
POST /v1/bbc/search-runs
```
