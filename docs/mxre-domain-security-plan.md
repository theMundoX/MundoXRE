# MXRE Domain And Security Plan

## Production Domains

MXRE uses separate subdomains for the private dashboard/admin surface and the protected API surface.

| Host | Purpose | Public? | Access |
| --- | --- | --- | --- |
| `mxre.mundox.ai` | Master real estate dashboard, admin tools, coverage monitoring, source freshness, internal operations | No general public access | Cloudflare Access / approved internal users only |
| `api.mxre.mundox.ai` | Read-only MXRE data API for Buy Box Club and approved clients | Publicly routable, privately usable | API key or signed JWT required on every `/v1/*` route |

## Boundary Rules

- `api.mxre.mundox.ai` must never expose admin, scraper-control, import, mutation, or raw database routes.
- `mxre.mundox.ai` can show admin dashboards and operational controls, but it must sit behind Cloudflare Access or an equivalent identity gate.
- Scraper workers do not need a public domain. They run as private scheduled/background jobs and write to the MXRE database.
- Buy Box Club should consume MXRE through `api.mxre.mundox.ai`, never through direct database credentials.
- Supabase service-role keys, database credentials, proxy credentials, and scraper secrets stay server-side only.

## Launch Posture

Start the external API as read-only.

Allowed initial route classes:

- Market summary
- Market completion / coverage
- Asset inventory
- Listings and market observations
- Property lookup and property history
- Time-series snapshots once snapshot tables are live

Blocked from the public API:

- Scraper start/stop controls
- Data imports
- Direct SQL/table passthrough
- Admin job queues
- Secret/config inspection
- Any write, update, delete, or mutation endpoint

## Cloudflare Controls

Apply these controls before exposing production traffic:

- WAF enabled
- DDoS protection enabled
- HTTPS only
- Rate limits by API key and IP
- Request body size limits
- Audit logging
- Cloudflare Access on `mxre.mundox.ai`
- Optional mTLS or IP allowlist for high-trust server-to-server clients

## API Auth

Every public API request must include one approved credential:

- `x-api-key` for server-to-server clients, or
- `Authorization: Bearer <jwt>` for signed client access

Keys must be scoped by:

- client
- route family
- market
- read/write permission, with public launch keys set to read-only
- rate limit tier

## Data Freshness

API responses should expose freshness and confidence metadata where relevant:

- `generated_at`
- `last_observed_at`
- `source_freshness`
- `coverage_status`
- `coverage_scope`
- `confidence`

If scraper jobs are blocked or delayed, the API should keep serving the latest known data with freshness warnings instead of doing live scraping inside request/response paths.
