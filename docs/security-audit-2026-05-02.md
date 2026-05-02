# MXRE Security Audit - 2026-05-02

Scope: private MXRE API, Buy Box Club sandbox integration, Cloudflare Worker gateway, and the new daily-change/event model.

## Current Controls

- Public API traffic terminates at Cloudflare Worker `mxre-api-gateway`.
- BBC production and sandbox use separate client ids and separate API keys.
- Worker accepts API keys only through `x-api-key` or Basic Auth for private docs.
- Worker compares API keys with constant-time comparison.
- Node origin also compares API keys with constant-time comparison.
- Worker injects the upstream origin key; BBC never receives or sends the origin key.
- Protected endpoints return `cache-control: no-store` unless they are low-risk coverage summary endpoints.
- Coverage summary endpoints are the only cacheable protected endpoints.
- Worker and Node reject request bodies over 1 MB.
- Worker and Node enforce pre-auth, failed-auth, and authenticated client rate limits.
- API responses include request ids for traceability.
- API request logs include client id, method, path, status, and latency, but do not log API keys.
- Public API property responses sanitize raw source URLs and expose provider-neutral source categories.
- Production dependency audit currently reports 0 vulnerabilities with `npm audit --omit=dev`.

## Live Checks Passed

- Sandbox key can call BBC property and market changes endpoints.
- Wrong or missing credentials return `401`.
- Wrong client id paired with a valid sandbox key returns `401`.
- Private docs return `401` plus a Basic Auth challenge when unauthenticated.
- BBC property endpoint returns `cache-control: no-store`.
- BBC changes endpoint now reads from `property_events`.
- Coverage endpoint is the only tested endpoint returning short private cache headers.
- Cloudflare Worker does not proxy `/preview/*`; public preview requests return `404`.
- Local preview dashboards only bypass auth and embed a browser key on localhost/127.0.0.1.
- Non-local Host headers against preview routes return `401`.
- Injection-shaped BBC search/change inputs returned safe empty/400 responses during smoke testing.
- Oversized protected POST bodies return `413`.
- Cloudflare Worker deployed successfully after hardening.
- Database migration created `source_refresh_runs`, `property_events`, `property_snapshots`, `market_daily_metrics`, and `api_sync_cursors`.
- Gateway deployment script now keeps BBC-facing client keys separate from the internal Worker-to-origin key.

## Remaining Risks

- The public Worker is production-like, but the Node origin should move from local/tunnel hosting to a managed private origin before heavy client usage.
- Current in-memory Worker rate limits are useful but not durable across all Cloudflare isolates. Add Cloudflare WAF/rate limiting rules for stronger DDoS/brute-force controls.
- BBC search SQL is sanitized and constrained, but it should move to parameterized Postgres functions/RPC before broad external usage.
- Per-client quotas are declared in config but not yet persisted and enforced monthly in the database.
- Refresh scripts should write structured `source_refresh_runs` summaries on every run, not only filesystem logs.
- Key rotation needs an operational runbook and a short overlap window for BBC cutovers.
- If browser clients ever call MXRE directly, CORS must stay deny-by-default or be tightly allowlisted. Current intended use is backend-to-backend only.

## Required Next Hardening

1. Move the Node origin to a managed host behind Cloudflare, with no direct public access except through the Worker.
2. Add Cloudflare dashboard WAF rules for `/v1/*`: block missing API key, rate-limit excessive requests, and alert on repeated auth failures.
3. Replace dynamic SQL in BBC search/change endpoints with parameterized database functions.
4. Add persistent client usage counters and monthly quota enforcement.
5. Add structured refresh-run writes to every market ingestion script.
6. Add automated security smoke tests for auth, cache headers, size limits, and docs protection.

## Security Position

Safe for BBC sandbox integration and controlled testing. Before commercial-scale production, finish the managed-origin deployment, WAF rules, persistent quotas, and parameterized SQL work.
