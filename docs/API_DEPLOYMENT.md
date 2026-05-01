# MXRE API Deployment

Target domains:

- Admin/dashboard: `mxre.mundox.ai`
- Protected API: `api.mxre.mundox.ai`

## Required Environment Variables

Set these in the hosting provider. Do not commit production values.

```text
NODE_ENV=production
MXRE_API_KEY=<long random API key>
SUPABASE_URL=<supabase project url>
SUPABASE_SERVICE_KEY=<supabase service role key>
```

The server listens on `PORT` when the host provides it, otherwise `MXRE_API_PORT`, otherwise `3100`.

## Build And Start Without Docker

```bash
npm ci
npm run build
npm start
```

This is the preferred deployment path. Docker is not required for MXRE API hosting.

## Render Node Service

Use [render.yaml](../render.yaml), or create a Web Service manually:

```text
Runtime: Node
Build Command: npm ci && npm run build
Start Command: npm start
Health Check Path: /health
```

## Railway Node Service

Use [railway.json](../railway.json), or create a Node/Nixpacks service:

```text
Build Command: npm ci && npm run build
Start Command: npm start
Health Check Path: /health
```

## Health Check

```bash
curl https://api.mxre.mundox.ai/health
```

Expected:

```json
{"status":"ok","version":"1.0.0"}
```

## Protected Endpoint Smoke Test

```bash
curl -H "x-api-key: $MXRE_API_KEY" \
  "https://api.mxre.mundox.ai/v1/markets/indianapolis/reports/creative-finance?limit=5"
```

## Cloudflare

Create DNS records after the host gives a target hostname.

```text
Type: CNAME
Name: api.mxre
Target: <hosting-provider-target>
Proxy: ON

Type: CNAME
Name: mxre
Target: <dashboard-host-target>
Proxy: ON
```

Recommended Cloudflare rules:

- SSL/TLS: Full strict
- WAF rate limit on `/v1/*`
- Block requests to `/v1/*` missing the `x-api-key` header if available on your plan
- Cache bypass for `/v1/*`
- Cache bypass for `/health`

## Current API Report Needed By Buy Box Club

Creative finance report:

```text
GET /v1/markets/indianapolis/reports/creative-finance
```

Useful query params:

```text
status=positive|negative|all
asset=all|single_family|multifamily
scope=city|core|metro
zip=46222
min_price=100000
max_price=300000
min_units=2
max_units=20
since=2026-04-01
until=2026-04-30
page=1
limit=50
```

All protected `/v1/*` endpoints require:

```text
x-api-key: <MXRE_API_KEY>
```
