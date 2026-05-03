# MXRE Daily Market Refresh Blueprint

This is the repeatable, non-LLM process for keeping MXRE market data current.
It is designed to run from a server scheduler such as systemd timers, cron, or
Windows Task Scheduler. ChatGPT/Codex is not part of the daily runtime.

## Production Model

```text
systemd timer -> scripts/run-market-refresh-jobs.ts -> market scripts -> database -> MXRE API
```

The configured markets live in:

```text
config/market-refresh-jobs.json
```

Each enabled job is run once by the daily scheduler. Job output is written to:

```text
logs/market-refresh/
```

## First Market

Indianapolis is the first production refresh job:

```json
{
  "id": "indianapolis-in",
  "label": "Indianapolis, IN",
  "enabled": true,
  "required": true,
  "command": ["npx", "tsx", "scripts/refresh-indianapolis-market.ts"]
}
```

## Adding Another City

1. Create a market refresh script, for example:

   ```text
   scripts/refresh-dallas-market.ts
   ```

2. Make the script source-specific and resumable. It should upsert records,
   write observations/events, and never delete historical listing or property
   data simply because a source no longer shows it.

3. Add the market to `config/market-refresh-jobs.json`:

   ```json
   {
     "id": "dallas-tx",
     "label": "Dallas, TX",
     "enabled": true,
     "required": false,
     "command": ["npx", "tsx", "scripts/refresh-dallas-market.ts"]
   }
   ```

4. Run a dry run when supported:

   ```powershell
   npx tsx scripts/run-market-refresh-jobs.ts --dry-run --only=dallas-tx
   ```

5. Run the real job once manually before enabling unattended daily execution.

## Refresh Rules

- Store `first_seen_at`, `last_seen_at`, `observed_at`, and source provenance.
- Record events for listed, delisted, sold, price_change, rent_change, and data_change.
- Do not delete delisted listings; mark their status and preserve history.
- Use pagination and batch sizes for large markets.
- Keep city refreshes sequential by default. Increase concurrency only after
  the API, database, and sources have been measured.
- Treat source failures as partial refresh failures, not data deletion events.

## Daily Cadence

The first production timer runs all enabled markets once per day. For hundreds of
cities, keep one market config and one scheduler, then tune job order, batch
sizes, and concurrency after real run times are measured.

For high-volume future usage, split the system into:

- API server
- database
- refresh worker
- queue
- object/log storage

The current VPS can run the first version as a single-server deployment.
