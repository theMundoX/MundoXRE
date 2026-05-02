# MXRE Local Coverage Supervisor

The coverage pipeline should run on this computer, not inside a Codex heartbeat.

Codex should create and maintain the scripts, fix failures, review logs, and make code changes. The local machine should do the repetitive work: scraping public/legal sources, enriching records, scoring listing text, writing to Supabase, and generating logs.

## Run One Local Cycle

```powershell
cd C:\Users\msanc\mxre
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run-local-coverage-supervisor.ps1 -Market indianapolis -Once
```

Logs are written to:

```text
C:\Users\msanc\mxre\logs\local-supervisor
```

The supervisor uses lock files in:

```text
C:\Users\msanc\mxre\.mxre-locks
```

That prevents overlapping runs for the same market.

## Install A Daily Windows Task

```powershell
cd C:\Users\msanc\mxre
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\install-local-coverage-task.ps1 -Market indianapolis -At 02:00
```

Start it manually:

```powershell
Start-ScheduledTask -TaskName "MXRE Local Coverage - Indianapolis"
```

Check recent local-supervisor logs:

```powershell
Get-ChildItem C:\Users\msanc\mxre\logs\local-supervisor | Sort-Object LastWriteTime -Descending | Select-Object -First 10
```

## Supported Markets

- `indianapolis`
- `west-chester`
- `columbus`

## Role Split

Local computer:

- runs ingestion and enrichment scripts,
- writes logs,
- writes records to Supabase,
- can run for hours without model tokens.

Codex:

- builds/refactors scripts,
- audits security and data quality,
- checks summaries,
- fixes failures,
- updates API/docs.

## Heartbeat Policy

Heartbeats should not run the long pipeline. They should only do lightweight checks or be disabled once the local scheduled task is installed.
