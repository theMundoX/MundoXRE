# MXRE Ingestion + Venture Studio UI — Handoff

**The one doc.** Covers (1) what data is in MXRE, (2) how the ingestion runs, (3) how it shows up in the mundox-venture-studio UI.

---

## The two systems

### 1. MXRE — data warehouse
**Where:** Self-hosted Postgres on your Contabo VPS, accessed via pg-meta HTTP endpoint at `${MXRE_PG_URL}` (defined in `mxre/.env`).

**What's in it:**
- `properties` (~98M rows nationwide; 583K Marion)
- `mortgage_records` (9.3M nationwide; 60K Marion — 14,885 linked to properties)
- `hmda_lar` (51M loan applications)
- `rent_snapshots`, `listing_signals`, `entities`, `entity_relationships`
- 7 analytics materialized views (mv_property_distress, mv_property_equity, mv_owner_portfolio, mv_sales_monthly, mv_rent_monthly, mv_appreciation_yearly, mv_county_coverage)

**Repo:** `C:\Users\msanc\mxre` — local-only, no git remote.

### 2. mundox-venture-studio — UI + orchestration brains
**Where:** Vite/React app + Deno edge functions on managed Supabase (`afdwvfmlywbxjcsqcixq.supabase.co`).

**What's in it:**
- React frontend at `mundox.ai` (Cloudflare deploy pending — was put on Worker by mistake, redo as Pages)
- Agent metadata in managed Supabase: `agents`, `agent_runs`, `agent_tasks`, `agent_conversations`, `divisions`, `companies`, `opportunities`
- Edge functions that bridge to MXRE: `mxre-stats`, `ryder-command-center`, `agent-intel-orchestrator`, `deal-scorer`, `agent-runner`, `backtest-run`

**Repo:** `C:\Users\msanc\mundox-venture-studio` → pushed to `github.com/munsanco13/mundox-venture-studio`

---

## How they wire together

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  USER OPENS mundox.ai (or localhost:5173)                       │
│       ↓                                                         │
│  React app loads → fetches from edge functions                  │
│       ↓                                                         │
│  ┌─────────────────────────────────────────────────────┐        │
│  │ edge: ryder-command-center                          │        │
│  │  - reads agent metadata from MANAGED Supabase       │        │
│  │  - reads property/mortgage stats from MXRE pg       │        │
│  │  - returns one big JSON blob                        │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

ORCHESTRATION CRON (every minute):
   pg_cron on managed Supabase → calls agent-intel-orchestrator edge fn
       ↓
   agent-intel-orchestrator:
   1. Queries MXRE for stale counties, missing counties, distress signals
   2. Creates agent_tasks rows assigned to Ryder/Intel/regional agents
   3. Done — returns

LOCAL WORKER (running on your Windows box):
   mxre/scripts/agent-runner-worker.mjs subscribes to agent_runs INSERTs
       ↓
   On new run:
   1. Loads agent's system_prompt + injects MXRE schema
   2. Calls local Ollama (phi4:14b) with schema-pinned prompt
   3. Agent decides which tool to call:
       - query_mxre(sql)        → reads MXRE pg, returns rows
       - post_message(to, text) → writes to managed Supabase
       - create_task(...)       → writes to managed Supabase
       - run_ingest(county_fips)→ spawns ingest-county.ts as child process
   4. Records run results (tokens, citations, output) in agent_runs

INGESTION SCRIPTS (in mxre/scripts/, run locally):
   ingest-county.ts → ArcGIS / Fidlar / qPublic → writes to MXRE pg
   link-mortgage-records.ts → matches mortgage_records.borrower_name
                              to properties.owner_name → sets property_id
   enrich-marion-pdf.ts → fetches Indy property card PDFs
                          → parses year_built/beds/baths/sale history
                          → updates properties row in MXRE pg
```

---

## What's Marion's actual state right now

```
Total parcels:           583,230
With owner_name:         237,877  (40.8%)  ← from ArcGIS
With mailing_address:     88,440  (15.2%)  ← from ArcGIS expanded field map
With market_value:        89,596  (15.4%)  ← from ArcGIS
With year_built:              28  (0.005%) ← needs PDF enricher to run
With last_sale_price:          0  (0.0%)   ← needs PDF enricher OR Fidlar deeds
Mortgages linked:         14,885  (24.6% of 60,480 Marion docs)
```

The 14K linked mortgages mean `mv_property_distress` flags (foreclosure / lis_pendens / tax_delinquent) ARE live for those parcels. Just refresh the view.

---

## Resume commands (copy-paste ready)

```sh
# === 1. Resume Marion ingest (overwrites stale rows with full 18-field map) ===
cd C:/Users/msanc/mxre
nohup npx tsx scripts/ingest-county.ts --county=Marion --state=IN \
  > logs/marion-ingest-$(date +%s).log 2>&1 &

# === 2. Resume PDF enricher (the big remaining unlock — overnight run) ===
nohup npx tsx scripts/enrich-marion-pdf.ts --workers=4 --resolve-legacy \
  > logs/enrich-marion-$(date +%s).log 2>&1 &
tail -f logs/enrich-marion-*.log
# Throughput: ~8 parcels/sec @ 4 workers, ~20 hours for all 580K parcels
# Hit rate: 33% (rest are vacant lots — expected)

# === 3. Re-run linker periodically (when new mortgages land) ===
npx tsx scripts/link-mortgage-records.ts --state=IN --county=Marion --strategy=owner

# === 4. Restart agent worker (always after pulling new code) ===
pkill -f agent-runner-worker
nohup node scripts/agent-runner-worker.mjs > worker.log 2>&1 &

# === 5. Refresh analytics MVs after big ingest batches ===
# Run via pg-meta or psql:
SELECT refresh_analytics_views();
REFRESH MATERIALIZED VIEW mv_owner_portfolio;
```

---

## Where to see it in the UI

**Command Center** (route `#/agents/ryder` or click Ryder agent card):
- "Ingest Jobs" panel — live view of `run_ingest` spawns (status, FIPS, county, pid, log path)
- "Data Estate · Total Scale" — total counts across MXRE
- "Agents in Formation" — Ryder + Intel + regional agents with token usage
- "Cross-Division Opportunities Bus" — deal_scorer outputs
- "Division Kanban" — agent_tasks board

**For Marion specifically**, drill in via:
- Click `mxre-midwest` agent card → AgentDashboardPage shows that agent's runs/tasks/messages
- Or query `mxre-stats?agent=mxre-midwest` edge function → returns Marion + other midwest counties' counts
- Or directly query `mv_property_distress`, `mv_property_equity` etc. for analytics

---

## What's still missing for "Marion is deployable"

| Need | Source | Effort |
|------|--------|--------|
| owner_name + mailing | ArcGIS (running) | finishes itself |
| asset class / property class | ArcGIS (running) | same |
| year_built / beds / baths | PDF enricher | overnight run |
| last_sale_price + date | PDF enricher (sale history) OR Fidlar deeds | same overnight run |
| foreclosure flags | mortgage_records linked → mv_property_distress | already wired, just refresh |
| MLS list price + DOM | Redfin/Zillow scraper (not built) | ~1 day to build, needs IPRoyal proxy |
| Average rents | RentCast API (not wired) | ~2 hours to wire — has free tier |
| Code violations | Indy ArcGIS (URL not located) | manual recon needed |
| Skip trace phones | BatchSkipTracing API (not wired) | ~half day to wire, paid per-skip |

---

## Recent commits in mxre repo (master branch, local-only)

```
a324467  Marion handoff doc (the previous one — superseded by this file)
9a03903  Re-apply 'deed' doc_type filter in migration 003
6f868da  mv_sales_monthly: include 'deed' doc_type → unlocks 2M sales
84f0425  Migration 003: analytics MVs + code_violations table
4a65f47  Add Marion property card PDF enricher
4cfd1e9  ArcGIS adapter: capture full per-parcel attribute set
53c2d75  Marion field_map: 18 fields incl mailing + assessed value
c6ff438  Wire Marion ingest end-to-end (indiana.json + spawn fix)
f23a0c5  Linker: register Marion/Allen/St.Joseph + long-tail discovery fix
564e60c  Worker: pin schema in agent prompt + add run_ingest tool
7c75673  Strip VPS IP + service-role JWT + DB password from tracked files
```

Recent commits in mundox-venture-studio (master, pushed to GitHub):
```
23f1137  Ignore mundox-dist.zip build artifact
fc7d1f9  Cloudflare Pages: SPA fallback, security headers, deploy runbook
268fc32  WIP checkpoint with all session changes
```

---

## One-paragraph briefing for next chat

> Continuing Marion County / Indianapolis ingestion for MXRE. Read `C:\Users\msanc\mxre\HANDOFF.md` first. Priority: kick off the overnight PDF enricher run to fill year_built/beds/baths/last_sale_price. The orchestrator + agent worker + analytics views are all wired and live; the data layer is the bottleneck. The mundox-venture-studio UI's Command Center already shows ingest jobs and agent activity, so once the data lands the UI surfaces it automatically.
