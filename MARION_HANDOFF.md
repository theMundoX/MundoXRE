# Marion County (Indianapolis, FIPS 18097) — Ingestion Handoff

**Date:** 2026-04-25
**Goal:** Get Marion County to "100% deployable" data quality so Buy Box Club / Ryder can actually use it for deal sourcing.

---

## Where things stand right now

### Marion property table — verified live counts

```
Total parcels in DB:    583,230
With owner_name:        237,877  (40.8%)
With mailing_address:    88,440  (15.2%)  ← absentee detection unlocked for these
With market_value:       89,596  (15.4%)
With year_built:             28  (0.005%)  ← PDF enricher just started
```

The mid-percentage fields (mailing, value, property_class) were unlocked by the
ArcGIS adapter expansion. They climb as the running re-ingest re-touches old rows
with the rich field_map.

### Two background processes that were running

1. **Marion ArcGIS re-ingest** — `scripts/ingest-county.ts --county=Marion --state=IN`
   spawned via Ryder's `run_ingest` tool. Walks the 347K parcel layer with the new
   18-field map. Each rerun overwrites stale rows with full data.

2. **Linker** — `scripts/link-mortgage-records.ts --state=IN --county=Marion --strategy=owner`
   FINISHED its first pass: **14,885 of 60,480 Marion docs linked** (32.6% match
   rate via owner_name). Distress flags now compute for those 14K parcels. Re-run
   it any time to catch new records.

3. **PDF enricher** — `scripts/enrich-marion-pdf.ts` (NEW this session). Verified
   end-to-end on parcel 6001480 (5307 W 52ND ST):
   - year_built 1960, living_sqft 1558, condition A, total_rooms 9, bedrooms 4
   - PDF source: `maps.indy.gov/AssessorPropertyCards.Reports.Service`
   - 100-parcel test: 33% enriched (others were vacant lots, no Dwelling row in PDF — expected)
   - Throughput: 2.2 parcels/sec single-worker, ~8/sec with --workers=4

   Real-world ETA for full Marion: ~20 hours continuous at 4 workers. Run overnight.

### Analytics views — populated and queryable on MXRE pg

```
mv_property_distress    70.4M rows  ✓ populated (flags compute now after linker)
mv_property_equity      70.4M rows  ✓ populated (free_clear, equity_percent)
mv_owner_portfolio      —           ✗ needs first refresh (REFRESH MATERIALIZED VIEW mv_owner_portfolio)
mv_sales_monthly        1,898 rows  ✓ populated (after 'deed' doc_type fix unlocked 2M sales)
mv_rent_monthly         3,052 rows  ✓ populated
mv_appreciation_yearly  305 rows    ✓ populated (186 with prior-year baselines)
mv_county_coverage      —           ✗ create timed out — needs simpler rewrite
```

Real signals already visible:
- 2024 sales volume top: NJ Atlantic (FIPS 34001) $34.9B, AR Saline $5.8B, IA Polk $4.9B
- 2024 YoY appreciation top: NJ multi-family 2-4 unit at +24%
- Marion sales are sparse (2020-21 only) until Fidlar refresh runs

---

## Critical files this session shipped

### `mxre` repo (commits in master, no remote yet)

```
4a65f47  Add Marion property card PDF enricher (pdfjs-dist parser)
9a03903  Re-apply 'deed' filter in migration source after revert
6f868da  Sales/appreciation MVs: include 'deed' doc_type — unlocks 2M sales rows
84f0425  Migration 003: analytics foundation + code_violations table
4cfd1e9  ArcGIS adapter: capture full per-parcel attribute set + derive owner signals
c6ff438  Wire Marion (Indianapolis) ingest end-to-end
f23a0c5  Linker: register Marion + Allen + St. Joseph IN; fix long-tail discovery
564e60c  Worker: pin schema in agent prompt + add run_ingest tool
7c75673  Strip VPS IP + service-role JWT + DB password from tracked files
```

Key files:
- `scripts/agent-runner-worker.mjs` — has `run_ingest` tool, schema doc pinned in
  every agent prompt, robust JSON parser, Windows spawn fix
- `scripts/enrich-marion-pdf.ts` — PDF enricher, pdfjs-dist parser, --resolve-legacy
  flag converts 18-digit STATEPARCELNUMBER to 7-digit PARCEL_I via ArcGIS
- `scripts/link-mortgage-records.ts` — Marion/Allen/St. Joseph registered
- `data/counties/indiana.json` — Marion ArcGIS config with 18-field map
- `migrations/003_analytics_views.sql` — MV definitions

### `mundox-venture-studio` repo (pushed to GitHub)

```
master at 268fc32        WIP checkpoint with all session changes + Mundo's parallel Jess work
master at fc7d1f9        Cloudflare Pages config + DEPLOY_CLOUDFLARE.md
claude/objective-einstein-7ac8be at 4b36053  Clean per-feature commits
```

Both branches pushed to https://github.com/munsanco13/mundox-venture-studio

---

## Resume commands for new session

```sh
# 1. Restart Marion ingest (overwrites stale rows with rich field map)
cd C:/Users/msanc/mxre
nohup npx tsx scripts/ingest-county.ts --county=Marion --state=IN \
  > logs/marion-ingest-$(date +%s).log 2>&1 &

# 2. Restart PDF enricher (the big unlock — fills year_built, beds, baths, sale prices)
nohup npx tsx scripts/enrich-marion-pdf.ts --workers=4 --resolve-legacy \
  > logs/enrich-marion-$(date +%s).log 2>&1 &
# Monitor: tail -f logs/enrich-marion-*.log
# Expected: 8 parcels/sec, ~20 hours for full Marion (~580K parcels)

# 3. Re-run linker after new mortgages land (should also be re-run periodically)
npx tsx scripts/link-mortgage-records.ts --state=IN --county=Marion --strategy=owner

# 4. Restart the agent worker with current code (prompt + tool changes need restart)
pkill -f agent-runner-worker
nohup node scripts/agent-runner-worker.mjs > worker.log 2>&1 &

# 5. Refresh analytics MVs after new data lands
# (run in psql or via pg/query against MXRE)
SELECT refresh_analytics_views();
REFRESH MATERIALIZED VIEW mv_owner_portfolio;
```

---

## What's still missing for Marion "100% deployable"

| Need | Status | How to fill |
|------|--------|-------------|
| Owner names + mailing | ⏳ ~41%, climbing | Let ingest finish |
| Asset class (SFR/MFR/Apt) | ⏳ ~15%, climbing | Same |
| Year built / beds / baths / sale price | ❌ 0.005% | Let PDF enricher run overnight |
| Mortgage / lien details | ✅ 14,885 linked | Re-run linker periodically |
| Foreclosure / lis pendens flags | ✅ live (post-link) | Just refresh `mv_property_distress` |
| MLS listings | ❌ 36 records | Build Redfin/Zillow scraper (needs IPRoyal proxy) |
| Code violations | ❌ 0 records | Couldn't locate Indy ArcGIS code-enf endpoint — manual recon needed |
| Rents | ❌ 0 records | RentCast API (free tier, no scraping needed) is fastest path |
| Skip trace (phones for owners) | ❌ 0 | BatchSkipTracing API ($0.13/skip) for commercial product |

---

## Strategic context (don't lose this)

The session also covered product strategy. Key conclusions to remember:

1. **mundox.ai** stays as your venture-studio brand site. LandingPage is meant to render at that domain.
2. **Buy Box Club** is the product where AI agents ship — NOT a separate SaaS. Add as a Pro tier inside BBC.
3. **MXRE is your moat** — 130M-row warehouse + county recorder integrations is hard to replicate. Don't dilute it by going horizontal.
4. **Jess (already built)** is the email/calendar/LOI agent template. Multi-tenant scoping (add `org_id` to jess_* tables) gets each BBC member their own.
5. **Pricing realism**: keep BBC at $149, absorb AI cost (~$5-8/mo per user with model routing). Pass through skip trace + dialer minutes as overage. Don't try to compete with Sierra/Decagon at $500+ enterprise tiers.
6. **Cost discipline matters**: route to Gemini Flash for triage, Haiku for drafting, Sonnet only when reasoning required. Most "agent runs" should be deterministic code, not LLM.

---

## Open items NOT to drop

- Cloudflare Pages deploy (mundox.ai) — was deployed as Worker (wrong product), needs to be redeployed to Pages instead. Old `mundox-ai` Worker can be deleted.
- VPS service-role JWT + Postgres password rotation (still pending — leaked in shell history, low risk because no public commits, but should rotate when convenient).
- Mundo's uncommitted parallel work in `mxre/`: `curated-ingest.ts`, `overnight-ingest.sh`, `supervisor.sh`, `tiger-bulk-ingest.ts`, etc. (left untouched intentionally — not mine to commit).

---

## One-paragraph context for the next session

> Continuing Marion County / Indianapolis ingestion for the Buy Box Club product. The MXRE data warehouse on Contabo VPS has 583K Marion parcels, with ArcGIS coverage at ~41% on owner names and ~15% on rich fields (climbing as the re-ingest finishes). A property-card PDF enricher exists and is verified working — ~33% hit rate (the other 67% are vacant lots, expected). The orchestrator + linker + analytics views are all wired. Major remaining work: (1) let the enricher run overnight to fill year_built/beds/baths/last_sale_price, (2) build a Redfin or RentCast scraper for listings/rents, (3) find the Indy code-enforcement ArcGIS endpoint for code_violations. Skip Cloudflare deploy chase — the AI agent SaaS strategy is "absorb into BBC at $149, don't build a separate product."
