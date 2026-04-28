# Indianapolis Market Refresh

MXRE market coverage should be reproducible. One-off fixes are allowed while developing a source adapter, but every repeatable task should become a script and then be added to the daily refresh orchestrator.

## Daily Command

```powershell
npm run market:indy:refresh
```

Dry run:

```powershell
npm run market:indy:refresh:dry
```

The orchestrator writes a JSON run record to:

```text
logs/market-refresh/
```

## Current Refresh Order

1. Classify Indianapolis parcel asset types.
2. Ingest Indianapolis public parcel signals.
3. Seed external CRE observations.
4. Upsert multifamily complex profiles.
5. Refresh on-market listing signals.

Each step stays as a separate source-specific script so it can be rerun, debugged, or replaced without changing the whole pipeline.

## Coverage Tiers

Use these tiers when deciding whether a county or asset class is done:

- Parcel Coverage: every official parcel exists once in `properties`.
- Core Coverage: identity, asset classification, owner, and value are present.
- Underwriting Coverage: core coverage plus physical facts and transaction fields.
- Market Coverage: listing, sale comp, rent comp, mortgage, lien, and public-signal refreshes are current.

## Daily Operating Rule

If a data source matters to Buy Box Club, it should have:

- a reusable script in `scripts/`,
- a dry-run mode when practical,
- deterministic upsert behavior,
- a log or JSON summary,
- dashboard completion metrics that expose remaining gaps.

## Near-Term Gaps

The current `Indianapolis Core` scope is Marion County. The `Indianapolis Metro` scope covers the 11-county Indianapolis-Carmel-Greenwood MSA.

Metro parcel shells exist, but surrounding counties still need assessor enrichment for owner, valuation, physical facts, and transaction fields. The completion dashboard is the source of truth for which county should be worked next.
