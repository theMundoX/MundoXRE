#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { makeDbClient } from "./lib/db.ts";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const label = arg("label");
const assetClass = arg("asset-class");
const sweepStartedAt = arg("sweep-started-at");
const dryRun = process.argv.includes("--dry-run");
const writeLog = !process.argv.includes("--no-write");

if (!label) throw new Error("Missing --label.");
if (!assetClass) throw new Error("Missing --asset-class.");
if (!sweepStartedAt) throw new Error("Missing --sweep-started-at.");

async function main() {
  const db = await makeDbClient();
  const before = (await db.query(`
    select
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
      )::int as scoped_rows,
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
          and coalesce(status, 'active') not in ('removed','sold')
          and coalesce(last_seen_at, observed_at, created_at) < $3::timestamptz
      )::int as absent_rows,
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
          and coalesce(status, 'active') not in ('removed','sold')
          and coalesce(last_seen_at, observed_at, created_at) < $3::timestamptz
          and nullif(raw->>'removed_candidate_at', '') is null
      )::int as first_absent_rows,
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
          and coalesce(status, 'active') not in ('removed','sold')
          and coalesce(last_seen_at, observed_at, created_at) < $3::timestamptz
          and nullif(raw->>'removed_candidate_at', '') is not null
      )::int as confirm_removed_rows
    from external_market_listings;
  `, [assetClass, label, sweepStartedAt])).rows[0] ?? {};

  if (!dryRun) {
    await db.query(`
      update external_market_listings
         set status = 'removed',
             raw = coalesce(raw, '{}'::jsonb)
               || jsonb_build_object(
                    'removed_confirmed_at', now(),
                    'removed_confirmed_sweep_started_at', $3::text,
                    'removed_source', 'crexi_public_feed_absent_second_sweep'
                  ),
             updated_at = now()
       where source = 'crexi_rapidapi'
         and asset_class = $1
         and raw->>'crexi_public_scope' = $2
         and coalesce(status, 'active') not in ('removed','sold')
         and coalesce(last_seen_at, observed_at, created_at) < $3::timestamptz
         and nullif(raw->>'removed_candidate_at', '') is not null;
    `, [assetClass, label, sweepStartedAt]);

    await db.query(`
      update external_market_listings
         set status = 'removed_candidate',
             raw = coalesce(raw, '{}'::jsonb)
               || jsonb_build_object(
                    'removed_candidate_at', now(),
                    'removed_candidate_sweep_started_at', $3::text,
                    'removed_source', 'crexi_public_feed_absent_first_sweep'
                  ),
             updated_at = now()
       where source = 'crexi_rapidapi'
         and asset_class = $1
         and raw->>'crexi_public_scope' = $2
         and coalesce(status, 'active') not in ('removed','sold')
         and coalesce(last_seen_at, observed_at, created_at) < $3::timestamptz
         and nullif(raw->>'removed_candidate_at', '') is null;
    `, [assetClass, label, sweepStartedAt]);
  }

  const after = (await db.query(`
    select
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
          and status = 'removed_candidate'
      )::int as removed_candidate_rows,
      count(*) filter (
        where source = 'crexi_rapidapi'
          and asset_class = $1
          and raw->>'crexi_public_scope' = $2
          and status = 'removed'
      )::int as removed_rows
    from external_market_listings;
  `, [assetClass, label])).rows[0] ?? {};
  await db.end();

  const report = {
    schemaVersion: "mxre.crexiFeedRemovals.v1",
    generatedAt: new Date().toISOString(),
    dryRun,
    label,
    assetClass,
    sweepStartedAt,
    before,
    after,
  };

  if (writeLog) {
    const dir = join(process.cwd(), "logs", "crexi-feed-removals");
    await mkdir(dir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    await writeFile(join(dir, `crexi-feed-removals-${label}-${stamp}.json`), `${JSON.stringify(report, null, 2)}\n`);
  }
  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
