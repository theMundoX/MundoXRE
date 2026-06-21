#!/usr/bin/env tsx
import "dotenv/config";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const pages = Math.max(1, Number.parseInt(arg("pages", "8") ?? "8", 10));
const limit = Math.max(1, Number.parseInt(arg("limit", "60") ?? "60", 10));
const detailLimit = Math.max(0, Number.parseInt(arg("detail-limit", "60") ?? "60", 10));
const sinceHours = Math.max(1, Number.parseInt(arg("since-hours", "24") ?? "24", 10));
const reset = process.argv.includes("--reset");
const tsxCli = join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");

function runStep(name: string, args: string[]) {
  console.log(`=== ${name} ===`);
  const result = spawnSync(process.execPath, [tsxCli, ...args], {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout: 900_000,
    windowsHide: true,
  });
  process.stdout.write(result.stdout ?? "");
  process.stderr.write(result.stderr ?? "");
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${name} failed with exit code ${result.status}`);
}

const commonFeedArgs = [
  `--pages=${pages}`,
  `--limit=${limit}`,
  `--detail-limit=${detailLimit}`,
  ...(reset ? ["--reset"] : []),
];

runStep("CREXI multifamily public feed", [
  "scripts/run-crexi-public-feed.ts",
  "--label=multifamily",
  "--asset-class=multifamily",
  "--crexi-public-type=Multifamily",
  ...commonFeedArgs,
]);

runStep("CREXI RV park public feed", [
  "scripts/run-crexi-public-feed.ts",
  "--label=rv-park",
  "--asset-class=mobile_home_rv",
  "--crexi-public-type=Multifamily",
  "--crexi-public-subtype=RV Park",
  ...commonFeedArgs,
]);

runStep("CREXI mobile-home park public feed", [
  "scripts/run-crexi-public-feed.ts",
  "--label=mobile-home-park",
  "--asset-class=mobile_home_rv",
  "--crexi-public-type=Mobile Home Park",
  ...commonFeedArgs,
]);

runStep("CREXI RV text backfill", ["scripts/backfill-crexi-rv-text-stats.ts"]);
runStep("CREXI multifamily stats backfill", ["scripts/backfill-crexi-multifamily-stats.ts"]);
runStep("CREXI class backfill", ["scripts/backfill-crexi-listing-class.ts"]);
runStep("CREXI coverage report", [
  "scripts/report-crexi-rv-refresh.ts",
  "--asset-class=all",
  `--since-hours=${sinceHours}`,
]);
