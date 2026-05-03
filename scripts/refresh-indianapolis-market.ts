#!/usr/bin/env tsx
/**
 * Reusable Indianapolis market refresh orchestrator.
 *
 * This is intentionally a thin runner around source-specific scripts. The goal is
 * to make the daily job repeatable without hiding the individual data sources.
 *
 * Usage:
 *   npx tsx scripts/refresh-indianapolis-market.ts
 *   npx tsx scripts/refresh-indianapolis-market.ts --dry-run
 *   npx tsx scripts/refresh-indianapolis-market.ts --skip-listings
 */

import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);

const DRY_RUN = hasFlag("dry-run");
const SKIP_LISTINGS = hasFlag("skip-listings");
const SKIP_CLASSIFY = hasFlag("skip-classify");
const SKIP_PUBLIC_SIGNALS = hasFlag("skip-public-signals");
const SKIP_COMPLEX_PROFILES = hasFlag("skip-complex-profiles");
const SKIP_EXTERNAL_CRE = hasFlag("skip-external-cre");
const SKIP_HAMILTON = hasFlag("skip-hamilton");
const SKIP_HENDRICKS = hasFlag("skip-hendricks");
const SKIP_MADISON = hasFlag("skip-madison");
const SKIP_LISTING_QUALITY = hasFlag("skip-listing-quality");
const SKIP_RECORDER_BACKFILL = hasFlag("skip-recorder-backfill");

interface Step {
  name: string;
  command: string[];
  required: boolean;
  supportsDryRun: boolean;
  skip?: boolean;
}

interface StepResult {
  name: string;
  command: string;
  status: "ok" | "failed" | "skipped";
  required: boolean;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  exit_code: number | null;
}

const steps: Step[] = [
  {
    name: "Classify Indianapolis parcel asset types",
    command: ["npx", "tsx", "scripts/classify-indy-assets.ts", "--batch-size=1000", ...(DRY_RUN ? ["--dry-run"] : [])],
    required: true,
    supportsDryRun: true,
    skip: SKIP_CLASSIFY,
  },
  {
    name: "Ingest Indianapolis public parcel signals",
    command: ["npx", "tsx", "scripts/ingest-indy-public-signals.ts", ...(DRY_RUN ? ["--dry-run"] : [])],
    required: true,
    supportsDryRun: true,
    skip: SKIP_PUBLIC_SIGNALS,
  },
  {
    name: "Enrich Hamilton County assessor coverage",
    command: ["npx", "tsx", "scripts/enrich-hamilton-in-assessor.ts", ...(DRY_RUN ? ["--dry-run", "--limit=10000"] : [])],
    required: false,
    supportsDryRun: true,
    skip: SKIP_HAMILTON,
  },
  {
    name: "Enrich Hendricks County assessor coverage",
    command: ["npx", "tsx", "scripts/enrich-hendricks-in-assessor.ts", ...(DRY_RUN ? ["--dry-run", "--limit=10000"] : [])],
    required: false,
    supportsDryRun: true,
    skip: SKIP_HENDRICKS,
  },
  {
    name: "Enrich Madison County parcel coverage",
    command: ["npx", "tsx", "scripts/enrich-madison-in-parcels.ts", ...(DRY_RUN ? ["--dry-run", "--limit=10000"] : [])],
    required: false,
    supportsDryRun: true,
    skip: SKIP_MADISON,
  },
  {
    name: "Seed external CRE market observations",
    command: ["npx", "tsx", "scripts/seed-indy-external-cre-listings.ts"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_EXTERNAL_CRE,
  },
  {
    name: "Upsert multifamily complex profiles",
    command: ["npx", "tsx", "scripts/upsert-complex-profiles.ts"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_COMPLEX_PROFILES,
  },
  {
    name: "Discover Indianapolis apartment websites",
    command: [
      "npx",
      "tsx",
      "scripts/discover-indy-websites-free.ts",
      "--limit=500",
      ...(DRY_RUN ? ["--dry-run"] : []),
    ],
    required: false,
    supportsDryRun: true,
  },
  {
    name: "Refresh apartment floorplan rent availability",
    command: [
      "npx",
      "tsx",
      "scripts/scrape-rents-bulk.ts",
      "--state=IN",
      "--city=INDIANAPOLIS",
      "--stale_days=1",
      "--limit=500",
      ...(DRY_RUN ? ["--dry-run"] : []),
    ],
    required: false,
    supportsDryRun: true,
  },
  {
    name: "Refresh on-market Redfin listing signals",
    command: ["npx", "tsx", "scripts/ingest-listings-fast.ts"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS,
  },
  {
    name: "Refresh Indianapolis Movoto listing/contact signals",
    command: ["npx", "tsx", "scripts/ingest-movoto-indy.ts"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS,
  },
  {
    name: "Refresh Indianapolis multi-source listing signals",
    command: ["npx", "tsx", "scripts/daily-listing-scan.ts", "--state", "IN", "--cities", "Indianapolis"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS,
  },
  {
    name: "Backfill listing agent contact fields",
    command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--limit=10000"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
  },
  {
    name: "Score creative finance listing descriptions",
    command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--limit=10000"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
  },
  {
    name: "Audit on-market agent/contact coverage",
    command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts"],
    required: false,
    supportsDryRun: false,
    skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
  },
  {
    name: "Backfill Marion recorder rows by active owner names",
    command: [
      "npx",
      "tsx",
      "scripts/fidlar-investor-lien-search.ts",
      "--name-source=on-market",
      "--from-year=2020",
      "--limit=1000",
      ...(DRY_RUN ? ["--dry-run"] : []),
    ],
    required: false,
    supportsDryRun: true,
    skip: SKIP_RECORDER_BACKFILL,
  },
  {
    name: "Link Marion recorder rows to properties",
    command: [
      "npx",
      "tsx",
      "scripts/link-mortgage-records.ts",
      "--state=IN",
      "--county=Marion",
      "--limit=10000",
      ...(DRY_RUN ? ["--dry-run"] : []),
    ],
    required: false,
    supportsDryRun: true,
    skip: SKIP_RECORDER_BACKFILL,
  },
];

function run(command: string[]): Promise<number> {
  return new Promise((resolve) => {
    const child = spawn(command[0], command.slice(1), {
      cwd: repoRoot,
      shell: process.platform === "win32",
      stdio: "inherit",
      env: process.env,
    });
    child.on("close", (code) => resolve(code ?? 1));
  });
}

async function main() {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const logDir = join(repoRoot, "logs", "market-refresh");
  const resultPath = join(logDir, `indianapolis-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE Indianapolis market refresh");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  for (const step of steps) {
    const stepStartedAt = new Date();

    const skipReason = step.skip
      ? "flag"
      : DRY_RUN && !step.supportsDryRun
        ? "dry-run unsupported"
        : null;

    if (skipReason) {
      console.log(`\nSKIP ${step.name} (${skipReason})`);
      results.push({
        name: step.name,
        command: step.command.join(" "),
        status: "skipped",
        required: step.required,
        started_at: stepStartedAt.toISOString(),
        finished_at: new Date().toISOString(),
        duration_ms: 0,
        exit_code: null,
      });
      continue;
    }

    console.log(`\nRUN ${step.name}`);
    console.log(`$ ${step.command.join(" ")}`);
    const exitCode = await run(step.command);
    const stepFinishedAt = new Date();
    const status = exitCode === 0 ? "ok" : "failed";

    results.push({
      name: step.name,
      command: step.command.join(" "),
      status,
      required: step.required,
      started_at: stepStartedAt.toISOString(),
      finished_at: stepFinishedAt.toISOString(),
      duration_ms: stepFinishedAt.getTime() - stepStartedAt.getTime(),
      exit_code: exitCode,
    });

    if (exitCode !== 0 && step.required) {
      console.error(`\nRequired step failed: ${step.name}`);
      break;
    }
  }

  const finishedAt = new Date();
  const failedRequired = results.some((result) => result.required && result.status === "failed");
  const summary = {
    market: "indianapolis",
    status: failedRequired ? "failed" : "ok",
    dry_run: DRY_RUN,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    steps: results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));
  console.log("\nRefresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`Steps: ${results.filter((r) => r.status === "ok").length} ok, ${results.filter((r) => r.status === "failed").length} failed, ${results.filter((r) => r.status === "skipped").length} skipped`);
  console.log(`Duration: ${Math.round(summary.duration_ms / 1000)}s`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal refresh error:", error);
  process.exit(1);
});
