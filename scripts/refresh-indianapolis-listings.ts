#!/usr/bin/env tsx
/**
 * Fast Indianapolis on-market listing refresh.
 *
 * This runner is intentionally separate from the full market refresh. Listing
 * freshness should not wait behind long-running parcel, assessor, rent, or
 * recorder jobs.
 *
 * Usage:
 *   npx tsx scripts/refresh-indianapolis-listings.ts
 *   npx tsx scripts/refresh-indianapolis-listings.ts --skip-agents
 *   npx tsx scripts/refresh-indianapolis-listings.ts --skip-reapi
 *   npx tsx scripts/refresh-indianapolis-listings.ts --skip-quality
 */

import "dotenv/config";
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");
const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);

const SKIP_AGENTS = hasFlag("skip-agents");
const SKIP_REAPI = hasFlag("skip-reapi");
const SKIP_QUALITY = hasFlag("skip-quality");

interface Step {
  name: string;
  command: string[];
  required: boolean;
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
    name: "Refresh Indianapolis Movoto listing/contact signals",
    command: ["npx", "tsx", "scripts/ingest-movoto-indy.ts"],
    required: false,
  },
  {
    name: "Refresh Indianapolis multi-source listing signals",
    command: ["npx", "tsx", "scripts/daily-listing-scan.ts", "--state", "IN", "--cities", "Indianapolis"],
    required: false,
  },
  {
    name: "Backfill active listings with RealEstateAPI PropertyDetail",
    command: [
      "npx",
      "tsx",
      "scripts/enrich-on-market-realestateapi.ts",
      "--city=Indianapolis",
      "--state=IN",
      "--limit=1000",
      "--max-calls=1000",
    ],
    required: false,
    skip: SKIP_REAPI,
  },
  {
    name: "Backfill listing agent contact fields",
    command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--limit=10000"],
    required: false,
    skip: SKIP_AGENTS || SKIP_QUALITY,
  },
  {
    name: "Score creative finance listing descriptions",
    command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--limit=10000"],
    required: false,
    skip: SKIP_QUALITY,
  },
  {
    name: "Audit on-market agent/contact coverage",
    command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts"],
    required: false,
    skip: SKIP_QUALITY,
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
    child.on("error", () => resolve(1));
  });
}

async function main() {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const logDir = join(repoRoot, "logs", "market-refresh");
  const resultPath = join(logDir, `indianapolis-listings-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE Indianapolis listing refresh");
  console.log("=".repeat(48));
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  for (const step of steps) {
    const stepStartedAt = new Date();

    if (step.skip) {
      console.log(`\nSKIP ${step.name} (flag)`);
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
    domain: "on_market_listings",
    status: failedRequired ? "failed" : "ok",
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    steps: results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));

  console.log("\nListing refresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`Steps: ${results.filter((r) => r.status === "ok").length} ok, ${results.filter((r) => r.status === "failed").length} failed, ${results.filter((r) => r.status === "skipped").length} skipped`);
  console.log(`Duration: ${Math.round(summary.duration_ms / 1000)}s`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal listing refresh error:", error);
  process.exit(1);
});
