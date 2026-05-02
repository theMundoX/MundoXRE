#!/usr/bin/env tsx
/**
 * Reusable West Chester, PA market refresh orchestrator.
 *
 * Scope starts with West Chester borough / Chester County. The steps are
 * intentionally source-specific so each one can be rerun or debugged directly.
 *
 * Usage:
 *   npx tsx scripts/refresh-west-chester-market.ts
 *   npx tsx scripts/refresh-west-chester-market.ts --dry-run
 *   npx tsx scripts/refresh-west-chester-market.ts --skip-parcels
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

const DRY_RUN = hasFlag("dry-run");
const SKIP_PARCELS = hasFlag("skip-parcels");
const SKIP_CLASSIFY = hasFlag("skip-classify");
const SKIP_WEBSITES = hasFlag("skip-websites");
const SKIP_RENTS = hasFlag("skip-rents");
const SKIP_LISTINGS = hasFlag("skip-listings");
const SKIP_LISTING_QUALITY = hasFlag("skip-listing-quality");

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

type Step = {
  name: string;
  command: string[];
  required: boolean;
  supportsDryRun: boolean;
  skip?: boolean;
};

type StepResult = {
  name: string;
  command: string;
  status: "ok" | "failed" | "skipped";
  required: boolean;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  exit_code: number | null;
};

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

async function resolveCountyId(): Promise<number> {
  const rows = await pg(`
    select id
      from counties
     where state_code = 'PA'
       and upper(county_name) = 'CHESTER'
     order by id
     limit 1;
  `);
  const id = Number(rows[0]?.id ?? 0);
  if (!id) throw new Error("Chester County, PA is not present in counties table. Run parcel ingestion first.");
  return id;
}

function run(command: string[]): Promise<number> {
  return new Promise((resolve) => {
    const usesLocalTsx = command[0] === "npx" && command[1] === "tsx";
    const executable = usesLocalTsx ? process.execPath : command[0];
    const childArgs = usesLocalTsx
      ? [join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs"), ...command.slice(2)]
      : command.slice(1);
    const child = spawn(executable, childArgs, {
      cwd: repoRoot,
      shell: false,
      stdio: "inherit",
      env: process.env,
    });
    child.on("close", (code) => resolve(code ?? 1));
  });
}

async function runStep(step: Step, results: StepResult[]) {
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
    return;
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
    throw new Error(`Required step failed: ${step.name}`);
  }
}

async function main() {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const logDir = join(repoRoot, "logs", "market-refresh");
  const resultPath = join(logDir, `west-chester-pa-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE West Chester, PA market refresh");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  await runStep({
    name: "Ingest Chester County PA public parcel coverage",
    command: ["npx", "tsx", "scripts/ingest-pa-statewide.ts", "--county=Chester"],
    required: true,
    supportsDryRun: false,
    skip: SKIP_PARCELS,
  }, results);

  const countyId = await resolveCountyId();
  console.log(`\nResolved Chester County ID: ${countyId}`);

  const steps: Step[] = [
    {
      name: "Classify West Chester parcel asset types",
      command: ["npx", "tsx", "scripts/classify-market-assets.ts", "--state=PA", "--city=WEST CHESTER", `--county_id=${countyId}`, "--batch-size=1000", ...(DRY_RUN ? ["--dry-run"] : [])],
      required: true,
      supportsDryRun: true,
      skip: SKIP_CLASSIFY,
    },
    {
      name: "Discover West Chester multifamily websites",
      command: ["npx", "tsx", "scripts/discover-websites.ts", "--city=West Chester", "--state=PA", `--county_id=${countyId}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_WEBSITES,
    },
    {
      name: "Refresh West Chester apartment floorplan rents",
      command: ["npx", "tsx", "scripts/scrape-rents-bulk.ts", "--state=PA", "--city=WEST CHESTER", `--county_id=${countyId}`, "--stale_days=1", "--limit=250", ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RENTS,
    },
    {
      name: "Refresh Chester County Redfin listing signals",
      command: ["npx", "tsx", "scripts/ingest-listings-fast.ts", "--state", "PA", "--zips", "19380,19381,19382,19383,19388", "--concurrency", "3"],
      required: false,
      supportsDryRun: false,
      skip: SKIP_LISTINGS,
    },
    {
      name: "Refresh West Chester multi-source listing signals",
      command: ["npx", "tsx", "scripts/daily-listing-scan.ts", "--state", "PA", "--cities", "West Chester"],
      required: false,
      supportsDryRun: false,
      skip: SKIP_LISTINGS,
    },
    {
      name: "Capture West Chester Redfin listing details",
      command: ["npx", "tsx", "scripts/enrich-redfin-detail-pages.ts", "--state=PA", "--city=WEST CHESTER", "--limit=250"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
    },
    {
      name: "Backfill West Chester listing agent contact fields",
      command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--state=PA", "--city=WEST CHESTER", "--limit=10000", ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
    },
    {
      name: "Run verified public agent email enrichment",
      command: ["npx", "tsx", "scripts/enrich-agent-emails-public.ts", "--state=PA", "--city=WEST CHESTER", "--limit=250"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
    },
    {
      name: "Score West Chester creative finance listing descriptions",
      command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--state=PA", "--city=WEST CHESTER", "--limit=10000", ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
    },
    {
      name: "Audit West Chester on-market agent/contact coverage",
      command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts", "--state=PA", "--city=WEST CHESTER"],
      required: false,
      supportsDryRun: false,
      skip: SKIP_LISTINGS || SKIP_LISTING_QUALITY,
    },
    {
      name: "Produce West Chester readiness and coverage metrics",
      command: ["npx", "tsx", "scripts/market-readiness-summary.ts", "--state=PA", "--city=WEST CHESTER", `--county_id=${countyId}`],
      required: false,
      supportsDryRun: false,
    },
  ];

  let failedRequired = false;
  for (const step of steps) {
    try {
      await runStep(step, results);
    } catch (error) {
      failedRequired = true;
      console.error(error instanceof Error ? error.message : error);
      break;
    }
  }

  const finishedAt = new Date();
  failedRequired ||= results.some((result) => result.required && result.status === "failed");
  const summary = {
    market: "west_chester_pa",
    scope: "West Chester borough first; Chester County backing parcel/listing scope",
    county_id: results.some((r) => r.name.includes("Ingest Chester")) ? await resolveCountyId().catch(() => null) : null,
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
  console.log(`Result log: ${resultPath}`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal refresh error:", error);
  process.exit(1);
});
