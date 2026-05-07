#!/usr/bin/env tsx
/**
 * Reusable Columbus, OH market coverage refresh orchestrator.
 *
 * Columbus keeps market-specific public sources for Franklin County, but reuses
 * the same bounded orchestration framework as Dallas/Indianapolis: repeatable
 * steps, dry-run support, paid fallback gates, audits, and unified dashboard
 * regeneration.
 *
 * Usage:
 *   npx tsx scripts/refresh-columbus-market.ts
 *   npx tsx scripts/refresh-columbus-market.ts --dry-run
 *   npx tsx scripts/refresh-columbus-market.ts --include-paid --paid-max-calls=1500
 */

import "dotenv/config";
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");
hydrateWindowsUserEnv();

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);
const valueArg = (name: string, fallback: string) =>
  args.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const DRY_RUN = hasFlag("dry-run");
const SKIP_PARCELS = hasFlag("skip-parcels");
const SKIP_CLASSIFY = hasFlag("skip-classify");
const SKIP_LISTINGS = hasFlag("skip-listings");
const SKIP_AGENT_CONTACTS = hasFlag("skip-agent-contacts");
const SKIP_AGENT_EMAILS = hasFlag("skip-agent-emails");
const SKIP_RECORDER = hasFlag("skip-recorder");
const SKIP_CREATIVE = hasFlag("skip-creative");
const SKIP_RENTS = hasFlag("skip-rents");
const SKIP_AUDITS = hasFlag("skip-audits");
const INCLUDE_PAID = hasFlag("include-paid");
const PAID_MAX_CALLS = Math.max(0, Number(valueArg("paid-max-calls", "0")));
const dryLimit = (value: string, dryValue: string) => DRY_RUN ? dryValue : value;

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const COUNTY_ID = 1698985;
const recorderEnd = new Date().toISOString().split("T")[0];
const recorderStart = (() => {
  const d = new Date();
  d.setDate(d.getDate() - (DRY_RUN ? 1 : 7));
  return d.toISOString().split("T")[0];
})();

type Step = {
  name: string;
  command: string[];
  required: boolean;
  supportsDryRun: boolean;
  skip?: boolean;
  timeoutMs?: number;
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
  try {
    const response = await fetch(PG_URL, {
      method: "POST",
      headers: {
        apikey: PG_KEY,
        Authorization: `Bearer ${PG_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
    return response.json();
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Columbus refresh cannot reach ${PG_URL}. Start the DB tunnel and check local env. Detail: ${detail}`);
  }
}

function run(command: string[], timeoutMs?: number): Promise<number> {
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
    let timeout: NodeJS.Timeout | null = null;
    let settled = false;
    const finish = (code: number) => {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      resolve(code);
    };
    if (timeoutMs && timeoutMs > 0) {
      timeout = setTimeout(() => {
        console.error(`Step timed out after ${Math.round(timeoutMs / 1000)}s: ${command.join(" ")}`);
        child.kill("SIGTERM");
        setTimeout(() => {
          if (!child.killed) child.kill("SIGKILL");
        }, 5_000).unref();
        finish(124);
      }, timeoutMs);
      timeout.unref();
    }
    child.on("close", (code) => finish(code ?? 1));
    child.on("error", () => finish(1));
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
  const exitCode = await run(step.command, step.timeoutMs);
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
  const resultPath = join(logDir, `columbus-oh-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE Columbus, OH market refresh");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Paid fallback: ${INCLUDE_PAID ? `enabled, max calls ${PAID_MAX_CALLS}` : "disabled"}`);
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  await pg("select 1 as ok;");

  const steps: Step[] = [
    {
      name: "Ingest Franklin County public parcels",
      command: ["npx", "tsx", "scripts/ingest-franklin-oh.ts"],
      required: true,
      supportsDryRun: false,
      skip: SKIP_PARCELS,
      timeoutMs: 45 * 60_000,
    },
    {
      name: "Classify Columbus parcel asset types",
      command: ["npx", "tsx", "scripts/classify-market-assets.ts", "--state=OH", "--city=COLUMBUS", `--county_id=${COUNTY_ID}`, `--batch-size=${dryLimit("2500", "250")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: true,
      supportsDryRun: true,
      skip: SKIP_CLASSIFY,
      timeoutMs: DRY_RUN ? 10 * 60_000 : 30 * 60_000,
    },
    {
      name: "Backfill Franklin OH classification details",
      command: ["npx", "tsx", "scripts/backfill-franklin-oh-classification.ts", "--city=COLUMBUS", `--county_id=${COUNTY_ID}`, `--limit=${dryLimit("50000", "500")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_CLASSIFY,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 30 * 60_000,
    },
    {
      name: "Refresh Columbus Redfin listing signals",
      command: ["npx", "tsx", "scripts/ingest-listings-fast.ts", "--state", "OH", "--county", "Franklin", "--concurrency", dryLimit("3", "1"), ...(DRY_RUN ? ["--dry-run", "--skip-match"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS,
      timeoutMs: DRY_RUN ? 10 * 60_000 : 45 * 60_000,
    },
    {
      name: "Enrich Columbus Redfin listing detail pages",
      command: ["npx", "tsx", "scripts/enrich-redfin-detail-pages.ts", "--state=OH", "--city=COLUMBUS", `--limit=${dryLimit("5000", "25")}`, `--delay-ms=${dryLimit("500", "200")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 90 * 60_000,
    },
    {
      name: "Normalize Columbus listing agent contacts",
      command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--state=OH", "--city=COLUMBUS", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AGENT_CONTACTS,
    },
    {
      name: "Bounded public Columbus agent email profiles",
      command: [
        "npx", "tsx", "scripts/enrich-agent-emails-public.ts",
        "--state=OH", "--city=COLUMBUS",
        `--limit=${dryLimit("75", "5")}`,
        `--delay-ms=${dryLimit("750", "200")}`,
        `--max-search-queries=${dryLimit("3", "1")}`,
        `--max-search-links=${dryLimit("4", "2")}`,
        `--max-direct-profile-urls=${dryLimit("2", "1")}`,
        `--max-profile-links-per-page=${dryLimit("3", "2")}`,
        `--fetch-timeout-ms=${dryLimit("5000", "2500")}`,
        `--row-timeout-ms=${dryLimit("15000", "8000")}`,
        ...(DRY_RUN ? ["--dry-run", "--disable-duckduckgo"] : []),
      ],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AGENT_EMAILS,
      timeoutMs: DRY_RUN ? 3 * 60_000 : 25 * 60_000,
    },
    {
      name: "Ingest Franklin County recorder docs",
      command: ["npx", "tsx", "scripts/ingest-oh-publicsearch.ts", "--county=Franklin", `--start=${recorderStart}`, `--end=${recorderEnd}`],
      required: false,
      supportsDryRun: false,
      skip: SKIP_RECORDER,
      timeoutMs: 25 * 60_000,
    },
    {
      name: "Link Franklin mortgages to properties",
      command: ["npx", "tsx", "scripts/link-mortgage-records.ts", "--state=OH", "--county=Franklin", `--limit=${dryLimit("10000", "100")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RECORDER,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 30 * 60_000,
    },
    {
      name: "Score Columbus creative finance signals",
      command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--state=OH", "--city=COLUMBUS", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_CREATIVE,
    },
    {
      name: "Paid Columbus Property Detail enrichment",
      command: [
        "npx", "tsx", "scripts/enrich-on-market-realestateapi.ts",
        "--state=OH", "--city=Columbus",
        `--limit=${dryLimit(String(Math.max(PAID_MAX_CALLS, 1)), "5")}`,
        `--max-calls=${dryLimit(String(PAID_MAX_CALLS), "0")}`,
        ...(DRY_RUN ? ["--dry-run"] : []),
      ],
      required: false,
      supportsDryRun: true,
      skip: !INCLUDE_PAID,
      timeoutMs: 90 * 60_000,
    },
    {
      name: "Paid Columbus Zillow/RapidAPI listing fallback",
      command: [
        "npx", "tsx", "scripts/enrich-on-market-zillow-rapidapi.ts",
        "--state=OH", "--city=Columbus",
        `--limit=${dryLimit(String(Math.max(PAID_MAX_CALLS, 1)), "5")}`,
        `--max-calls=${dryLimit(String(PAID_MAX_CALLS), "0")}`,
        "--concurrency=4",
        ...(DRY_RUN ? ["--dry-run"] : []),
      ],
      required: false,
      supportsDryRun: true,
      skip: !INCLUDE_PAID,
      timeoutMs: 90 * 60_000,
    },
    {
      name: "Discover Columbus multifamily websites",
      command: ["npx", "tsx", "scripts/discover-market-websites-free.ts", "--state=OH", "--city=Columbus", `--county_id=${COUNTY_ID}`, `--limit=${dryLimit("500", "10")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RENTS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 30 * 60_000,
    },
    {
      name: "Refresh Columbus multifamily rent snapshots",
      command: ["npx", "tsx", "scripts/scrape-rents-bulk.ts", "--state=OH", "--city=Columbus", `--county_id=${COUNTY_ID}`, "--stale_days=1", `--limit=${dryLimit("500", "5")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RENTS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 45 * 60_000,
    },
    {
      name: "Audit Columbus agent coverage",
      command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts", "--state=OH", "--city=COLUMBUS"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Audit Columbus rent coverage",
      command: ["npx", "tsx", "scripts/audit-indy-multifamily-rent-coverage.ts", "--state=OH", "--city=COLUMBUS", `--county_id=${COUNTY_ID}`],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Audit Columbus readiness",
      command: ["npx", "tsx", "scripts/market-readiness-summary.ts", "--state=OH", "--city=COLUMBUS", `--county_id=${COUNTY_ID}`],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Regenerate unified coverage dashboard",
      command: ["npx", "tsx", "scripts/generate-market-coverage-dashboard.ts"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
  ];

  for (const step of steps) {
    await runStep(step, results);
  }

  const finishedAt = new Date();
  const failedRequired = results.some((result) => result.required && result.status === "failed");
  const summary = {
    status: failedRequired ? "failed" : "ok",
    market: "columbus-oh",
    dry_run: DRY_RUN,
    include_paid: INCLUDE_PAID,
    paid_max_calls: PAID_MAX_CALLS,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));

  console.log("\nColumbus market refresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`OK: ${results.filter((result) => result.status === "ok").length}`);
  console.log(`Failed: ${results.filter((result) => result.status === "failed").length}`);
  console.log(`Skipped: ${results.filter((result) => result.status === "skipped").length}`);
  console.log(`Result log: ${resultPath}`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal Columbus market refresh error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
