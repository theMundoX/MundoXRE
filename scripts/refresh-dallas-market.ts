#!/usr/bin/env tsx
/**
 * Reusable Dallas, TX market coverage refresh orchestrator.
 *
 * This runner keeps Dallas enrichment repeatable without hiding the underlying
 * source-specific scripts. It intentionally uses public/legal source scripts
 * and keeps paid API fallbacks outside the default path.
 *
 * Usage:
 *   npx tsx scripts/refresh-dallas-market.ts
 *   npx tsx scripts/refresh-dallas-market.ts --dry-run
 *   npx tsx scripts/refresh-dallas-market.ts --skip-rents
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
const SKIP_CLASSIFY = hasFlag("skip-classify");
const SKIP_REDFIN_DETAILS = hasFlag("skip-redfin-details");
const SKIP_AGENT_CONTACTS = hasFlag("skip-agent-contacts");
const SKIP_AGENT_EMAILS = hasFlag("skip-agent-emails");
const SKIP_RECORDER = hasFlag("skip-recorder");
const SKIP_CREATIVE = hasFlag("skip-creative");
const SKIP_RENTS = hasFlag("skip-rents");
const SKIP_AUDITS = hasFlag("skip-audits");
const dryLimit = (value: string, dryValue: string) => DRY_RUN ? dryValue : value;
const RECORDER_DAYS = dryLimit("7", "1");

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

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
    throw new Error(`Dallas refresh cannot reach ${PG_URL}. Check SUPABASE_URL/pg-query service or MXRE_DIRECT_PG_URL. Detail: ${detail}`);
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
  const resultPath = join(logDir, `dallas-tx-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE Dallas, TX market refresh");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  await pg("select 1 as ok;");

  const steps: Step[] = [
    {
      name: "Classify Dallas parcel asset types",
      command: ["npx", "tsx", "scripts/classify-market-assets.ts", "--state=TX", "--city=DALLAS", "--county_id=7", `--batch-size=${dryLimit("2500", "250")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: true,
      supportsDryRun: true,
      skip: SKIP_CLASSIFY,
    },
    {
      name: "Enrich Dallas Redfin listing detail pages",
      command: ["npx", "tsx", "scripts/enrich-redfin-detail-pages.ts", "--state=TX", "--city=DALLAS", `--limit=${dryLimit("5000", "5")}`, `--delay-ms=${dryLimit("500", "250")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_REDFIN_DETAILS,
    },
    {
      name: "Normalize Dallas listing agent contacts",
      command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--state=TX", "--city=DALLAS", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AGENT_CONTACTS,
    },
    {
      name: "Search public Dallas agent email profiles",
      command: [
        "npx", "tsx", "scripts/enrich-agent-emails-public.ts",
        "--state=TX", "--city=DALLAS",
        `--limit=${dryLimit("500", "5")}`,
        `--delay-ms=${dryLimit("1500", "250")}`,
        `--max-search-queries=${dryLimit("6", "2")}`,
        `--max-search-links=${dryLimit("10", "4")}`,
        `--max-direct-profile-urls=${dryLimit("4", "1")}`,
        `--max-profile-links-per-page=${dryLimit("6", "3")}`,
        `--fetch-timeout-ms=${dryLimit("8000", "3000")}`,
        `--row-timeout-ms=${dryLimit("45000", "15000")}`,
        ...(DRY_RUN ? ["--dry-run", "--disable-duckduckgo"] : []),
      ],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AGENT_EMAILS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 90 * 60_000,
    },
    {
      name: "Ingest Dallas County recorded deeds and mortgages",
      command: ["npx", "tsx", "scripts/ingest-recorder-tx.ts", "--county=Dallas", `--days=${RECORDER_DAYS}`, `--max-docs=${dryLimit("1000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RECORDER,
      timeoutMs: 20 * 60_000,
    },
    {
      name: "Score Dallas creative finance signals",
      command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--state=TX", "--city=DALLAS", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_CREATIVE,
    },
    {
      name: "Refresh Dallas multifamily rent snapshots",
      command: ["npx", "tsx", "scripts/scrape-rents-bulk.ts", "--city=Dallas", "--state=TX", "--county_id=7", `--limit=${dryLimit("250", "5")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_RENTS,
    },
    {
      name: "Audit Dallas agent coverage",
      command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts", "--state=TX", "--city=DALLAS"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Audit Dallas readiness",
      command: ["npx", "tsx", "scripts/market-readiness-summary.ts", "--state=TX", "--city=DALLAS", "--county_id=7"],
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
    dry_run: DRY_RUN,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));

  console.log("\nDallas market refresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`OK: ${results.filter((result) => result.status === "ok").length}`);
  console.log(`Failed: ${results.filter((result) => result.status === "failed").length}`);
  console.log(`Skipped: ${results.filter((result) => result.status === "skipped").length}`);
  console.log(`Result log: ${resultPath}`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal Dallas market refresh error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
