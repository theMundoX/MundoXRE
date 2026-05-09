#!/usr/bin/env tsx
/**
 * Repeatable Peoria, IL market coverage refresh.
 *
 * Peoria uses Peoria County public parcels plus shared listing, contact,
 * creative, paid-detail, audit, and dashboard steps. Paid calls are opt-in,
 * property-scoped, cached, address-validated, and bounded.
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
const INCLUDE_PAID = hasFlag("include-paid");
const SKIP_PARCELS = hasFlag("skip-parcels");
const SKIP_LISTINGS = hasFlag("skip-listings");
const SKIP_CLASSIFY = hasFlag("skip-classify");
const SKIP_AGENT_CONTACTS = hasFlag("skip-agent-contacts");
const SKIP_CREATIVE = hasFlag("skip-creative");
const SKIP_AUDITS = hasFlag("skip-audits");
const PAID_MAX_CALLS = Math.max(0, Number(valueArg("paid-max-calls", "0")));
const dryLimit = (value: string, dryValue: string) => DRY_RUN ? dryValue : value;

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const COUNTY_ID = 2338837;
const PEORIA_ZIPS = [
  "61602", "61603", "61604", "61605", "61606", "61607", "61614", "61615",
  "61625",
].join(",");

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
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
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
    child.on("close", code => finish(code ?? 1));
    child.on("error", () => finish(1));
  });
}

async function runStep(step: Step, results: StepResult[]) {
  const startedAt = new Date();
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
      started_at: startedAt.toISOString(),
      finished_at: new Date().toISOString(),
      duration_ms: 0,
      exit_code: null,
    });
    return;
  }

  console.log(`\nRUN ${step.name}`);
  console.log(`$ ${step.command.join(" ")}`);
  const exitCode = await run(step.command, step.timeoutMs);
  const finishedAt = new Date();
  const status = exitCode === 0 ? "ok" : "failed";
  results.push({
    name: step.name,
    command: step.command.join(" "),
    status,
    required: step.required,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    exit_code: exitCode,
  });

  if (exitCode !== 0 && step.required) throw new Error(`Required step failed: ${step.name}`);
}

async function main() {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const logDir = join(repoRoot, "logs", "market-refresh");
  const resultPath = join(logDir, `peoria-il-${stamp}.json`);
  const results: StepResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE Peoria, IL market refresh");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Paid fallback: ${INCLUDE_PAID ? `enabled, max calls ${PAID_MAX_CALLS}` : "disabled"}`);
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Result log: ${resultPath}`);

  await pg("select 1 as ok;");

  const steps: Step[] = [
    {
      name: "Ingest Peoria County public parcels",
      command: ["npx", "tsx", "scripts/ingest-peoria-il.ts"],
      required: true,
      supportsDryRun: false,
      skip: SKIP_PARCELS,
      timeoutMs: DRY_RUN ? 10 * 60_000 : 60 * 60_000,
    },
    {
      name: "Refresh Peoria Redfin listing signals",
      command: ["npx", "tsx", "scripts/ingest-listings-fast.ts", "--state", "IL", "--zips", PEORIA_ZIPS, "--concurrency", dryLimit("3", "1"), ...(DRY_RUN ? ["--dry-run", "--skip-match"] : ["--skip-match"])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS,
      timeoutMs: DRY_RUN ? 10 * 60_000 : 45 * 60_000,
    },
    {
      name: "Link Peoria active listings",
      command: ["npx", "tsx", "scripts/link-market-listings-fast.ts", "--state=IL", "--city=PEORIA", `--county_id=${COUNTY_ID}`, "--create-shells", ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 20 * 60_000,
    },
    {
      name: "Enrich Peoria Redfin listing detail pages",
      command: ["npx", "tsx", "scripts/enrich-redfin-detail-pages.ts", "--state=IL", "--city=PEORIA", `--limit=${dryLimit("5000", "25")}`, `--delay-ms=${dryLimit("300", "100")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_LISTINGS,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 90 * 60_000,
    },
    {
      name: "Classify Peoria active parcel assets",
      command: ["npx", "tsx", "scripts/classify-market-assets.ts", "--state=IL", "--city=PEORIA", `--county_id=${COUNTY_ID}`, "--active-listings-only", `--batch-size=${dryLimit("1500", "250")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_CLASSIFY,
      timeoutMs: DRY_RUN ? 5 * 60_000 : 20 * 60_000,
    },
    {
      name: "Normalize Peoria listing agent contacts",
      command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", "--state=IL", "--city=PEORIA", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AGENT_CONTACTS,
    },
    {
      name: "Score Peoria creative finance signals",
      command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", "--state=IL", "--city=PEORIA", `--limit=${dryLimit("5000", "25")}`, ...(DRY_RUN ? ["--dry-run"] : [])],
      required: false,
      supportsDryRun: true,
      skip: SKIP_CREATIVE,
    },
    {
      name: "Paid Peoria Property Detail enrichment",
      command: [
        "npx", "tsx", "scripts/enrich-on-market-realestateapi.ts",
        "--state=IL", "--city=PEORIA",
        `--limit=${dryLimit(String(Math.max(PAID_MAX_CALLS, 1)), "5")}`,
        `--max-calls=${dryLimit(String(PAID_MAX_CALLS), "0")}`,
        ...(DRY_RUN ? ["--dry-run"] : []),
      ],
      required: false,
      supportsDryRun: true,
      skip: !INCLUDE_PAID,
      timeoutMs: 60 * 60_000,
    },
    {
      name: "Audit Peoria agent coverage",
      command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts", "--state=IL", "--city=PEORIA"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Audit Peoria readiness",
      command: ["npx", "tsx", "scripts/market-readiness-summary.ts", "--state=IL", "--city=PEORIA", `--county_id=${COUNTY_ID}`],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
    },
    {
      name: "Regenerate unified coverage dashboard",
      command: ["npx", "tsx", "scripts/generate-market-coverage-dashboard.ts", "--query-timeout-ms=8000"],
      required: false,
      supportsDryRun: true,
      skip: SKIP_AUDITS,
      timeoutMs: 4 * 60_000,
    },
  ];

  for (const step of steps) await runStep(step, results);

  const finishedAt = new Date();
  const failedRequired = results.some(result => result.required && result.status === "failed");
  const summary = {
    status: failedRequired ? "failed" : "ok",
    market: "peoria-il",
    dry_run: DRY_RUN,
    include_paid: INCLUDE_PAID,
    paid_max_calls: PAID_MAX_CALLS,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));
  console.log("\nPeoria market refresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`OK: ${results.filter(result => result.status === "ok").length}`);
  console.log(`Failed: ${results.filter(result => result.status === "failed").length}`);
  console.log(`Skipped: ${results.filter(result => result.status === "skipped").length}`);
  console.log(`Result log: ${resultPath}`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal Peoria market refresh error:", error instanceof Error ? error.message : error);
  process.exit(1);
});

