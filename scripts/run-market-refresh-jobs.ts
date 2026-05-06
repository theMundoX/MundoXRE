#!/usr/bin/env tsx
/**
 * Runs configured MXRE market refresh jobs without any LLM dependency.
 *
 * This is the durable scheduler target for local Task Scheduler, cron, or
 * systemd timers. Add new markets to config/market-refresh-jobs.json as their
 * refresh scripts become available.
 */

import "dotenv/config";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");

interface MarketRefreshJob {
  id: string;
  label: string;
  enabled: boolean;
  required?: boolean;
  continueOnFailure?: boolean;
  command: string[];
}

interface MarketRefreshConfig {
  version: number;
  jobs: MarketRefreshJob[];
}

interface JobResult {
  id: string;
  label: string;
  command: string;
  status: "ok" | "failed" | "skipped";
  required: boolean;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  exit_code: number | null;
}

function argValue(name: string): string | null {
  const prefix = `--${name}=`;
  const hit = process.argv.slice(2).find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : null;
}

function hasFlag(name: string): boolean {
  return process.argv.slice(2).includes(`--${name}`);
}

async function loadConfig(): Promise<MarketRefreshConfig> {
  const configPath = argValue("config") ?? join(repoRoot, "config", "market-refresh-jobs.json");
  const raw = await readFile(configPath, "utf8");
  const parsed = JSON.parse(raw) as MarketRefreshConfig;

  if (!Array.isArray(parsed.jobs)) {
    throw new Error(`Invalid market refresh config: jobs must be an array (${configPath})`);
  }

  for (const job of parsed.jobs) {
    if (!job.id || !job.label || !Array.isArray(job.command) || job.command.length === 0) {
      throw new Error(`Invalid market refresh job: ${JSON.stringify(job)}`);
    }
  }

  return parsed;
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
    child.on("error", () => resolve(1));
  });
}

async function main() {
  const startedAt = new Date();
  const only = argValue("only");
  const dryRun = hasFlag("dry-run");
  const config = await loadConfig();
  const logDir = join(repoRoot, "logs", "market-refresh");
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const resultPath = join(logDir, `daily-market-refresh-${stamp}.json`);
  const results: JobResult[] = [];

  await mkdir(logDir, { recursive: true });

  console.log("MXRE daily market refresh");
  console.log("=".repeat(48));
  console.log(`Started: ${startedAt.toISOString()}`);
  console.log(`Dry run: ${dryRun}`);
  console.log(`Only: ${only ?? "all enabled jobs"}`);
  console.log(`Result log: ${resultPath}`);

  for (const job of config.jobs) {
    const jobStartedAt = new Date();
    const required = job.required !== false;
    const command = dryRun && !job.command.includes("--dry-run")
      ? [...job.command, "--dry-run"]
      : job.command;

    if (!job.enabled || (only && only !== job.id)) {
      results.push({
        id: job.id,
        label: job.label,
        command: command.join(" "),
        status: "skipped",
        required,
        started_at: jobStartedAt.toISOString(),
        finished_at: new Date().toISOString(),
        duration_ms: 0,
        exit_code: null,
      });
      continue;
    }

    console.log(`\nRUN ${job.label}`);
    console.log(`$ ${command.join(" ")}`);
    const exitCode = await run(command);
    const jobFinishedAt = new Date();
    const status = exitCode === 0 ? "ok" : "failed";

    results.push({
      id: job.id,
      label: job.label,
      command: command.join(" "),
      status,
      required,
      started_at: jobStartedAt.toISOString(),
      finished_at: jobFinishedAt.toISOString(),
      duration_ms: jobFinishedAt.getTime() - jobStartedAt.getTime(),
      exit_code: exitCode,
    });

    if (status === "failed" && required && job.continueOnFailure === false) {
      console.error(`Required market refresh failed: ${job.label}`);
      break;
    } else if (status === "failed" && required) {
      console.error(`Required market refresh failed: ${job.label}; continuing to remaining market jobs.`);
    }
  }

  const finishedAt = new Date();
  const failedRequired = results.some((result) => result.required && result.status === "failed");
  const summary = {
    status: failedRequired ? "failed" : "ok",
    dry_run: dryRun,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    results,
  };

  await writeFile(resultPath, JSON.stringify(summary, null, 2));

  console.log("\nDaily market refresh summary");
  console.log("=".repeat(48));
  console.log(`Status: ${summary.status}`);
  console.log(`OK: ${results.filter((result) => result.status === "ok").length}`);
  console.log(`Failed: ${results.filter((result) => result.status === "failed").length}`);
  console.log(`Skipped: ${results.filter((result) => result.status === "skipped").length}`);

  if (failedRequired) process.exit(1);
}

main().catch((error) => {
  console.error("Fatal daily market refresh error:", error);
  process.exit(1);
});
