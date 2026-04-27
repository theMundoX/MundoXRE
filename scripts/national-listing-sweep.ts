#!/usr/bin/env tsx
/**
 * Rent Tracker - national county sweep runner.
 *
 * Runs the existing ZIP-level Redfin ingester county by county, with a JSONL
 * resume log so long sweeps can restart without repeating completed counties.
 *
 * Usage:
 *   npx tsx scripts/national-listing-sweep.ts --dry-run --limit 5
 *   npx tsx scripts/national-listing-sweep.ts --states TX,IN --concurrency 5
 *   npx tsx scripts/national-listing-sweep.ts --resume-file logs/listing-sweep.jsonl
 */

import "dotenv/config";
import { existsSync, mkdirSync, readFileSync, appendFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { spawn } from "node:child_process";
import { getCounties, type County } from "../src/db/queries.js";
import { initProxies, getProxyStats } from "../src/utils/proxy.js";

interface Options {
  states: Set<string> | null;
  limit: number | null;
  offset: number;
  dryRun: boolean;
  perCountyConcurrency: number;
  delayMs: number;
  resumeFile: string;
  force: boolean;
  runMatch: boolean;
}

interface SweepLogEntry {
  key: string;
  state: string;
  county: string;
  started_at: string;
  completed_at: string;
  status: "completed" | "failed";
  exit_code: number | null;
  duration_ms: number;
}

function parseArgs(): Options {
  const args = process.argv.slice(2);
  const opts: Options = {
    states: null,
    limit: null,
    offset: 0,
    dryRun: false,
    perCountyConcurrency: 5,
    delayMs: 2_000,
    resumeFile: join("logs", `listing-sweep-${new Date().toISOString().slice(0, 10)}.jsonl`),
    force: false,
    runMatch: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];
    switch (arg) {
      case "--state":
        opts.states = new Set([(next ?? "").toUpperCase()]);
        i++;
        break;
      case "--states":
        opts.states = new Set((next ?? "").split(",").map((s) => s.trim().toUpperCase()).filter(Boolean));
        i++;
        break;
      case "--limit":
        opts.limit = Math.max(1, parseInt(next ?? "0", 10));
        i++;
        break;
      case "--offset":
        opts.offset = Math.max(0, parseInt(next ?? "0", 10));
        i++;
        break;
      case "--concurrency":
        opts.perCountyConcurrency = Math.max(1, parseInt(next ?? "5", 10));
        i++;
        break;
      case "--delay-ms":
        opts.delayMs = Math.max(0, parseInt(next ?? "2000", 10));
        i++;
        break;
      case "--resume-file":
        opts.resumeFile = next ?? opts.resumeFile;
        i++;
        break;
      case "--dry-run":
        opts.dryRun = true;
        break;
      case "--force":
        opts.force = true;
        break;
      case "--run-match":
        opts.runMatch = true;
        break;
      case "--help":
        printHelp();
        process.exit(0);
    }
  }

  return opts;
}

function printHelp() {
  console.log(`Rent Tracker - national county sweep

Options:
  --state <ST>             Run one state
  --states <A,B>           Run comma-separated states
  --limit <n>              Stop after n counties
  --offset <n>             Skip first n counties after filtering
  --concurrency <n>        ZIP workers per county (default: 5)
  --delay-ms <n>           Pause between counties (default: 2000)
  --resume-file <path>     JSONL progress log
  --dry-run                Fetch and normalize, but do not write
  --force                  Re-run counties already completed in resume log
  --run-match              Let each county run address matching
`);
}

function countyKey(county: County): string {
  return `${county.state_code}:${county.county_name}`;
}

function loadCompleted(resumeFile: string): Set<string> {
  const completed = new Set<string>();
  if (!existsSync(resumeFile)) return completed;

  for (const line of readFileSync(resumeFile, "utf-8").split(/\r?\n/)) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line) as SweepLogEntry;
      if (entry.status === "completed") completed.add(entry.key);
    } catch {
      // Keep going if a prior run was interrupted mid-write.
    }
  }
  return completed;
}

function appendLog(resumeFile: string, entry: SweepLogEntry) {
  mkdirSync(dirname(resumeFile), { recursive: true });
  appendFileSync(resumeFile, `${JSON.stringify(entry)}\n`, "utf-8");
}

function runCounty(county: County, opts: Options): Promise<number | null> {
  return new Promise((resolve) => {
    const tsxCli = join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");
    const args = [
      tsxCli,
      "scripts/ingest-listings-fast.ts",
      "--state",
      county.state_code,
      "--county",
      county.county_name,
      "--concurrency",
      String(opts.perCountyConcurrency),
    ];

    if (opts.dryRun) args.push("--dry-run");
    if (!opts.runMatch) args.push("--skip-match");

    const child = spawn(process.execPath, args, {
      cwd: process.cwd(),
      env: process.env,
      stdio: "inherit",
      shell: false,
    });

    child.on("close", (code) => resolve(code));
    child.on("error", () => resolve(null));
  });
}

async function main() {
  const opts = parseArgs();
  initProxies();

  const proxyStats = getProxyStats();
  const completed = opts.force ? new Set<string>() : loadCompleted(opts.resumeFile);
  const allCounties = await getCounties();

  let counties = allCounties.filter((c) => c.state_code && c.county_name);
  if (opts.states) {
    counties = counties.filter((c) => opts.states?.has(c.state_code));
  }
  counties = counties.slice(opts.offset);
  if (opts.limit) counties = counties.slice(0, opts.limit);
  counties = counties.filter((c) => !completed.has(countyKey(c)));

  console.log("Rent Tracker - National County Sweep");
  console.log("====================================");
  console.log(`Counties queued: ${counties.length}`);
  console.log(`Resume file: ${opts.resumeFile}`);
  console.log(`Dry run: ${opts.dryRun}`);
  console.log(`ZIP concurrency per county: ${opts.perCountyConcurrency}`);
  console.log(`Residential proxies: ${proxyStats.residential.alive}/${proxyStats.residential.total}`);
  console.log(`Datacenter proxies: ${proxyStats.datacenter.alive}/${proxyStats.datacenter.total}`);
  console.log();

  let ok = 0;
  let failed = 0;
  const sweepStart = Date.now();

  for (let i = 0; i < counties.length; i++) {
    const county = counties[i];
    const key = countyKey(county);
    const startedAt = new Date();

    console.log(`\n[${i + 1}/${counties.length}] ${county.state_code} ${county.county_name}`);
    const exitCode = await runCounty(county, opts);
    const durationMs = Date.now() - startedAt.getTime();
    const status = exitCode === 0 ? "completed" : "failed";
    if (status === "completed") ok++;
    else failed++;

    appendLog(opts.resumeFile, {
      key,
      state: county.state_code,
      county: county.county_name,
      started_at: startedAt.toISOString(),
      completed_at: new Date().toISOString(),
      status,
      exit_code: exitCode,
      duration_ms: durationMs,
    });

    console.log(`[${status}] ${key} in ${(durationMs / 1000).toFixed(1)}s`);
    if (opts.delayMs > 0 && i < counties.length - 1) {
      await new Promise((r) => setTimeout(r, opts.delayMs));
    }
  }

  const elapsed = ((Date.now() - sweepStart) / 1000).toFixed(1);
  console.log("\nSweep complete.");
  console.log(`Completed: ${ok}`);
  console.log(`Failed: ${failed}`);
  console.log(`Duration: ${elapsed}s`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
