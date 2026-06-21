#!/usr/bin/env tsx
import "dotenv/config";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

type CrawlState = {
  nextOffset: number;
  minPopulation?: number;
  lastStartedAt?: string;
  lastCompletedAt?: string;
  runs: Array<{
    startedAt: string;
    completedAt: string;
    offset: number;
    targetLimit: number;
    minPopulation?: number;
    exitCode: number | null;
    totalUpserted: number;
    scannedMarkets?: number;
  }>;
};

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const statePath = arg("state-file", join("data", "external", "crexi-rv-crawl-state.json"))!;
const chunks = Math.max(1, Number.parseInt(arg("chunks", "2") ?? "2", 10));
const targetLimit = Math.max(1, Number.parseInt(arg("target-limit", "25") ?? "25", 10));
const requestedMinPopulation = Math.max(0, Number.parseInt(arg("min-population", "50000") ?? "50000", 10));
const maxPages = Math.max(1, Number.parseInt(arg("max-pages", "4") ?? "4", 10));
const maxTotal = Math.max(1, Number.parseInt(arg("max-total", "1000") ?? "1000", 10));
const limit = Math.max(1, Number.parseInt(arg("limit", "60") ?? "60", 10));
const detailLimit = Math.max(0, Number.parseInt(arg("detail-limit", "60") ?? "60", 10));
const initialOffset = Math.max(0, Number.parseInt(arg("initial-offset", "450") ?? "450", 10));
const tsxCli = join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");
const populationBands = [50_000, 25_000, 10_000, 5_000, 0];

function loadState(): CrawlState {
  if (!existsSync(statePath)) {
    return { nextOffset: initialOffset, runs: [] };
  }
  const parsed = JSON.parse(readFileSync(statePath, "utf8")) as Partial<CrawlState>;
  return {
    nextOffset: Number.isFinite(parsed.nextOffset) ? Number(parsed.nextOffset) : initialOffset,
    minPopulation: Number.isFinite(parsed.minPopulation) ? Number(parsed.minPopulation) : requestedMinPopulation,
    lastStartedAt: parsed.lastStartedAt,
    lastCompletedAt: parsed.lastCompletedAt,
    runs: Array.isArray(parsed.runs) ? parsed.runs.slice(-100) as CrawlState["runs"] : [],
  };
}

function saveState(state: CrawlState) {
  mkdirSync(dirname(statePath), { recursive: true });
  writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`);
}

function runCommand(command: string, args: string[]) {
  return spawnSync(command, args, {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout: 900_000,
    windowsHide: true,
  });
}

function nextPopulationBand(current: number): number | null {
  const currentIndex = populationBands.findIndex((band) => band === current);
  if (currentIndex >= 0) return populationBands[currentIndex + 1] ?? null;
  return populationBands.find((band) => band < current) ?? null;
}

const state = loadState();
state.minPopulation ??= requestedMinPopulation;
state.lastStartedAt = new Date().toISOString();
saveState(state);

console.log(JSON.stringify({
  schemaVersion: "mxre.crexiRvContinuation.v1",
  statePath,
  startingOffset: state.nextOffset,
  chunks,
  targetLimit,
  minPopulation: state.minPopulation,
  maxPages,
}, null, 2));

for (let i = 0; i < chunks; i++) {
  const offset = state.nextOffset;
  const minPopulation = state.minPopulation ?? requestedMinPopulation;
  const startedAt = new Date().toISOString();
  console.log(`=== CREXI RV continuation offset=${offset} targetLimit=${targetLimit} minPopulation=${minPopulation} ===`);
  const seed = runCommand(process.execPath, [
    tsxCli,
    "scripts/run-crexi-rv-national-seed.ts",
    "--target-source=uscities",
    `--target-offset=${offset}`,
    `--target-limit=${targetLimit}`,
    `--min-population=${minPopulation}`,
    `--max-total=${maxTotal}`,
    `--limit=${limit}`,
    `--detail-limit=${detailLimit}`,
    `--max-pages=${maxPages}`,
  ]);
  const seedOutput = `${seed.stdout ?? ""}${seed.stderr ?? ""}`;
  process.stdout.write(seedOutput);
  if (seed.error) console.warn(`Seed spawn error at offset ${offset}: ${seed.error.message}`);
  const totalUpserted = Number(seedOutput.match(/"totalUpserted"\s*:\s*(\d+)/)?.[1] ?? 0);
  const scannedMarkets = Number(seedOutput.match(/"scannedMarkets"\s*:\s*(\d+)/)?.[1] ?? 0);

  if (!seed.error) {
    if (seed.status === 0 && scannedMarkets === 0) {
      const nextBand = nextPopulationBand(minPopulation);
      if (nextBand === null) {
        state.nextOffset = offset + targetLimit;
      } else {
        state.minPopulation = nextBand;
        console.log(`No targets remained at minPopulation=${minPopulation}; continuing at minPopulation=${nextBand} from offset=${offset}.`);
      }
    } else {
      state.nextOffset = offset + targetLimit;
    }
  }
  state.runs.push({
    startedAt,
    completedAt: new Date().toISOString(),
    offset,
    targetLimit,
    minPopulation,
    exitCode: seed.status,
    totalUpserted,
    scannedMarkets,
  });
  state.runs = state.runs.slice(-100);
  state.lastCompletedAt = new Date().toISOString();
  saveState(state);

  if (seed.error || seed.status !== 0) {
    console.warn(`Seed chunk failed at offset ${offset}; nextOffset=${state.nextOffset}. Exit code: ${seed.status}`);
    if (seed.error) break;
  }
}

console.log("=== CREXI RV continuation backfill ===");
const backfill = runCommand(process.execPath, [tsxCli, "scripts/backfill-crexi-rv-text-stats.ts"]);
process.stdout.write(`${backfill.stdout ?? ""}${backfill.stderr ?? ""}`);
if (backfill.status !== 0) process.exit(backfill.status ?? 1);

console.log("=== CREXI RV continuation report ===");
const report = runCommand(process.execPath, [tsxCli, "scripts/report-crexi-rv-refresh.ts", "--no-write"]);
process.stdout.write(`${report.stdout ?? ""}${report.stderr ?? ""}`);
if (report.status !== 0) process.exit(report.status ?? 1);

saveState(state);
