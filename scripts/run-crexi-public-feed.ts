#!/usr/bin/env tsx
import "dotenv/config";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

type FeedState = {
  nextPage: number;
  pageSize?: number;
  lastStartedAt?: string;
  lastCompletedAt?: string;
  lastTotalCount?: number | null;
  emptyPages: number;
  currentSweepStartedAt?: string;
  completedSweepAt?: string;
  runs: Array<{
    startedAt: string;
    completedAt: string;
    page: number;
    exitCode: number | null;
    searchRows: number;
    matched: number;
    upserted: number;
    totalCount: number | null;
  }>;
};

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const label = (arg("label", "multifamily") ?? "multifamily").toLowerCase();
const assetClass = (arg("asset-class", "multifamily") ?? "multifamily").toLowerCase();
const publicType = arg("crexi-public-type", "Multifamily") ?? "Multifamily";
const publicSubtype = arg("crexi-public-subtype");
const statePath = arg("state-file", join("data", "external", `crexi-${label}-public-feed-state.json`))!;
const pages = Math.max(1, Number.parseInt(arg("pages", "10") ?? "10", 10));
const maxEmptyPages = Math.max(1, Number.parseInt(arg("max-empty-pages", "3") ?? "3", 10));
const initialPage = Math.max(1, Number.parseInt(arg("initial-page", "1") ?? "1", 10));
const limit = Math.max(1, Number.parseInt(arg("limit", "60") ?? "60", 10));
const detailLimit = Math.max(0, Number.parseInt(arg("detail-limit", "60") ?? "60", 10));
const reset = process.argv.includes("--reset");
const tsxCli = join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");

function loadState(): FeedState {
  if (reset || !existsSync(statePath)) return { nextPage: initialPage, emptyPages: 0, runs: [] };
  const parsed = JSON.parse(readFileSync(statePath, "utf8")) as Partial<FeedState>;
  const parsedPageSize = Number.isFinite(parsed.pageSize) ? Number(parsed.pageSize) : null;
  if (parsedPageSize === null) {
    return { nextPage: initialPage, pageSize: limit, emptyPages: 0, runs: Array.isArray(parsed.runs) ? parsed.runs.slice(-500) as FeedState["runs"] : [] };
  }
  if (parsedPageSize !== null && parsedPageSize !== limit) {
    return { nextPage: initialPage, pageSize: limit, emptyPages: 0, runs: Array.isArray(parsed.runs) ? parsed.runs.slice(-500) as FeedState["runs"] : [] };
  }
  return {
    nextPage: Number.isFinite(parsed.nextPage) ? Number(parsed.nextPage) : initialPage,
    pageSize: parsedPageSize ?? limit,
    lastStartedAt: parsed.lastStartedAt,
    lastCompletedAt: parsed.lastCompletedAt,
    lastTotalCount: Number.isFinite(parsed.lastTotalCount) ? Number(parsed.lastTotalCount) : null,
    emptyPages: Number.isFinite(parsed.emptyPages) ? Number(parsed.emptyPages) : 0,
    currentSweepStartedAt: parsed.currentSweepStartedAt,
    completedSweepAt: parsed.completedSweepAt,
    runs: Array.isArray(parsed.runs) ? parsed.runs.slice(-500) as FeedState["runs"] : [],
  };
}

function saveState(state: FeedState) {
  mkdirSync(dirname(statePath), { recursive: true });
  writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`);
}

function runPage(page: number) {
  const args = [
    tsxCli,
    "scripts/ingest-crexi-rapidapi.ts",
    `--asset-class=${assetClass}`,
    `--market=${label}`,
    `--limit=${limit}`,
    `--detail-limit=${detailLimit}`,
    `--page=${page}`,
    `--crexi-public-type=${publicType}`,
    `--crexi-public-scope=${label}`,
  ];
  if (state.currentSweepStartedAt) args.push(`--sweep-started-at=${state.currentSweepStartedAt}`);
  if (publicSubtype) args.push(`--crexi-public-subtype=${publicSubtype}`);
  return spawnSync(process.execPath, args, {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout: 300_000,
    windowsHide: true,
  });
}

const state = loadState();
state.pageSize = limit;
state.lastStartedAt = new Date().toISOString();
if (!state.currentSweepStartedAt || state.nextPage <= initialPage) {
  state.currentSweepStartedAt = state.lastStartedAt;
}
saveState(state);

console.log(JSON.stringify({
  schemaVersion: "mxre.crexiPublicFeed.v1",
  label,
  assetClass,
  publicType,
  publicSubtype: publicSubtype ?? null,
  statePath,
  startingPage: state.nextPage,
  pages,
  limit,
  detailLimit,
  reset,
  currentSweepStartedAt: state.currentSweepStartedAt,
}, null, 2));

function markRemovals() {
  const sweepStartedAt = state.currentSweepStartedAt;
  if (!sweepStartedAt) return;
  const result = spawnSync(process.execPath, [
    tsxCli,
    "scripts/mark-crexi-feed-removals.ts",
    `--label=${label}`,
    `--asset-class=${assetClass}`,
    `--sweep-started-at=${sweepStartedAt}`,
  ], {
    cwd: process.cwd(),
    encoding: "utf8",
    timeout: 180_000,
    windowsHide: true,
  });
  process.stdout.write(result.stdout ?? "");
  process.stderr.write(result.stderr ?? "");
  if (result.error || result.status !== 0) {
    throw result.error ?? new Error(`mark-crexi-feed-removals failed with exit code ${result.status}`);
  }
}

for (let i = 0; i < pages; i++) {
  if (state.emptyPages >= maxEmptyPages) break;
  const page = state.nextPage;
  const startedAt = new Date().toISOString();
  console.log(`=== CREXI public feed label=${label} page=${page} ===`);
  const result = runPage(page);
  const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
  process.stdout.write(output);

  const searchRows = Number(output.match(/Search rows:\s*(\d+)/)?.[1] ?? 0);
  const matched = Number(output.match(/"matched"\s*:\s*(\d+)/)?.[1] ?? 0);
  const upserted = Number(output.match(/"upserted"\s*:\s*(\d+)/)?.[1] ?? 0);
  const totalCountRaw = output.match(/"publicSearchTotal"\s*:\s*(\d+|null)/)?.[1] ?? null;
  const totalCount = totalCountRaw && totalCountRaw !== "null" ? Number(totalCountRaw) : null;

  if (!result.error && result.status === 0) {
    state.nextPage = page + 1;
    state.emptyPages = searchRows === 0 ? state.emptyPages + 1 : 0;
    state.lastTotalCount = totalCount;
    if (searchRows > 0 && searchRows < limit) {
      state.completedSweepAt = new Date().toISOString();
      markRemovals();
      state.nextPage = initialPage;
      state.emptyPages = 0;
      state.currentSweepStartedAt = state.completedSweepAt;
    }
  }

  state.runs.push({
    startedAt,
    completedAt: new Date().toISOString(),
    page,
    exitCode: result.status,
    searchRows,
    matched,
    upserted,
    totalCount,
  });
  state.runs = state.runs.slice(-500);
  state.lastCompletedAt = new Date().toISOString();
  saveState(state);

  if (result.error || result.status !== 0) {
    console.warn(`CREXI public feed failed at label=${label} page=${page}. Exit code: ${result.status}`);
    if (result.error) break;
  }
  if (searchRows > 0 && searchRows < limit) break;
}

if (state.emptyPages >= maxEmptyPages) {
  state.completedSweepAt = new Date().toISOString();
  markRemovals();
  state.nextPage = initialPage;
  state.emptyPages = 0;
  state.currentSweepStartedAt = state.completedSweepAt;
  saveState(state);
}

console.log(JSON.stringify({
  label,
  statePath,
  nextPage: state.nextPage,
  emptyPages: state.emptyPages,
  lastTotalCount: state.lastTotalCount ?? null,
  currentSweepStartedAt: state.currentSweepStartedAt ?? null,
  completedSweepAt: state.completedSweepAt ?? null,
  stopped: state.emptyPages >= maxEmptyPages,
}, null, 2));
