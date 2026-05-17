#!/usr/bin/env tsx
/**
 * Repeatable public-first refresh for ZIP-scoped expansion markets.
 *
 * This runner intentionally avoids paid APIs. It is for markets where we have:
 * - a public parcel ingest script
 * - Redfin-derived public listing rows by ZIP
 * - shared detail/contact/creative/dashboard steps
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
const valueArg = (name: string, fallback = "") =>
  args.find(arg => arg.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const DRY_RUN = hasFlag("dry-run");
const SKIP_PARCELS = hasFlag("skip-parcels");
const SKIP_LISTINGS = hasFlag("skip-listings");
const SKIP_DETAIL = hasFlag("skip-detail");
const SKIP_CONTACTS = hasFlag("skip-agent-contacts");
const SKIP_CREATIVE = hasFlag("skip-creative");
const SKIP_AUDITS = hasFlag("skip-audits");
const SKIP_DASHBOARD = hasFlag("skip-dashboard");
const MARKET = valueArg("market");

type Market = {
  key: string;
  label: string;
  state: string;
  city: string;
  countyId: number;
  parcelCommands: Array<{ name: string; command: string[] }>;
  postLinkCommands?: Array<{ name: string; command: string[] }>;
  zips: string[];
  detailLimit: string;
};

const MARKETS: Record<string, Market> = {
  "birmingham-al": {
    key: "birmingham-al",
    label: "Birmingham, AL",
    state: "AL",
    city: "BIRMINGHAM",
    countyId: 1973348,
    parcelCommands: [
      { name: "Ingest Jefferson County public parcels", command: ["npx", "tsx", "scripts/ingest-jefferson-al.ts"] },
      { name: "Ingest Shelby County public parcels", command: ["npx", "tsx", "scripts/ingest-shelby-al.ts"] },
    ],
    postLinkCommands: [
      {
        name: "Relink Shelby County listing shells",
        command: ["npx", "tsx", "scripts/link-market-listings-to-parcels.ts", "--state=AL", "--city=BIRMINGHAM", "--county_id=2338841", "--relink-existing-shells", "--limit=5000"],
      },
    ],
    zips: [
      "35203", "35204", "35205", "35206", "35207", "35208", "35209", "35210", "35211",
      "35212", "35213", "35214", "35215", "35216", "35217", "35218", "35222", "35223",
      "35224", "35226", "35228", "35233", "35234", "35235", "35242", "35243",
    ],
    detailLimit: "2000",
  },
  "detroit-mi": {
    key: "detroit-mi",
    label: "Detroit, MI",
    state: "MI",
    city: "DETROIT",
    countyId: 1973412,
    parcelCommands: [
      { name: "Ingest public parcels", command: ["npx", "tsx", "scripts/ingest-detroit-mi.ts"] },
    ],
    zips: [
      "48201", "48202", "48203", "48204", "48205", "48206", "48207", "48208", "48209",
      "48210", "48211", "48212", "48213", "48214", "48215", "48216", "48217", "48219",
      "48221", "48223", "48224", "48226", "48227", "48228", "48234", "48235", "48238", "48243",
    ],
    detailLimit: "4500",
  },
};

if (!MARKET || !MARKETS[MARKET]) {
  throw new Error(`Usage: npx tsx scripts/refresh-public-zip-market.ts --market=${Object.keys(MARKETS).join("|")}`);
}

const market = MARKETS[MARKET];

type Step = {
  name: string;
  command: string[];
  required: boolean;
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

function run(command: string[], timeoutMs?: number): Promise<number> {
  return new Promise(resolve => {
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
    if (timeoutMs) {
      timeout = setTimeout(() => {
        child.kill("SIGTERM");
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
  if (step.skip) {
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
  const exitCode = DRY_RUN && step.name !== "Regenerate unified coverage dashboard" ? 0 : await run(step.command, step.timeoutMs);
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
  if (status === "failed" && step.required) throw new Error(`Required step failed: ${step.name}`);
}

async function main() {
  const startedAt = new Date();
  const stamp = startedAt.toISOString().replace(/[:.]/g, "-");
  const logDir = join(repoRoot, "logs", "market-refresh");
  const resultPath = join(logDir, `${market.key}-${stamp}.json`);
  const results: StepResult[] = [];
  await mkdir(logDir, { recursive: true });

  console.log(`MXRE ${market.label} public ZIP market refresh`);
  console.log("=".repeat(56));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Paid fallback: disabled`);
  console.log(`Result log: ${resultPath}`);

  const steps: Step[] = [
    ...market.parcelCommands.map(parcelStep => ({
      name: parcelStep.name,
      command: parcelStep.command,
      required: true,
      skip: SKIP_PARCELS,
      timeoutMs: 60 * 60_000,
    })),
    {
      name: "Refresh Redfin listing signals",
      command: ["npx", "tsx", "scripts/ingest-listings-fast.ts", "--state", market.state, "--zips", market.zips.join(","), "--concurrency", "3", "--skip-match", "--allow-partial"],
      required: false,
      skip: SKIP_LISTINGS,
      timeoutMs: 45 * 60_000,
    },
    {
      name: "Link active listings",
      command: ["npx", "tsx", "scripts/link-market-listings-fast.ts", `--state=${market.state}`, `--city=${market.city}`, `--county_id=${market.countyId}`, "--source=redfin", "--create-shells"],
      required: false,
      skip: SKIP_LISTINGS,
      timeoutMs: 20 * 60_000,
    },
    ...(market.postLinkCommands ?? []).map(postLinkStep => ({
      name: postLinkStep.name,
      command: postLinkStep.command,
      required: false,
      skip: SKIP_LISTINGS,
      timeoutMs: 20 * 60_000,
    })),
    {
      name: "Enrich Redfin detail pages",
      command: ["npx", "tsx", "scripts/enrich-redfin-detail-pages.ts", `--state=${market.state}`, `--city=${market.city}`, `--limit=${market.detailLimit}`, "--delay-ms=300"],
      required: false,
      skip: SKIP_DETAIL,
      timeoutMs: 90 * 60_000,
    },
    {
      name: "Normalize listing agent contacts",
      command: ["npx", "tsx", "scripts/enrich-listing-agent-contacts.ts", `--state=${market.state}`, `--city=${market.city}`, `--limit=${market.detailLimit}`],
      required: false,
      skip: SKIP_CONTACTS,
    },
    {
      name: "Score creative finance signals",
      command: ["npx", "tsx", "scripts/score-creative-finance-signals.ts", `--state=${market.state}`, `--city=${market.city}`, `--limit=${market.detailLimit}`],
      required: false,
      skip: SKIP_CREATIVE,
    },
    {
      name: "Audit listing agent coverage",
      command: ["npx", "tsx", "scripts/audit-on-market-agent-coverage.ts", `--state=${market.state}`, `--city=${market.city}`],
      required: false,
      skip: SKIP_AUDITS,
      timeoutMs: 10 * 60_000,
    },
    {
      name: "Audit debt source coverage",
      command: ["npx", "tsx", "scripts/audit-market-debt-source-coverage.ts", `--state=${market.state}`, `--city=${market.city}`, `--county_id=${market.countyId}`],
      required: false,
      skip: SKIP_AUDITS,
      timeoutMs: 10 * 60_000,
    },
    {
      name: "Summarize market readiness",
      command: ["npx", "tsx", "scripts/market-readiness-summary.ts", `--state=${market.state}`, `--city=${market.city}`, `--county_id=${market.countyId}`],
      required: false,
      skip: SKIP_AUDITS,
      timeoutMs: 10 * 60_000,
    },
    {
      name: "Regenerate unified coverage dashboard",
      command: ["npx", "tsx", "scripts/generate-market-coverage-dashboard.ts", "--query-timeout-ms=120000"],
      required: false,
      skip: SKIP_DASHBOARD || DRY_RUN,
      timeoutMs: 10 * 60_000,
    },
  ];

  for (const step of steps) {
    await runStep(step, results);
  }

  const finishedAt = new Date();
  const summary = {
    market: market.key,
    status: results.some(result => result.status === "failed" && result.required) ? "failed" : "ok",
    dry_run: DRY_RUN,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    results,
  };
  await writeFile(resultPath, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
  if (summary.status === "failed") process.exit(1);
}

main().catch(error => {
  console.error("Fatal public ZIP market refresh error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
