#!/usr/bin/env tsx
import "dotenv/config";
import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const OUT = process.argv.find(a => a.startsWith("--out="))?.split("=").slice(1).join("=")
  ?? "logs/market-refresh/market-coverage-dashboard.html";
const QUERY_TIMEOUT_MS = Math.max(
  5_000,
  Number(process.argv.find(a => a.startsWith("--query-timeout-ms="))?.split("=")[1] ?? "20000"),
);

type Row = Record<string, unknown>;
type MarketConfig = {
  key: string;
  label: string;
  city: string;
  state: string;
  countyId?: number;
  countyName?: string;
  propertyCityLike?: string;
  listingCity?: string;
  propertyScope?: "city_situs" | "active_listing_properties";
  listingScopeNote: string;
  parcelScopeNote: string;
  recorderSourcePattern?: string;
  sourceDiscoveryOnly?: boolean;
  targetCountyHints?: string[];
  progressFiles?: string[];
  rerunCommands: string[];
};

type RefreshJobStatus = {
  marketKey: string;
  state: "running" | "queued" | "idle";
  detail: string;
  latestLog: string | null;
  latestLogAt: string | null;
};

const MARKETS: MarketConfig[] = [
  {
    key: "dallas-tx",
    label: "Dallas, TX",
    city: "DALLAS",
    state: "TX",
    countyId: 7,
    countyName: "Dallas",
    propertyCityLike: "DALLAS",
    listingCity: "DALLAS",
    listingScopeNote: "Current on-market rows are source-limited Redfin/paid-enrichment rows, not a guaranteed full MLS feed.",
    parcelScopeNote: "Dallas County parcel/account rows are tracked separately from the Dallas city situs subset.",
    recorderSourcePattern: "dallas.tx.publicsearch.us",
    progressFiles: ["realestateapi-dallas-tx-progress.html", "dallas-reapi-progress.html"],
    rerunCommands: [
      "npm run market:dallas:refresh",
      "npm run market:dallas:refresh -- --include-paid --paid-max-calls=1500",
    ],
  },
  {
    key: "indianapolis-in",
    label: "Indianapolis, IN",
    city: "INDIANAPOLIS",
    state: "IN",
    countyId: 797583,
    countyName: "Marion",
    propertyCityLike: "INDIANAPOLIS",
    listingCity: "INDIANAPOLIS",
    listingScopeNote: "Indianapolis combines Redfin, Movoto, and local refresh sources where available.",
    parcelScopeNote: "Core dashboard view uses Marion County / Indianapolis city coverage.",
    recorderSourcePattern: "inmarion.fidlar.com/INMarion",
    progressFiles: ["realestateapi-indianapolis-in-progress.html", "indianapolis-reapi-progress.html"],
    rerunCommands: [
      "npm run market:indy:refresh",
      "npm run market:indy:listings",
    ],
  },
  {
    key: "columbus-oh",
    label: "Columbus, OH",
    city: "COLUMBUS",
    state: "OH",
    countyId: 1698985,
    countyName: "Franklin",
    propertyCityLike: "COLUMBUS",
    listingCity: "COLUMBUS",
    listingScopeNote: "Columbus is a pilot market and currently source-limited.",
    parcelScopeNote: "Franklin County parcel coverage is tracked separately from Columbus city situs rows.",
    recorderSourcePattern: "franklin.oh.publicsearch",
    progressFiles: ["realestateapi-columbus-oh-progress.html", "columbus-reapi-progress.html"],
    rerunCommands: [
      "npm run market:columbus:refresh -- --include-paid --paid-max-calls=1500",
      "npx tsx scripts/market-readiness-summary.ts --city=Columbus --state=OH --county_id=1698985",
    ],
  },
  {
    key: "west-chester-pa",
    label: "West Chester, PA",
    city: "WEST CHESTER",
    state: "PA",
    countyName: "Chester",
    propertyCityLike: "WEST CHESTER",
    listingCity: "WEST CHESTER",
    listingScopeNote: "West Chester is a pilot market covering borough-first listing/rent signals.",
    parcelScopeNote: "Chester County parcel rows are the county universe; West Chester city rows are a subset.",
    progressFiles: ["realestateapi-west-chester-pa-progress.html", "west-chester-reapi-progress.html"],
    rerunCommands: [
      "npm run market:west-chester:refresh",
      "npm run market:west-chester:refresh:dry",
    ],
  },
  {
    key: "dayton-oh",
    label: "Dayton, OH",
    city: "DAYTON",
    state: "OH",
    countyId: 1698991,
    countyName: "Montgomery",
    targetCountyHints: ["Montgomery County"],
    listingCity: "DAYTON",
    propertyCityLike: "DAYTON",
    propertyScope: "active_listing_properties",
    listingScopeNote: "Dayton is source-limited to public Redfin-derived active rows plus property-scoped paid detail backfill.",
    parcelScopeNote: "Coverage tiles use linked active Dayton listing properties; Montgomery County parcel universe is loaded separately because active listings may resolve to nearby city situs values.",
    progressFiles: ["realestateapi-dayton-oh-progress.html"],
    rerunCommands: [
      "npm run market:dayton:refresh",
      "npm run market:dayton:refresh -- --include-paid --paid-max-calls=100",
    ],
  },
  {
    key: "toledo-oh",
    label: "Toledo, OH",
    city: "TOLEDO",
    state: "OH",
    countyId: 2338836,
    countyName: "Lucas",
    targetCountyHints: ["Lucas County"],
    listingCity: "TOLEDO",
    propertyCityLike: "TOLEDO",
    propertyScope: "active_listing_properties",
    listingScopeNote: "Toledo is in active build/backfill mode using Lucas County parcels, public Redfin-derived active rows, and property-scoped paid detail backfill.",
    parcelScopeNote: "Coverage tiles use linked active Toledo listing properties; Lucas County parcel universe is loaded separately for market buildout.",
    progressFiles: ["realestateapi-toledo-oh-progress.html"],
    rerunCommands: [
      "npx tsx scripts/ingest-lucas-oh.ts --city=TOLEDO",
      "npx tsx scripts/ingest-listings-fast.ts --state OH --county Lucas --concurrency 3 --skip-match",
      "npx tsx scripts/link-market-listings-fast.ts --state=OH --city=TOLEDO --county_id=2338836 --create-shells",
      "npx tsx scripts/enrich-redfin-detail-pages.ts --state=OH --city=TOLEDO --limit=500 --delay-ms=150",
      "npx tsx scripts/enrich-on-market-realestateapi.ts --state=OH --city=TOLEDO --limit=100 --max-calls=100",
    ],
  },
  {
    key: "san-antonio-tx",
    label: "San Antonio, TX",
    city: "SAN ANTONIO",
    state: "TX",
    countyId: 1741238,
    countyName: "Bexar",
    targetCountyHints: ["Bexar County"],
    listingCity: "SAN ANTONIO",
    propertyCityLike: "SAN ANTONIO",
    propertyScope: "active_listing_properties",
    listingScopeNote: "San Antonio is in active build/backfill mode using Bexar County parcels and public Redfin-derived active rows. Paid detail has not been broadly run yet.",
    parcelScopeNote: "Coverage tiles use linked active San Antonio listing properties; Bexar County parcel universe is loaded separately for market buildout.",
    recorderSourcePattern: "bexar",
    progressFiles: ["realestateapi-san-antonio-tx-progress.html"],
    rerunCommands: [
      "npm run market:san-antonio:refresh",
      "npm run market:san-antonio:refresh -- --include-paid --paid-max-calls=100",
    ],
  },
  {
    key: "memphis-tn",
    label: "Memphis, TN",
    city: "MEMPHIS",
    state: "TN",
    countyId: 1741244,
    countyName: "Shelby",
    targetCountyHints: ["Shelby County"],
    listingCity: "MEMPHIS",
    propertyCityLike: "MEMPHIS",
    propertyScope: "active_listing_properties",
    listingScopeNote: "Memphis is in active build/backfill mode using Shelby County parcels and public Redfin-derived active rows. Paid detail should wait until public ingestion/linking is complete.",
    parcelScopeNote: "Coverage tiles use linked active Memphis listing properties; Shelby County parcel universe is loaded separately for market buildout.",
    progressFiles: ["realestateapi-memphis-tn-progress.html"],
    rerunCommands: [
      "npm run market:memphis:refresh",
      "npm run market:memphis:refresh -- --include-paid --paid-max-calls=100",
    ],
  },
  {
    key: "cleveland-oh",
    label: "Cleveland, OH",
    city: "CLEVELAND",
    state: "OH",
    countyName: "Cuyahoga",
    targetCountyHints: ["Cuyahoga County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: Cuyahoga County.",
    progressFiles: ["realestateapi-cleveland-oh-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Cleveland --state=OH"],
  },
  {
    key: "akron-oh",
    label: "Akron, OH",
    city: "AKRON",
    state: "OH",
    countyName: "Summit",
    targetCountyHints: ["Summit County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: Summit County.",
    progressFiles: ["realestateapi-akron-oh-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Akron --state=OH"],
  },
  {
    key: "fort-wayne-in",
    label: "Fort Wayne, IN",
    city: "FORT WAYNE",
    state: "IN",
    countyName: "Allen",
    targetCountyHints: ["Allen County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: Allen County.",
    progressFiles: ["realestateapi-fort-wayne-in-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Fort Wayne --state=IN"],
  },
  {
    key: "south-bend-in",
    label: "South Bend, IN",
    city: "SOUTH BEND",
    state: "IN",
    countyName: "St. Joseph",
    targetCountyHints: ["St. Joseph County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: St. Joseph County.",
    progressFiles: ["realestateapi-south-bend-in-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=South Bend --state=IN"],
  },
  {
    key: "peoria-il",
    label: "Peoria, IL",
    city: "PEORIA",
    state: "IL",
    countyName: "Peoria",
    targetCountyHints: ["Peoria County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: Peoria County.",
    progressFiles: ["realestateapi-peoria-il-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Peoria --state=IL"],
  },
  {
    key: "birmingham-al",
    label: "Birmingham, AL",
    city: "BIRMINGHAM",
    state: "AL",
    countyName: "Jefferson",
    targetCountyHints: ["Jefferson County", "Shelby County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting counties: Jefferson County first, then Shelby County expansion.",
    progressFiles: ["realestateapi-birmingham-al-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Birmingham --state=AL"],
  },
  {
    key: "detroit-mi",
    label: "Detroit, MI",
    city: "DETROIT",
    state: "MI",
    countyName: "Wayne",
    targetCountyHints: ["Wayne County"],
    sourceDiscoveryOnly: true,
    listingScopeNote: "Queued cash-flow market; source discovery and county-specific scripts are pending.",
    parcelScopeNote: "Expected starting county: Wayne County plus Detroit city parcel data.",
    progressFiles: ["realestateapi-detroit-mi-progress.html"],
    rerunCommands: ["npx tsx scripts/explore-market-coverage.ts --city=Detroit --state=MI"],
  },
];

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(QUERY_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

async function pgOptional<T extends Row = Row>(query: string, label: string): Promise<T[]> {
  try {
    return await pg<T>(query);
  } catch (error) {
    console.warn(`Optional dashboard query failed (${label}): ${error instanceof Error ? error.message : error}`);
    return [];
  }
}

const sql = (value: string) => value.replace(/'/g, "''");
const n = (value: unknown) => Number(value ?? 0);
const fmt = (value: unknown) => Math.round(n(value)).toLocaleString();
const money = (value: unknown) => n(value) > 0 ? `$${Math.round(n(value)).toLocaleString()}` : "-";
const pctNumber = (value: unknown, total: unknown) => {
  const denominator = n(total);
  if (denominator <= 0) return 0;
  return Math.round((n(value) / denominator) * 10000) / 100;
};
const pct = (value: unknown, total: unknown) => `${pctNumber(value, total)}%`;
const clampPct = (value: number) => Math.max(0, Math.min(100, value));
const esc = (value: unknown) => String(value ?? "")
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;");

function averagePct(values: number[]): number {
  const usable = values.filter(value => Number.isFinite(value));
  if (!usable.length) return 0;
  return clampPct(usable.reduce((sum, value) => sum + clampPct(value), 0) / usable.length);
}

function bar(label: string, value: unknown, total: unknown) {
  const p = pctNumber(value, total);
  const klass = p >= 85 ? "" : p >= 25 ? " warn" : " bad";
  return `<div class="row"><div>${esc(label)}</div><div class="track"><div class="fill${klass}" style="width:${Math.min(100, p)}%"></div></div><strong>${p}%</strong></div>`;
}

function healthClass(value: number) {
  if (value >= 85) return "good";
  if (value >= 60) return "warn";
  return "bad";
}

function healthTile(label: string, value: number, detail: string) {
  const safeValue = clampPct(value);
  return `<div class="health ${healthClass(safeValue)}">
    <div class="label">${esc(label)}</div>
    <div class="health-value">${safeValue.toFixed(0)}%</div>
    <div class="track"><div class="fill" style="width:${safeValue}%"></div></div>
    <div class="note">${esc(detail)}</div>
  </div>`;
}

function unknownHealthTile(label: string, detail: string) {
  return `<div class="health neutral">
    <div class="label">${esc(label)}</div>
    <div class="health-value">N/A</div>
    <div class="note">${esc(detail)}</div>
  </div>`;
}

function categoryRowsForOverall(coverage: {
  listingCoverage: number | null;
  agentCoverage: number | null;
  parcelCoverage: number | null;
  creativeCoverage: number | null;
  debtCoverage: number | null;
  rentCoverage: number | null;
}) {
  return [
    coverage.listingCoverage,
    coverage.agentCoverage,
    coverage.parcelCoverage,
    coverage.creativeCoverage,
    coverage.debtCoverage,
    coverage.rentCoverage,
  ].filter((value): value is number => value != null);
}

function refreshScriptName(marketKey: string): string | null {
  if (marketKey === "dallas-tx") return "refresh-dallas-market.ts";
  if (marketKey === "indianapolis-in") return "refresh-indianapolis-market.ts";
  if (marketKey === "columbus-oh") return "refresh-columbus-market.ts";
  if (marketKey === "dayton-oh") return "refresh-dayton-market.ts";
  if (marketKey === "toledo-oh") return "refresh-toledo-market.ts";
  if (marketKey === "san-antonio-tx") return "refresh-san-antonio-market.ts";
  if (marketKey === "memphis-tn") return "refresh-memphis-market.ts";
  if (marketKey === "west-chester-pa") return "refresh-west-chester-market.ts";
  return null;
}

function logPatterns(market: MarketConfig): RegExp[] {
  const escapedKey = market.key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const first = market.key.split("-")[0];
  return [
    new RegExp(escapedKey, "i"),
    new RegExp(`heartbeat-${first}`, "i"),
    new RegExp(first, "i"),
  ];
}

async function latestMarketLog(market: MarketConfig): Promise<{ name: string; at: string } | null> {
  const dir = join(process.cwd(), "logs", "market-refresh");
  const patterns = logPatterns(market);
  try {
    const files = await readdir(dir);
    const matches = await Promise.all(files
      .filter(file => patterns.some(pattern => pattern.test(file)))
      .map(async file => ({ file, stats: await stat(join(dir, file)) })));
    const latest = matches
      .filter(match => match.stats.isFile())
      .sort((a, b) => b.stats.mtimeMs - a.stats.mtimeMs)[0];
    return latest ? { name: latest.file, at: latest.stats.mtime.toLocaleString() } : null;
  } catch {
    return null;
  }
}

function runningProcessText(): string {
  try {
    return execSync(
      "powershell -NoProfile -Command \"Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'refresh-|ingest-|enrich-|score-creative|scrape-rents' } | Select-Object -ExpandProperty CommandLine\"",
      { encoding: "utf8", timeout: 5_000, windowsHide: true },
    );
  } catch {
    return "";
  }
}

async function collectRefreshJobStatuses(markets: MarketConfig[]): Promise<Map<string, RefreshJobStatus>> {
  const processText = runningProcessText();
  const statuses = new Map<string, RefreshJobStatus>();
  for (const market of markets) {
    const script = refreshScriptName(market.key);
    const latest = await latestMarketLog(market);
    const running = script ? processText.toLowerCase().includes(script.toLowerCase()) : false;
    statuses.set(market.key, {
      marketKey: market.key,
      state: running ? "running" : market.sourceDiscoveryOnly ? "queued" : "idle",
      detail: running
        ? `running ${script}`
        : market.sourceDiscoveryOnly
          ? "source discovery queued"
          : "no refresh process detected",
      latestLog: latest?.name ?? null,
      latestLogAt: latest?.at ?? null,
    });
  }
  return statuses;
}

function renderCompletionSummary(
  markets: Awaited<ReturnType<typeof collectMarket>>[],
  jobStatuses: Map<string, RefreshJobStatus>,
) {
  return `<section class="panel summary-panel">
    <div class="panel-head">
      <div><h2>Market Coverage Composite & Running Jobs</h2><div class="note">This is a blended data-quality score, not a publishability verdict. It combines listing, contact, parcel, creative, debt, and applicable rent coverage from the latest dashboard generation.</div></div>
    </div>
    <div class="summary-grid">
      ${markets.map(data => {
        const coverage = categoryCoverage(data);
        const status = jobStatuses.get(data.market.key);
        const state = status?.state ?? "idle";
        return `<a class="summary-tile ${healthClass(coverage.overall)} job-${state}" href="#${esc(data.market.key)}">
          <div class="summary-top"><strong>${esc(data.market.label)}</strong><span>${esc(state.toUpperCase())}</span></div>
          <div class="summary-main">${coverage.overall.toFixed(0)}%</div>
          <div class="summary-sub">${fmt(data.listings.active_properties)} active properties · ${fmt(data.listings.creative_positive)} creative</div>
          <div class="note">${esc(status?.detail ?? "not checked")}${status?.latestLog ? ` · latest ${esc(status.latestLog)} ${esc(status.latestLogAt)}` : ""}</div>
        </a>`;
      }).join("")}
    </div>
  </section>`;
}

function findProgressFile(market: MarketConfig): string | null {
  for (const file of market.progressFiles ?? []) {
    if (existsSync(join(process.cwd(), "logs", "market-refresh", file))) return file;
  }
  return null;
}

function renderProgressPanel(market: MarketConfig) {
  const progressFile = findProgressFile(market);
  const commands = market.rerunCommands.map(command => `<code>${esc(command)}</code>`).join("<br>");
  if (progressFile) {
    return `<section class="panel live-progress">
      <div class="panel-head">
        <div><h2>Live Enrichment Progress</h2><div class="note">Per-property enrichment progress for ${esc(market.label)}. Refresh this dashboard to pick up new generated files.</div></div>
        <a class="open-link" href="${esc(progressFile)}" target="_blank" rel="noopener">Open full progress view</a>
      </div>
      <iframe class="live-frame" title="${esc(market.label)} live enrichment progress" src="${esc(progressFile)}"></iframe>
    </section>`;
  }
  return `<section class="panel live-progress">
    <div class="panel-head">
      <div><h2>Live Enrichment Progress</h2><div class="note">${market.sourceDiscoveryOnly ? "This queued market has not started enrichment yet." : "No live enrichment progress file exists yet for this market."}</div></div>
    </div>
    <div class="empty-progress">
      <strong>${market.sourceDiscoveryOnly ? "Queued for source discovery" : "Waiting for first enrichment run"}</strong>
      <span>Expected progress files: ${esc((market.progressFiles ?? []).join(", ") || "none configured")}</span>
      <span>Next command:<br>${commands}</span>
    </div>
  </section>`;
}

function emptyMarketData(market: MarketConfig) {
  const zero = {
    total: 0,
    active_properties: 0,
    unlinked_listings: 0,
    distinct_listing_urls: 0,
    source_count: 0,
    sources: [],
    price: 0,
    agent_name: 0,
    first_last: 0,
    phone: 0,
    email: 0,
    brokerage: 0,
    redfin_detail: 0,
    mls_description: 0,
    zillow_rapidapi_detail: 0,
    zillow_rapidapi_contact: 0,
    zillow_rapidapi_error: 0,
    creative_evaluated: 0,
    creative_positive: 0,
    creative_negative: 0,
    creative_no_data: 0,
    parcel_id: 0,
    address: 0,
    asset_type: 0,
    owner: 0,
    value: 0,
    total_units: 0,
    year_built: 0,
    sqft: 0,
    cities: 0,
    price_changed: 0,
    listed: 0,
    latest_event: null,
    records: 0,
    paid_records: 0,
    public_records: 0,
    properties: 0,
    amount_rows: 0,
    payment_rows: 0,
    total_amount: 0,
    total_estimated_payment: 0,
    latest_recording: null,
    source_docs: 0,
    mortgage_docs: 0,
    lien_docs: 0,
    debt_docs: 0,
    linked_docs: 0,
    linked_properties: 0,
    amount_docs: 0,
    payment_docs: 0,
    cached_details: 0,
    latest_fetch: null,
    complex_count: 0,
    website_properties: 0,
    floorplan_properties: 0,
    floorplan_rows: 0,
    rent_properties: 0,
    rent_rows: 0,
    fresh_rent_rows: 0,
    rent_amount_rows: 0,
    rent_per_door_rows: 0,
    total_monthly_rows: 0,
    latest_rent_observed: null,
    reported_total_monthly_rent: 0,
  };
  return {
    market,
    countyId: null,
    listings: zero,
    cityParcels: zero,
    countyParcels: zero,
    events: zero,
    debt: zero,
    recorder: zero,
    paidDetails: zero,
    mf: zero,
    lienSamples: [] as Row[],
    rentSamples: [] as Row[],
    creativeListings: [] as Row[],
    readinessGaps: [
      `source discovery queued for ${market.targetCountyHints?.join(", ") || market.countyName || market.label}`,
      "ingestion has not started yet",
    ],
  };
}

function firstOr<T extends Row>(rows: T[], fallback: T): T {
  return rows[0] ?? fallback;
}

const zeroListings = {
  total: 0, active_properties: 0, unlinked_listings: 0, distinct_listing_urls: 0, source_count: 0, sources: [],
  price: 0, agent_name: 0, first_last: 0, phone: 0, email: 0, brokerage: 0, redfin_detail: 0, mls_description: 0,
  zillow_rapidapi_detail: 0, zillow_rapidapi_contact: 0, zillow_rapidapi_error: 0,
  creative_evaluated: 0, creative_positive: 0, creative_negative: 0, creative_no_data: 0,
};
const zeroParcels = {
  total: 0, parcel_id: 0, address: 0, asset_type: 0, owner: 0, value: 0, total_units: 0, year_built: 0, sqft: 0, cities: 0,
};
const zeroEvents = { total: 0, price_changed: 0, listed: 0, latest_event: null };
const zeroDebt = {
  records: 0, paid_records: 0, public_records: 0, properties: 0, amount_rows: 0, payment_rows: 0,
  paid_properties: 0, public_properties: 0, overlap_paid_public_properties: 0,
  total_amount: 0, total_estimated_payment: 0, latest_recording: null,
  valid_reapi_details: 0, reapi_free_clear: 0, reapi_response_balance: 0, reapi_response_equity: 0,
  debt_covered_properties: 0, debt_unknown_properties: 0,
};
const zeroRecorder = {
  source_docs: 0, mortgage_docs: 0, lien_docs: 0, debt_docs: 0, linked_docs: 0, linked_properties: 0,
  amount_docs: 0, payment_docs: 0, latest_recording: null,
};
const zeroPaidDetails = { cached_details: 0, latest_fetch: null };
const zeroMf = {
  complex_count: 0, website_properties: 0, floorplan_properties: 0, floorplan_rows: 0, rent_properties: 0,
  rent_rows: 0, fresh_rent_rows: 0, rent_amount_rows: 0, rent_per_door_rows: 0, total_monthly_rows: 0,
  latest_rent_observed: null, reported_total_monthly_rent: 0,
};

async function resolveCountyId(market: MarketConfig): Promise<number | null> {
  if (market.countyId) return market.countyId;
  if (!market.countyName) return null;
  const rows = await pg<{ id: number }>(`
    select id
      from counties
     where state_code = '${sql(market.state)}'
       and upper(county_name) = '${sql(market.countyName.toUpperCase())}'
     order by id
     limit 1;
  `);
  return Number(rows[0]?.id ?? 0) || null;
}

async function collectMarket(market: MarketConfig) {
  if (market.sourceDiscoveryOnly) {
    console.log(`  ${market.label}: source discovery queued; skipping DB coverage queries`);
    return emptyMarketData(market);
  }

  const countyId = await resolveCountyId(market);
  const stateSql = sql(market.state);
  const listingCitySql = sql((market.listingCity ?? market.city).toUpperCase());
  const propertyCitySql = sql((market.propertyCityLike ?? market.city).toUpperCase());
  const countyClause = countyId ? `county_id = ${countyId}` : "false";
  const countyPropertyWhere = `${countyClause} and state_code = '${stateSql}'`;
  const listingWhere = `is_on_market = true and state_code = '${stateSql}' and upper(coalesce(city,'')) = '${listingCitySql}'`;
  const activeListingPropertyIds = `(
      select distinct property_id
        from listing_signals
       where ${listingWhere}
         and property_id is not null
    )`;
  const activeListingPropertyWhere = `
    id in ${activeListingPropertyIds}
    and state_code = '${stateSql}'`;
  const cityPropertyWhere = market.propertyScope === "active_listing_properties"
    ? activeListingPropertyWhere
    : `${countyPropertyWhere} and city = '${propertyCitySql}'`;
  const rentSamplePropertyWhere = market.propertyScope === "active_listing_properties"
    ? `p.id in ${activeListingPropertyIds} and p.state_code = '${stateSql}'`
    : `${countyId ? `p.county_id = ${countyId}` : "false"} and p.state_code = '${stateSql}' and p.city = '${propertyCitySql}'`;
  const mfWhere = `${cityPropertyWhere} and (coalesce(total_units,1) >= 2 or asset_type in ('small_multifamily','apartment','commercial_multifamily','multifamily'))`;
  const recorderWhere = market.recorderSourcePattern
    ? `source_url ilike '%${sql(market.recorderSourcePattern)}%'`
    : "false";

  const listings = firstOr(await pgOptional(`
    select count(*)::int as total,
           count(distinct property_id)::int as active_properties,
           count(*) filter (where property_id is null)::int as unlinked_listings,
           count(distinct listing_url)::int as distinct_listing_urls,
           count(distinct listing_source)::int as source_count,
           array_agg(distinct listing_source order by listing_source) filter (where listing_source is not null) as sources,
           count(*) filter (where mls_list_price is not null)::int as price,
           count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name,
           count(*) filter (where nullif(listing_agent_first_name,'') is not null and nullif(listing_agent_last_name,'') is not null)::int as first_last,
           count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as phone,
           count(*) filter (
             where nullif(listing_agent_email,'') is not null
               and (
                 agent_contact_source = 'realestateapi'
                 or agent_contact_confidence = 'public_profile_verified'
               )
           )::int as email,
           count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage,
           count(*) filter (where raw ? 'redfinDetail')::int as redfin_detail,
           count(*) filter (
             where nullif(coalesce(
               raw #>> '{description}',
               raw #>> '{publicRemarks}',
               raw #>> '{public_remarks}',
               raw #>> '{remarks}',
               raw #>> '{listingRemarks}',
               raw #>> '{marketingRemarks}',
               raw #>> '{propertyDescription}',
               raw #>> '{redfinDetail,publicRemarks}',
               raw #>> '{redfinDetail,description}',
               raw #>> '{zillow_rapidapi_detail,raw,property,description}',
               raw #>> '{zillow_rapidapi_detail,raw,description}',
               raw #>> '{zillow_rapidapi_detail,raw,data,description}',
               raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
               raw #>> '{mls,remarks}',
               raw #>> '{mls,description}'
             ), '') is not null
           )::int as mls_description,
           count(*) filter (where raw ? 'zillow_rapidapi_detail')::int as zillow_rapidapi_detail,
           count(*) filter (where raw ? 'zillow_rapidapi_contact')::int as zillow_rapidapi_contact,
           count(*) filter (where raw ? 'zillow_rapidapi_error')::int as zillow_rapidapi_error,
           count(*) filter (where creative_finance_status is not null)::int as creative_evaluated,
           count(*) filter (where creative_finance_status = 'positive')::int as creative_positive,
           count(*) filter (where creative_finance_status = 'negative')::int as creative_negative,
           count(*) filter (where creative_finance_status = 'no_data')::int as creative_no_data
      from listing_signals
     where ${listingWhere};
  `, `${market.label} listings`), zeroListings);
  console.log(`  ${market.label}: listings`);

  const cityParcels = firstOr(await pgOptional(`
    select count(*)::int as total,
           count(*) filter (where parcel_id is not null and parcel_id <> '')::int as parcel_id,
           count(*) filter (where nullif(address,'') is not null)::int as address,
           count(*) filter (where asset_type is not null)::int as asset_type,
           count(*) filter (where owner_name is not null)::int as owner,
           count(*) filter (where coalesce(market_value, assessed_value, taxable_value, 0) > 0)::int as value,
           count(*) filter (where total_units is not null)::int as total_units,
           count(*) filter (where year_built is not null)::int as year_built,
           count(*) filter (where total_sqft is not null or living_sqft is not null)::int as sqft
      from properties
     where ${cityPropertyWhere};
  `, `${market.label} city parcels`), zeroParcels);
  console.log(`  ${market.label}: city parcels`);

  const countyParcels = firstOr(await pgOptional(`
    select count(*)::int as total,
           count(*) filter (where parcel_id is not null and parcel_id <> '')::int as parcel_id,
           count(*) filter (where nullif(address,'') is not null)::int as address,
           count(*) filter (where asset_type is not null)::int as asset_type,
           count(*) filter (where owner_name is not null)::int as owner,
           count(*) filter (where coalesce(market_value, assessed_value, taxable_value, 0) > 0)::int as value,
           count(*) filter (where total_units is not null)::int as total_units,
           count(*) filter (where year_built is not null)::int as year_built,
           count(*) filter (where total_sqft is not null or living_sqft is not null)::int as sqft,
           count(distinct upper(coalesce(city,'')))::int as cities
      from properties
     where ${countyPropertyWhere};
  `, `${market.label} county parcels`), zeroParcels);
  console.log(`  ${market.label}: county parcels`);

  const events = firstOr(await pgOptional(`
    select count(*)::int as total,
           count(*) filter (where event_type = 'price_changed')::int as price_changed,
           count(*) filter (where event_type = 'listed')::int as listed,
           max(event_at)::text as latest_event
      from listing_signal_events
     where state_code = '${stateSql}'
       and upper(coalesce(city,'')) = '${listingCitySql}';
  `, `${market.label} listing events`), zeroEvents);
  console.log(`  ${market.label}: events`);

  const debt = firstOr(await pgOptional(`
    with active_market_properties as (
      select distinct property_id
        from listing_signals
       where ${listingWhere}
         and property_id is not null
    ),
    record_summary as (
      select count(*)::int as records,
             count(*) filter (where source_url = 'realestateapi')::int as paid_records,
             count(*) filter (where source_url <> 'realestateapi')::int as public_records,
             count(distinct property_id)::int as properties,
             count(distinct property_id) filter (where source_url = 'realestateapi')::int as paid_properties,
             count(distinct property_id) filter (where source_url <> 'realestateapi')::int as public_properties,
             count(distinct property_id) filter (where source_url <> 'realestateapi' and property_id in (
               select property_id from mortgage_records where source_url = 'realestateapi' and property_id in (select property_id from active_market_properties)
             ))::int as overlap_paid_public_properties,
             count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_rows,
             count(*) filter (where coalesce(estimated_monthly_payment, 0) > 0)::int as payment_rows,
             sum(coalesce(loan_amount, original_amount, estimated_current_balance, 0))::numeric as total_amount,
             sum(coalesce(estimated_monthly_payment, 0))::numeric as total_estimated_payment,
             max(recording_date)::text as latest_recording
        from mortgage_records mr
       where mr.property_id in (select property_id from active_market_properties)
    ),
    reapi_summary as (
      select count(*) filter (
               where jsonb_typeof(rapi.response_body) = 'object'
                 and rapi.response_body <> '{}'::jsonb
                 and coalesce(
                   rapi.response_body->>'id',
                   rapi.response_body->>'propertyId',
                   rapi.response_body->>'apn',
                   rapi.response_body->>'address',
                   rapi.response_body->>'formattedAddress',
                   rapi.response_body->>'owner1FullName'
                 ) is not null
             )::int as valid_reapi_details,
             count(*) filter (
               where jsonb_typeof(rapi.response_body) = 'object'
                 and rapi.response_body <> '{}'::jsonb
                 and coalesce(
                   rapi.response_body->>'id',
                   rapi.response_body->>'propertyId',
                   rapi.response_body->>'apn',
                   rapi.response_body->>'address',
                   rapi.response_body->>'formattedAddress',
                   rapi.response_body->>'owner1FullName'
                 ) is not null
                 and coalesce(jsonb_array_length(case when jsonb_typeof(rapi.response_body->'currentMortgages') = 'array' then rapi.response_body->'currentMortgages' else '[]'::jsonb end), 0) = 0
                 and coalesce(nullif(regexp_replace(coalesce(rapi.response_body->>'estimatedMortgageBalance', rapi.response_body->>'openMortgageBalance', '0'), '[^0-9.-]', '', 'g'), '')::numeric, 0) = 0
             )::int as reapi_free_clear,
             count(*) filter (where nullif(rapi.response_body->>'estimatedMortgageBalance','') is not null or nullif(rapi.response_body->>'openMortgageBalance','') is not null)::int as reapi_response_balance,
             count(*) filter (where nullif(rapi.response_body->>'estimatedEquity','') is not null)::int as reapi_response_equity
        from realestateapi_property_details rapi
        join active_market_properties amp on amp.property_id = rapi.property_id
    ),
    covered_properties as (
      select property_id from mortgage_records where property_id in (select property_id from active_market_properties)
      union
      select rapi.property_id
        from realestateapi_property_details rapi
       where rapi.property_id in (select property_id from active_market_properties)
         and jsonb_typeof(rapi.response_body) = 'object'
         and rapi.response_body <> '{}'::jsonb
         and coalesce(
           rapi.response_body->>'id',
           rapi.response_body->>'propertyId',
           rapi.response_body->>'apn',
           rapi.response_body->>'address',
           rapi.response_body->>'formattedAddress',
           rapi.response_body->>'owner1FullName'
         ) is not null
         and coalesce(jsonb_array_length(case when jsonb_typeof(rapi.response_body->'currentMortgages') = 'array' then rapi.response_body->'currentMortgages' else '[]'::jsonb end), 0) = 0
         and coalesce(nullif(regexp_replace(coalesce(rapi.response_body->>'estimatedMortgageBalance', rapi.response_body->>'openMortgageBalance', '0'), '[^0-9.-]', '', 'g'), '')::numeric, 0) = 0
    )
    select rs.*,
           rapi.valid_reapi_details,
           rapi.reapi_free_clear,
           rapi.reapi_response_balance,
           rapi.reapi_response_equity,
           (select count(distinct property_id)::int from covered_properties) as debt_covered_properties,
           greatest((select count(*) from active_market_properties) - (select count(distinct property_id) from covered_properties), 0)::int as debt_unknown_properties
      from record_summary rs, reapi_summary rapi;
  `, `${market.label} linked debt`), zeroDebt);
  console.log(`  ${market.label}: linked debt`);

  const recorder = firstOr(await pgOptional(`
    select count(*)::int as source_docs,
           count(*) filter (where document_type = 'mortgage')::int as mortgage_docs,
           count(*) filter (where document_type in ('lien','tax_lien','mechanics_lien','judgment_lien'))::int as lien_docs,
           count(*) filter (where document_type in ('mortgage','lien','tax_lien','mechanics_lien','judgment_lien'))::int as debt_docs,
           count(*) filter (where property_id is not null)::int as linked_docs,
           count(distinct property_id) filter (where property_id is not null)::int as linked_properties,
           count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_docs,
           count(*) filter (where coalesce(estimated_monthly_payment, 0) > 0)::int as payment_docs,
           max(recording_date)::text as latest_recording
      from mortgage_records
     where ${recorderWhere};
  `, `${market.label} recorder`), zeroRecorder);
  console.log(`  ${market.label}: recorder`);

  const paidDetails = firstOr(await pgOptional(`
    with active_market_properties as (
      select distinct property_id
        from listing_signals
       where ${listingWhere}
         and property_id is not null
    )
    select count(*)::int as cached_details,
           max(rapi.fetched_at)::text as latest_fetch
      from realestateapi_property_details rapi
      join active_market_properties amp on amp.property_id = rapi.property_id;
  `, `${market.label} paid details`), zeroPaidDetails);
  console.log(`  ${market.label}: paid details`);

  const mf = firstOr(await pgOptional(`
    with mf as (
      select id from properties where ${mfWhere}
    )
    select count(distinct mf.id)::int as complex_count,
           count(distinct pw.property_id)::int as website_properties,
           count(distinct fp.property_id)::int as floorplan_properties,
           count(distinct fp.id)::int as floorplan_rows,
           count(distinct rs.property_id)::int as rent_properties,
           count(*) filter (where rs.id is not null)::int as rent_rows,
           count(*) filter (where rs.observed_at >= now() - interval '1 day')::int as fresh_rent_rows,
           count(*) filter (where coalesce(rs.effective_rent, rs.asking_rent) > 0)::int as rent_amount_rows,
           count(*) filter (where rs.rent_per_door > 0)::int as rent_per_door_rows,
           count(*) filter (where rs.total_monthly_rent > 0)::int as total_monthly_rows,
           max(rs.observed_at)::text as latest_rent_observed,
           sum(coalesce(rs.total_monthly_rent, 0))::numeric as reported_total_monthly_rent
      from mf
      left join property_websites pw on pw.property_id = mf.id and pw.active = true
      left join floorplans fp on fp.property_id = mf.id
      left join rent_snapshots rs on rs.property_id = mf.id;
  `, `${market.label} multifamily`), zeroMf);
  console.log(`  ${market.label}: multifamily`);

  const lienSamples = await pgOptional(`
    select document_type, recording_date::text, borrower_name, lender_name,
           coalesce(loan_amount, original_amount, estimated_current_balance, 0)::numeric as amount,
           estimated_monthly_payment
      from mortgage_records
     where ${recorderWhere}
       and document_type in ('mortgage','lien','tax_lien','mechanics_lien','judgment_lien')
     order by recording_date desc nulls last
     limit 10;
  `, `${market.label} lien samples`);
  console.log(`  ${market.label}: lien samples`);

  const rentSamples = await pgOptional(`
    with recent_rents as (
      select *
        from rent_snapshots
       where coalesce(effective_rent, asking_rent, 0) > 0
       order by observed_at desc nulls last
       limit 5000
    )
    select coalesce(cp.complex_name, p.address) as complex_name,
           p.address,
           fp.name as floorplan,
           rs.beds,
           rs.baths,
           rs.sqft,
           coalesce(rs.effective_rent, rs.asking_rent)::numeric as rent,
           rs.rent_per_door,
           rs.estimated_unit_count,
           rs.rent_unit_basis,
           rs.total_monthly_rent,
           rs.observed_at::text as observed_at
      from recent_rents rs
      join properties p on p.id = rs.property_id
      left join floorplans fp on fp.id = rs.floorplan_id
      left join property_complex_profiles cp on cp.property_id = p.id
     where ${rentSamplePropertyWhere}
       and coalesce(rs.effective_rent, rs.asking_rent, 0) > 0
     order by rs.observed_at desc nulls last
     limit 10;
  `, `${market.label} rent samples`);
  console.log(`  ${market.label}: rent samples`);

  const creativeListings = await pgOptional(`
    select id,
           property_id,
           address,
           city,
           state_code,
           mls_list_price,
           listing_source,
           listing_url,
           creative_finance_score,
           creative_finance_status,
           creative_finance_terms,
           creative_finance_rate_text,
           coalesce(
             raw #>> '{description}',
             raw #>> '{publicRemarks}',
             raw #>> '{public_remarks}',
             raw #>> '{remarks}',
             raw #>> '{listingRemarks}',
             raw #>> '{marketingRemarks}',
             raw #>> '{propertyDescription}',
             raw #>> '{redfinDetail,publicRemarks}',
             raw #>> '{redfinDetail,description}',
             raw #>> '{zillow_rapidapi_detail,raw,property,description}',
             raw #>> '{zillow_rapidapi_detail,raw,description}',
             raw #>> '{zillow_rapidapi_detail,raw,data,description}',
             raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
             raw #>> '{mls,remarks}',
             raw #>> '{mls,description}'
           ) as description
      from listing_signals
     where ${listingWhere}
       and creative_finance_status = 'positive'
     order by creative_finance_score desc nulls last,
              coalesce(last_seen_at, first_seen_at) desc nulls last,
              id desc
     limit 200;
  `, `${market.label} creative listing details`);
  console.log(`  ${market.label}: creative listing details`);

  const readinessGaps = [
    n(listings.total) === 0 ? "active listing ingestion has no current rows" : null,
    n(listings.source_count) <= 1 ? "active listing coverage is source-limited" : null,
    n(listings.unlinked_listings) > 0 ? `${fmt(listings.unlinked_listings)} listing rows are not linked to property_id` : null,
    pctNumber(listings.email, listings.total) < 50 ? `verified agent email coverage is low (${pct(listings.email, listings.total)})` : null,
    n(debt.records) > 0 && n(debt.amount_rows) === 0 ? "linked debt rows exist but loan/balance/payment amounts are missing" : null,
    n(mf.rent_properties) === 0 ? "multifamily rent snapshots are empty" : null,
  ].filter(Boolean);

  return {
    market,
    countyId,
    listings,
    cityParcels,
    countyParcels,
    events,
    debt,
    recorder,
    paidDetails,
    mf,
    lienSamples,
    rentSamples,
    creativeListings,
    readinessGaps,
  };
}

function categoryCoverage(data: Awaited<ReturnType<typeof collectMarket>>) {
  const { listings, cityParcels, countyParcels, debt, paidDetails, mf } = data;
  const listingRows = n(listings.total);
  const activeProperties = n(listings.active_properties);
  const cityParcelRows = n(cityParcels.total);
  const countyParcelRows = n(countyParcels.total);
  const mfCount = n(mf.complex_count);

  const listingCoverage = listingRows > 0 ? averagePct([
    pctNumber(n(listings.total) - n(listings.unlinked_listings), listingRows),
    pctNumber(listings.price, listingRows),
    pctNumber(listings.mls_description, listingRows),
  ]) : null;
  const agentCoverage = listingRows > 0 ? averagePct([
    pctNumber(listings.agent_name, listingRows),
    pctNumber(listings.first_last, listingRows),
    pctNumber(listings.phone, listingRows),
    pctNumber(listings.email, listingRows),
    pctNumber(listings.brokerage, listingRows),
  ]) : null;
  const parcelCoverage = countyParcelRows > 0 ? averagePct([
    pctNumber(countyParcels.parcel_id, countyParcelRows),
    pctNumber(countyParcels.address, countyParcelRows),
    pctNumber(countyParcels.asset_type, countyParcelRows),
    pctNumber(countyParcels.owner, countyParcelRows),
    pctNumber(countyParcels.value, countyParcelRows),
    pctNumber(cityParcels.total_units, cityParcelRows),
    pctNumber(cityParcels.year_built, cityParcelRows),
    pctNumber(cityParcels.sqft, cityParcelRows),
  ]) : null;
  const creativeCoverage = listingRows > 0 ? pctNumber(listings.creative_evaluated, listingRows) : null;
  const debtCoverage = activeProperties > 0 ? averagePct([
    pctNumber(paidDetails.cached_details, activeProperties),
    pctNumber(debt.debt_covered_properties, activeProperties),
  ]) : null;
  const rentCoverage = mfCount > 0
    ? averagePct([
        pctNumber(mf.website_properties, mfCount),
        pctNumber(mf.floorplan_properties, mfCount),
        pctNumber(mf.rent_properties, mfCount),
      ])
    : null;
  const overall = averagePct(categoryRowsForOverall({
    listingCoverage,
    agentCoverage,
    parcelCoverage,
    creativeCoverage,
    debtCoverage,
    rentCoverage,
  }));

  return {
    overall,
    listingCoverage,
    agentCoverage,
    parcelCoverage,
    creativeCoverage,
    debtCoverage,
    rentCoverage,
  };
}

function renderMarket(data: Awaited<ReturnType<typeof collectMarket>>, index: number) {
  const { market, listings, cityParcels, countyParcels, events, debt, recorder, paidDetails, mf, lienSamples, rentSamples, creativeListings, readinessGaps } = data;
  const coverage = categoryCoverage(data);
  const panelId = `panel-${market.key}`;
  return `<section id="${panelId}" class="market-panel${index === 0 ? " active" : ""}" role="tabpanel" aria-labelledby="tab-${market.key}">
    <div class="hero-metrics">
      <div class="overall-card ${healthClass(coverage.overall)}">
        <div class="label">Coverage Composite</div>
        <div class="overall-value">${coverage.overall.toFixed(0)}%</div>
        <div class="note">Data-quality composite, not publishability. Rent/floorplans are included only when multifamily candidates exist.</div>
      </div>
      <div class="health-grid">
        ${coverage.listingCoverage == null ? unknownHealthTile("Listings", "No active listing rows found for this dashboard scope.") : healthTile("Listings", coverage.listingCoverage, `${fmt(listings.active_properties)} unique active properties; ${fmt(listings.total)} raw rows`)}
        ${coverage.agentCoverage == null ? unknownHealthTile("Agent Contact", "No active listing rows found for this dashboard scope.") : healthTile("Agent Contact", coverage.agentCoverage, `${pct(listings.email, listings.total)} verified email; ${pct(listings.phone, listings.total)} phone`)}
        ${coverage.parcelCoverage == null ? unknownHealthTile("Parcels", "Parcel query returned no dashboard total or timed out; this is unknown, not zero coverage.") : healthTile("Parcels", coverage.parcelCoverage, `${fmt(countyParcels.total)} county parcels; ${fmt(cityParcels.total)} city subset`)}
        ${coverage.creativeCoverage == null ? unknownHealthTile("Creative", "No active listing rows found for this dashboard scope.") : healthTile("Creative", coverage.creativeCoverage, `${fmt(listings.creative_positive)} positive / ${fmt(listings.creative_negative)} negative; ${fmt(listings.mls_description)} descriptions`)}
        ${coverage.debtCoverage == null ? unknownHealthTile("Debt / Liens", "No linked active properties available for debt coverage calculation.") : healthTile("Debt / Liens", coverage.debtCoverage, `${fmt(debt.debt_covered_properties)} covered; ${fmt(debt.debt_unknown_properties)} unknown`)}
        ${coverage.rentCoverage == null
          ? unknownHealthTile("Rent", "No multifamily candidates in this market subset, so rent is not included in the composite.")
          : healthTile("Rent", coverage.rentCoverage, `${fmt(mf.rent_properties)} rent properties / ${fmt(mf.complex_count)} multifamily candidates`)}
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="label">County Parcels</div><div class="metric">${fmt(countyParcels.total)}</div><div class="note">${pct(countyParcels.parcel_id, countyParcels.total)} have parcel IDs across ${fmt(countyParcels.cities)} city labels. City subset: ${fmt(cityParcels.total)}.</div></div>
      <div class="card"><div class="label">Raw Active Listing Rows</div><div class="metric">${fmt(listings.total)}</div><div class="note">${fmt(n(listings.total) - n(listings.unlinked_listings))} linked rows; ${fmt(listings.unlinked_listings)} unlinked. ${esc(market.listingScopeNote)}</div></div>
      <div class="card"><div class="label">BBC Searchable Properties</div><div class="metric">${fmt(listings.active_properties)}</div><div class="note">Unique active properties with property_id; this is the API-searchable denominator, not raw source rows.</div></div>
      <a class="card creative-card card-link" href="#creative-${esc(market.key)}"><div class="label">Creative Listings</div><div class="metric">${fmt(listings.creative_positive)}</div><div class="note">${fmt(listings.creative_negative)} explicit negatives; ${pct(listings.creative_evaluated, listings.total)} scored coverage; ${fmt(listings.mls_description)} descriptions saved. Click to inspect.</div></a>
      <div class="card"><div class="label">Debt Coverage</div><div class="metric">${fmt(debt.debt_covered_properties)}</div><div class="note">${fmt(debt.properties)} with mortgage rows; ${fmt(debt.reapi_free_clear)} RealEstateAPI free-clear proofs; ${fmt(debt.debt_unknown_properties)} still unknown.</div></div>
      <div class="card"><div class="label">Rent / Floorplans</div><div class="metric">${fmt(mf.rent_rows)}</div><div class="note">${fmt(mf.floorplan_rows)} floorplans; ${fmt(mf.rent_properties)} complexes with rent snapshots. Latest: ${esc(mf.latest_rent_observed ?? "-")}.</div></div>
    </div>

    <div class="status ${readinessGaps.length ? "building" : "ready"}">
      <strong>${readinessGaps.length ? "Still Building" : "Ready"}</strong>
      <span>${readinessGaps.length ? esc(readinessGaps.join("; ")) : "No dashboard blocking gaps detected by the current coverage checks."}</span>
    </div>

    ${renderProgressPanel(market)}

    <section id="creative-${esc(market.key)}" class="panel creative-listings">
      <div class="panel-head">
        <div>
          <h2>Creative Listing Details</h2>
          <div class="note">Every positive creative-finance listing in this market tab, with saved listing description for verification. This is signal yield, not scoring coverage.</div>
        </div>
        <div class="count-pill">${fmt(creativeListings.length)} shown / ${fmt(listings.creative_positive)} total</div>
      </div>
      <div class="creative-list">
        ${creativeListings.length ? creativeListings.map((r, i) => {
          const price = n(r.mls_list_price) > 0 ? money(r.mls_list_price) : "-";
          const description = String(r.description ?? "").trim();
          return `<article class="creative-item">
            <div class="creative-rank">${i + 1}</div>
            <div class="creative-body">
              <div class="creative-title">
                <strong>${esc(r.address || "Address missing")}</strong>
                <span>${esc([r.city, r.state_code].filter(Boolean).join(", "))}</span>
                <span>${price}</span>
              </div>
              <div class="creative-meta">
                <span>Score ${esc(r.creative_finance_score ?? "-")}</span>
                <span>${esc(r.creative_finance_terms ?? r.creative_finance_rate_text ?? r.creative_finance_status ?? "positive")}</span>
                <span>${esc(r.listing_source ?? "unknown source")}</span>
                ${r.property_id ? `<span>property ${esc(r.property_id)}</span>` : `<span>unlinked</span>`}
                ${r.listing_url ? `<a href="${esc(r.listing_url)}" target="_blank" rel="noopener">Source</a>` : ""}
              </div>
              <p>${description ? esc(description) : "No saved listing description is available for this positive creative signal."}</p>
            </div>
          </article>`;
        }).join("") : `<div class="empty-progress"><strong>No positive creative listings found for this market.</strong><span>If the creative count above is nonzero, regenerate the dashboard after scoring finishes.</span></div>`}
      </div>
    </section>

    <section class="two">
      <div class="panel"><h2>Listing & Agent Coverage</h2><div class="bars">
        ${bar("MLS/list price", listings.price, listings.total)}
        ${bar("MLS description saved", listings.mls_description, listings.total)}
        ${bar("Agent name", listings.agent_name, listings.total)}
        ${bar("Agent first / last", listings.first_last, listings.total)}
        ${bar("Agent phone", listings.phone, listings.total)}
        ${bar("Brokerage", listings.brokerage, listings.total)}
        ${bar("Verified agent email", listings.email, listings.total)}
      </div></div>
      <div class="panel"><h2>Parcel Coverage</h2><div class="bars">
        ${bar("County parcel id", countyParcels.parcel_id, countyParcels.total)}
        ${bar("County address", countyParcels.address, countyParcels.total)}
        ${bar("County asset type", countyParcels.asset_type, countyParcels.total)}
        ${bar("County owner", countyParcels.owner, countyParcels.total)}
        ${bar("County value", countyParcels.value, countyParcels.total)}
        ${bar("City unit count", cityParcels.total_units, cityParcels.total)}
        ${bar("City year built", cityParcels.year_built, cityParcels.total)}
        ${bar("City sqft", cityParcels.sqft, cityParcels.total)}
      </div></div>
    </section>

    <section class="panel">
      <h2>Debt Coverage In Human English</h2>
      <div class="grid">
        <div class="card"><div class="label">Active Listings</div><div class="metric">${fmt(listings.active_properties)}</div><div class="note">Unique active listing properties in this market tab. Raw rows: ${fmt(listings.total)}.</div></div>
        <div class="card"><div class="label">MXRE Public Recorder</div><div class="metric">${fmt(debt.public_records)}</div><div class="note">${fmt(debt.public_properties)} properties with public recorder rows; public rows prove document coverage, while usable loan amounts depend on what the county source exposes.</div></div>
        <div class="card"><div class="label">RealEstateAPI</div><div class="metric">${fmt(debt.paid_records)}</div><div class="note">${fmt(debt.paid_properties)} properties with paid mortgage rows, ${fmt(debt.amount_rows)} amount-bearing rows, plus ${fmt(debt.reapi_free_clear)} valid free-clear proofs.</div></div>
        <div class="card"><div class="label">Remaining Gap</div><div class="metric">${fmt(debt.debt_unknown_properties)}</div><div class="note">Properties without a mortgage/lien row and without valid RealEstateAPI free-clear proof.</div></div>
      </div>
      <p class="note" style="margin:12px 0 0">Plain English: MXRE public recorder coverage tells us a document exists. RealEstateAPI tells us amount-bearing debt or that a property appears free and clear. Free-and-clear is real coverage, but it correctly has no mortgage row because there is no current mortgage to insert.</p>
    </section>

    <section class="panel">
      <h2>Coverage Detail</h2>
      <table>
        <tr><th>Metric</th><th>Count</th><th>Coverage / Yield</th><th>Notes</th></tr>
        <tr><td>Sources</td><td>${fmt(listings.source_count)}</td><td>-</td><td>${esc(Array.isArray(listings.sources) ? listings.sources.join(", ") : "")}</td></tr>
        <tr><td>Creative evaluated</td><td>${fmt(listings.creative_evaluated)} / ${fmt(listings.total)}</td><td>${pct(listings.creative_evaluated, listings.total)}</td><td>Coverage means rows were evaluated and status was saved. Hits are a yield, not coverage.</td></tr>
        <tr><td>Creative hits</td><td>${fmt(listings.creative_positive)} positive / ${fmt(listings.creative_negative)} negative</td><td>${pct(n(listings.creative_positive) + n(listings.creative_negative), listings.total)}</td><td>MLS descriptions saved: ${fmt(listings.mls_description)}.</td></tr>
        <tr><td>Paid listing fallback attempts</td><td>${fmt(listings.zillow_rapidapi_detail)} details / ${fmt(listings.zillow_rapidapi_error)} failed lookups</td><td>${pct(n(listings.zillow_rapidapi_detail) + n(listings.zillow_rapidapi_error), listings.total)}</td><td>Property-scoped Zillow/RapidAPI attempts cached on listing rows to prevent repeat paid calls.</td></tr>
        <tr><td>Price-change tracking</td><td>${fmt(events.price_changed)} price changes / ${fmt(events.total)} events</td><td>-</td><td>Latest event: ${esc(events.latest_event ?? "-")}.</td></tr>
        <tr><td>Listing-property matching</td><td>${fmt(n(listings.total) - n(listings.unlinked_listings))} linked / ${fmt(listings.unlinked_listings)} unlinked</td><td>${pct(n(listings.total) - n(listings.unlinked_listings), listings.total)}</td><td>Unlinked rows should not be counted as unique active properties.</td></tr>
        <tr><td>BBC searchable properties</td><td>${fmt(listings.active_properties)} unique properties</td><td>-</td><td>API search uses linked rows with property_id. Raw source rows may include duplicates and unlinked listings.</td></tr>
        <tr><td>County parcel universe</td><td>${fmt(countyParcels.total)}</td><td>${pct(countyParcels.parcel_id, countyParcels.total)}</td><td>${esc(market.parcelScopeNote)}</td></tr>
        <tr><td>City parcel subset</td><td>${fmt(cityParcels.total)}</td><td>${pct(cityParcels.parcel_id, cityParcels.total)}</td><td>City/situs-labelled subset used for local coverage quality checks.</td></tr>
        <tr><td>Recorder source docs</td><td>${fmt(recorder.source_docs)}</td><td>-</td><td>${fmt(recorder.linked_properties)} linked properties; latest recording ${esc(recorder.latest_recording ?? "-")}.</td></tr>
        <tr><td>Recorded liens/debt</td><td>${fmt(recorder.debt_docs)}</td><td>${pct(recorder.debt_docs, recorder.source_docs)}</td><td>${fmt(recorder.amount_docs)} rows include amount/balance data; ${fmt(recorder.payment_docs)} include estimated monthly payment.</td></tr>
        <tr><td>Paid property detail cache</td><td>${fmt(paidDetails.cached_details)} properties</td><td>-</td><td>Latest RealEstateAPI fetch: ${esc(paidDetails.latest_fetch ?? "-")}.</td></tr>
        <tr><td>Debt coverage state</td><td>${fmt(debt.debt_covered_properties)} covered / ${fmt(debt.debt_unknown_properties)} unknown</td><td>${pct(debt.debt_covered_properties, listings.active_properties)}</td><td>Covered means either a mortgage/lien row exists or RealEstateAPI returned a valid identity with currentMortgages empty/free-clear. Unknown means neither proof exists yet.</td></tr>
        <tr><td>Mortgage record rows</td><td>${fmt(debt.records)} records / ${fmt(debt.properties)} properties</td><td>${pct(debt.properties, listings.active_properties)}</td><td>${fmt(debt.paid_records)} RealEstateAPI rows and ${fmt(debt.public_records)} public recorder rows. This is not the same as total debt coverage because free-clear properties intentionally have no mortgage row.</td></tr>
        <tr><td>Amount-bearing debt rows</td><td>${fmt(debt.amount_rows)} amount rows / ${fmt(debt.payment_rows)} payment rows</td><td>${pct(debt.amount_rows, debt.records)}</td><td>Total amount: ${money(debt.total_amount)}; total estimated payment: ${money(debt.total_estimated_payment)}. Public Franklin recorder rows currently prove documents but generally do not expose usable amounts.</td></tr>
        <tr><td>RealEstateAPI free-clear proof</td><td>${fmt(debt.reapi_free_clear)} active properties</td><td>${pct(debt.reapi_free_clear, listings.active_properties)}</td><td>Valid property-detail responses with identity, zero open balance, and empty currentMortgages. These support openMortgageBalance=0/equity computation without inserting fake mortgage_records rows.</td></tr>
        <tr><td>RealEstateAPI equity fields</td><td>${fmt(debt.reapi_response_equity)} equity / ${fmt(debt.reapi_response_balance)} balance</td><td>${pct(debt.reapi_response_equity, listings.active_properties)}</td><td>Cached provider fields available for API-facing equity/backfill checks.</td></tr>
        <tr><td>Multifamily websites</td><td>${fmt(mf.website_properties)} / ${fmt(mf.complex_count)}</td><td>${pct(mf.website_properties, mf.complex_count)}</td><td>Public/free discovery only unless paid enrichment is explicitly run.</td></tr>
        <tr><td>Floorplans</td><td>${fmt(mf.floorplan_rows)} rows / ${fmt(mf.floorplan_properties)} properties</td><td>${pct(mf.floorplan_properties, mf.complex_count)}</td><td>By bed type where source pages expose it.</td></tr>
        <tr><td>Rent snapshots</td><td>${fmt(mf.rent_amount_rows)} rent rows / ${fmt(mf.rent_properties)} properties</td><td>${pct(mf.rent_properties, mf.complex_count)}</td><td>${fmt(mf.total_monthly_rows)} rows include total monthly rent; reported total/mo ${money(mf.reported_total_monthly_rent)}.</td></tr>
      </table>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Recorded Lien / Debt Samples</h2>
        <table><tr><th>Type</th><th>Date</th><th>Borrower</th><th>Lender</th><th>Amount</th><th>Payment</th></tr>
          ${lienSamples.length ? lienSamples.map(r => `<tr><td>${esc(r.document_type)}</td><td>${esc(r.recording_date)}</td><td>${esc(r.borrower_name)}</td><td>${esc(r.lender_name)}</td><td>${money(r.amount)}</td><td>${money(r.estimated_monthly_payment)}</td></tr>`).join("") : `<tr><td colspan="6">No recorded lien/debt samples available yet.</td></tr>`}
        </table>
      </div>
      <div class="panel">
        <h2>Rent / Floorplan Samples</h2>
        <table><tr><th>Complex</th><th>Floorplan</th><th>Unit</th><th>Rent</th><th>Total/mo</th><th>Observed</th></tr>
          ${rentSamples.length ? rentSamples.map(r => `<tr><td>${esc(r.complex_name)}<div class="note">${esc(r.address)}</div></td><td>${esc(r.floorplan)}</td><td>${esc(r.beds)} bd / ${esc(r.baths)} ba / ${esc(r.sqft)} sf</td><td>${money(r.rent)}<div class="note">${esc(r.rent_unit_basis || "per_unit")}</div></td><td>${n(r.total_monthly_rent) > 0 ? money(r.total_monthly_rent) : "Unit count not reported"}<div class="note">${n(r.estimated_unit_count) > 0 ? `${fmt(r.estimated_unit_count)} units` : ""}</div></td><td>${esc(r.observed_at)}</td></tr>`).join("") : `<tr><td colspan="6">No public rent samples available yet.</td></tr>`}
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Rerun Commands</h2>
      <table>
        <tr><th>Purpose</th><th>Command</th></tr>
        ${market.rerunCommands.map((command, i) => `<tr><td>${i === 0 ? "Market refresh" : "Follow-up"}</td><td><code>${esc(command)}</code></td></tr>`).join("")}
        <tr><td>Regenerate tabbed dashboard only</td><td><code>npx tsx scripts/generate-market-coverage-dashboard.ts</code></td></tr>
      </table>
    </section>
  </section>`;
}

async function main() {
  try {
    await pg("select 1 as ok;");
  } catch (error) {
    throw new Error(
      `DB bridge unavailable; refusing to overwrite dashboard with fallback zero metrics. ` +
      `Start the tunnel with scripts/start-mxre-db-tunnel.ps1. Root error: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  const markets = [];
  for (const market of MARKETS) {
    console.log(`Collecting ${market.label}...`);
    try {
      markets.push(await collectMarket(market));
    } catch (error) {
      console.warn(`Dashboard coverage collection failed for ${market.label}: ${error instanceof Error ? error.message : error}`);
      const fallback = emptyMarketData(market);
      fallback.readinessGaps = [
        `coverage query failed during dashboard generation: ${error instanceof Error ? error.message : String(error)}`,
        "live progress and rerun commands are still shown",
      ];
      markets.push(fallback);
    }
  }
  const jobStatuses = await collectRefreshJobStatuses(MARKETS);

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MXRE Market Coverage Dashboard</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:#f5f7f9; color:#16212f; }
    * { box-sizing:border-box; } body { margin:0; background:#f5f7f9; } header { padding:26px 32px 18px; border-bottom:1px solid #d8e0e8; background:#fff; position:sticky; top:0; z-index:10; }
    .eyebrow { color:#5a6b7e; font-size:13px; font-weight:700; text-transform:uppercase; } h1 { margin:5px 0 4px; font-size:30px; letter-spacing:0; } .sub,.note { color:#5a6b7e; font-size:13px; line-height:1.35; }
    .tabs { display:flex; flex-wrap:wrap; gap:8px; margin-top:16px; } .tab { appearance:none; border:1px solid #c9d4df; background:#fff; color:#233143; border-radius:6px; padding:9px 12px; font-weight:750; cursor:pointer; }
    .tab.active { background:#1f5f56; color:#fff; border-color:#1f5f56; } main { padding:24px 32px 36px; } .market-panel { display:none; } .market-panel.active { display:block; }
    .hero-metrics { display:grid; grid-template-columns:minmax(220px,300px) 1fr; gap:14px; margin-bottom:16px; }
    .overall-card,.health { background:#fff; border:1px solid #d8e0e8; border-radius:8px; padding:16px; }
    .overall-card.good,.health.good { border-color:#94c9bc; background:#f3fbf8; }
    .overall-card.warn,.health.warn { border-color:#e1c37a; background:#fffaf0; }
    .overall-card.bad,.health.bad { border-color:#df9da4; background:#fff5f6; }
    .creative-card { border-color:#b9d5cd; background:#f5fbf9; }
    .overall-value { margin-top:10px; font-size:54px; line-height:.95; font-weight:850; }
    .health-grid { display:grid; grid-template-columns:repeat(3,minmax(150px,1fr)); gap:10px; }
    .health-value { margin:8px 0 8px; font-size:28px; line-height:1; font-weight:850; }
    .summary-panel { margin-top:0; margin-bottom:18px; }
    .summary-grid { display:grid; grid-template-columns:repeat(4,minmax(190px,1fr)); gap:10px; }
    .summary-tile { display:block; color:#16212f; text-decoration:none; border:1px solid #d8e0e8; border-radius:8px; padding:13px; background:#fff; }
    .summary-tile.good { border-color:#94c9bc; background:#f3fbf8; }
    .summary-tile.warn { border-color:#e1c37a; background:#fffaf0; }
    .summary-tile.bad { border-color:#df9da4; background:#fff5f6; }
    .summary-tile.job-running { box-shadow: inset 4px 0 0 #1f9d7a; }
    .summary-tile.job-queued { box-shadow: inset 4px 0 0 #c27803; }
    .summary-top { display:flex; justify-content:space-between; gap:10px; align-items:center; font-size:13px; }
    .summary-top span { font-size:11px; font-weight:850; color:#4b5d70; background:#eef3f7; border-radius:999px; padding:3px 7px; }
    .summary-main { font-size:32px; line-height:1; font-weight:850; margin:10px 0 4px; }
    .summary-sub { color:#26384c; font-size:13px; font-weight:700; }
    .grid { display:grid; grid-template-columns:repeat(4,minmax(170px,1fr)); gap:14px; } .card,.panel,.status { background:#fff; border:1px solid #d8e0e8; border-radius:8px; padding:16px; }
    .card-link { display:block; color:inherit; text-decoration:none; }
    .card { min-height:116px; } .label { color:#5a6b7e; font-size:13px; font-weight:700; } .metric { margin-top:9px; font-size:30px; line-height:1; font-weight:800; }
    section { margin-top:22px; } h2 { margin:0 0 12px; font-size:18px; letter-spacing:0; } .bars { display:grid; gap:12px; } .row { display:grid; grid-template-columns:210px 1fr 76px; gap:12px; align-items:center; font-size:14px; }
    .track { height:12px; border-radius:999px; background:#e7edf3; overflow:hidden; } .fill { height:100%; background:#2f7d70; border-radius:999px; } .fill.warn { background:#c27803; } .fill.bad { background:#b53b45; }
    .two { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:14px; } table { width:100%; border-collapse:collapse; font-size:14px; } th,td { padding:10px 8px; border-bottom:1px solid #e4eaf0; text-align:left; vertical-align:top; } th { color:#5a6b7e; font-size:12px; text-transform:uppercase; }
    code { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size:12px; } .status { margin-top:14px; display:flex; gap:10px; align-items:flex-start; } .status.ready { border-color:#94c9bc; background:#f3fbf8; } .status.building { border-color:#e1c37a; background:#fffaf0; }
    .panel-head { display:flex; align-items:flex-start; justify-content:space-between; gap:16px; margin-bottom:12px; } .open-link { border:1px solid #c9d4df; color:#1f5f56; background:#fff; border-radius:6px; padding:9px 11px; text-decoration:none; font-weight:750; white-space:nowrap; }
    .live-frame { width:100%; height:460px; border:1px solid #d8e0e8; border-radius:6px; background:#f8fafc; } .empty-progress { display:grid; gap:9px; border:1px dashed #c9d4df; border-radius:6px; padding:14px; background:#f8fafc; color:#5a6b7e; line-height:1.45; }
    .count-pill { color:#1f5f56; background:#edf8f5; border:1px solid #b9d5cd; border-radius:999px; padding:7px 10px; font-size:12px; font-weight:850; white-space:nowrap; }
    .creative-list { display:grid; gap:12px; }
    .creative-item { display:grid; grid-template-columns:42px 1fr; gap:12px; border:1px solid #d8e0e8; border-radius:8px; padding:13px; background:#fbfdfe; }
    .creative-rank { width:30px; height:30px; display:grid; place-items:center; border-radius:999px; background:#1f5f56; color:#fff; font-weight:850; }
    .creative-title { display:flex; flex-wrap:wrap; gap:10px 14px; align-items:baseline; font-size:16px; }
    .creative-title strong { font-size:17px; }
    .creative-title span { color:#4b5d70; font-weight:750; }
    .creative-meta { display:flex; flex-wrap:wrap; gap:7px; margin:8px 0; }
    .creative-meta span,.creative-meta a { border:1px solid #d8e0e8; background:#fff; color:#3d5066; border-radius:999px; padding:4px 8px; font-size:12px; font-weight:750; text-decoration:none; }
    .creative-body p { margin:0; color:#26384c; line-height:1.48; font-size:14px; white-space:pre-wrap; }
    .api-form { display:grid; grid-template-columns:2fr 1fr 90px 110px 1.2fr 130px; gap:10px; align-items:end; }
    .api-field { display:grid; gap:5px; } .api-field label { color:#5a6b7e; font-size:12px; font-weight:800; text-transform:uppercase; }
    .api-field input { width:100%; border:1px solid #c9d4df; border-radius:6px; padding:10px 11px; font:inherit; background:#fff; }
    .api-button { border:1px solid #1f5f56; background:#1f5f56; color:#fff; border-radius:6px; padding:10px 12px; font-weight:850; cursor:pointer; }
    .api-button:disabled { opacity:.6; cursor:wait; } .api-result { margin-top:12px; min-height:180px; max-height:520px; overflow:auto; border:1px solid #d8e0e8; border-radius:6px; background:#0f1720; color:#d7f8e8; padding:12px; font-size:12px; line-height:1.45; white-space:pre-wrap; }
    @media (max-width:960px){ .hero-metrics,.health-grid,.summary-grid,.grid,.two{grid-template-columns:1fr;} .row{grid-template-columns:150px 1fr 62px;} header,main{padding-left:18px;padding-right:18px;} }
    @media (max-width:1180px){ .api-form{grid-template-columns:1fr 1fr;} }
  </style>
</head>
<body>
  <header>
    <div class="eyebrow">MXRE Market Coverage</div>
    <h1>Coverage Dashboard</h1>
    <div class="sub">Tabbed coverage snapshot generated ${esc(new Date().toLocaleString())}. This replaces separate per-market dashboard files.</div>
    <div class="tabs" role="tablist">
      ${markets.map((data, i) => `<button id="tab-${data.market.key}" class="tab${i === 0 ? " active" : ""}" role="tab" aria-selected="${i === 0}" aria-controls="panel-${data.market.key}" data-target="panel-${data.market.key}">${esc(data.market.label)}</button>`).join("")}
    </div>
  </header>
  <main>
    <section class="panel">
      <div class="panel-head">
        <div><h2>Live API Address Lookup</h2><div class="note">Calls the BBC exact-address endpoint and prints the raw JSON. API key is stored only in this browser session.</div></div>
      </div>
      <form id="apiLookupForm" class="api-form">
        <div class="api-field"><label for="apiAddress">Address</label><input id="apiAddress" autocomplete="street-address" placeholder="9105 Kinlock Dr" required></div>
        <div class="api-field"><label for="apiCity">City</label><input id="apiCity" autocomplete="address-level2" value="Indianapolis" required></div>
        <div class="api-field"><label for="apiState">State</label><input id="apiState" autocomplete="address-level1" value="IN" required></div>
        <div class="api-field"><label for="apiZip">ZIP</label><input id="apiZip" autocomplete="postal-code" placeholder="optional"></div>
        <div class="api-field"><label for="apiKey">BBC API Key</label><input id="apiKey" type="password" autocomplete="off" placeholder="paste once per session"></div>
        <button id="apiLookupButton" class="api-button" type="submit">Run Lookup</button>
      </form>
      <pre id="apiLookupResult" class="api-result">Enter an address and run lookup.</pre>
    </section>
    ${renderCompletionSummary(markets, jobStatuses)}
    ${markets.map(renderMarket).join("")}
  </main>
  <script>
    const tabs = Array.from(document.querySelectorAll(".tab"));
    const panels = Array.from(document.querySelectorAll(".market-panel"));
    function activate(id) {
      tabs.forEach(tab => {
        const active = tab.dataset.target === id;
        tab.classList.toggle("active", active);
        tab.setAttribute("aria-selected", String(active));
      });
      panels.forEach(panel => panel.classList.toggle("active", panel.id === id));
      history.replaceState(null, "", "#" + id.replace(/^panel-/, ""));
    }
    tabs.forEach(tab => tab.addEventListener("click", () => activate(tab.dataset.target)));
    const initial = location.hash ? "panel-" + location.hash.slice(1) : null;
    if (initial && document.getElementById(initial)) activate(initial);
    const apiForm = document.getElementById("apiLookupForm");
    const apiResult = document.getElementById("apiLookupResult");
    const apiKeyInput = document.getElementById("apiKey");
    const savedApiKey = sessionStorage.getItem("mxre_bbc_api_key") || "";
    if (savedApiKey) apiKeyInput.value = savedApiKey;
    apiForm.addEventListener("submit", async event => {
      event.preventDefault();
      const button = document.getElementById("apiLookupButton");
      const apiKey = apiKeyInput.value.trim();
      if (!apiKey) {
        apiResult.textContent = "Paste the BBC sandbox API key first. It stays in sessionStorage for this browser tab only.";
        return;
      }
      sessionStorage.setItem("mxre_bbc_api_key", apiKey);
      const params = new URLSearchParams({
        address: document.getElementById("apiAddress").value.trim(),
        city: document.getElementById("apiCity").value.trim(),
        state: document.getElementById("apiState").value.trim(),
      });
      const zip = document.getElementById("apiZip").value.trim();
      if (zip) params.set("zip", zip);
      const url = "/api/bbc/property?" + params.toString();
      button.disabled = true;
      apiResult.textContent = "GET " + url + "\\nLoading...";
      try {
        const response = await fetch(url, {
          headers: { "x-client-id": "buy_box_club_sandbox", "x-api-key": apiKey },
        });
        const text = await response.text();
        let body = text;
        try { body = JSON.stringify(JSON.parse(text), null, 2); } catch {}
        apiResult.textContent = "HTTP " + response.status + " " + response.statusText + "\\nGET " + url + "\\n\\n" + body;
      } catch (error) {
        apiResult.textContent = "Lookup failed: " + (error && error.message ? error.message : String(error));
      } finally {
        button.disabled = false;
      }
    });
  </script>
</body>
</html>`;

  await mkdir(join(process.cwd(), "logs", "market-refresh"), { recursive: true });
  await writeFile(join(process.cwd(), OUT), html, "utf8");
  const aliases: string[] = [];
  if (OUT.replace(/\\/g, "/").endsWith("market-coverage-dashboard.html")) {
    const dallasAlias = "logs/market-refresh/dallas-coverage-dashboard.html";
    await writeFile(join(process.cwd(), dallasAlias), html, "utf8");
    aliases.push(dallasAlias);
  }
  console.log(JSON.stringify({ wrote: OUT, aliases, markets: MARKETS.map(m => m.key), generated_at: new Date().toISOString() }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
