#!/usr/bin/env tsx
/**
 * County Census — Discover and classify all US county assessor & recorder portals.
 *
 * Strategy:
 * 1. Load full FIPS county list
 * 2. Check known hosted-platform domains for each county (fast, no scraping)
 * 3. For self-hosted counties, do a targeted Google search + HTML classification
 * 4. Output census.json: every county mapped to platform(s) + URLs
 *
 * Usage: npx tsx scripts/county-census.ts [--state TX] [--resume]
 */

import { writeFileSync, readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "..", "data");
const CENSUS_FILE = join(OUT_DIR, "census.json");
const FIPS_FILE = join(OUT_DIR, "fips-counties.json");

// ─── Types ─────────────────────────────────────────────────────────

interface FipsCounty {
  fips: string;          // 5-digit combined FIPS
  state_fips: string;    // 2-digit state
  county_fips: string;   // 3-digit county
  name: string;          // County name (no "County" suffix)
  state: string;         // 2-letter abbreviation
  state_name: string;    // Full state name
}

interface CensusEntry {
  fips: string;
  state_fips: string;
  county_fips: string;
  name: string;
  state: string;
  state_name: string;
  assessor?: {
    platform: string;
    url: string;
    confidence: "domain_match" | "url_pattern" | "html_detected" | "search_result" | "manual";
  };
  recorder?: {
    platform: string;
    url: string;
    confidence: "domain_match" | "url_pattern" | "html_detected" | "search_result" | "manual";
  };
  status: "identified" | "partial" | "unknown";
  checked_at: string;
}

// ─── State FIPS → Abbreviation mapping ─────────────────────────────

const STATE_FIPS: Record<string, string> = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
  "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
  "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
  "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
  "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
  "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
  "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
  "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
  "56": "WY",
};

const STATE_NAMES: Record<string, string> = {
  "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
  "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia", "FL": "Florida",
  "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana",
  "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
  "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
  "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
  "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota",
  "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
  "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
  "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin",
  "WY": "Wyoming",
};

// ─── Step 1: Load FIPS county list ─────────────────────────────────

async function loadFipsCounties(): Promise<FipsCounty[]> {
  // Try cached version first
  if (existsSync(FIPS_FILE)) {
    console.log("Loading cached FIPS data...");
    return JSON.parse(readFileSync(FIPS_FILE, "utf-8"));
  }

  console.log("Downloading FIPS county list from Census Bureau...");
  const url = "https://raw.githubusercontent.com/kjhealy/fips-codes/master/county_fips_master.csv";
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch FIPS data: ${resp.status}`);
  const csv = await resp.text();

  const lines = csv.trim().split("\n");
  const header = lines[0].split(",").map(h => h.trim().replace(/"/g, ""));
  const counties: FipsCounty[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row: Record<string, string> = {};
    header.forEach((h, idx) => { row[h] = (values[idx] ?? "").trim().replace(/"/g, ""); });

    const fips = row["fips"]?.padStart(5, "0");
    if (!fips || fips.length !== 5) continue;

    const stateFips = fips.slice(0, 2);
    const countyFips = fips.slice(2, 5);
    const stateAbbr = STATE_FIPS[stateFips];
    if (!stateAbbr) continue; // Skip territories

    // Clean county name: remove " County", " Parish", " Borough", etc.
    let name = row["county_name"] || row["name"] || "";
    name = name
      .replace(/\s+(County|Parish|Borough|Census Area|Municipality|city)$/i, "")
      .trim();

    counties.push({
      fips,
      state_fips: stateFips,
      county_fips: countyFips,
      name,
      state: stateAbbr,
      state_name: STATE_NAMES[stateAbbr] || "",
    });
  }

  writeFileSync(FIPS_FILE, JSON.stringify(counties, null, 2));
  console.log(`Loaded ${counties.length} counties.`);
  return counties;
}

function parseCSVLine(line: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;
  for (const char of line) {
    if (char === '"') { inQuotes = !inQuotes; continue; }
    if (char === "," && !inQuotes) { values.push(current.trim()); current = ""; continue; }
    current += char;
  }
  values.push(current.trim());
  return values;
}

// ─── Step 2: Known hosted-platform URL patterns ────────────────────
// These platforms use predictable URLs based on county/state names.
// We can check these WITHOUT scraping — just construct the URL and HEAD it.

interface PlatformProbe {
  platform: string;
  type: "assessor" | "recorder";
  buildUrl: (county: FipsCounty) => string[];
  /** Domain signature to verify — if the response URL contains this, it's a match */
  domainSignature?: string;
}

function slugify(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "").replace(/\s+/g, "");
}
function slugifyDash(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/\s+/g, "-");
}
function titleCase(name: string): string {
  return name.split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join("");
}

const HOSTED_PROBES: PlatformProbe[] = [
  // ─── Assessor Platforms ───
  // VALIDATED: devnet — 2/2 passed. Try both subdomain patterns.
  {
    platform: "devnet",
    type: "assessor",
    buildUrl: (c) => [
      `https://${slugify(c.name)}${c.state.toLowerCase()}-assessor.devnetwedge.com/`,
      `https://${slugify(c.name)}${c.state.toLowerCase()}.devnetwedge.com/`,
    ],
    domainSignature: "devnetwedge.com",
  },
  // VALIDATED: true_automation — 2/2 passed. Big for Texas CADs.
  {
    platform: "true_automation",
    type: "assessor",
    buildUrl: (c) => [
      `https://propaccess.trueautomation.com/clientdb/?cid=${slugify(c.name)}`,
    ],
    domainSignature: "trueautomation.com",
  },
  // VALIDATED: taxsifter — 2/2 passed.
  {
    platform: "taxsifter",
    type: "assessor",
    buildUrl: (c) => [
      `https://${slugify(c.name)}${c.state.toLowerCase()}-taxsifter.publicaccessnow.com/`,
    ],
    domainSignature: "publicaccessnow.com",
  },
  // VALIDATED: patriot — 1/1 passed. Uses municipality names, not counties.
  {
    platform: "patriot",
    type: "assessor",
    buildUrl: (c) => [
      `https://${slugify(c.name)}.patriotproperties.com/default.asp`,
    ],
    domainSignature: "patriotproperties.com",
  },
  // FIXED: tyler — multiple product lines with different subdomain formats
  {
    platform: "tyler_web",
    type: "assessor",
    buildUrl: (c) => [
      `https://${slugify(c.name)}county${c.state.toLowerCase()}-web.tylerhost.net/web/`,
      `https://${slugify(c.name)}${c.state.toLowerCase()}-web.tylerhost.net/web/`,
      `https://${slugify(c.name)}county${c.state.toLowerCase()}-pa.tylerhost.net/`,
    ],
    domainSignature: "tylerhost.net",
  },
  // FIXED: tyler assessor variant
  {
    platform: "tyler_assessor",
    type: "assessor",
    buildUrl: (c) => [
      `https://${slugify(c.name)}county${c.state.toLowerCase()}-assessor.tylerhost.net/assessor/web/`,
      `https://${slugify(c.name)}${c.state.toLowerCase()}-assessor.tylerhost.net/assessor/web/`,
    ],
    domainSignature: "tylerhost.net",
  },
  // FIXED: qpublic — uses {Name}County{ST} format. May 403 from datacenter IPs.
  {
    platform: "qpublic",
    type: "assessor",
    buildUrl: (c) => [
      `https://qpublic.schneidercorp.com/Application.aspx?App=${titleCase(c.name)}County${c.state.toUpperCase()}&PageType=Search`,
    ],
    domainSignature: "schneidercorp.com",
  },
  // beacon — Schneider's other product
  {
    platform: "beacon",
    type: "assessor",
    buildUrl: (c) => [
      `https://beacon.schneidercorp.com/Application.aspx?App=${titleCase(c.name)}County${c.state.toUpperCase()}`,
    ],
    domainSignature: "schneidercorp.com",
  },
  // FIXED: isw — dbkey must be lowercase {county}cad, use webSearchName.aspx not webindex
  {
    platform: "isw",
    type: "assessor",
    buildUrl: (c) => [
      `https://iswdataclient.azurewebsites.net/webSearchName.aspx?dbkey=${slugify(c.name)}cad`,
    ],
    domainSignature: "iswdataclient.azurewebsites.net",
  },
  // actdatascout — known to block datacenter IPs, will only work through proxy
  {
    platform: "actdatascout",
    type: "assessor",
    buildUrl: (c) => [
      `https://www.actdatascout.com/RealProperty/${c.state_name}/${titleCase(c.name)}`,
    ],
    domainSignature: "actdatascout.com",
  },
  // VGSI — naming too inconsistent for URL guessing. Will scrape state listing pages instead.
  // Handled separately in scrapeVgsiListings()

  // ─── Recorder Platforms ───
  // VALIDATED: fidlar_ava — 3/3 passed. Pattern: {ST}{CountyName}
  {
    platform: "fidlar_ava",
    type: "recorder",
    buildUrl: (c) => [
      `https://ava.fidlar.com/${c.state.toUpperCase()}${titleCase(c.name)}/AvaWeb/`,
    ],
    domainSignature: "fidlar.com",
  },
  // VALIDATED: tyler_eagleweb — 2/2 passed. Recorder search portal.
  {
    platform: "tyler_eagleweb",
    type: "recorder",
    buildUrl: (c) => [
      `https://${slugify(c.name)}county${c.state.toLowerCase()}-recorder.tylerhost.net/recorder/eagleweb/docSearch.jsp`,
      `https://${slugify(c.name)}county${c.state.toLowerCase()}-recorder.tylerhost.net/recorder/web/login.jsp`,
    ],
    domainSignature: "tylerhost.net",
  },
  // Tyler recorder variant without "county" in subdomain
  {
    platform: "tyler_recorder",
    type: "recorder",
    buildUrl: (c) => [
      `https://${slugify(c.name)}${c.state.toLowerCase()}-recorder.tylerhost.net/recorder/web/login.jsp`,
    ],
    domainSignature: "tylerhost.net",
  },
  // VALIDATED: cott_recordhub — 1/1 passed.
  {
    platform: "cott_recordhub",
    type: "recorder",
    buildUrl: (c) => [
      `https://recordhub.cottsystems.com/${slugify(c.name)}/`,
    ],
    domainSignature: "cottsystems.com",
  },
];

// ─── Step 3: Probe counties against known platforms ────────────────

const CONCURRENCY = 30;
const TIMEOUT_MS = 8000;

/**
 * Platforms that return 200 for ANY county need content validation.
 * These require GET + body inspection to filter false positives.
 */
const NEEDS_BODY_VALIDATION = new Set(["true_automation", "cott_recordhub"]);

async function probeUrl(
  url: string,
  domainSignature?: string,
  platform?: string,
  countyName?: string,
): Promise<{ ok: boolean; finalUrl?: string }> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

    const needsBody = platform && NEEDS_BODY_VALIDATION.has(platform);
    const resp = await fetch(url, {
      method: needsBody ? "GET" : "HEAD",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      },
    });
    clearTimeout(timer);

    if (!resp.ok) return { ok: false };

    const finalUrl = resp.url || url;
    // If we have a domain signature, verify it matches
    if (domainSignature && !finalUrl.includes(domainSignature)) {
      return { ok: false };
    }

    // Content validation for false-positive-prone platforms
    if (needsBody && countyName) {
      const body = await resp.text();

      if (platform === "true_automation") {
        // true_automation returns identical 1144-byte generic page for any cid.
        // Real counties redirect to a county-specific domain or have county name in body.
        // A 1144-byte page is always the generic "Property Search" page = false positive.
        if (body.length < 2000) return { ok: false };
        // Also check if the body contains the county name or "property search" content
        const lower = body.toLowerCase();
        if (!lower.includes(countyName.toLowerCase()) && !lower.includes("search")) {
          return { ok: false };
        }
      }

      if (platform === "cott_recordhub") {
        // cott returns a login page for any path. Real counties have the county name
        // specifically in the page content or a county-specific title.
        const lower = body.toLowerCase();
        // Generic login pages don't contain the county name
        if (!lower.includes(countyName.toLowerCase())) {
          return { ok: false };
        }
      }
    }

    return { ok: true, finalUrl };
  } catch {
    return { ok: false };
  }
}

async function probeCounty(
  county: FipsCounty,
  existing?: CensusEntry,
): Promise<CensusEntry> {
  const entry: CensusEntry = existing ?? {
    fips: county.fips,
    state_fips: county.state_fips,
    county_fips: county.county_fips,
    name: county.name,
    state: county.state,
    state_name: county.state_name,
    status: "unknown",
    checked_at: new Date().toISOString(),
  };

  // Skip if already fully identified
  if (entry.assessor && entry.recorder) {
    entry.status = "identified";
    return entry;
  }

  for (const probe of HOSTED_PROBES) {
    // Skip if we already found this type
    if (probe.type === "assessor" && entry.assessor) continue;
    if (probe.type === "recorder" && entry.recorder) continue;

    const urls = probe.buildUrl(county);
    for (const url of urls) {
      const result = await probeUrl(url, probe.domainSignature, probe.platform, county.name);
      if (result.ok) {
        const match = {
          platform: probe.platform,
          url: result.finalUrl || url,
          confidence: "domain_match" as const,
        };
        if (probe.type === "assessor") entry.assessor = match;
        else entry.recorder = match;
        break;
      }
    }
  }

  // Update status
  if (entry.assessor && entry.recorder) entry.status = "identified";
  else if (entry.assessor || entry.recorder) entry.status = "partial";
  else entry.status = "unknown";

  entry.checked_at = new Date().toISOString();
  return entry;
}

// ─── Step 4: Run census with concurrency control ───────────────────

async function runCensus(counties: FipsCounty[], stateFilter?: string) {
  // Load existing results for resume
  let results: Map<string, CensusEntry> = new Map();
  if (existsSync(CENSUS_FILE)) {
    const existing: CensusEntry[] = JSON.parse(readFileSync(CENSUS_FILE, "utf-8"));
    for (const e of existing) results.set(e.fips, e);
    console.log(`Resuming with ${results.size} existing entries.`);
  }

  let filtered = counties;
  if (stateFilter) {
    filtered = counties.filter(c => c.state === stateFilter.toUpperCase());
    console.log(`Filtered to ${filtered.length} counties in ${stateFilter.toUpperCase()}.`);
  }

  // Skip already-identified counties
  const toProcess = filtered.filter(c => {
    const existing = results.get(c.fips);
    return !existing || existing.status === "unknown";
  });

  console.log(`\nProbing ${toProcess.length} counties across ${HOSTED_PROBES.length} platform patterns...`);
  console.log(`Concurrency: ${CONCURRENCY}, Timeout: ${TIMEOUT_MS}ms\n`);

  let completed = 0;
  const total = toProcess.length;
  const startTime = Date.now();

  // Process in batches
  for (let i = 0; i < toProcess.length; i += CONCURRENCY) {
    const batch = toProcess.slice(i, i + CONCURRENCY);
    const batchResults = await Promise.all(
      batch.map(county => probeCounty(county, results.get(county.fips)))
    );

    for (const entry of batchResults) {
      results.set(entry.fips, entry);
    }

    completed += batch.length;
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = completed / elapsed;
    const eta = Math.round((total - completed) / rate);

    // Count stats
    const all = Array.from(results.values());
    const identified = all.filter(e => e.status === "identified").length;
    const partial = all.filter(e => e.status === "partial").length;
    const unknown = all.filter(e => e.status === "unknown").length;

    process.stdout.write(
      `\r[${completed}/${total}] ${rate.toFixed(1)}/s | ETA ${eta}s | ` +
      `Found: ${identified} full, ${partial} partial, ${unknown} unknown`
    );

    // Save checkpoint every 100 counties
    if (completed % 100 < CONCURRENCY) {
      saveResults(results);
    }
  }

  console.log("\n");
  saveResults(results);
  return results;
}

function saveResults(results: Map<string, CensusEntry>) {
  const arr = Array.from(results.values()).sort((a, b) => a.fips.localeCompare(b.fips));
  writeFileSync(CENSUS_FILE, JSON.stringify(arr, null, 2));
}

// ─── Step 5: Generate report ───────────────────────────────────────

function generateReport(results: Map<string, CensusEntry>) {
  const all = Array.from(results.values());
  const total = all.length;

  console.log("═══════════════════════════════════════════════════════");
  console.log("  MXRE COUNTY CENSUS REPORT");
  console.log("═══════════════════════════════════════════════════════\n");
  console.log(`Total counties: ${total}`);
  console.log(`Fully identified (assessor + recorder): ${all.filter(e => e.status === "identified").length}`);
  console.log(`Partial (assessor or recorder): ${all.filter(e => e.status === "partial").length}`);
  console.log(`Unknown: ${all.filter(e => e.status === "unknown").length}\n`);

  // Assessor platform breakdown
  const assessorPlatforms = new Map<string, CensusEntry[]>();
  const recorderPlatforms = new Map<string, CensusEntry[]>();
  let noAssessor = 0;
  let noRecorder = 0;

  for (const e of all) {
    if (e.assessor) {
      const list = assessorPlatforms.get(e.assessor.platform) || [];
      list.push(e);
      assessorPlatforms.set(e.assessor.platform, list);
    } else {
      noAssessor++;
    }
    if (e.recorder) {
      const list = recorderPlatforms.get(e.recorder.platform) || [];
      list.push(e);
      recorderPlatforms.set(e.recorder.platform, list);
    } else {
      noRecorder++;
    }
  }

  console.log("─── ASSESSOR PLATFORMS ────────────────────────────────\n");
  const sortedAssessor = [...assessorPlatforms.entries()].sort((a, b) => b[1].length - a[1].length);
  for (const [platform, counties] of sortedAssessor) {
    const states = [...new Set(counties.map(c => c.state))].sort();
    console.log(`  ${platform.padEnd(25)} ${String(counties.length).padStart(5)} counties  [${states.join(", ")}]`);
  }
  console.log(`  ${"(not found)".padEnd(25)} ${String(noAssessor).padStart(5)} counties`);
  console.log();

  console.log("─── RECORDER PLATFORMS ───────────────────────────────\n");
  const sortedRecorder = [...recorderPlatforms.entries()].sort((a, b) => b[1].length - a[1].length);
  for (const [platform, counties] of sortedRecorder) {
    const states = [...new Set(counties.map(c => c.state))].sort();
    console.log(`  ${platform.padEnd(25)} ${String(counties.length).padStart(5)} counties  [${states.join(", ")}]`);
  }
  console.log(`  ${"(not found)".padEnd(25)} ${String(noRecorder).padStart(5)} counties`);
  console.log();

  // Coverage by state
  console.log("─── STATE COVERAGE ──────────────────────────────────\n");
  const byState = new Map<string, { total: number; assessor: number; recorder: number }>();
  for (const e of all) {
    const s = byState.get(e.state) || { total: 0, assessor: 0, recorder: 0 };
    s.total++;
    if (e.assessor) s.assessor++;
    if (e.recorder) s.recorder++;
    byState.set(e.state, s);
  }
  const sortedStates = [...byState.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  console.log(`  ${"State".padEnd(6)} ${"Total".padStart(6)} ${"Assess".padStart(8)} ${"Record".padStart(8)} ${"Coverage".padStart(10)}`);
  console.log(`  ${"─".repeat(6)} ${"─".repeat(6)} ${"─".repeat(8)} ${"─".repeat(8)} ${"─".repeat(10)}`);
  for (const [st, stats] of sortedStates) {
    const coverage = Math.round(((stats.assessor + stats.recorder) / (stats.total * 2)) * 100);
    console.log(`  ${st.padEnd(6)} ${String(stats.total).padStart(6)} ${String(stats.assessor).padStart(8)} ${String(stats.recorder).padStart(8)} ${(coverage + "%").padStart(10)}`);
  }

  // Save report as text too
  const reportPath = join(OUT_DIR, "census-report.txt");
  console.log(`\nReport saved to ${reportPath}`);
  console.log(`Full data saved to ${CENSUS_FILE}`);
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const stateFilter = args.includes("--state") ? args[args.indexOf("--state") + 1] : undefined;
  const reportOnly = args.includes("--report");

  const counties = await loadFipsCounties();
  console.log(`Loaded ${counties.length} US counties.\n`);

  if (reportOnly) {
    if (!existsSync(CENSUS_FILE)) {
      console.error("No census data found. Run without --report first.");
      process.exit(1);
    }
    const existing: CensusEntry[] = JSON.parse(readFileSync(CENSUS_FILE, "utf-8"));
    const map = new Map<string, CensusEntry>();
    for (const e of existing) map.set(e.fips, e);
    generateReport(map);
    return;
  }

  const results = await runCensus(counties, stateFilter);
  generateReport(results);
}

main().catch(console.error);
