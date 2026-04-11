#!/usr/bin/env tsx
/**
 * Fast County Ingest v2 — Section-level concurrent search.
 *
 * Key discoveries from testing the live site:
 *   - Correct search URL: /RealProperty/Oklahoma/Comanche (not /Search/40031)
 *   - Form submits via AJAX (requestSubmit), results returned as HTML partial
 *   - Must intercept POST response to parse results (not DOM after navigation)
 *   - Must override navigator.userAgentData to hide HeadlessChrome
 *   - Must use --disable-blink-features=AutomationControlled
 *   - Page stays at same URL; form values are updated for each search
 *
 * Usage:
 *   SUPABASE_URL=http://207.244.225.239:8000 \
 *   SUPABASE_SERVICE_KEY="..." \
 *   PROXY_URL="http://user:pass@host:port" \
 *   npx tsx scripts/fast-ingest-v2.ts
 */

import "dotenv/config";
import { chromium, type Page, type BrowserContext, type Browser, type Response } from "playwright";
import { createClient } from "@supabase/supabase-js";

// ─── Config ─────────────────────────────────────────────────────────

const CONCURRENCY = 4;
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 5_000;
const BATCH_SIZE = 200;
const COUNTY_ID = 3; // Comanche County in self-hosted Supabase

// The correct URL that loads the county search page with all forms
const COUNTY_PAGE_URL = "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche";

const PROXY_URL = process.env.PROXY_URL || "";

// S-T-R grid for Comanche County
const TOWNSHIPS = [1, 2, 3, 4, -1, -2, -3]; // positive = North, negative = South
const RANGES = [9, 10, 11, 12, 13, 14, 15, 16];
const SECTIONS = Array.from({ length: 36 }, (_, i) => i + 1);

// ─── Database ───────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY env vars.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── Stealth Config ─────────────────────────────────────────────────

const CHROME_VERSION = 148;

function randomItem<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function getStealthConfig() {
  const userAgents = [
    `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION - 1}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${CHROME_VERSION}.0.0.0 Safari/537.36`,
  ];
  const viewports = [
    { width: 1920, height: 1080 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1366, height: 768 },
  ];
  return {
    userAgent: randomItem(userAgents),
    viewport: randomItem(viewports),
    timezoneId: "America/Chicago",
    locale: "en-US",
    extraHTTPHeaders: {
      "Accept-Language": "en-US,en;q=0.9",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
    },
  };
}

// Enhanced stealth: hides HeadlessChrome from navigator.userAgentData
const STEALTH_INIT_SCRIPT = `
  Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
  Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
  if (window.chrome) { window.chrome.runtime = undefined; }
  if (navigator.userAgentData) {
    Object.defineProperty(navigator, 'userAgentData', {
      get: () => ({
        brands: [
          {brand: "Google Chrome", version: "${CHROME_VERSION}"},
          {brand: "Chromium", version: "${CHROME_VERSION}"},
          {brand: "Not_A Brand", version: "24"},
        ],
        mobile: false,
        platform: "Windows",
        getHighEntropyValues: () => Promise.resolve({
          brands: [
            {brand: "Google Chrome", version: "${CHROME_VERSION}.0.0.0"},
            {brand: "Chromium", version: "${CHROME_VERSION}.0.0.0"},
            {brand: "Not_A Brand", version: "24.0.0.0"},
          ],
          fullVersionList: [
            {brand: "Google Chrome", version: "${CHROME_VERSION}.0.0.0"},
            {brand: "Chromium", version: "${CHROME_VERSION}.0.0.0"},
          ],
          mobile: false,
          model: "",
          platform: "Windows",
          platformVersion: "15.0.0",
          architecture: "x86",
          bitness: "64",
        }),
      }),
    });
  }
`;

// ─── Types ──────────────────────────────────────────────────────────

interface SearchResult {
  crpid: string;
  rpid: string;
  parcel: string;
  ownerName: string;
  businessName: string;
  address: string;
  str: string;
  subdivision: string;
  legal: string;
  acres: string;
}

interface SearchTask {
  section: number;
  township: number;
  range: number;
  label: string;
}

// ─── Browser Pool ───────────────────────────────────────────────────

interface WorkerContext {
  id: number;
  browser: Browser;
  context: BrowserContext;
  page: Page;
  lastRequest: number;
  pageReady: boolean; // Whether the county search page is loaded
  consecutiveErrors: number;
}

async function createWorker(id: number): Promise<WorkerContext> {
  const stealth = getStealthConfig();

  const launchOpts: Parameters<typeof chromium.launch>[0] = {
    headless: true,
    args: ["--disable-blink-features=AutomationControlled"],
  };

  if (PROXY_URL) {
    try {
      const parsed = new URL(PROXY_URL);
      launchOpts.proxy = {
        server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
        username: parsed.username || undefined,
        password: parsed.password || undefined,
      };
    } catch { /* skip */ }
  }

  const browser = await chromium.launch(launchOpts);
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
    timezoneId: stealth.timezoneId,
    extraHTTPHeaders: stealth.extraHTTPHeaders,
  });

  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);
  page.setDefaultTimeout(30_000);

  return { id, browser, context, page, lastRequest: 0, pageReady: false, consecutiveErrors: 0 };
}

async function closeWorker(w: WorkerContext) {
  try { await w.page.close(); } catch {}
  try { await w.context.close(); } catch {}
  try { await w.browser.close(); } catch {}
}

/**
 * Navigate to the county search page and wait for the STR form to be available.
 * Must be called once per worker (or after a browser restart).
 */
async function loadSearchPage(worker: WorkerContext): Promise<boolean> {
  try {
    await worker.page.goto(COUNTY_PAGE_URL, { waitUntil: "networkidle", timeout: 45_000 });
    await worker.page.waitForTimeout(2000);

    // Verify the form loaded
    const hasForm = await worker.page.evaluate(() => {
      return !!document.querySelector("#RealFormSTRSearch input[name=Section]");
    });

    if (!hasForm) {
      const title = await worker.page.title();
      console.log(`  [W${worker.id}] Page loaded but form not found. Title: "${title}"`);
      return false;
    }

    // Click the S-T-R tab to make form visible
    await worker.page.evaluate(() => {
      const strTab = [...document.querySelectorAll("a")].find(a => a.textContent?.trim() === "S-T-R");
      if (strTab) strTab.click();
      // Also make the tab pane visible
      const pane = document.querySelector("#rpstr") as HTMLElement;
      if (pane) {
        pane.classList.add("active", "in");
        pane.style.display = "block";
      }
    });
    await worker.page.waitForTimeout(500);

    worker.pageReady = true;
    return true;
  } catch (err) {
    console.error(`  [W${worker.id}] Failed to load search page: ${err instanceof Error ? err.message : err}`);
    return false;
  }
}

// ─── Search Logic ───────────────────────────────────────────────────

async function rateLimit(worker: WorkerContext) {
  const elapsed = Date.now() - worker.lastRequest;
  const wait = Math.max(0, RATE_LIMIT_MS - elapsed);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  worker.lastRequest = Date.now();
}

/**
 * Parse search results from the AJAX response HTML.
 */
function parseSearchResultsFromHTML(html: string, page: Page): Promise<SearchResult[]> {
  return page.evaluate((responseHtml: string) => {
    const parser = new DOMParser();
    const doc = parser.parseFromString(responseHtml, "text/html");
    const rows = doc.querySelectorAll("#RealPropertyResultsTable tbody tr");
    const results: any[] = [];
    for (const row of rows) {
      const cells = row.querySelectorAll("td");
      if (cells.length < 9) continue;
      results.push({
        crpid: cells[1]?.textContent?.trim() || "",
        rpid: cells[2]?.textContent?.trim() || "",
        parcel: cells[3]?.textContent?.trim() || "",
        ownerName: cells[4]?.textContent?.trim() || "",
        businessName: cells[5]?.textContent?.trim() || "",
        address: cells[6]?.textContent?.trim() || "",
        str: cells[7]?.textContent?.trim() || "",
        subdivision: cells[8]?.textContent?.trim() || "",
        legal: cells[9]?.textContent?.trim() || "",
        acres: cells[10]?.textContent?.trim() || "",
      });
    }
    return results;
  }, html) as Promise<SearchResult[]>;
}

async function searchSection(
  worker: WorkerContext,
  task: SearchTask,
): Promise<SearchResult[]> {
  await rateLimit(worker);

  // Ensure search page is loaded
  if (!worker.pageReady) {
    const loaded = await loadSearchPage(worker);
    if (!loaded) throw new Error("Could not load search page");
  }

  try {
    // Set up response interceptor for this specific search
    let searchResponseResolve: (value: string) => void;
    const searchResponsePromise = new Promise<string>((resolve) => {
      searchResponseResolve = resolve;
      // Timeout after 30 seconds
      setTimeout(() => resolve(""), 30_000);
    });

    const responseHandler = async (resp: Response) => {
      if (resp.url().includes("/RealProperty/Search") && resp.request().method() === "POST") {
        try {
          const body = await resp.text();
          searchResponseResolve(body);
        } catch {
          searchResponseResolve("");
        }
      }
    };
    worker.page.on("response", responseHandler);

    // Fill form fields and submit
    const searchArgs = { section: task.section, township: Math.abs(task.township), range: task.range };
    await worker.page.evaluate((args) => {
      const form = document.querySelector("#RealFormSTRSearch") as HTMLFormElement;
      if (!form) throw new Error("Form not found");

      const secInput = form.querySelector("input[name=Section]") as HTMLInputElement;
      const twpInput = form.querySelector("input[name=Township]") as HTMLInputElement;
      const rngInput = form.querySelector("input[name=Range]") as HTMLInputElement;

      if (secInput) secInput.value = String(args.section);
      if (twpInput) twpInput.value = String(args.township);
      if (rngInput) rngInput.value = String(args.range);

      form.requestSubmit();
    }, searchArgs);

    // Wait for the AJAX response
    const responseHtml = await searchResponsePromise;

    // Remove listener
    worker.page.removeListener("response", responseHandler);

    // Wait a moment for page to settle
    await worker.page.waitForTimeout(500);

    if (!responseHtml) {
      throw new Error("No search response received");
    }

    // Check if we got blocked
    if (responseHtml.includes("BlockedAccess") || responseHtml.includes("currently blocked")) {
      worker.pageReady = false;
      throw new Error("Blocked by WAF");
    }

    // Check for "No results found"
    if (responseHtml.includes("No results found")) {
      return [];
    }

    // Parse results from response HTML
    const results = await parseSearchResultsFromHTML(responseHtml, worker.page);
    return results;

  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown";
    // If blocked, form not found, or no response, mark page as needing reload
    if (msg.includes("Blocked") || msg.includes("Form not found") || msg.includes("not load") || msg.includes("No search response")) {
      worker.pageReady = false;
    }
    throw new Error(`Search ${task.label}: ${msg}`);
  }
}

async function searchWithRetry(
  worker: WorkerContext,
  task: SearchTask,
): Promise<SearchResult[]> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const results = await searchSection(worker, task);
      worker.consecutiveErrors = 0;
      return results;
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown";
      worker.consecutiveErrors++;

      if (attempt < MAX_RETRIES) {
        const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
        console.log(`  [W${worker.id}] Retry ${attempt}/${MAX_RETRIES} for ${task.label} in ${(delay / 1000).toFixed(1)}s: ${msg}`);
        await new Promise(r => setTimeout(r, delay));

        // Restart browser if blocked or too many consecutive errors
        if (msg.includes("Blocked") || worker.consecutiveErrors >= 5) {
          console.log(`  [W${worker.id}] Restarting browser (${worker.consecutiveErrors} consecutive errors)...`);
          await closeWorker(worker);
          const fresh = await createWorker(worker.id);
          worker.browser = fresh.browser;
          worker.context = fresh.context;
          worker.page = fresh.page;
          worker.lastRequest = fresh.lastRequest;
          worker.pageReady = false;
          worker.consecutiveErrors = 0;

          // Extra delay after restart
          await new Promise(r => setTimeout(r, 5000));
        } else {
          // Just reload the page
          worker.pageReady = false;
        }
      } else {
        console.error(`  [W${worker.id}] FAILED ${task.label} after ${MAX_RETRIES} attempts: ${msg}`);
        return [];
      }
    }
  }
  return [];
}

// ─── Address / City Normalization (inline) ──────────────────────────

function normalizeAddress(address: string): string {
  return address.replace(/\s+/g, " ").trim().replace(/^(\d+)\s+/, "$1 ").toUpperCase();
}

function inferCity(rawCity: string, subdivision?: string): string {
  const sub = (subdivision ?? "").toUpperCase().trim();
  const city = rawCity.toUpperCase().trim();

  if (sub.startsWith("LAWTON") || sub === "BISHOP" || sub === "FLOWER MOUND") return "LAWTON";
  if (sub.startsWith("CACHE")) return "CACHE";
  if (sub.startsWith("ELGIN")) return "ELGIN";
  if (sub.startsWith("GERONIMO") || sub.startsWith("GREATER GERONIMO")) return "GERONIMO";
  if (sub.startsWith("FLETCHER")) return "FLETCHER";
  if (sub.startsWith("STERLING")) return "STERLING";
  if (sub.startsWith("INDIAHOMA")) return "INDIAHOMA";
  if (sub.startsWith("MEDICINE PARK")) return "MEDICINE PARK";
  if (sub.startsWith("CHATTANOOGA")) return "CHATTANOOGA";
  if (sub.startsWith("FAXON")) return "FAXON";

  return city || sub;
}

// ─── Property Type Classification ───────────────────────────────────

const APARTMENT_KEYWORDS = [
  "apartment", "apt", "multi-family", "multifamily", "multi family",
  "duplex", "triplex", "fourplex", "quadplex", "4-plex",
];
const CONDO_KEYWORDS = ["condo", "condominium", "townhouse", "townhome"];
const COMMERCIAL_KEYWORDS = ["commercial", "office", "retail", "warehouse", "industrial", "hotel", "motel"];
const LAND_KEYWORDS = ["vacant", "land", "lot", "acreage", "farm", "ranch", "agricultural", "pasture"];

function classifyType(address: string, legal: string) {
  const combined = `${address} ${legal}`.toLowerCase();
  if (APARTMENT_KEYWORDS.some(k => combined.includes(k)))
    return { property_type: "multifamily", is_apartment: true, is_sfr: false, is_condo: false };
  if (CONDO_KEYWORDS.some(k => combined.includes(k)))
    return { property_type: "condo", is_apartment: false, is_sfr: false, is_condo: true };
  if (COMMERCIAL_KEYWORDS.some(k => combined.includes(k)))
    return { property_type: "commercial", is_apartment: false, is_sfr: false, is_condo: false };
  if (LAND_KEYWORDS.some(k => combined.includes(k)))
    return { property_type: "land", is_apartment: false, is_sfr: false, is_condo: false };
  return { property_type: "single_family", is_apartment: false, is_sfr: true, is_condo: false };
}

// ─── Convert search result to DB row ────────────────────────────────

function resultToRow(r: SearchResult) {
  const addr = normalizeAddress(r.address);
  const city = inferCity(r.subdivision || "COMANCHE", r.subdivision);
  const cls = classifyType(r.address, r.legal);

  return {
    county_id: COUNTY_ID,
    parcel_id: r.parcel || undefined,
    address: addr,
    city,
    state_code: "OK",
    zip: "",
    property_type: cls.property_type,
    is_apartment: cls.is_apartment,
    is_sfr: cls.is_sfr,
    is_condo: cls.is_condo,
    owner_name: (r.ownerName || r.businessName || "").trim() || undefined,
    assessor_url: "https://www.actdatascout.com",
    source: "assessor",
    updated_at: new Date().toISOString(),
  };
}

// ─── DB Batch Upsert ────────────────────────────────────────────────

let totalUpserted = 0;
let totalDbErrors = 0;

async function flushToDB(records: SearchResult[]): Promise<number> {
  if (records.length === 0) return 0;

  const rows = records
    .map(resultToRow)
    .filter(r => r.address && r.city);

  if (rows.length === 0) return 0;

  let upserted = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const { data, error } = await db
      .from("properties")
      .upsert(batch, { onConflict: "county_id,parcel_id" })
      .select("id");

    if (error) {
      console.error(`  DB batch error: ${error.message}`);
      totalDbErrors++;
    } else {
      upserted += data?.length ?? 0;
    }
  }

  totalUpserted += upserted;
  return upserted;
}

// ─── Work Queue ─────────────────────────────────────────────────────

function buildTaskQueue(): SearchTask[] {
  const tasks: SearchTask[] = [];
  for (const township of TOWNSHIPS) {
    for (const range of RANGES) {
      for (const section of SECTIONS) {
        const tDir = township >= 0 ? "N" : "S";
        const label = `S${section}-T${Math.abs(township)}${tDir}-R${range}W`;
        tasks.push({ section, township, range, label });
      }
    }
  }
  return tasks;
}

// ─── Worker Loop ────────────────────────────────────────────────────

async function workerLoop(
  worker: WorkerContext,
  tasks: SearchTask[],
  taskIndex: { value: number },
  seenParcels: Set<string>,
  pendingRecords: SearchResult[],
  stats: { completed: number; totalResults: number; errors: number },
) {
  // Load the search page initially (retry up to 3 times with browser restart)
  let loaded = false;
  for (let attempt = 1; attempt <= 3; attempt++) {
    loaded = await loadSearchPage(worker);
    if (loaded) break;
    console.log(`  [W${worker.id}] Retrying initial page load (attempt ${attempt}/3)...`);
    await new Promise(r => setTimeout(r, 5000 * attempt));
    // Restart browser for fresh IP
    await closeWorker(worker);
    const fresh = await createWorker(worker.id);
    worker.browser = fresh.browser;
    worker.context = fresh.context;
    worker.page = fresh.page;
    worker.lastRequest = fresh.lastRequest;
    worker.pageReady = false;
  }
  if (!loaded) {
    console.error(`  [W${worker.id}] Could not load search page after 3 attempts, worker exiting`);
    return;
  }
  console.log(`  [W${worker.id}] Search page loaded, starting work`);

  while (true) {
    // Grab next task atomically
    const idx = taskIndex.value++;
    if (idx >= tasks.length) break;

    const task = tasks[idx];
    const results = await searchWithRetry(worker, task);

    let newCount = 0;
    for (const r of results) {
      const pid = r.parcel || r.crpid;
      if (!pid || seenParcels.has(pid)) continue;
      seenParcels.add(pid);
      pendingRecords.push(r);
      newCount++;
    }

    stats.completed++;
    stats.totalResults += newCount;

    // Log progress for non-empty results or periodically
    if (results.length > 0 || stats.completed % 50 === 0) {
      const pct = ((stats.completed / tasks.length) * 100).toFixed(1);
      console.log(
        `  [W${worker.id}] ${task.label}: ${results.length} results, ${newCount} new | ` +
        `${stats.completed}/${tasks.length} (${pct}%) | ${seenParcels.size} unique total`
      );
    }

    // Flush to DB periodically (every ~500 new records)
    if (pendingRecords.length >= 500) {
      const toFlush = pendingRecords.splice(0, pendingRecords.length);
      const n = await flushToDB(toFlush);
      console.log(`  [DB] Flushed ${n} records (${totalUpserted} total upserted)`);
    }
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  const tasks = buildTaskQueue();
  const totalTasks = tasks.length;

  console.log(`\nMXRE Fast Ingest v2 — Section-Level Concurrent Search`);
  console.log(`${"=".repeat(60)}`);
  console.log(`Target: Comanche County, OK (county_id=${COUNTY_ID})`);
  console.log(`DB: ${SUPABASE_URL}`);
  console.log(`Proxy: ${PROXY_URL ? "configured" : "none"}`);
  console.log(`Search URL: ${COUNTY_PAGE_URL}`);
  console.log(`Grid: ${TOWNSHIPS.length} townships x ${RANGES.length} ranges x ${SECTIONS.length} sections = ${totalTasks} searches`);
  console.log(`Concurrency: ${CONCURRENCY} browser contexts`);
  console.log(`Rate limit: ${RATE_LIMIT_MS}ms per context`);
  console.log(`Estimated time: ${((totalTasks / CONCURRENCY * (RATE_LIMIT_MS + 3000)) / 60000).toFixed(0)} minutes`);
  console.log();

  // Verify DB connectivity
  const { data: countyCheck, error: countyErr } = await db
    .from("counties")
    .select("id, county_name")
    .eq("id", COUNTY_ID)
    .single();

  if (countyErr || !countyCheck) {
    console.error(`Cannot find county_id=${COUNTY_ID} in database. Error: ${countyErr?.message}`);
    process.exit(1);
  }
  console.log(`County verified: ${countyCheck.county_name} (id=${countyCheck.id})`);

  // Launch workers
  console.log(`\nLaunching ${CONCURRENCY} browser workers...`);
  const workers: WorkerContext[] = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    try {
      const w = await createWorker(i);
      workers.push(w);
      console.log(`  Worker ${i} ready`);
    } catch (err) {
      console.error(`  Worker ${i} failed to launch: ${err instanceof Error ? err.message : err}`);
    }
  }

  if (workers.length === 0) {
    console.error("No workers launched. Exiting.");
    process.exit(1);
  }
  console.log(`${workers.length} workers active\n`);

  const startTime = Date.now();
  const seenParcels = new Set<string>();
  const pendingRecords: SearchResult[] = [];
  const taskIndex = { value: 0 };
  const stats = { completed: 0, totalResults: 0, errors: 0 };

  // Progress reporting every 60 seconds
  const progressInterval = setInterval(() => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = stats.completed > 0 ? (stats.completed / ((Date.now() - startTime) / 1000)).toFixed(2) : "0";
    const remaining = stats.completed > 0
      ? (((totalTasks - stats.completed) / (stats.completed / ((Date.now() - startTime) / 1000))) / 60).toFixed(1)
      : "?";
    console.log(
      `\n  --- PROGRESS: ${stats.completed}/${totalTasks} searches (${((stats.completed / totalTasks) * 100).toFixed(1)}%) | ` +
      `${seenParcels.size} unique parcels | ${totalUpserted} in DB | ` +
      `${rate} searches/sec | ~${remaining} min remaining | ${elapsed}s elapsed ---\n`
    );
  }, 60_000);

  // Run workers concurrently
  try {
    await Promise.all(
      workers.map(w => workerLoop(w, tasks, taskIndex, seenParcels, pendingRecords, stats))
    );

    // Final flush
    if (pendingRecords.length > 0) {
      const n = await flushToDB(pendingRecords.splice(0, pendingRecords.length));
      console.log(`  [DB] Final flush: ${n} records`);
    }
  } finally {
    clearInterval(progressInterval);

    // Close all workers
    for (const w of workers) {
      await closeWorker(w);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${"=".repeat(60)}`);
  console.log(`COMPLETE`);
  console.log(`  Searches: ${stats.completed}/${totalTasks}`);
  console.log(`  Unique parcels found: ${seenParcels.size}`);
  console.log(`  Upserted to DB: ${totalUpserted}`);
  console.log(`  DB errors: ${totalDbErrors}`);
  console.log(`  Elapsed: ${elapsed}s (${(parseFloat(elapsed) / 60).toFixed(1)} min)`);
  console.log(`${"=".repeat(60)}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
