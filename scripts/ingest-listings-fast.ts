#!/usr/bin/env tsx
/**
 * Rent Tracker — Fast parallel listing ingestion.
 *
 * Scrapes Redfin listings across multiple ZIPs concurrently (5 workers),
 * then runs SQL address-match to link listing_signals to properties.
 *
 * Usage:
 *   npx tsx scripts/ingest-listings-fast.ts --state TX --county Tarrant
 *   npx tsx scripts/ingest-listings-fast.ts --state TX --zips 76101,76102,76103
 *   npx tsx scripts/ingest-listings-fast.ts --state TX --county Tarrant --dry-run
 *   npx tsx scripts/ingest-listings-fast.ts --state TX --county Tarrant --concurrency 3
 *   npx tsx scripts/ingest-listings-fast.ts --state TX --zips 76101 --allow-partial
 */

import "dotenv/config";
import { RedfinListingAdapter } from "../src/rent-tracker/adapters/redfin.js";
import { normalizeListing, crossReferenceListings } from "../src/rent-tracker/normalizer.js";
import { upsertListingSignals, type ListingSignal } from "../src/db/queries.js";
import { getDb } from "../src/db/client.js";
import { initProxies } from "../src/utils/proxy.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

// ─── Config ─────────────────────────────────────────────────────────

hydrateWindowsUserEnv();

const CONCURRENCY = 5;
const INTRA_BATCH_DELAY_MS = 300;
const UPSERT_BATCH_SIZE = 50;

// ─── CLI Args ───────────────────────────────────────────────────────

interface FastIngestOptions {
  state: string;
  county?: string;
  zips?: string[];
  dryRun: boolean;
  concurrency: number;
  skipMatch: boolean;
  allowPartial: boolean;
}

function parseArgs(): FastIngestOptions {
  const args = process.argv.slice(2);
  const opts: FastIngestOptions = {
    state: "",
    dryRun: false,
    concurrency: CONCURRENCY,
    skipMatch: false,
    allowPartial: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = args[i + 1];

    switch (arg) {
      case "--state":
        opts.state = next?.toUpperCase() ?? "";
        i++;
        break;
      case "--county":
        opts.county = next;
        i++;
        break;
      case "--zips":
        opts.zips = next?.split(",").map((z) => z.trim());
        i++;
        break;
      case "--dry-run":
        opts.dryRun = true;
        break;
      case "--concurrency":
        opts.concurrency = parseInt(next ?? "5", 10) || CONCURRENCY;
        i++;
        break;
      case "--skip-match":
        opts.skipMatch = true;
        break;
      case "--allow-partial":
        opts.allowPartial = true;
        break;
    }
  }

  return opts;
}

// ─── Known County ZIPs (fallback when properties table lacks zip data) ──

const KNOWN_COUNTY_ZIPS: Record<string, string[]> = {
  "TX:Tarrant": [
    "76001","76002","76003","76004","76005","76006","76007","76008","76009","76010",
    "76011","76012","76013","76014","76015","76016","76017","76018","76019","76020",
    "76021","76022","76023","76028","76034","76036","76039","76040","76044","76048",
    "76049","76050","76051","76052","76053","76054","76058","76059","76060","76061",
    "76063","76066","76071","76078","76082","76092","76094","76095","76096","76097",
    "76098","76099","76101","76102","76103","76104","76105","76106","76107","76108",
    "76109","76110","76111","76112","76113","76114","76115","76116","76117","76118",
    "76119","76120","76121","76122","76123","76124","76126","76127","76129","76130",
    "76131","76132","76133","76134","76135","76136","76137","76140","76147","76148",
    "76150","76155","76161","76162","76163","76164","76166","76177","76179","76180",
    "76181","76182","76185","76190","76191","76192","76193","76195","76196","76197",
    "76198","76199","76244","76248","76262",
  ],
  "TX:Dallas": [
    "75001","75006","75007","75009","75010","75011","75019","75023","75024","75025",
    "75028","75032","75034","75035","75038","75039","75040","75041","75042","75043",
    "75044","75048","75050","75051","75052","75054","75060","75061","75062","75063",
    "75067","75068","75069","75070","75074","75075","75078","75080","75081","75082",
    "75083","75085","75086","75087","75088","75089","75093","75094","75098","75104",
    "75115","75116","75134","75137","75141","75146","75149","75150","75159","75166",
    "75172","75180","75181","75182","75201","75202","75203","75204","75205","75206",
    "75207","75208","75209","75210","75211","75212","75214","75215","75216","75217",
    "75218","75219","75220","75223","75224","75225","75226","75227","75228","75229",
    "75230","75231","75232","75233","75234","75235","75236","75237","75238","75240",
    "75241","75243","75244","75246","75247","75248","75249","75250","75251","75252",
    "75253","75254","75270","75275","75287",
  ],
  "TX:Harris": [
    "77001","77002","77003","77004","77005","77006","77007","77008","77009","77010",
    "77011","77012","77013","77014","77015","77016","77017","77018","77019","77020",
    "77021","77022","77023","77024","77025","77026","77027","77028","77029","77030",
    "77031","77032","77033","77034","77035","77036","77037","77038","77039","77040",
    "77041","77042","77043","77044","77045","77046","77047","77048","77049","77050",
    "77051","77053","77054","77055","77056","77057","77058","77059","77060","77061",
    "77062","77063","77064","77065","77066","77067","77068","77069","77070","77071",
    "77072","77073","77074","77075","77076","77077","77078","77079","77080","77081",
    "77082","77083","77084","77085","77086","77087","77088","77089","77090","77091",
    "77092","77093","77094","77095","77096","77098","77099","77336","77338","77339",
    "77345","77346","77357","77365","77373","77375","77377","77379","77386","77388",
    "77389","77396","77401","77407","77429","77433","77447","77449","77450","77477",
    "77478","77489","77493","77494","77502","77503","77504","77505","77506","77507",
    "77520","77530","77532","77536","77546","77547","77562","77571","77581","77584",
    "77586","77587","77598",
  ],
  "TX:Denton": [
    "75007","75009","75010","75019","75022","75023","75024","75025","75027","75028",
    "75034","75035","75056","75057","75065","75067","75068","75069","75070","75071",
    "75077","75078","76051","76052","76177","76201","76202","76203","76204","76205",
    "76206","76207","76208","76209","76210","76226","76227","76234","76247","76249",
    "76258","76259","76262","76266","76272",
  ],
};

// ─── ZIP Resolution ─────────────────────────────────────────────────

async function getZipsForCounty(state: string, countyName: string): Promise<string[]> {
  const db = getDb();

  // First find the county_id
  const { data: counties, error: cErr } = await db
    .from("counties")
    .select("id, county_name, state_code")
    .eq("state_code", state)
    .ilike("county_name", `%${countyName}%`);

  if (cErr || !counties?.length) {
    console.error(`Could not find county "${countyName}" in ${state}`);
    process.exit(1);
  }

  const county = counties[0];
  console.log(`Resolved county: ${county.county_name} (id=${county.id})`);

  // Try properties table first
  const { data: rows } = await db
    .from("properties")
    .select("zip")
    .eq("county_id", county.id)
    .not("zip", "is", null)
    .neq("zip", "");

  if (rows && rows.length > 0) {
    const zips = [
      ...new Set(
        rows
          .map((r: { zip: string }) => String(r.zip ?? "").match(/\d{5}/)?.[0])
          .filter((zip): zip is string => Boolean(zip)),
      ),
    ];
    zips.sort();
    console.log(`Found ${zips.length} ZIPs from properties table`);
    return zips;
  }

  // Fallback to hardcoded map
  const key = `${state}:${county.county_name}`;
  if (KNOWN_COUNTY_ZIPS[key]) {
    console.log(`Using hardcoded ZIP list for ${key} (${KNOWN_COUNTY_ZIPS[key].length} ZIPs)`);
    return KNOWN_COUNTY_ZIPS[key];
  }

  console.error(`No ZIPs found for ${county.county_name}. Use --zips to specify manually.`);
  process.exit(1);
}

// ─── Worker Stats ───────────────────────────────────────────────────

interface ZipResult {
  zip: string;
  listings: number;
  upserted: number;
  errors: number;
  durationMs: number;
}

interface GlobalStats {
  totalZips: number;
  completedZips: number;
  totalListings: number;
  totalUpserted: number;
  totalErrors: number;
  startedAt: number;
  activeWorkers: Map<number, string>; // worker index -> current ZIP
}

function printProgress(stats: GlobalStats) {
  const elapsed = ((Date.now() - stats.startedAt) / 1000).toFixed(1);
  const pct = stats.totalZips > 0 ? ((stats.completedZips / stats.totalZips) * 100).toFixed(1) : "0";
  const active = [...stats.activeWorkers.entries()]
    .map(([i, zip]) => `W${i}:${zip}`)
    .join("  ");

  process.stdout.write(
    `\r[${elapsed}s] ${stats.completedZips}/${stats.totalZips} ZIPs (${pct}%) | ` +
      `${stats.totalListings} found, ${stats.totalUpserted} upserted, ${stats.totalErrors} err | ` +
      `Active: ${active || "idle"}    `,
  );
}

// ─── Single ZIP Scrape ──────────────────────────────────────────────

async function scrapeZip(
  adapter: RedfinListingAdapter,
  state: string,
  zip: string,
  dryRun: boolean,
): Promise<ZipResult> {
  const start = Date.now();
  const result: ZipResult = { zip, listings: 0, upserted: 0, errors: 0, durationMs: 0 };

  try {
    const records = [];
    for await (const record of adapter.fetchListings({ state, zip })) {
      records.push(record);
    }

    result.listings = records.length;

    if (records.length === 0) {
      result.durationMs = Date.now() - start;
      return result;
    }

    // Normalize
    const signals: ListingSignal[] = [];
    for (const record of records) {
      const normalized = normalizeListing(record);
      if (normalized) signals.push(normalized);
    }

    // Upsert in batches
    if (!dryRun && signals.length > 0) {
      for (let i = 0; i < signals.length; i += UPSERT_BATCH_SIZE) {
        const batch = signals.slice(i, i + UPSERT_BATCH_SIZE);
        try {
          const upserted = await upsertListingSignals(batch);
          result.upserted += upserted.length;
        } catch (err) {
          console.error(`  Upsert failed for ${zip} batch ${Math.floor(i / UPSERT_BATCH_SIZE) + 1}: ${err instanceof Error ? err.message : String(err)}`);
          result.errors++;
        }
      }
    } else {
      result.upserted = signals.length;
    }
  } catch (err) {
    result.errors++;
  }

  result.durationMs = Date.now() - start;
  return result;
}

// ─── Batch Runner ───────────────────────────────────────────────────

async function runParallel(
  zips: string[],
  state: string,
  concurrency: number,
  dryRun: boolean,
): Promise<ZipResult[]> {
  const adapter = new RedfinListingAdapter();
  const allResults: ZipResult[] = [];

  const stats: GlobalStats = {
    totalZips: zips.length,
    completedZips: 0,
    totalListings: 0,
    totalUpserted: 0,
    totalErrors: 0,
    startedAt: Date.now(),
    activeWorkers: new Map(),
  };

  // Process in batches of `concurrency`
  for (let batchStart = 0; batchStart < zips.length; batchStart += concurrency) {
    const batchZips = zips.slice(batchStart, batchStart + concurrency);

    const promises = batchZips.map(async (zip, idx) => {
      const workerIdx = idx;
      stats.activeWorkers.set(workerIdx, zip);
      printProgress(stats);

      // Stagger start within batch: 300ms between each
      if (idx > 0) {
        await new Promise((r) => setTimeout(r, INTRA_BATCH_DELAY_MS * idx));
      }

      const result = await scrapeZip(adapter, state, zip, dryRun);

      stats.completedZips++;
      stats.totalListings += result.listings;
      stats.totalUpserted += result.upserted;
      stats.totalErrors += result.errors;
      stats.activeWorkers.delete(workerIdx);
      printProgress(stats);

      return result;
    });

    const batchResults = await Promise.allSettled(promises);

    for (const settled of batchResults) {
      if (settled.status === "fulfilled") {
        allResults.push(settled.value);
      } else {
        stats.totalErrors++;
        allResults.push({
          zip: "unknown",
          listings: 0,
          upserted: 0,
          errors: 1,
          durationMs: 0,
        });
      }
    }
  }

  // Final newline after progress
  console.log();

  return allResults;
}

// ─── Address Match ──────────────────────────────────────────────────

async function runAddressMatch() {
  console.log("\nRunning SQL address match to link listing_signals to properties...");

  const sql = `UPDATE listing_signals ls SET property_id = p.id FROM properties p JOIN counties c ON c.id = p.county_id WHERE ls.property_id IS NULL AND ls.state_code = c.state_code AND UPPER(TRIM(ls.city)) = UPPER(TRIM(p.city)) AND UPPER(TRIM(ls.address)) = UPPER(TRIM(p.address));`;
  const pgUrl = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
  const pgKey = process.env.SUPABASE_SERVICE_KEY ?? "";

  if (!pgUrl || !pgKey) {
    console.log("Address match skipped: SUPABASE_URL/SUPABASE_SERVICE_KEY is not set.");
    return;
  }

  try {
    const response = await fetch(pgUrl, {
      method: "POST",
      headers: { apikey: pgKey, Authorization: `Bearer ${pgKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: sql }),
      signal: AbortSignal.timeout(300_000),
    });
    if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
    console.log("Address match result:", await response.text());
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("Address match failed:", msg);
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  const opts = parseArgs();

  if (!opts.state) {
    console.error("Error: --state is required");
    console.log("\nUsage:");
    console.log("  npx tsx scripts/ingest-listings-fast.ts --state TX --county Tarrant");
    console.log("  npx tsx scripts/ingest-listings-fast.ts --state TX --zips 76101,76102,76103");
    console.log("\nOptions:");
    console.log("  --state <ST>        State code (required)");
    console.log("  --county <name>     County name (resolves ZIPs from properties table)");
    console.log("  --zips <list>       Comma-separated ZIP codes");
    console.log("  --concurrency <n>   Parallel workers (default: 5)");
    console.log("  --dry-run           Don't write to database");
    console.log("  --skip-match        Skip address match SQL after ingestion");
    process.exit(1);
  }

  if (!opts.county && !opts.zips?.length) {
    console.error("Error: --county or --zips is required");
    process.exit(1);
  }

  initProxies();

  // Resolve ZIPs
  let zips: string[];
  if (opts.zips?.length) {
    zips = opts.zips;
  } else {
    zips = await getZipsForCounty(opts.state, opts.county!);
  }

  console.log("Rent Tracker — Fast Parallel Ingestion");
  console.log("======================================");
  console.log(`State: ${opts.state}`);
  console.log(`Area: ${opts.county ?? "manual ZIPs"}`);
  console.log(`ZIPs: ${zips.length}`);
  console.log(`Concurrency: ${opts.concurrency}`);
  console.log(`Dry run: ${opts.dryRun}`);
  console.log(`Allow partial ZIP errors: ${opts.allowPartial}`);
  console.log(`ZIP codes: ${zips.join(", ")}`);
  console.log();

  // Run parallel scrape
  const results = await runParallel(zips, opts.state, opts.concurrency, opts.dryRun);

  // Summary table
  console.log("\n── Results by ZIP ──");
  const successZips = results.filter((r) => r.listings > 0);
  const emptyZips = results.filter((r) => r.listings === 0 && r.errors === 0);
  const errorZips = results.filter((r) => r.errors > 0);

  for (const r of successZips) {
    console.log(`  ${r.zip}: ${r.listings} listings, ${r.upserted} upserted (${(r.durationMs / 1000).toFixed(1)}s)`);
  }
  if (emptyZips.length > 0) {
    console.log(`  Empty (${emptyZips.length}): ${emptyZips.map((r) => r.zip).join(", ")}`);
  }
  if (errorZips.length > 0) {
    console.log(`  Errors (${errorZips.length}): ${errorZips.map((r) => r.zip).join(", ")}`);
  }

  // Totals
  const totalListings = results.reduce((s, r) => s + r.listings, 0);
  const totalUpserted = results.reduce((s, r) => s + r.upserted, 0);
  const totalErrors = results.reduce((s, r) => s + r.errors, 0);
  const totalDuration = results.reduce((s, r) => s + r.durationMs, 0);
  const wallTime = (Date.now() - (results[0]?.durationMs ? Date.now() - totalDuration : Date.now())) / 1000;

  console.log("\n── Summary ──");
  console.log(`ZIPs: ${zips.length} total, ${successZips.length} with listings, ${emptyZips.length} empty, ${errorZips.length} errors`);
  console.log(`Listings: ${totalListings} found, ${totalUpserted} upserted`);
  console.log(`Errors: ${totalErrors}`);

  // Address match
  if (!opts.dryRun && !opts.skipMatch && totalUpserted > 0) {
    await runAddressMatch();
  }

  console.log("\nDone.");
  process.exit(totalErrors > 0 && !opts.allowPartial ? 1 : 0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
