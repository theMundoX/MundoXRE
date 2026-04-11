#!/usr/bin/env tsx
/**
 * Florida NAL Bulk Import Script
 *
 * Imports Florida property data from NAL CSV files into the MXRE database.
 * Creates county records, batch upserts properties, then generates
 * rent and mortgage estimates.
 *
 * Usage:
 *   npx tsx scripts/import-florida.ts --county=01          # Alachua by DOR CO_NO
 *   npx tsx scripts/import-florida.ts --county=Alachua     # By name
 *   npx tsx scripts/import-florida.ts --all                # All 67 counties
 *   npx tsx scripts/import-florida.ts --county=01 --dry-run
 *   npx tsx scripts/import-florida.ts --county=01 --skip-estimates
 *   npx tsx scripts/import-florida.ts --county=01 --dir=/opt/mxre/data/florida
 *   npx tsx scripts/import-florida.ts --county=01 --batch=1000
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { readdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { FloridaNALAdapter, FL_COUNTY_MAP } from "../src/discovery/adapters/florida-nal.js";
import { normalizeProperty } from "../src/discovery/normalizer.js";
import type { CountyConfig } from "../src/discovery/adapters/base.js";
import type { Property } from "../src/db/queries.js";
import { estimateRent } from "../src/utils/rent-estimator.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

// ─── CLI Args ────────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}
function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const DEFAULT_DATA_DIR = process.platform === "win32"
  ? join(process.env.USERPROFILE || "C:\\Users\\Public", "mxre-data", "florida")
  : "/opt/mxre/data/florida";

const dataDir = getArg("dir") || DEFAULT_DATA_DIR;
const countyArg = getArg("county");
const allCounties = hasFlag("all");
const dryRun = hasFlag("dry-run");
const skipEstimates = hasFlag("skip-estimates");
const batchSize = parseInt(getArg("batch") || "500", 10);
const maxRecords = getArg("max") ? parseInt(getArg("max")!, 10) : undefined;

if (!countyArg && !allCounties) {
  console.log(`Usage: npx tsx scripts/import-florida.ts --county=<CO_NO|name> [options]
       npx tsx scripts/import-florida.ts --all [options]

Options:
  --county=<n|name>   DOR county number (01-67) or county name
  --all               Import all 67 counties
  --dir=<path>        Data directory (default: ${DEFAULT_DATA_DIR})
  --batch=<n>         Batch size for upserts (default: 500)
  --max=<n>           Maximum records to import per county
  --skip-estimates    Skip rent/mortgage estimation after import
  --dry-run           Parse CSV but don't write to database
`);
  process.exit(1);
}

// ─── Database Setup ──────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.");
  console.error("Set them in .env or export them before running.");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// ─── County Resolution ───────────────────────────────────────────────

function resolveCounties(): string[] {
  if (allCounties) {
    return Object.keys(FL_COUNTY_MAP).sort((a, b) => parseInt(a) - parseInt(b));
  }

  if (!countyArg) return [];

  // Check if it's a number
  const num = parseInt(countyArg);
  if (!isNaN(num) && num >= 1 && num <= 67) {
    const coNo = String(num).padStart(2, "0");
    if (FL_COUNTY_MAP[coNo]) return [coNo];
  }

  // Search by name (case-insensitive)
  const needle = countyArg.toLowerCase();
  for (const [coNo, info] of Object.entries(FL_COUNTY_MAP)) {
    if (info.name.toLowerCase() === needle) return [coNo];
  }

  console.error(`Unknown county: "${countyArg}"`);
  console.error("Valid county numbers: 01-67. Or use a county name like 'Alachua'.");
  process.exit(1);
}

// ─── Find CSV for a County ───────────────────────────────────────────

function findCsvFile(coNo: string): string | null {
  if (!existsSync(dataDir)) return null;

  const coNum = parseInt(coNo);
  const files = readdirSync(dataDir);

  // Try exact match first: NAL{coNum}F*.csv
  let match = files.find(
    (f) => f.toUpperCase().startsWith(`NAL${coNum}F`) && (f.endsWith(".csv") || f.endsWith(".CSV")),
  );
  if (match) return join(dataDir, match);

  // Try with zero-padded: NAL{coNo}F*.csv (e.g., NAL01F*.csv)
  match = files.find(
    (f) => f.toUpperCase().startsWith(`NAL${coNo}F`) && (f.endsWith(".csv") || f.endsWith(".CSV")),
  );
  if (match) return join(dataDir, match);

  // Try matching county name in surrounding directory (for manually downloaded files)
  const info = FL_COUNTY_MAP[coNo];
  if (info) {
    // Look for any NAL CSV that contains the county name in the same dir
    match = files.find((f) => {
      const upper = f.toUpperCase();
      return upper.startsWith("NAL") && (upper.endsWith(".CSV"));
    });
    // If there's only one NAL CSV, use it regardless
    const nalFiles = files.filter((f) => f.toUpperCase().startsWith("NAL") && (f.endsWith(".csv") || f.endsWith(".CSV")));
    if (nalFiles.length === 1) return join(dataDir, nalFiles[0]);
  }

  return null;
}

// ─── Ensure County Record ────────────────────────────────────────────

/**
 * Create or find the county record. If fipsOverride is provided (from CENSUS_BK),
 * use that instead of the FL_COUNTY_MAP lookup.
 */
async function ensureCounty(coNo: string, fipsOverride?: string): Promise<number> {
  const info = FL_COUNTY_MAP[coNo];
  if (!info) throw new Error(`Unknown CO_NO: ${coNo}`);

  const stateFips = "12"; // Florida
  const countyFips = fipsOverride || info.fips;

  // Check if county already exists
  const { data: existing } = await db
    .from("counties")
    .select("id")
    .eq("state_fips", stateFips)
    .eq("county_fips", countyFips)
    .single();

  if (existing) return existing.id;

  // Create county record
  const { data: created, error } = await db
    .from("counties")
    .upsert(
      {
        state_fips: stateFips,
        county_fips: countyFips,
        state_code: "FL",
        county_name: info.name,
        active: true,
      },
      { onConflict: "state_fips,county_fips" },
    )
    .select("id")
    .single();

  if (error) throw new Error(`Failed to create county ${info.name}: ${error.message}`);
  return created!.id;
}

// ─── Batch Upsert ────────────────────────────────────────────────────

async function batchUpsertProperties(properties: Property[]): Promise<number> {
  const rows = properties.map((p) => ({
    ...p,
    updated_at: new Date().toISOString(),
  }));

  const { data, error } = await db
    .from("properties")
    .upsert(rows, { onConflict: "county_id,parcel_id" })
    .select("id");

  if (error) {
    console.error(`    Upsert error: ${error.message}`);
    return 0;
  }

  return data?.length ?? 0;
}

// ─── Generate Estimates ──────────────────────────────────────────────

async function generateEstimates(countyId: number, countyName: string) {
  console.log(`\n  Generating estimates for ${countyName}...`);
  const today = new Date().toISOString().split("T")[0];

  let offset = 0;
  let totalRent = 0;
  let totalMortgage = 0;

  while (true) {
    // Get properties for this county
    const { data: properties, error } = await db
      .from("properties")
      .select("id, city, state_code, zip, total_sqft, year_built, assessed_value, total_units, property_type, last_sale_price, last_sale_date, owner_name")
      .eq("county_id", countyId)
      .gt("id", offset)
      .order("id")
      .limit(batchSize);

    if (error) {
      console.error(`    Query error: ${error.message}`);
      break;
    }
    if (!properties || properties.length === 0) break;

    // ── Rent Estimates ──
    const propIds = properties.map((p) => p.id);

    // Check which already have rent snapshots
    const { data: existingSnaps } = await db
      .from("rent_snapshots")
      .select("property_id")
      .in("property_id", propIds);
    const hasSnap = new Set((existingSnaps ?? []).map((s: any) => s.property_id));

    const needSnap = properties.filter((p) => !hasSnap.has(p.id));
    if (needSnap.length > 0) {
      const snapshots = needSnap.map((p) => {
        const isMultiUnit = p.total_units && p.total_units > 4;
        const sqftForEst = isMultiUnit ? undefined : (p.total_sqft || undefined);
        const valueForEst = isMultiUnit ? undefined : (p.assessed_value || undefined);

        const est = estimateRent({
          city: p.city || "",
          state: p.state_code,
          zip: p.zip,
          sqft: sqftForEst,
          yearBuilt: p.year_built || undefined,
          assessedValue: valueForEst,
          totalUnits: p.total_units || undefined,
          propertyType: p.property_type || undefined,
        });

        return {
          property_id: p.id,
          observed_at: today,
          beds: est.beds,
          asking_rent: est.estimated_rent,
          asking_psf: est.estimated_rent_psf ? Math.round(est.estimated_rent_psf * 100) : null,
          raw: {
            fmr: est.fmr_rent,
            method: est.estimation_source,
            source: "estimated",
            confidence: est.confidence_level,
            confidence_score: est.confidence_score,
          },
        };
      }).filter((s) => s.asking_rent > 0);

      if (snapshots.length > 0) {
        const { error: insertErr } = await db.from("rent_snapshots").insert(snapshots);
        if (insertErr) {
          console.error(`    Rent insert error: ${insertErr.message}`);
        } else {
          totalRent += snapshots.length;
        }
      }
    }

    // ── Mortgage Estimates ──
    const { data: existingMort } = await db
      .from("mortgage_records")
      .select("property_id")
      .in("property_id", propIds);
    const hasMort = new Set((existingMort ?? []).map((m: any) => m.property_id));

    const needMort = properties.filter(
      (p) => !hasMort.has(p.id) && p.last_sale_price && p.last_sale_price > 0 && p.last_sale_date,
    );

    if (needMort.length > 0) {
      const mortgages = needMort.map((p) => {
        const loanAmount = Math.round(p.last_sale_price * 0.80);
        if (loanAmount < 10000) return null;

        const mortFields = computeMortgageFields({
          originalAmount: loanAmount,
          recordingDate: p.last_sale_date,
        });

        return {
          property_id: p.id,
          document_type: "estimated_mortgage",
          recording_date: p.last_sale_date,
          loan_amount: loanAmount,
          original_amount: loanAmount,
          lender_name: "ESTIMATED",
          borrower_name: p.owner_name || "UNKNOWN",
          source_url: "estimated",
          interest_rate: mortFields.interest_rate,
          term_months: mortFields.term_months,
          estimated_monthly_payment: mortFields.estimated_monthly_payment,
          estimated_current_balance: mortFields.estimated_current_balance,
          balance_as_of: mortFields.balance_as_of,
          maturity_date: mortFields.maturity_date,
        };
      }).filter((m): m is NonNullable<typeof m> => m !== null);

      if (mortgages.length > 0) {
        const { error: insertErr } = await db.from("mortgage_records").insert(mortgages);
        if (insertErr) {
          // Try with basic columns only
          const basic = mortgages.map((m) => ({
            property_id: m.property_id,
            document_type: m.document_type,
            recording_date: m.recording_date,
            loan_amount: m.original_amount,
            lender_name: "ESTIMATED",
            borrower_name: m.borrower_name,
            source_url: m.source_url,
          }));
          const { error: basicErr } = await db.from("mortgage_records").insert(basic);
          if (basicErr) {
            console.error(`    Mortgage insert error: ${basicErr.message}`);
          } else {
            totalMortgage += basic.length;
          }
        } else {
          totalMortgage += mortgages.length;
        }
      }
    }

    offset = properties[properties.length - 1].id;

    if ((totalRent + totalMortgage) % 5000 < batchSize) {
      process.stdout.write(`    Estimates: ${totalRent} rent, ${totalMortgage} mortgage\r`);
    }
  }

  console.log(`    Estimates: ${totalRent} rent, ${totalMortgage} mortgage`);
}

// ─── Import Single County ────────────────────────────────────────────

async function importCounty(coNo: string): Promise<{ properties: number; errors: number }> {
  const info = FL_COUNTY_MAP[coNo];
  if (!info) throw new Error(`Unknown CO_NO: ${coNo}`);

  console.log(`\n${"═".repeat(60)}`);
  console.log(`Importing: ${info.name} County (CO_NO=${coNo}, FIPS=12${info.fips})`);
  console.log(`${"═".repeat(60)}`);

  // Find CSV file
  const csvFile = findCsvFile(coNo);
  if (!csvFile) {
    console.error(`  No CSV file found for ${info.name} in ${dataDir}`);
    console.error(`  Expected: NAL${parseInt(coNo)}F*.csv`);
    console.error(`  Run download-florida-nal.ts first.`);
    return { properties: 0, errors: 1 };
  }
  console.log(`  CSV: ${csvFile}`);

  // Pre-scan first few records to extract real FIPS from CENSUS_BK
  let realFips: string | undefined;
  {
    const prescanAdapter = new FloridaNALAdapter();
    const prescanDir = csvFile.substring(0, Math.max(csvFile.lastIndexOf("/"), csvFile.lastIndexOf("\\")));
    const prescanConfig: CountyConfig = {
      state_fips: "12", county_fips: info.fips, name: info.name, state: "FL",
      platform: "florida_nal", base_url: "",
      search_params: { data_dir: prescanDir || dataDir },
    };
    let scanned = 0;
    for await (const rec of prescanAdapter.fetchProperties(prescanConfig)) {
      if (rec.raw.countyFips && typeof rec.raw.countyFips === "string" && rec.raw.countyFips.length === 3) {
        realFips = rec.raw.countyFips as string;
        break;
      }
      scanned++;
      if (scanned >= 50) break;
    }
    if (realFips && realFips !== info.fips) {
      console.log(`  FIPS from CENSUS_BK: ${realFips} (map says ${info.fips}; using CENSUS_BK value)`);
    }
  }

  // Ensure county record exists in DB
  let countyId: number;
  if (dryRun) {
    countyId = -1;
    console.log(`  [DRY RUN] Would create county: ${info.name} (12/${realFips || info.fips})`);
  } else {
    countyId = await ensureCounty(coNo, realFips);
    console.log(`  County ID: ${countyId}`);
  }

  // Build adapter config — point data_dir at the directory containing the CSV
  const csvDir = csvFile.substring(0, csvFile.lastIndexOf("/") >= 0 ? csvFile.lastIndexOf("/") : csvFile.lastIndexOf("\\"));
  const config: CountyConfig = {
    state_fips: "12",
    county_fips: info.fips,
    name: info.name,
    state: "FL",
    platform: "florida_nal",
    base_url: "",
    search_params: { data_dir: csvDir || dataDir },
  };

  // Run the adapter
  const adapter = new FloridaNALAdapter();
  let batch: Property[] = [];
  let totalUpserted = 0;
  let totalErrors = 0;
  let totalSkipped = 0;
  let totalRecords = 0;

  const startTime = Date.now();

  for await (const raw of adapter.fetchProperties(config)) {
    totalRecords++;

    // Normalize
    const property = normalizeProperty(raw, countyId);

    // Skip properties without parcel_id (can't upsert without it)
    if (!property.parcel_id) {
      totalSkipped++;
      continue;
    }

    batch.push(property);

    if (batch.length >= batchSize) {
      if (dryRun) {
        console.log(`  [DRY RUN] Would upsert ${batch.length} properties`);
        totalUpserted += batch.length;
      } else {
        const upserted = await batchUpsertProperties(batch);
        totalUpserted += upserted;
        if (upserted < batch.length) {
          totalErrors += batch.length - upserted;
        }
      }
      batch = [];

      // Progress logging
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = Math.round(totalRecords / ((Date.now() - startTime) / 1000));
      process.stdout.write(
        `  Progress: ${totalUpserted.toLocaleString()} upserted, ${totalRecords.toLocaleString()} processed (${rate}/s, ${elapsed}s)\r`,
      );
    }

    // Check max records limit
    if (maxRecords && totalRecords >= maxRecords) {
      console.log(`\n  Reached max records limit (${maxRecords})`);
      break;
    }
  }

  // Flush remaining batch
  if (batch.length > 0) {
    if (dryRun) {
      totalUpserted += batch.length;
    } else {
      const upserted = await batchUpsertProperties(batch);
      totalUpserted += upserted;
      if (upserted < batch.length) {
        totalErrors += batch.length - upserted;
      }
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n  ${info.name} County: ${totalUpserted.toLocaleString()} upserted, ${totalSkipped} skipped, ${totalErrors} errors (${elapsed}s)`);

  // Generate estimates
  if (!dryRun && !skipEstimates && totalUpserted > 0) {
    await generateEstimates(countyId, info.name);
  }

  return { properties: totalUpserted, errors: totalErrors };
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE Florida NAL Bulk Import");
  console.log("─".repeat(50));
  console.log(`Database: ${SUPABASE_URL}`);
  console.log(`Data dir: ${dataDir}`);
  console.log(`Batch size: ${batchSize}`);
  if (dryRun) console.log("Mode: DRY RUN (no database writes)");
  if (skipEstimates) console.log("Skipping rent/mortgage estimates");
  if (maxRecords) console.log(`Max records per county: ${maxRecords}`);

  const countyNos = resolveCounties();
  console.log(`Counties to import: ${countyNos.length}`);

  const startTime = Date.now();
  let totalProperties = 0;
  let totalErrors = 0;
  let successCount = 0;

  for (const coNo of countyNos) {
    try {
      const result = await importCounty(coNo);
      totalProperties += result.properties;
      totalErrors += result.errors;
      if (result.properties > 0) successCount++;
    } catch (err: any) {
      console.error(`\n  FATAL ERROR for county ${coNo}: ${err.message}`);
      totalErrors++;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${"═".repeat(60)}`);
  console.log("── Final Summary ──");
  console.log(`Counties processed: ${successCount}/${countyNos.length}`);
  console.log(`Total properties: ${totalProperties.toLocaleString()}`);
  console.log(`Total errors: ${totalErrors}`);
  console.log(`Elapsed: ${elapsed} minutes`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
