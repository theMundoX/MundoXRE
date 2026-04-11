#!/usr/bin/env tsx
/**
 * Download Florida NAL (Name-Address-Legal) bulk CSV files from FL DOR.
 *
 * Downloads ZIP files for all 67 Florida counties (or a specific county),
 * extracts the NAL CSV, and stores them in a data directory.
 *
 * URL pattern (SharePoint):
 *   https://floridarevenue.com/property/dataportal/Documents/
 *   PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/
 *   {CountyName}%20{CO_NO}%20Final%20NAL%20{YEAR}.zip
 *
 * Usage:
 *   npx tsx scripts/download-florida-nal.ts
 *   npx tsx scripts/download-florida-nal.ts --county=01
 *   npx tsx scripts/download-florida-nal.ts --county=Alachua
 *   npx tsx scripts/download-florida-nal.ts --year=2025
 *   npx tsx scripts/download-florida-nal.ts --dir=/opt/mxre/data/florida
 *   npx tsx scripts/download-florida-nal.ts --list
 */

import { mkdirSync, existsSync, writeFileSync, createWriteStream, readdirSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";
import { FL_COUNTY_MAP } from "../src/discovery/adapters/florida-nal.js";

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
const year = getArg("year") || "2025";
const countyArg = getArg("county");
const listOnly = hasFlag("list");
const dryRun = hasFlag("dry-run");

const BASE_URL = "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files";

// ─── County Config ───────────────────────────────────────────────────

interface CountyDownload {
  coNo: string;       // DOR county number (01-67)
  name: string;       // County name
  fips: string;       // 3-digit FIPS county code
  zipFileName: string; // Expected ZIP filename
  csvFileName: string; // Expected CSV filename inside ZIP
  url: string;         // Download URL
}

function buildCountyDownloads(filterCoNo?: string): CountyDownload[] {
  const downloads: CountyDownload[] = [];

  for (const [coNo, info] of Object.entries(FL_COUNTY_MAP)) {
    if (filterCoNo && coNo !== filterCoNo) continue;

    // URL-encode the county name (spaces → %20, periods → %2E)
    const urlName = encodeURIComponent(`${info.name} ${coNo} Final NAL ${year}`);
    const zipFileName = `${info.name} ${coNo} Final NAL ${year}.zip`;
    // CSV naming: NAL{CO_NO_padded}F{YEAR}01.csv — CO_NO in CSV is unpadded for single digits
    // Actually from the test file: NAL11F202501.csv (no padding). But to be safe, use the raw number.
    const csvFileName = `NAL${parseInt(coNo)}F${year}01.csv`;

    downloads.push({
      coNo,
      name: info.name,
      fips: info.fips,
      zipFileName,
      csvFileName,
      url: `${BASE_URL}/${urlName}.zip`,
    });
  }

  return downloads.sort((a, b) => parseInt(a.coNo) - parseInt(b.coNo));
}

function resolveCountyFilter(): string | undefined {
  if (!countyArg) return undefined;

  // Check if it's a number (CO_NO)
  const num = parseInt(countyArg);
  if (!isNaN(num) && num >= 1 && num <= 67) {
    return String(num).padStart(2, "0");
  }

  // Search by name
  const needle = countyArg.toLowerCase();
  for (const [coNo, info] of Object.entries(FL_COUNTY_MAP)) {
    if (info.name.toLowerCase() === needle) return coNo;
  }

  console.error(`Unknown county: "${countyArg}". Use --list to see all counties.`);
  process.exit(1);
}

// ─── Download Logic ──────────────────────────────────────────────────

async function downloadFile(url: string, destPath: string): Promise<boolean> {
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
      redirect: "follow",
    });

    if (!response.ok) {
      console.error(`    HTTP ${response.status}: ${response.statusText}`);
      return false;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    writeFileSync(destPath, buffer);
    return true;
  } catch (err: any) {
    console.error(`    Download error: ${err.message}`);
    return false;
  }
}

function extractZip(zipPath: string, destDir: string): boolean {
  try {
    // Use PowerShell on Windows, unzip on Unix
    if (process.platform === "win32") {
      execSync(
        `powershell -NoProfile -Command "Expand-Archive -Path '${zipPath}' -DestinationPath '${destDir}' -Force"`,
        { stdio: "pipe" },
      );
    } else {
      execSync(`unzip -o "${zipPath}" -d "${destDir}"`, { stdio: "pipe" });
    }
    return true;
  } catch (err: any) {
    console.error(`    Extract error: ${err.message}`);
    return false;
  }
}

function csvAlreadyExists(county: CountyDownload): boolean {
  // Check if the CSV file already exists in the data directory
  const csvPath = join(dataDir, county.csvFileName);
  if (existsSync(csvPath)) return true;

  // Also check for alternate naming patterns (e.g., padded CO_NO)
  const paddedCsv = `NAL${county.coNo}F${year}01.csv`;
  if (existsSync(join(dataDir, paddedCsv))) return true;

  // Check for any NAL CSV matching this county
  try {
    const files = readdirSync(dataDir);
    const coNum = parseInt(county.coNo);
    return files.some(
      (f) =>
        f.toUpperCase().startsWith(`NAL${coNum}F`) && f.endsWith(".csv"),
    );
  } catch {
    return false;
  }
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE Florida NAL Downloader");
  console.log("─".repeat(50));
  console.log(`Year: ${year}`);
  console.log(`Data directory: ${dataDir}`);
  console.log();

  const filterCoNo = resolveCountyFilter();
  const counties = buildCountyDownloads(filterCoNo);

  if (listOnly) {
    console.log("Florida Counties (DOR CO_NO → FIPS):");
    console.log("─".repeat(50));
    for (const c of counties) {
      const exists = existsSync(dataDir) && csvAlreadyExists(c);
      const status = exists ? " [downloaded]" : "";
      console.log(`  ${c.coNo}  ${c.name.padEnd(15)} FIPS=${c.fips}${status}`);
    }
    console.log(`\nTotal: ${counties.length} counties`);
    return;
  }

  // Ensure data directory exists
  mkdirSync(dataDir, { recursive: true });

  let downloaded = 0;
  let skipped = 0;
  let failed = 0;

  for (const county of counties) {
    const label = `[${county.coNo}/${counties.length > 1 ? "67" : "1"}] ${county.name}`;

    // Check if already downloaded
    if (csvAlreadyExists(county)) {
      console.log(`${label}: already downloaded, skipping`);
      skipped++;
      continue;
    }

    if (dryRun) {
      console.log(`${label}: would download from ${county.url}`);
      continue;
    }

    console.log(`${label}: downloading...`);
    console.log(`  URL: ${county.url}`);

    const zipPath = join(dataDir, county.zipFileName);

    // Download ZIP
    const ok = await downloadFile(county.url, zipPath);
    if (!ok) {
      console.log(`  FAILED to download`);
      failed++;
      continue;
    }

    // Extract
    console.log(`  Extracting...`);
    const extracted = extractZip(zipPath, dataDir);
    if (!extracted) {
      console.log(`  FAILED to extract`);
      failed++;
      continue;
    }

    // Verify CSV exists
    if (csvAlreadyExists(county)) {
      console.log(`  OK: ${county.csvFileName}`);
      downloaded++;
    } else {
      // List extracted files to help debug
      const extracted = readdirSync(dataDir).filter((f) => f.endsWith(".csv") || f.endsWith(".CSV"));
      console.log(`  WARNING: Expected CSV not found. Files in dir: ${extracted.join(", ")}`);
      downloaded++; // Still count as downloaded since we got the ZIP
    }

    // Clean up ZIP
    try {
      unlinkSync(zipPath);
    } catch {
      // Ignore cleanup errors
    }
  }

  console.log();
  console.log("── Summary ──");
  console.log(`Downloaded: ${downloaded}`);
  console.log(`Skipped (existing): ${skipped}`);
  console.log(`Failed: ${failed}`);
  console.log(`Total counties: ${counties.length}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
