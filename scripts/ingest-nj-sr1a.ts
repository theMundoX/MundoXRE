#!/usr/bin/env tsx
/**
 * MXRE — NJ SR1A Deed Recording Ingest
 *
 * Downloads and parses New Jersey SR1A fixed-width deed recording files
 * from the NJ Treasury bulk download site and loads them into mortgage_records.
 *
 * Sources:
 *   https://www.nj.gov/treasury/taxation/lpt/YTDSR1A2026.zip (2026 YTD)
 *   https://www.nj.gov/treasury/taxation/lpt/Sales2025.zip   (2025 full)
 *   https://www.nj.gov/treasury/taxation/lpt/Sales2024.zip   (2024 full)
 *
 * Usage:
 *   npx tsx scripts/ingest-nj-sr1a.ts
 *   npx tsx scripts/ingest-nj-sr1a.ts --year=2025
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

// ─── Config ──────────────────────────────────────────────────────

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 1000;

const SR1A_SOURCES: { year: string; url: string; sourceTag: string }[] = [
  { year: "2024", url: "https://www.nj.gov/treasury/taxation/pdf/lpt/statdata/Sales2024.zip", sourceTag: "nj-sr1a-2024" },
  { year: "2025", url: "https://www.nj.gov/treasury/taxation/pdf/lpt/statdata/Sales2025.zip", sourceTag: "nj-sr1a-2025" },
  { year: "2026", url: "https://www.nj.gov/treasury/taxation/pdf/lpt/statdata/YTDSR1A2026.zip", sourceTag: "nj-sr1a-2026" },
];

const NJ_COUNTY_CODES: Record<string, string> = {
  "01": "Atlantic",
  "02": "Bergen",
  "03": "Burlington",
  "04": "Camden",
  "05": "Cape May",
  "06": "Cumberland",
  "07": "Essex",
  "08": "Gloucester",
  "09": "Hudson",
  "10": "Hunterdon",
  "11": "Mercer",
  "12": "Middlesex",
  "13": "Monmouth",
  "14": "Morris",
  "15": "Ocean",
  "16": "Passaic",
  "17": "Salem",
  "18": "Somerset",
  "19": "Sussex",
  "20": "Union",
  "21": "Warren",
};

const NJ_COUNTY_FIPS: Record<string, string> = {
  "Atlantic": "001", "Bergen": "003", "Burlington": "005", "Camden": "007",
  "Cape May": "009", "Cumberland": "011", "Essex": "013", "Gloucester": "015",
  "Hudson": "017", "Hunterdon": "019", "Mercer": "021", "Middlesex": "023",
  "Monmouth": "025", "Morris": "027", "Ocean": "029", "Passaic": "031",
  "Salem": "033", "Somerset": "035", "Sussex": "037", "Union": "039",
  "Warren": "041",
};

// ─── CLI Args ────────────────────────────────────────────────────

const args = process.argv.slice(2);
const yearFilter = args.find(a => a.startsWith("--year="))?.split("=")[1];

// ─── Fixed-width parser ──────────────────────────────────────────

interface SR1ARecord {
  countyCode: string;
  districtCode: string;
  reportedSalesPrice: number;
  verifiedSalesPrice: number;
  grantorName: string;
  granteeName: string;
  propertyLocation: string;
  deedBook: string;
  deedPage: string;
  deedDate: string | null;
  dateRecorded: string | null;
  block: string;
  lot: string;
  propertyClass: string;
  realtyTransferFee: number;
}

function parseSR1ADate(raw: string): string | null {
  const s = raw.trim();
  if (!s || s.length < 6 || s === "000000") return null;
  // SR1A dates are actually YYMMDD format (not MMDDYY as documented)
  const yy = s.substring(0, 2);
  const mm = s.substring(2, 4);
  const dd = s.substring(4, 6);
  const yyNum = parseInt(yy, 10);
  const year = yyNum < 50 ? 2000 + yyNum : 1900 + yyNum;
  const month = parseInt(mm, 10);
  const day = parseInt(dd, 10);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return `${year}-${mm.padStart(2, "0")}-${dd.toString().padStart(2, "0")}`;
}

function parseSR1ALine(line: string): SR1ARecord | null {
  // Each record is 662 bytes fixed-width. Positions are 1-indexed.
  if (line.length < 628) return null;

  const substr = (start: number, len: number) => {
    // Convert 1-indexed to 0-indexed
    const s = start - 1;
    return line.substring(s, s + len).trim();
  };

  const countyCode = substr(1, 2);
  if (!NJ_COUNTY_CODES[countyCode]) return null; // invalid county

  const reportedPrice = parseInt(substr(38, 9), 10) || 0;
  const verifiedPrice = parseInt(substr(47, 9), 10) || 0;
  const rtf = parseInt(substr(88, 9), 10) || 0;

  const grantorName = substr(110, 35);
  const granteeName = substr(204, 35);
  const propertyLocation = substr(298, 25);
  const deedBook = substr(329, 5);
  const deedPage = substr(334, 5);
  const deedDate = parseSR1ADate(substr(339, 6));
  const dateRecorded = parseSR1ADate(substr(345, 6));
  const block = substr(351, 5);
  const lot = substr(360, 5);
  const propertyClass = substr(627, 2);

  return {
    countyCode,
    districtCode: substr(3, 2),
    reportedSalesPrice: reportedPrice,
    verifiedSalesPrice: verifiedPrice,
    grantorName,
    granteeName,
    propertyLocation,
    deedBook,
    deedPage,
    deedDate,
    dateRecorded,
    block,
    lot,
    propertyClass,
    realtyTransferFee: rtf,
  };
}

// ─── Download helper ─────────────────────────────────────────────

function toUnixPath(p: string): string {
  return p.replace(/\\/g, "/");
}

function downloadFile(url: string, dest: string): void {
  // NJ.gov uses Imperva CDN that requires cookies — do a 2-pass download in one shell
  const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
  const cookieJar = toUnixPath(dest + ".cookies");
  const destUnix = toUnixPath(dest);
  const nullFile = toUnixPath(dest + ".null");
  // Combined: pass 1 gets cookies, pass 2 downloads with them
  execSync(
    `curl -sL -c "${cookieJar}" -A "${ua}" "${url}" -o "${nullFile}" && curl -sL -b "${cookieJar}" -A "${ua}" "${url}" -o "${destUnix}"`,
    { stdio: "pipe", timeout: 180_000, shell: "bash" },
  );
  try { fs.unlinkSync(dest + ".null"); } catch {}
  try { fs.unlinkSync(dest + ".cookies"); } catch {}
  if (!fs.existsSync(dest) || fs.statSync(dest).size < 1000) {
    throw new Error(`Download failed or file too small for ${url}`);
  }
}

// ─── County ID lookup/creation ───────────────────────────────────

const countyIdCache: Record<string, number> = {};

async function getOrCreateCountyId(countyName: string): Promise<number> {
  if (countyIdCache[countyName]) return countyIdCache[countyName];

  // Look up existing
  const { data } = await db
    .from("counties")
    .select("id")
    .eq("county_name", countyName)
    .eq("state_code", "NJ")
    .single();

  if (data) {
    countyIdCache[countyName] = data.id;
    return data.id;
  }

  // Create county
  console.log(`  Creating county: ${countyName}, NJ`);
  const fips = NJ_COUNTY_FIPS[countyName] || "000";
  const { data: newCounty, error } = await db
    .from("counties")
    .upsert({
      county_name: countyName,
      state_code: "NJ",
      state_fips: "34",
      county_fips: fips,
      active: true,
    }, { onConflict: "state_fips,county_fips" })
    .select("id")
    .single();

  if (error || !newCounty) {
    throw new Error(`Failed to create county ${countyName}: ${error?.message}`);
  }
  countyIdCache[countyName] = newCounty.id;
  return newCounty.id;
}

// ─── Dedup check ─────────────────────────────────────────────────

async function getExistingDocNumbers(sourceUrl: string): Promise<Set<string>> {
  const existing = new Set<string>();
  let from = 0;
  const pageSize = 5000;

  while (true) {
    const { data } = await db
      .from("mortgage_records")
      .select("document_number")
      .eq("source_url", sourceUrl)
      .not("document_number", "is", null)
      .range(from, from + pageSize - 1);

    if (!data || data.length === 0) break;
    for (const r of data) {
      if (r.document_number) existing.add(r.document_number);
    }
    if (data.length < pageSize) break;
    from += pageSize;
  }

  return existing;
}

// ─── Property linking ────────────────────────────────────────────

async function linkRecordsByAddress(sourceTag: string, addressMap: Map<string, { countyName: string; address: string }>) {
  console.log(`\n  Linking records to properties for ${sourceTag}...`);
  console.log(`  Address map has ${addressMap.size} entries to try`);

  let linked = 0;
  let checked = 0;

  // Get NJ county IDs from DB
  const { data: njCounties } = await db
    .from("counties")
    .select("id, county_name")
    .eq("state_code", "NJ");

  if (!njCounties || njCounties.length === 0) {
    console.log(`  No NJ counties in DB, skipping linking`);
    return;
  }

  const countyNameToId = new Map<string, number>();
  for (const c of njCounties) {
    countyNameToId.set(c.county_name, c.id);
  }

  // Group by county
  const byCounty = new Map<number, { docNum: string; address: string }[]>();
  for (const [docNum, { countyName, address }] of addressMap) {
    if (!address || address.length < 5) continue;
    const cid = countyNameToId.get(countyName);
    if (!cid) continue;
    const list = byCounty.get(cid) || [];
    list.push({ docNum, address });
    byCounty.set(cid, list);
  }

  for (const [countyId, records] of byCounty) {
    const countyName = njCounties.find(c => c.id === countyId)?.county_name || "?";
    process.stdout.write(`    ${countyName}: loading properties...`);

    // Build address lookup by paginating through all properties
    const propByAddr = new Map<string, number>();
    let from = 0;
    const pg = 10000;
    while (true) {
      const { data: properties } = await db
        .from("properties")
        .select("id, address")
        .eq("county_id", countyId)
        .not("address", "is", null)
        .range(from, from + pg - 1);

      if (!properties || properties.length === 0) break;
      for (const p of properties) {
        if (p.address) {
          const norm = p.address.toUpperCase().replace(/[^A-Z0-9 ]/g, "").replace(/\s+/g, " ").trim();
          propByAddr.set(norm, p.id);
        }
      }
      if (properties.length < pg) break;
      from += pg;
    }

    if (propByAddr.size === 0) {
      process.stdout.write(` 0 props, skipping\n`);
      continue;
    }

    process.stdout.write(` ${propByAddr.size.toLocaleString()} props, matching...\n`);

    let countyLinked = 0;
    for (const rec of records) {
      checked++;
      const norm = rec.address.toUpperCase().replace(/[^A-Z0-9 ]/g, "").replace(/\s+/g, " ").trim();
      const propId = propByAddr.get(norm);
      if (propId) {
        const { error } = await db
          .from("mortgage_records")
          .update({ property_id: propId })
          .eq("document_number", rec.docNum)
          .eq("source_url", sourceTag);
        if (!error) { linked++; countyLinked++; }
      }
    }
    if (countyLinked > 0) {
      console.log(`      -> linked ${countyLinked.toLocaleString()} records`);
    }
  }

  console.log(`  Checked ${checked}, linked ${linked} records to properties`);
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  console.log(`\nMXRE — NJ SR1A Deed Recording Ingest`);
  console.log(`${"─".repeat(55)}`);
  console.log(`DB: ${process.env.SUPABASE_URL}`);
  if (yearFilter) console.log(`Year filter: ${yearFilter}`);
  console.log();

  const sources = yearFilter
    ? SR1A_SOURCES.filter(s => s.year === yearFilter)
    : SR1A_SOURCES;

  if (sources.length === 0) {
    console.error(`No sources found for year ${yearFilter}`);
    process.exit(1);
  }

  // Use data/ dir under project root (Windows tmpdir paths break curl in Git Bash)
  const projectRoot = path.resolve(import.meta.dirname, "..");
  const tmpDir = path.join(projectRoot, "data", "sr1a-cache");
  fs.mkdirSync(tmpDir, { recursive: true });

  const totalCounts: Record<string, number> = {};
  let grandTotal = 0;
  let grandSkipped = 0;

  for (const source of sources) {
    console.log(`\n${"═".repeat(55)}`);
    console.log(`Processing: ${source.sourceTag} (${source.url})`);
    console.log(`${"═".repeat(55)}`);

    // 1. Download — check for pre-downloaded files first
    const zipFilename = source.url.split("/").pop()!;
    const preDownloaded = path.join(tmpDir, zipFilename);
    const zipPath = path.join(tmpDir, `${source.sourceTag}.zip`);

    if (fs.existsSync(preDownloaded) && fs.statSync(preDownloaded).size > 10000) {
      // Use pre-downloaded file (renamed or as-is)
      if (preDownloaded !== zipPath) {
        fs.copyFileSync(preDownloaded, zipPath);
      }
      console.log(`  Using pre-downloaded: ${zipFilename} (${(fs.statSync(zipPath).size / 1024 / 1024).toFixed(1)} MB)`);
    } else if (fs.existsSync(zipPath) && fs.statSync(zipPath).size > 10000) {
      console.log(`  Using cached: ${zipPath}`);
    } else {
      console.log(`  Downloading ${source.url}...`);
      downloadFile(source.url, zipPath);
      const size = fs.statSync(zipPath).size;
      console.log(`  Downloaded: ${(size / 1024 / 1024).toFixed(1)} MB`);
    }

    // 2. Unzip
    const extractDir = path.join(tmpDir, source.sourceTag);
    if (!fs.existsSync(extractDir)) {
      fs.mkdirSync(extractDir, { recursive: true });
      console.log(`  Extracting...`);
      execSync(`unzip -o "${toUnixPath(zipPath)}" -d "${toUnixPath(extractDir)}"`, { stdio: "pipe", shell: "bash" });
    }

    // Find the data file(s)
    const files = fs.readdirSync(extractDir).filter(f =>
      !f.startsWith(".") && !f.startsWith("__") && (f.endsWith(".txt") || f.endsWith(".TXT") || !f.includes("."))
    );

    // If no .txt files, try all files (SR1A files sometimes have no extension)
    const dataFiles = files.length > 0 ? files : fs.readdirSync(extractDir).filter(f => !f.startsWith(".") && !f.startsWith("__"));

    if (dataFiles.length === 0) {
      console.log(`  No data files found in ${extractDir}`);
      continue;
    }

    console.log(`  Data files: ${dataFiles.join(", ")}`);

    // 3. Load existing doc numbers for dedup
    console.log(`  Loading existing records for dedup...`);
    const existingDocs = await getExistingDocNumbers(source.sourceTag);
    console.log(`  Found ${existingDocs.size} existing records`);

    // 4. Parse and insert
    let inserted = 0;
    let skipped = 0;
    let parseErrors = 0;
    let batch: Record<string, unknown>[] = [];
    const countyCounts: Record<string, number> = {};
    const addressMap = new Map<string, { countyName: string; address: string }>();

    for (const file of dataFiles) {
      const filePath = path.join(extractDir, file);
      console.log(`  Parsing: ${file}`);

      const content = fs.readFileSync(filePath, "latin1"); // SR1A is ASCII/Latin-1
      const lines = content.split(/\r?\n/);
      console.log(`  Lines: ${lines.length}`);

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (line.length < 100) continue; // skip short/empty lines

        const rec = parseSR1ALine(line);
        if (!rec) {
          parseErrors++;
          continue;
        }

        const countyName = NJ_COUNTY_CODES[rec.countyCode]!;

        // Build document_number for dedup
        const docNum = `${rec.deedBook}-${rec.deedPage}`;

        // Skip if no deed book/page (can't dedup)
        if (!rec.deedBook && !rec.deedPage) {
          skipped++;
          continue;
        }

        // Dedup
        if (existingDocs.has(docNum)) {
          skipped++;
          continue;
        }
        existingDocs.add(docNum); // prevent intra-file dupes

        // Skip zero-price records (non-arm's-length or exempt)
        const salePrice = rec.verifiedSalesPrice || rec.reportedSalesPrice;

        // Ensure county exists in DB (needed for property linking)
        await getOrCreateCountyId(countyName);

        const record: Record<string, unknown> = {
          property_id: null,
          document_type: "deed",
          recording_date: rec.dateRecorded,
          loan_amount: salePrice || null,
          original_amount: salePrice || null,
          borrower_name: rec.granteeName || null,        // buyer (empty in public SR1A)
          lender_name: rec.grantorName || null,          // seller (empty in public SR1A)
          document_number: docNum,
          book_page: `${rec.block}-${rec.lot}`,          // store block-lot for property linking
          source_url: source.sourceTag,
          deed_type: rec.propertyClass || null,          // property class code
        };

        batch.push(record);
        countyCounts[countyName] = (countyCounts[countyName] || 0) + 1;

        // Track for address linking
        if (rec.propertyLocation) {
          addressMap.set(docNum, { countyName, address: rec.propertyLocation });
        }

        // Flush batch
        if (batch.length >= BATCH_SIZE) {
          const { error } = await db.from("mortgage_records").insert(batch);
          if (error) {
            console.error(`  Batch insert error: ${error.message}`);
            // Try inserting one by one to skip individual dupes
            let singles = 0;
            for (const r of batch) {
              const { error: singleErr } = await db.from("mortgage_records").insert(r);
              if (!singleErr) singles++;
            }
            inserted += singles;
          } else {
            inserted += batch.length;
          }
          process.stdout.write(`  Inserted: ${inserted} | Skipped: ${skipped} | Errors: ${parseErrors}\r`);
          batch = [];
        }
      }
    }

    // Flush remaining
    if (batch.length > 0) {
      const { error } = await db.from("mortgage_records").insert(batch);
      if (error) {
        console.error(`  Final batch error: ${error.message}`);
        let singles = 0;
        for (const r of batch) {
          const { error: singleErr } = await db.from("mortgage_records").insert(r);
          if (!singleErr) singles++;
        }
        inserted += singles;
      } else {
        inserted += batch.length;
      }
    }

    console.log(`\n  ${source.sourceTag} results:`);
    console.log(`    Inserted:     ${inserted.toLocaleString()}`);
    console.log(`    Skipped/Dups: ${skipped.toLocaleString()}`);
    console.log(`    Parse errors: ${parseErrors.toLocaleString()}`);

    // Per-county counts for this file
    console.log(`\n  Per-county breakdown:`);
    const sorted = Object.entries(countyCounts).sort((a, b) => b[1] - a[1]);
    for (const [county, count] of sorted) {
      console.log(`    ${county.padEnd(15)} ${count.toLocaleString()}`);
      totalCounts[county] = (totalCounts[county] || 0) + count;
    }

    grandTotal += inserted;
    grandSkipped += skipped;

    // 5. Link to properties by address
    await linkRecordsByAddress(source.sourceTag, addressMap);
  }

  // ─── Summary ─────────────────────────────────────────────────
  console.log(`\n${"═".repeat(55)}`);
  console.log(`GRAND TOTAL`);
  console.log(`${"═".repeat(55)}`);
  console.log(`  Inserted:     ${grandTotal.toLocaleString()}`);
  console.log(`  Skipped/Dups: ${grandSkipped.toLocaleString()}`);
  console.log(`\n  Records per county (all years):`);
  const allSorted = Object.entries(totalCounts).sort((a, b) => b[1] - a[1]);
  for (const [county, count] of allSorted) {
    console.log(`    ${county.padEnd(15)} ${count.toLocaleString()}`);
  }

  // Cleanup temp dir
  console.log(`\n  Temp dir: ${tmpDir} (keeping for re-runs)`);
  console.log(`Done.\n`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
