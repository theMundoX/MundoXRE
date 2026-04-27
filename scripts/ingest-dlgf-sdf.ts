#!/usr/bin/env tsx
/**
 * MXRE — Indiana DLGF Sales Disclosure Form ingest
 *
 * Downloads bulk SDF ZIP files from stats.indiana.edu and ingests deed transfer
 * records (buyer, seller, sale price, parcel, date) into mortgage_records as
 * document_type='deed'. Also attempts direct property_id linkage via parcel number.
 *
 * File formats:
 *   Modern (2021+): SDF_{year}.zip — UTF-16 LE, tab-delimited, double-quoted.
 *     Contains SALEDISC.txt (sale metadata), SALECONTAC.txt (parties),
 *     SALEPARCEL.txt (parcel details).
 *   Legacy (2008–2020): SDF_View_CSV_{year}.zip — pipe-delimited single file.
 *     (See LEGACY_FIELDS below for column mapping.)
 *
 * Usage:
 *   npx tsx scripts/ingest-dlgf-sdf.ts
 *   npx tsx scripts/ingest-dlgf-sdf.ts --county=Marion
 *   npx tsx scripts/ingest-dlgf-sdf.ts --year=2024
 *   npx tsx scripts/ingest-dlgf-sdf.ts --from-year=2021 --to-year=2025
 *   npx tsx scripts/ingest-dlgf-sdf.ts --all-counties
 *   npx tsx scripts/ingest-dlgf-sdf.ts --dry-run
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { execSync, spawnSync } from "child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// ─── CLI args ──────────────────────────────────────────────────────────────

const argv = process.argv.slice(2);
const getArg = (n: string) => argv.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => argv.includes(`--${n}`);

const COUNTY_FILTER = getArg("county") ?? "Marion";
const ALL_COUNTIES  = hasFlag("all-counties");
const DRY_RUN       = hasFlag("dry-run");
const YEAR_ARG      = getArg("year");
const FROM_YEAR     = YEAR_ARG ? parseInt(YEAR_ARG) : parseInt(getArg("from-year") ?? "2021");
const TO_YEAR       = YEAR_ARG ? parseInt(YEAR_ARG) : parseInt(getArg("to-year") ?? new Date().getFullYear().toString());

// Marion County DLGF ID = 49 (alphabetical position among IN's 92 counties)
const MARION_COUNTY_ID = "49";

// ─── DB ────────────────────────────────────────────────────────────────────

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
  { auth: { persistSession: false } },
);

// ─── Helpers ───────────────────────────────────────────────────────────────

function padCountyId(id: string): string {
  return id.padStart(2, "0");
}

async function downloadFile(url: string, dest: string): Promise<void> {
  console.log(`  Downloading ${url}...`);
  const res = await fetch(url, {
    headers: { "User-Agent": "MXRE-Ingest/1.0" },
    signal: AbortSignal.timeout(300_000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  const buf = await res.arrayBuffer();
  writeFileSync(dest, Buffer.from(buf));
  const mb = (buf.byteLength / 1024 / 1024).toFixed(1);
  console.log(`  Downloaded ${mb} MB → ${dest}`);
}

function extractZip(zipPath: string, outDir: string): void {
  mkdirSync(outDir, { recursive: true });
  // Try system unzip (Linux/Mac VPS), fall back to PowerShell on Windows
  const unzipResult = spawnSync("unzip", ["-o", "-q", zipPath, "-d", outDir], { encoding: "utf8" });
  if (unzipResult.error || (unzipResult.status !== 0 && unzipResult.status !== null)) {
    // Fallback: PowerShell Expand-Archive (Windows)
    execSync(
      `powershell -Command "Expand-Archive -Force -Path '${zipPath}' -DestinationPath '${outDir}'"`,
      { stdio: "inherit" },
    );
  }
}

/**
 * Parse a UTF-16 LE tab-delimited file (modern SDF format).
 * Returns array of objects keyed by the header row.
 */
function parseTsvUtf16(filePath: string): Record<string, string>[] {
  const raw = readFileSync(filePath);
  // Strip UTF-16 LE BOM (0xFF 0xFE) if present
  const hasBom = raw[0] === 0xFF && raw[1] === 0xFE;
  const text = raw.slice(hasBom ? 2 : 0).toString("utf16le");
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return [];

  const unquote = (s: string) => s.replace(/^"|"$/g, "").replace(/""/g, '"').trim();
  const headers = lines[0].split("\t").map(unquote);

  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split("\t").map(unquote);
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = cols[j] ?? "";
    }
    rows.push(row);
  }
  return rows;
}

/**
 * Parse a legacy pipe-delimited single-file SDF (2008–2020).
 * Returns array of objects keyed by header row.
 */
function parsePipeDelimited(filePath: string): Record<string, string>[] {
  const text = readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = lines[0].split("|").map(h => h.trim());
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split("|");
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) row[headers[j]] = (cols[j] ?? "").trim();
    rows.push(row);
  }
  return rows;
}

// ─── Property linker ───────────────────────────────────────────────────────

/** Cache: parcel_id string → property id */
const parcelCache = new Map<string, number>();

async function lookupPropertyId(parcelNum: string, countyId: number): Promise<number | null> {
  if (!parcelNum) return null;
  const clean = parcelNum.replace(/[-.\s]/g, "");
  if (parcelCache.has(clean)) return parcelCache.get(clean)!;

  // Try exact parcel_id match (strips formatting)
  const { data } = await db.from("properties")
    .select("id, parcel_id")
    .eq("county_id", countyId)
    .or(`parcel_id.eq.${parcelNum},parcel_id.eq.${clean}`)
    .limit(1)
    .single();

  const pid = (data as any)?.id ?? null;
  if (pid) parcelCache.set(clean, pid);
  return pid;
}

// ─── County DB resolver ────────────────────────────────────────────────────

let marionDbId: number | null = null;

async function getMarionCountyDbId(): Promise<number | null> {
  if (marionDbId) return marionDbId;
  const { data } = await db.from("counties")
    .select("id")
    .eq("state_fips", "18")
    .eq("county_fips", "097")
    .single();
  marionDbId = (data as any)?.id ?? null;
  return marionDbId;
}

// ─── Modern format (2021+) ─────────────────────────────────────────────────

async function ingestModernYear(
  year: number,
  extractDir: string,
  countyDbId: number,
): Promise<{ inserted: number; dupes: number; linked: number }> {
  const stats = { inserted: 0, dupes: 0, linked: 0 };

  const saleDiscPath  = join(extractDir, "SALEDISC.txt");
  const contactPath   = join(extractDir, "SALECONTAC.txt");
  const parcelPath    = join(extractDir, "SALEPARCEL.txt");

  if (!existsSync(saleDiscPath)) {
    console.error(`  SALEDISC.txt not found in ${extractDir}`);
    return stats;
  }

  // Step 1: Load main sale records and filter to target county
  console.log("  Parsing SALEDISC.txt...");
  const allDisc = parseTsvUtf16(saleDiscPath);
  const targetId = ALL_COUNTIES ? null : padCountyId(MARION_COUNTY_ID);
  const filtered = targetId
    ? allDisc.filter(r => padCountyId(r["County_ID"] ?? "") === targetId || r["County_Name"]?.toLowerCase() === COUNTY_FILTER.toLowerCase())
    : allDisc;
  console.log(`  ${allDisc.length.toLocaleString()} total SDF rows → ${filtered.length.toLocaleString()} for ${targetId ? COUNTY_FILTER : "all counties"}`);

  if (filtered.length === 0) return stats;

  const sdfIds = new Set(filtered.map(r => r["SDF_ID"]).filter(Boolean));

  // Step 2: Load contacts (buyer/seller) for our SDF IDs
  const buyerMap  = new Map<string, string>();
  const sellerMap = new Map<string, string>();

  if (existsSync(contactPath)) {
    console.log("  Parsing SALECONTAC.txt...");
    const contacts = parseTsvUtf16(contactPath);
    for (const c of contacts) {
      const id = c["SDF_ID"];
      if (!id || !sdfIds.has(id)) continue;
      const name = [c["Company"] || c["Name"], c["Suffix"]].filter(Boolean).join(" ").trim().toUpperCase();
      if (!name) continue;
      if (c["Contact_Type"] === "B") {
        buyerMap.set(id, buyerMap.has(id) ? `${buyerMap.get(id)}; ${name}` : name);
      } else if (c["Contact_Type"] === "S") {
        sellerMap.set(id, sellerMap.has(id) ? `${sellerMap.get(id)}; ${name}` : name);
      }
    }
  }

  // Step 3: Load parcel details for our SDF IDs
  const parcelMap = new Map<string, string>();  // SDF_ID → parcel number
  const addrMap   = new Map<string, string>();  // SDF_ID → property address

  if (existsSync(parcelPath)) {
    console.log("  Parsing SALEPARCEL.txt...");
    const parcels = parseTsvUtf16(parcelPath);
    for (const p of parcels) {
      const id = p["SDF_ID"];
      if (!id || !sdfIds.has(id)) continue;
      const parcel = (p["P2_1_Parcel_Num_Verified"] || p["A1_Parcel_Number"] || "").trim();
      if (parcel && !parcelMap.has(id)) parcelMap.set(id, parcel);
      const addr = [p["A5_Street1"], p["A5_City"], p["A5_State"]].filter(Boolean).join(", ");
      if (addr && !addrMap.has(id)) addrMap.set(id, addr);
    }
  }

  // Step 4: Dedup against existing records
  const existingNums = new Set<string>();
  const sdfIdArr = [...sdfIds];
  for (let i = 0; i < sdfIdArr.length; i += 200) {
    const chunk = sdfIdArr.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .in("document_number", chunk);
    if (data) for (const r of data) existingNums.add(r.document_number!);
  }

  // Step 5: Build and insert records
  const SOURCE_URL = "https://www.stats.indiana.edu/sdfdata/";
  const batch: Record<string, unknown>[] = [];

  async function flush() {
    if (batch.length === 0) return;
    if (DRY_RUN) {
      stats.inserted += batch.length;
      batch.length = 0;
      return;
    }
    const { error } = await db.from("mortgage_records").insert(batch);
    if (error) {
      console.error(`  Insert error: ${error.message.slice(0, 120)}`);
    } else {
      stats.inserted += batch.length;
    }
    batch.length = 0;
  }

  for (const disc of filtered) {
    const sdfId = disc["SDF_ID"];
    if (!sdfId) continue;
    if (existingNums.has(sdfId)) { stats.dupes++; continue; }

    const conveyanceDate = disc["C7_Conveyance_Date"] || disc["P3_5_Transfer_Date"] || disc["P2_13_Date_Sale"];
    if (!conveyanceDate) continue;

    // Sale price is stored in CENTS; convert to dollars
    const priceRaw = parseFloat(disc["E1_Sales_Price"] ?? "0");
    const salePrice = priceRaw > 0 ? Math.round(priceRaw / 100) : null;

    const buyer  = buyerMap.get(sdfId) ?? null;
    const seller = sellerMap.get(sdfId) ?? null;
    const parcel = parcelMap.get(sdfId) ?? null;

    // Try direct property linkage via parcel number
    let propId: number | null = null;
    if (parcel) {
      propId = await lookupPropertyId(parcel, countyDbId);
      if (propId) stats.linked++;
    }

    batch.push({
      property_id:     propId,
      document_type:   "deed",
      recording_date:  conveyanceDate,
      borrower_name:   seller,
      lender_name:     buyer,
      document_number: sdfId,
      loan_amount:     salePrice,
      original_amount: salePrice,
      source_url:      SOURCE_URL,
    });

    if (batch.length >= 200) await flush();
  }
  await flush();

  return stats;
}

// ─── Legacy format (2008–2020) ─────────────────────────────────────────────

/**
 * Legacy pipe-delimited files have a flat structure — all parties and parcels
 * in one row. Field names vary by year but follow a consistent pattern.
 * This mapping covers the known stable fields across 2008–2020.
 */
const LEGACY_FIELD_ALIASES: Record<string, string[]> = {
  sdf_id:          ["SDF_FORM_ID", "UNIQUE_SALES_ID", "SDF_ID"],
  county_id:       ["COUNTY_ID", "CNTY_ID"],
  county_name:     ["COUNTY_NAME", "CNTY_NM"],
  conveyance_date: ["C7_CONVEYANCE_DATE", "CONVEYANCE_DATE", "C7_DATE"],
  sale_price:      ["E1_SALES_PRICE", "E1SALESPRICE", "SALE_PRICE"],
  buyer_name:      ["BUYER_NAME", "GRANTEE_NM", "PURCHASER_NAME"],
  seller_name:     ["SELLER_NAME", "GRANTOR_NM", "TRANSFEROR_NAME"],
  parcel_number:   ["PARCEL_NUM", "PARCEL_NUMBER", "A1_PARCEL_NUMBER", "PARCEL_NO"],
};

function legacyField(row: Record<string, string>, key: string): string {
  const aliases = LEGACY_FIELD_ALIASES[key] ?? [];
  for (const alias of aliases) {
    if (row[alias] !== undefined && row[alias] !== "") return row[alias];
  }
  return "";
}

async function ingestLegacyYear(
  year: number,
  extractDir: string,
  countyDbId: number,
): Promise<{ inserted: number; dupes: number; linked: number }> {
  const stats = { inserted: 0, dupes: 0, linked: 0 };

  // Legacy ZIPs may extract a single .txt or multiple files — find the main one
  let mainFile: string | null = null;
  for (const candidate of [`SDF_View_CSV_${year}.txt`, `SDF_${year}.txt`, `sdf${year}.txt`]) {
    const p = join(extractDir, candidate);
    if (existsSync(p)) { mainFile = p; break; }
  }
  // Fallback: find any .txt file
  if (!mainFile) {
    const { readdirSync } = await import("fs");
    const txt = readdirSync(extractDir).find(f => f.endsWith(".txt"));
    if (txt) mainFile = join(extractDir, txt);
  }
  if (!mainFile) {
    console.error(`  No .txt file found in ${extractDir} for legacy year ${year}`);
    return stats;
  }

  console.log(`  Parsing legacy file: ${mainFile}...`);
  const rows = parsePipeDelimited(mainFile);
  const targetId = ALL_COUNTIES ? null : padCountyId(MARION_COUNTY_ID);
  const filtered = targetId
    ? rows.filter(r => padCountyId(legacyField(r, "county_id")) === targetId || legacyField(r, "county_name").toLowerCase() === COUNTY_FILTER.toLowerCase())
    : rows;
  console.log(`  ${rows.length.toLocaleString()} rows → ${filtered.length.toLocaleString()} for ${targetId ? COUNTY_FILTER : "all counties"}`);

  if (filtered.length === 0) return stats;

  const sdfIds = filtered.map(r => legacyField(r, "sdf_id")).filter(Boolean);
  const existingNums = new Set<string>();
  for (let i = 0; i < sdfIds.length; i += 200) {
    const chunk = sdfIds.slice(i, i + 200);
    const { data } = await db.from("mortgage_records")
      .select("document_number")
      .in("document_number", chunk);
    if (data) for (const r of data) existingNums.add(r.document_number!);
  }

  const SOURCE_URL = "https://www.stats.indiana.edu/sdfdata/";
  const batch: Record<string, unknown>[] = [];

  async function flush() {
    if (batch.length === 0) return;
    if (DRY_RUN) { stats.inserted += batch.length; batch.length = 0; return; }
    const { error } = await db.from("mortgage_records").insert(batch);
    if (error) console.error(`  Insert error: ${error.message.slice(0, 120)}`);
    else stats.inserted += batch.length;
    batch.length = 0;
  }

  for (const row of filtered) {
    const sdfId = legacyField(row, "sdf_id");
    if (!sdfId) continue;
    if (existingNums.has(sdfId)) { stats.dupes++; continue; }

    const conveyanceDate = legacyField(row, "conveyance_date");
    if (!conveyanceDate) continue;

    const priceRaw = parseFloat(legacyField(row, "sale_price") || "0");
    // Legacy files may store price in dollars OR cents; heuristic: >1,000,000 && not a known large sale → cents
    const salePrice = priceRaw > 0
      ? (priceRaw > 100_000_000 ? Math.round(priceRaw / 100) : Math.round(priceRaw))
      : null;

    const buyer  = legacyField(row, "buyer_name").toUpperCase() || null;
    const seller = legacyField(row, "seller_name").toUpperCase() || null;
    const parcel = legacyField(row, "parcel_number") || null;

    let propId: number | null = null;
    if (parcel) {
      propId = await lookupPropertyId(parcel, countyDbId);
      if (propId) stats.linked++;
    }

    batch.push({
      property_id:     propId,
      document_type:   "deed",
      recording_date:  conveyanceDate,
      borrower_name:   seller,
      lender_name:     buyer,
      document_number: sdfId,
      loan_amount:     salePrice,
      original_amount: salePrice,
      source_url:      SOURCE_URL,
    });

    if (batch.length >= 200) await flush();
  }
  await flush();

  return stats;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — Indiana DLGF Sales Disclosure Form ingest");
  console.log("═".repeat(60));
  console.log(`Years    : ${FROM_YEAR} – ${TO_YEAR}`);
  console.log(`County   : ${ALL_COUNTIES ? "ALL" : COUNTY_FILTER}`);
  console.log(`Dry run  : ${DRY_RUN}`);
  console.log();

  const countyDbId = await getMarionCountyDbId();
  if (!countyDbId && !ALL_COUNTIES) {
    throw new Error("Marion County not found in DB counties table");
  }

  const workDir = join(tmpdir(), "mxre-sdf");
  mkdirSync(workDir, { recursive: true });

  let totalInserted = 0, totalDupes = 0, totalLinked = 0;

  for (let year = FROM_YEAR; year <= TO_YEAR; year++) {
    const isLegacy = year <= 2020;
    const zipUrl = isLegacy
      ? `https://www.stats.indiana.edu/sdfdata/SDF_View_CSV_${year}.zip`
      : `https://www.stats.indiana.edu/sdfdata/SDF_${year}.zip`;

    const zipPath   = join(workDir, `SDF_${year}.zip`);
    const extractDir = join(workDir, `SDF_${year}`);

    console.log(`\n── Year ${year} (${isLegacy ? "legacy" : "modern"}) ──`);

    try {
      // Download (skip if already cached)
      if (!existsSync(zipPath)) {
        await downloadFile(zipUrl, zipPath);
      } else {
        console.log(`  Using cached ${zipPath}`);
      }

      // Extract
      if (!existsSync(extractDir)) {
        console.log(`  Extracting to ${extractDir}...`);
        extractZip(zipPath, extractDir);
      }

      // Ingest
      const stats = isLegacy
        ? await ingestLegacyYear(year, extractDir, countyDbId!)
        : await ingestModernYear(year, extractDir, countyDbId!);

      console.log(`  Inserted: ${stats.inserted.toLocaleString()} | Dupes: ${stats.dupes.toLocaleString()} | Linked to property: ${stats.linked.toLocaleString()}`);
      totalInserted += stats.inserted;
      totalDupes    += stats.dupes;
      totalLinked   += stats.linked;

      // Clean up extract dir to save disk space; keep ZIP for re-runs
      rmSync(extractDir, { recursive: true, force: true });
    } catch (err: any) {
      console.error(`  Error on year ${year}: ${err.message}`);
    }
  }

  console.log(`\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${totalInserted.toLocaleString()} inserted, ${totalDupes.toLocaleString()} dupes, ${totalLinked.toLocaleString()} directly linked to properties`);
  console.log("Done.\n");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
