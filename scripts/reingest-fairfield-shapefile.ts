#!/usr/bin/env tsx
/**
 * Re-ingest Fairfield County OH parcel shapefile with ALL 71 fields properly mapped.
 *
 * Usage:
 *   npx tsx scripts/reingest-fairfield-shapefile.ts
 *   npx tsx scripts/reingest-fairfield-shapefile.ts --dry-run
 *   npx tsx scripts/reingest-fairfield-shapefile.ts --add-columns   (only run ALTER TABLE)
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { DBFFile } from "dbffile";
import { execSync } from "child_process";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const DBF_PATH = "C:/Users/msanc/mxre/.cache/shapefiles/fairfield_parcels/parcels.dbf";
const BATCH_SIZE = 100;
const DRY_RUN = process.argv.includes("--dry-run");
const ADD_COLUMNS_ONLY = process.argv.includes("--add-columns");

const SSH_KEY = "/tmp/mxre_db_key";
const SSH_HOST = "root@${process.env.MXRE_PG_HOST}";
const SSH_OPTS = `-i ${SSH_KEY} -o StrictHostKeyChecking=no`;
const PSQL_CMD = `docker exec -i supabase-db psql -U supabase_admin -d postgres`;

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ---------------------------------------------------------------------------
// Decode maps for coded fields
// ---------------------------------------------------------------------------
// Ohio auditor numeric codes
const BSMT_CODES: Record<string, string> = {
  "1": "Full", "2": "Partial", "3": "Crawl", "4": "Slab", "5": "None",
};
const HEAT_CODES: Record<string, string> = {
  "1": "Hot Water/Steam", "2": "Forced Air", "3": "Electric", "4": "Radiant", "5": "Gravity", "6": "Other", "7": "None",
};
const FUEL_CODES: Record<string, string> = {
  "1": "Coal", "2": "Gas", "3": "Electric", "4": "Oil", "5": "Wood", "6": "Solar", "7": "Geo-Thermal", "8": "Other",
};
const EXTWALL_CODES: Record<string, string> = {
  "1": "Wood", "2": "Brick", "3": "Stone", "4": "Stucco", "5": "Concrete Block",
  "6": "Vinyl/Alum", "7": "Metal", "8": "Log", "9": "Eifs", "10": "Other",
};
const STYLE_CODES: Record<string, string> = {
  "1": "Cape Cod", "2": "Colonial", "3": "Ranch", "4": "Georgian", "5": "Two Story",
  "6": "Split Level", "7": "Bi-Level", "8": "Tri-Level", "9": "Bungalow",
  "13": "Victorian", "14": "Tudor", "15": "A-Frame", "16": "Contemporary",
  "17": "Manufactured", "18": "Log Home", "19": "Other",
};
const ATTIC_CODES: Record<string, string> = {
  "1": "Full", "2": "Partial", "3": "Finished", "4": "Scuttle", "5": "None",
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function toInt(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = parseInt(String(v), 10);
  return isNaN(n) || n === 0 ? null : n;
}

function toFloat(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = parseFloat(String(v));
  return isNaN(n) ? null : n;
}

function toStr(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function decode(v: unknown, map: Record<string, string>): string | null {
  const s = toStr(v);
  if (!s) return null;
  return map[s.toUpperCase()] ?? s;
}

function buildAddress(adrno: unknown, adrdir: unknown, adrstr: unknown, adrsuf: unknown, adrsuf2: unknown): string | null {
  const parts = [toStr(adrno), toStr(adrdir), toStr(adrstr), toStr(adrsuf), toStr(adrsuf2)].filter(Boolean);
  return parts.length > 0 ? parts.join(" ") : null;
}

function buildMailingAddress(m1: unknown, m2: unknown, m3: unknown): string | null {
  const parts = [toStr(m1), toStr(m2), toStr(m3)].filter(Boolean);
  return parts.length > 0 ? parts.join(", ") : null;
}

function parseDate(v: unknown): string | null {
  const s = toStr(v);
  if (!s) return null;
  // Dates may come as MMDDYYYY, MM/DD/YYYY, YYYYMMDD, or YYYY-MM-DD
  const cleaned = s.replace(/[\/\-]/g, "");
  if (cleaned.length === 8) {
    // Try MMDDYYYY first
    const mm = cleaned.slice(0, 2);
    const dd = cleaned.slice(2, 4);
    const yyyy = cleaned.slice(4, 8);
    if (parseInt(yyyy) > 1800 && parseInt(yyyy) < 2100) {
      return `${yyyy}-${mm}-${dd}`;
    }
    // Try YYYYMMDD
    const y2 = cleaned.slice(0, 4);
    const m2 = cleaned.slice(4, 6);
    const d2 = cleaned.slice(6, 8);
    if (parseInt(y2) > 1800 && parseInt(y2) < 2100) {
      return `${y2}-${m2}-${d2}`;
    }
  }
  // If it already looks like a date, return as-is
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  return null;
}

function computeBathrooms(fullBaths: unknown, halfBaths: unknown): number | null {
  const full = toInt(fullBaths);
  const half = toInt(halfBaths);
  if (full == null && half == null) return null;
  return (full ?? 0) + (half ?? 0) * 0.5;
}

// ---------------------------------------------------------------------------
// Step 1: Add missing columns via SSH
// ---------------------------------------------------------------------------
function runSql(sql: string): void {
  const escaped = sql.replace(/'/g, "'\\''");
  const cmd = `ssh ${SSH_OPTS} ${SSH_HOST} "${PSQL_CMD} -c '${escaped}'"`;
  try {
    const out = execSync(cmd, { encoding: "utf-8", timeout: 30_000 });
    console.log(`    OK: ${out.trim()}`);
  } catch (e: any) {
    const msg = e.stderr?.trim() || e.message;
    if (msg.includes("already exists")) {
      console.log(`    SKIP (already exists)`);
    } else {
      console.error(`    ERR: ${msg}`);
    }
  }
}

async function addColumns() {
  console.log("\n[1/3] Adding missing columns to properties table...\n");

  const columns: Array<[string, string]> = [
    ["bedrooms", "INTEGER"],
    ["bathrooms", "NUMERIC(3,1)"],
    ["half_baths", "INTEGER"],
    ["total_rooms", "INTEGER"],
    ["stories", "NUMERIC(3,1)"],
    ["basement_type", "TEXT"],
    ["basement_car_spaces", "INTEGER"],
    ["finished_basement_sqft", "INTEGER"],
    ["recreation_room_sqft", "INTEGER"],
    ["master_bedroom_sqft", "INTEGER"],
    ["exterior_wall", "TEXT"],
    ["heating_type", "TEXT"],
    ["fuel_type", "TEXT"],
    ["style", "TEXT"],
    ["fireplace_count", "INTEGER"],
    ["has_attic", "BOOLEAN"],
    ["lot_acres", "NUMERIC(10,4)"],
    ["lot_sqft", "INTEGER"],
    ["legal_description", "TEXT"],
    ["mailing_address", "TEXT"],
    ["mailing_city", "TEXT"],
    ["mailing_state", "TEXT"],
    ["mailing_zip", "TEXT"],
    ["last_sale_date", "DATE"],
    ["last_sale_price", "NUMERIC"],
    ["sale_year", "INTEGER"],
    ["condition_code", "TEXT"],
    ["property_class", "TEXT"],
    ["year_remodeled", "INTEGER"],
    ["appraised_land", "INTEGER"],
    ["appraised_building", "INTEGER"],
    ["neighborhood_code", "TEXT"],
    ["latitude", "NUMERIC"],
    ["longitude", "NUMERIC"],
    ["owner_name2", "TEXT"],
  ];

  for (const [col, typ] of columns) {
    console.log(`  ALTER TABLE properties ADD COLUMN ${col} ${typ};`);
    if (!DRY_RUN) {
      runSql(`ALTER TABLE properties ADD COLUMN IF NOT EXISTS ${col} ${typ}`);
    }
  }

  console.log(`\n  ${columns.length} columns processed.`);
}

// ---------------------------------------------------------------------------
// Step 2: Parse DBF + map fields
// ---------------------------------------------------------------------------
function mapRecord(p: Record<string, unknown>): Record<string, unknown> | null {
  const parcelId = toStr(p.PARID);
  if (!parcelId) return null;

  const acres = toFloat(p.ACRES);
  const lotSqft = acres != null ? Math.round(acres * 43560) : null;

  return {
    parcel_id: parcelId,
    owner_name: toStr(p.OWN1),
    owner_name2: toStr(p.OWN2),
    address: buildAddress(p.ADRNO, p.ADRDIR, p.ADRSTR, p.ADRSUF, p.ADRSUF2) ?? toStr(p.PADDR1),
    mailing_address: buildMailingAddress(p.MADDR1, p.MADDR2, p.MADDR3),
    mailing_city: toStr(p.MCITYNAME),
    mailing_state: toStr(p.MSTATECODE),
    mailing_zip: toStr(p.MZIP1),
    legal_description: [toStr(p.LEGAL1), toStr(p.LEGAL2), toStr(p.LEGAL3)].filter(Boolean).join(" ") || null,
    lot_acres: acres,
    lot_sqft: lotSqft,
    stories: toFloat(p.STORIES),
    total_sqft: toInt(p.SFLA),
    year_built: toInt(p.YRBLT),
    year_remodeled: toInt(p.YRREMOD),
    total_rooms: toInt(p.RMTOT),
    bedrooms: toInt(p.RMBED),
    bathrooms: computeBathrooms(p.FIXBATH, p.FIXHALF),
    half_baths: toInt(p.FIXHALF),
    basement_type: decode(p.BSMT, BSMT_CODES),
    basement_car_spaces: toInt(p.BSMTCAR),
    finished_basement_sqft: toInt(p.FINBSMTARE),
    recreation_room_sqft: toInt(p.RECROMAREA),
    master_bedroom_sqft: toInt(p.MASTRIMARE),
    heating_type: decode(p.HEAT, HEAT_CODES),
    fuel_type: decode(p.FUEL, FUEL_CODES),
    exterior_wall: decode(p.EXTWALL, EXTWALL_CODES),
    style: decode(p.STYLE, STYLE_CODES),
    fireplace_count: toInt(p.WBFP_O),
    has_attic: toStr(p.ATTIC) != null && String(p.ATTIC).trim() !== "5" ? true : null,
    last_sale_date: parseDate(p.SALEDT),
    last_sale_price: toFloat(p.PRICE),
    sale_year: toInt(p.SALEYEAR),
    assessed_value: toFloat(p.APPRVAL),
    appraised_land: toInt(p.APRLAND),
    appraised_building: toInt(p.APRBLDG),
    condition_code: toStr(p.CDU),
    property_class: toStr(p.CLASS),
    neighborhood_code: toStr(p.NBHD),
  };
}

// ---------------------------------------------------------------------------
// Step 3: Update properties in batches
// ---------------------------------------------------------------------------
async function ingestRecords() {
  console.log("\n[2/3] Reading DBF file...\n");

  const dbf = await DBFFile.open(DBF_PATH);
  console.log(`  Records: ${dbf.recordCount.toLocaleString()}`);
  console.log(`  Fields:  ${dbf.fields.map((f: any) => f.name).join(", ")}`);

  // Get county ID
  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) { console.error("  ERROR: Fairfield County OH not found in DB"); return; }
  console.log(`  County ID: ${county.id}`);

  // Read all records
  const records = await dbf.readRecords(dbf.recordCount);
  console.log(`  Loaded ${records.length.toLocaleString()} records from DBF`);

  // Map records
  const mapped: Array<Record<string, unknown>> = [];
  let skipped = 0;

  for (const raw of records) {
    const rec = mapRecord(raw as Record<string, unknown>);
    if (!rec) { skipped++; continue; }
    mapped.push(rec);
  }

  console.log(`  Mapped: ${mapped.length.toLocaleString()}, Skipped (no PARID): ${skipped}`);

  // Print sample
  console.log("\n  Sample records:");
  for (const s of mapped.slice(0, 3)) {
    console.log(`    ${s.parcel_id}: ${s.owner_name} | ${s.address}`);
    console.log(`      beds=${s.bedrooms} bath=${s.bathrooms} sqft=${s.total_sqft} yr=${s.year_built}`);
    console.log(`      style=${s.style} heat=${s.heating_type} fuel=${s.fuel_type} wall=${s.exterior_wall}`);
    console.log(`      lot=${s.lot_acres}ac bsmt=${s.basement_type} attic=${s.has_attic}`);
    console.log(`      sale=${s.last_sale_date} $${s.last_sale_price} appraised=$${s.assessed_value}`);
  }

  if (DRY_RUN) {
    console.log("\n  DRY RUN — no updates performed.");
    return;
  }

  // Update in batches
  console.log(`\n[3/3] Updating properties (batch size ${BATCH_SIZE})...\n`);

  let updated = 0;
  let errors = 0;
  const startTime = Date.now();

  for (let i = 0; i < mapped.length; i += BATCH_SIZE) {
    const batch = mapped.slice(i, i + BATCH_SIZE);

    // Process each record individually (upsert by parcel_id + county_id)
    const promises = batch.map(async (rec) => {
      const { error } = await db.from("properties")
        .update(rec)
        .eq("county_id", county.id)
        .eq("parcel_id", rec.parcel_id);

      if (error) {
        if (errors < 5) console.error(`    Update error (${rec.parcel_id}): ${error.message}`);
        errors++;
      } else {
        updated++;
      }
    });

    await Promise.all(promises);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const rate = (updated / parseFloat(elapsed)).toFixed(0);
    process.stdout.write(`\r  Updated: ${updated.toLocaleString()} / ${mapped.length.toLocaleString()} | Errors: ${errors} | ${elapsed}s (${rate}/s)`);
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n\n  Done: ${updated.toLocaleString()} updated, ${errors} errors in ${totalTime}s`);

  // Verify
  const { data: sample } = await db.from("properties")
    .select("parcel_id, owner_name, address, bedrooms, bathrooms, total_sqft, year_built, style, last_sale_price, lot_acres")
    .eq("county_id", county.id)
    .not("bedrooms", "is", null)
    .limit(5);

  console.log("\n  Verification samples:");
  for (const s of sample || []) {
    console.log(`    ${s.parcel_id}: ${s.owner_name} | ${s.address}`);
    console.log(`      ${s.bedrooms}bd/${s.bathrooms}ba ${s.total_sqft}sqft ${s.year_built} ${s.style}`);
    console.log(`      lot=${s.lot_acres}ac sale=$${s.last_sale_price}`);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log("=== MXRE: Re-ingest Fairfield County OH Shapefile (All 71 Fields) ===");
  console.log(`  Mode: ${DRY_RUN ? "DRY RUN" : ADD_COLUMNS_ONLY ? "ADD COLUMNS ONLY" : "FULL INGEST"}`);
  console.log(`  DBF:  ${DBF_PATH}`);

  await addColumns();

  if (ADD_COLUMNS_ONLY) {
    console.log("\n  Done (columns only).");
    return;
  }

  await ingestRecords();

  console.log("\n=== Complete ===");
}

main().catch(console.error);
