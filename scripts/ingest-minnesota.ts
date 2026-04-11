#!/usr/bin/env tsx
/**
 * Minnesota Parcel Ingestion
 *
 * Downloads the statewide GeoPackage from MnGeo and ingests into properties table.
 * Falls back to ArcGIS Feature Service if download fails.
 *
 * Usage:
 *   npx tsx scripts/ingest-minnesota.ts
 *   npx tsx scripts/ingest-minnesota.ts --arcgis    # Force ArcGIS FeatureServer mode
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { execSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, createWriteStream, readFileSync, unlinkSync, readdirSync } from "node:fs";
import { join } from "node:path";

const BATCH_SIZE = 500;
const PAGE_SIZE = 2000;
const CONCURRENT_REQUESTS = 3;
const RETRY_MAX = 5;
const RETRY_BASE_MS = 3000;

const DATA_DIR = "C:/Users/msanc/mxre/data";
const GPKG_URL = "https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_mngeo/plan_parcels_open/gpkg_plan_parcels_open.zip";
const FGDB_URL = "https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_mngeo/plan_parcels_open/fgdb_plan_parcels_open.zip";

// ArcGIS Feature Service - MnGeo statewide parcels (enterprise server)
const ARCGIS_URL = "https://enterprise.gisdata.mn.gov/aghost/rest/services/us_mn_state_mngeo/plan_parcels_open/FeatureServer";
const ARCGIS_LAYER = 1; // Layer 1 = Plan Parcels Open (2.7M records)

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── County Management ──────────────────────────────────────────────

async function getOrCreateCounty(name: string, countyFips = "000"): Promise<number> {
  const cleanName = name.trim();
  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", cleanName)
    .eq("state_code", "MN")
    .single();
  if (data) return data.id;

  const { data: created, error } = await db.from("counties")
    .insert({ county_name: cleanName, state_code: "MN", state_fips: "27", county_fips: countyFips, active: true })
    .select("id")
    .single();
  if (error) throw new Error(`Failed to create county ${cleanName}: ${error.message}`);
  return created!.id;
}

// Minnesota county FIPS codes (state FIPS = 27)
const MN_COUNTY_FIPS: Record<string, string> = {
  "AITKIN": "001", "ANOKA": "003", "BECKER": "005", "BELTRAMI": "007",
  "BENTON": "009", "BIG STONE": "011", "BLUE EARTH": "013", "BROWN": "015",
  "CARLTON": "017", "CARVER": "019", "CASS": "021", "CHIPPEWA": "023",
  "CHISAGO": "025", "CLAY": "027", "CLEARWATER": "029", "COOK": "031",
  "COTTONWOOD": "033", "CROW WING": "035", "DAKOTA": "037", "DODGE": "039",
  "DOUGLAS": "041", "FARIBAULT": "043", "FILLMORE": "045", "FREEBORN": "047",
  "GOODHUE": "049", "GRANT": "051", "HENNEPIN": "053", "HOUSTON": "055",
  "HUBBARD": "057", "ISANTI": "059", "ITASCA": "061", "JACKSON": "063",
  "KANABEC": "065", "KANDIYOHI": "067", "KITTSON": "069", "KOOCHICHING": "071",
  "LAC QUI PARLE": "073", "LAKE": "075", "LAKE OF THE WOODS": "077", "LE SUEUR": "079",
  "LINCOLN": "081", "LYON": "083", "MCLEOD": "085", "MAHNOMEN": "087",
  "MARSHALL": "089", "MARTIN": "091", "MEEKER": "093", "MILLE LACS": "095",
  "MORRISON": "097", "MOWER": "099", "MURRAY": "101", "NICOLLET": "103",
  "NOBLES": "105", "NORMAN": "107", "OLMSTED": "109", "OTTER TAIL": "111",
  "PENNINGTON": "113", "PINE": "115", "PIPESTONE": "117", "POLK": "119",
  "POPE": "121", "RAMSEY": "123", "RED LAKE": "125", "REDWOOD": "127",
  "RENVILLE": "129", "RICE": "131", "ROCK": "133", "ROSEAU": "135",
  "ST. LOUIS": "137", "SAINT LOUIS": "137", "ST LOUIS": "137", "SCOTT": "139",
  "SHERBURNE": "141", "SIBLEY": "143", "STEARNS": "145", "STEELE": "147",
  "STEVENS": "149", "SWIFT": "151", "TODD": "153", "TRAVERSE": "155",
  "WABASHA": "157", "WADENA": "159", "WASECA": "161", "WASHINGTON": "163",
  "WATONWAN": "165", "WILKIN": "167", "WINONA": "169", "WRIGHT": "171",
  "YELLOW MEDICINE": "173",
};

const countyIdCache = new Map<string, number>();
async function resolveCountyId(name: string): Promise<number> {
  const upper = name.toUpperCase().trim();
  if (countyIdCache.has(upper)) return countyIdCache.get(upper)!;
  // Title-case the name
  const titleCase = upper.split(" ").map(w => w.charAt(0) + w.slice(1).toLowerCase()).join(" ");
  const countyFips = MN_COUNTY_FIPS[upper] || "000";
  const id = await getOrCreateCounty(titleCase, countyFips);
  countyIdCache.set(upper, id);
  return id;
}

// ─── Property Row Mapping ───────────────────────────────────────────

function classifyLandUse(useCode: string, useDesc: string): string {
  const c = (useCode || "").toLowerCase();
  const d = (useDesc || "").toLowerCase();
  const combined = `${c} ${d}`;
  if (combined.match(/resid|single|sfr|dwelling|house|home/)) return "single_family";
  if (combined.match(/multi|apart|duplex|triplex|fourplex/)) return "multifamily";
  if (combined.match(/condo|townho/)) return "condo";
  if (combined.match(/commerc|office|retail|store/)) return "commercial";
  if (combined.match(/industr|warehouse|manufact/)) return "industrial";
  if (combined.match(/vacan|agri|farm|ranch|timber|forest|pasture|crop|undevel/)) return "land";
  if (combined.match(/exempt|govern|school|church|relig|hospital|park|util/)) return "exempt";
  return "residential";
}

function mapMNAttributes(a: Record<string, any>, countyId: number): any {
  const INT_MAX = 2_147_483_647;
  const parseNum = (v: any) => {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (isNaN(n) || n <= 0) return null;
    const rounded = Math.round(n);
    return rounded > INT_MAX ? null : rounded;
  };

  // MN parcels use these lowercase field names from the enterprise ArcGIS FeatureServer
  // Fields: county_pin, state_pin, anumber, st_pre_dir, st_name, st_pos_typ, st_pos_dir,
  //         zip, ctu_name, postcomm, co_name, owner_name, emv_total, emv_land, emv_bldg,
  //         year_built, fin_sq_ft, sale_value, sale_date, useclass1, dwell_type, acres_poly, total_tax
  const parcelId = a.county_pin || a.state_pin || "";
  // Build address from street components
  const addrParts = [a.anumber, a.st_pre_dir, a.st_name, a.st_pos_typ, a.st_pos_dir].filter(Boolean);
  const address = addrParts.length > 0
    ? addrParts.join(" ").replace(/\s+/g, " ").trim()
    : "";

  const city = (a.ctu_name || a.postcomm || "").trim();
  const zip = String(a.zip || "").substring(0, 5);
  const owner = (a.owner_name || "").trim();
  const totalVal = parseNum(a.emv_total);
  const landVal = parseNum(a.emv_land);
  const yearBuilt = a.year_built;
  const sqft = parseNum(a.fin_sq_ft);
  const salePrice = parseNum(a.sale_value);
  const useCode = a.useclass1 || "";
  const useDesc = a.dwell_type || "";
  const acres = parseNum(a.acres_poly);

  let saleDate: string | null = null;
  const rawDate = a.sale_date || "";
  if (rawDate) {
    const d = String(rawDate);
    if (d.match(/^\d{4}-\d{2}-\d{2}/)) saleDate = d.substring(0, 10);
    else if (d.match(/^\d+$/) && d.length > 8) {
      // Epoch ms
      const dt = new Date(parseInt(d));
      if (dt.getFullYear() > 1970) saleDate = dt.toISOString().substring(0, 10);
    }
  }

  return {
    county_id: countyId,
    parcel_id: String(parcelId).trim(),
    address: address.toUpperCase(),
    city: city.toUpperCase(),
    state_code: "MN",
    zip,
    owner_name: owner,
    assessed_value: totalVal,
    year_built: yearBuilt && yearBuilt > 1700 && yearBuilt < 2030 ? yearBuilt : null,
    total_sqft: sqft,
    total_units: null,
    property_type: classifyLandUse(useCode, useDesc),
    source: "mngeo-parcels-open",
    land_value: landVal,
    last_sale_price: salePrice,
    last_sale_date: saleDate,
    property_tax: parseNum(a.total_tax),
    land_sqft: acres ? Math.min(Math.round(acres * 43560), INT_MAX) : null,
  };
}

// ─── ArcGIS Fetching ────────────────────────────────────────────────

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "MXRE-Ingester/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const json = await resp.json();
    if (json.error) throw new Error(`ArcGIS: ${json.error.message}`);
    return json;
  } catch (err: any) {
    if (attempt >= RETRY_MAX) throw err;
    const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
    console.log(`    Retry ${attempt}/${RETRY_MAX}: ${err.message}`);
    await new Promise(r => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

// ─── ArcGIS Mode ────────────────────────────────────────────────────

async function ingestViaArcGIS() {
  console.log("  Mode: ArcGIS Feature Service (offset pagination)");
  console.log(`  URL: ${ARCGIS_URL}/${ARCGIS_LAYER}`);

  // Get count
  const countUrl = `${ARCGIS_URL}/${ARCGIS_LAYER}/query?where=1%3D1&returnCountOnly=true&f=json`;
  const countData = await fetchWithRetry(countUrl);
  const totalCount = countData.count || 0;
  console.log(`  Total records: ${totalCount.toLocaleString()}`);

  // Support --offset=N to resume from a record offset
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;
  if (offset > 0) console.log(`  Resuming from offset ${offset.toLocaleString()}`);

  let totalInserted = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  console.log(`\n  Starting ingestion (${totalCount.toLocaleString()} total)\n`);

  while (offset < totalCount) {
    const MN_FIELDS = "county_pin,state_pin,anumber,st_pre_dir,st_name,st_pos_typ,st_pos_dir,zip,ctu_name,postcomm,co_name,owner_name,emv_total,emv_land,emv_bldg,year_built,fin_sq_ft,sale_value,sale_date,useclass1,dwell_type,acres_poly,total_tax,num_units";
    const url = `${ARCGIS_URL}/${ARCGIS_LAYER}/query?where=1%3D1&outFields=${MN_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;
    let data: any;
    try {
      data = await fetchWithRetry(url);
    } catch (err: any) {
      console.error(`  Fetch error at offset ${offset}: ${err.message}`);
      totalErrors += PAGE_SIZE;
      offset += PAGE_SIZE;
      continue;
    }

    const features = data.features || [];
    if (features.length === 0) {
      console.log(`  No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const f of features) {
      const a = f.attributes;
      const countyName = a.co_name || a.COUNTYNAME || a.COUNTY_NAME || a.CO_NAME || "";
      if (!countyName) { totalSkipped++; continue; }
      try {
        const countyId = await resolveCountyId(countyName);
        const row = mapMNAttributes(a, countyId);
        if (row.parcel_id || row.address) rows.push(row);
        else totalSkipped++;
      } catch { totalSkipped++; }
    }

    // Dedup within page
    const seen = new Map<string, any>();
    for (const row of rows) seen.set(`${row.county_id}|${row.parcel_id}`, row);
    const dedupedRows = Array.from(seen.values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const b = dedupedRows.slice(i, i + BATCH_SIZE);
      let ok = false;
      let lastErr: any;
      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
          const { error } = await db.from("properties").upsert(b, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
          if (error) { lastErr = error; break; }
          ok = true; break;
        } catch (err: any) {
          lastErr = err;
          if (attempt < 5) await new Promise(r => setTimeout(r, 5000 * attempt));
        }
      }
      if (!ok) {
        console.error(`    DB error: ${lastErr?.message?.substring(0, 80)}`);
        totalErrors += b.length;
      } else {
        totalInserted += b.length;
      }
    }

    offset += features.length;
    const pct = ((offset / totalCount) * 100).toFixed(1);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = (totalInserted / (parseFloat(elapsed) || 1)).toFixed(0);
    const eta = rate > "0" ? Math.round((totalCount - offset) / parseFloat(rate) / 60) : "?";
    if (offset % (PAGE_SIZE * 10) < PAGE_SIZE) {
      console.log(
        `  [MN] offset=${offset.toLocaleString()} (${pct}%) | ` +
        `${totalInserted.toLocaleString()} inserted | ${rate}/s | ETA ${eta}min`
      );
    }
  }

  return totalInserted;
}

// ─── GeoJSON Conversion Mode ────────────────────────────────────────

async function downloadFile(url: string, dest: string) {
  console.log(`  Downloading: ${url}`);
  const resp = await fetch(url, {
    headers: { "User-Agent": "MXRE-Ingester/1.0" },
    signal: AbortSignal.timeout(600_000), // 10 min timeout for large file
  });
  if (!resp.ok) throw new Error(`Download failed: HTTP ${resp.status}`);
  if (!resp.body) throw new Error("No response body");

  const writer = createWriteStream(dest);
  const reader = resp.body.getReader();
  let downloaded = 0;
  const contentLength = parseInt(resp.headers.get("content-length") || "0");

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    writer.write(Buffer.from(value));
    downloaded += value.length;
    if (contentLength > 0 && downloaded % (10 * 1024 * 1024) < value.length) {
      const pct = ((downloaded / contentLength) * 100).toFixed(1);
      const mb = (downloaded / 1024 / 1024).toFixed(1);
      process.stdout.write(`\r    Downloaded: ${mb} MB (${pct}%)`);
    }
  }

  writer.end();
  await new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
  console.log(`\n  Download complete: ${(downloaded / 1024 / 1024).toFixed(1)} MB`);
}

async function ingestViaGeoPackage() {
  console.log("  Mode: GeoPackage Download + ogr2ogr conversion");

  // Check if ogr2ogr is available
  let hasOgr = false;
  try {
    execSync("ogr2ogr --version", { stdio: "pipe" });
    hasOgr = true;
    console.log("  ogr2ogr: available");
  } catch {
    console.log("  ogr2ogr: NOT available, falling back to ArcGIS mode");
    return ingestViaArcGIS();
  }

  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });

  const zipPath = join(DATA_DIR, "mn-parcels-open.zip");
  const gpkgPath = join(DATA_DIR, "mn-parcels-open.gpkg");
  const geojsonPath = join(DATA_DIR, "mn-parcels-open.geojson");

  // Download if not cached
  if (!existsSync(gpkgPath) && !existsSync(geojsonPath)) {
    await downloadFile(GPKG_URL, zipPath);

    // Unzip
    console.log("  Extracting...");
    try {
      execSync(`cd "${DATA_DIR}" && tar -xf "${zipPath}"`, { stdio: "pipe" });
    } catch {
      // Try PowerShell extraction on Windows
      execSync(`powershell -Command "Expand-Archive -Path '${zipPath}' -DestinationPath '${DATA_DIR}' -Force"`, { stdio: "pipe" });
    }

    // Find the .gpkg file
    const files = readdirSync(DATA_DIR).filter(f => f.endsWith(".gpkg"));
    if (files.length === 0) {
      console.error("  No .gpkg file found after extraction");
      return ingestViaArcGIS();
    }
    console.log(`  Found: ${files[0]}`);
  }

  // Find the gpkg file
  const gpkgFiles = readdirSync(DATA_DIR).filter(f => f.endsWith(".gpkg"));
  const actualGpkg = gpkgFiles.length > 0 ? join(DATA_DIR, gpkgFiles[0]) : gpkgPath;

  if (!existsSync(actualGpkg)) {
    console.log("  GeoPackage not found, falling back to ArcGIS mode");
    return ingestViaArcGIS();
  }

  // Convert to GeoJSON (no geometry, just attributes)
  if (!existsSync(geojsonPath)) {
    console.log("  Converting GeoPackage to GeoJSON...");
    execSync(
      `ogr2ogr -f GeoJSON "${geojsonPath}" "${actualGpkg}" -select "*" -lco RFC7946=NO`,
      { stdio: "inherit", timeout: 600_000 }
    );
  }

  // Parse and ingest GeoJSON
  console.log("  Reading GeoJSON...");
  const geojson = JSON.parse(readFileSync(geojsonPath, "utf-8"));
  const features = geojson.features || [];
  console.log(`  Features: ${features.length.toLocaleString()}`);

  let totalInserted = 0;
  let totalSkipped = 0;
  let batch: any[] = [];
  const startTime = Date.now();

  for (let i = 0; i < features.length; i++) {
    const a = features[i].properties;
    const countyName = a.co_name || a.COUNTYNAME || a.COUNTY_NAME || a.CO_NAME || "";
    if (!countyName) { totalSkipped++; continue; }

    try {
      const countyId = await resolveCountyId(countyName);
      const row = mapMNAttributes(a, countyId);
      if (row.parcel_id || row.address) {
        batch.push(row);
      } else {
        totalSkipped++;
      }
    } catch {
      totalSkipped++;
    }

    if (batch.length >= BATCH_SIZE) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
      if (error) {
        const { error: ie } = await db.from("properties").insert(batch);
        if (ie && totalInserted < 100) console.error(`    DB: ${ie.message.substring(0, 80)}`);
        else totalInserted += batch.length;
      } else {
        totalInserted += batch.length;
      }
      batch = [];

      if (totalInserted % 50000 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const pct = ((i / features.length) * 100).toFixed(1);
        const rate = (totalInserted / (parseFloat(elapsed) || 1)).toFixed(0);
        console.log(
          `  [MN] ${pct}% | ${totalInserted.toLocaleString()} inserted | ${rate}/sec | ${elapsed}s`
        );
      }
    }
  }

  // Final batch
  if (batch.length > 0) {
    const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
    if (error) {
      const { error: ie } = await db.from("properties").insert(batch);
      if (!ie) totalInserted += batch.length;
    } else {
      totalInserted += batch.length;
    }
  }

  return totalInserted;
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  const useArcGIS = process.argv.includes("--arcgis");

  console.log(`\nMXRE — Minnesota Parcel Ingestion`);
  console.log(`${"═".repeat(60)}`);
  console.log(`DB: ${process.env.SUPABASE_URL}`);

  const { count } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`Current properties in DB: ${(count || 0).toLocaleString()}`);

  const startTime = Date.now();
  let totalInserted: number;

  if (useArcGIS) {
    totalInserted = await ingestViaArcGIS();
  } else {
    // Try GeoPackage first, fall back to ArcGIS
    try {
      totalInserted = await ingestViaGeoPackage();
    } catch (err: any) {
      console.error(`  GeoPackage mode failed: ${err.message}`);
      console.log("  Falling back to ArcGIS mode...");
      totalInserted = await ingestViaArcGIS();
    }
  }

  const { count: finalCount } = await db.from("properties").select("*", { count: "exact", head: true });
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n${"═".repeat(60)}`);
  console.log(`MN COMPLETE`);
  console.log(`  Inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  Properties in DB: ${(finalCount || 0).toLocaleString()}`);
  console.log(`  Time: ${elapsed}s (${(parseFloat(elapsed) / 60).toFixed(1)} min)`);
  console.log(`${"═".repeat(60)}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
