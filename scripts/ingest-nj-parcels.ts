#!/usr/bin/env tsx
/**
 * MXRE — Ingest NJ MOD-IV Statewide Parcel Data
 *
 * Source: NJ Office of GIS — Parcels_Composite_NJ_WM (ArcGIS FeatureServer)
 * https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/ArcGIS/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0
 *
 * ~3.47M parcels statewide. No geometry fetched (attribute data only).
 *
 * Key fields:
 *   PAMS_PIN      — statewide parcel ID (muni_block_lot.qual format)
 *   PROP_CLASS    — NJ property class code (1=res, 2=res>4fam, 4A/4B=commercial, etc.)
 *   OWNER_NAME    — owner (may be blank in public layer)
 *   PROP_LOC      — property street address
 *   CITY_STATE    — "CITY, NJ" or "CITY NJ"
 *   ZIP5/ZIP_CODE — ZIP code
 *   LAND_VAL      — assessed land value
 *   IMPRVT_VAL    — assessed improvement value
 *   NET_VALUE     — total assessed value (land + improvements)
 *   YR_CONSTR     — year built
 *   SALE_PRICE    — last recorded sale price
 *   DEED_DATE     — last deed date (YYMMDD format)
 *   COUNTY        — county name (uppercase, e.g. "CUMBERLAND")
 *   MUN_NAME      — municipality name
 *
 * Note: NJ assesses at 100% of market value by statute, though many
 * municipalities lag. NET_VALUE is stored as-is without adjustment.
 *
 * Usage:
 *   npx tsx scripts/ingest-nj-parcels.ts
 *   npx tsx scripts/ingest-nj-parcels.ts --offset=500000   # resume
 *   npx tsx scripts/ingest-nj-parcels.ts --county=ESSEX    # single county
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL =
  "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/ArcGIS/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0";

const OUT_FIELDS = [
  "PAMS_PIN",
  "PROP_CLASS",
  "OWNER_NAME",
  "PROP_LOC",
  "CITY_STATE",
  "ZIP5",
  "ZIP_CODE",
  "LAND_VAL",
  "IMPRVT_VAL",
  "NET_VALUE",
  "YR_CONSTR",
  "SALE_PRICE",
  "DEED_DATE",
  "COUNTY",
  "MUN_NAME",
  "BLDG_DESC",
  "DWELL",
].join(",");

const PAGE_SIZE = 2000;   // service maxRecordCount
const BATCH_SIZE = 500;   // upsert batch to Supabase
const MAX_RETRIES = 5;

// ─── NJ County FIPS (state FIPS = 34) ───────────────────────────────────────

const NJ_COUNTY_FIPS: Record<string, string> = {
  "ATLANTIC": "001",
  "BERGEN": "003",
  "BURLINGTON": "005",
  "CAMDEN": "007",
  "CAPE MAY": "009",
  "CUMBERLAND": "011",
  "ESSEX": "013",
  "GLOUCESTER": "015",
  "HUDSON": "017",
  "HUNTERDON": "019",
  "MERCER": "021",
  "MIDDLESEX": "023",
  "MONMOUTH": "025",
  "MORRIS": "027",
  "OCEAN": "029",
  "PASSAIC": "031",
  "SALEM": "033",
  "SOMERSET": "035",
  "SUSSEX": "037",
  "UNION": "039",
  "WARREN": "041",
};

// Title-case county name for DB storage
function titleCase(s: string): string {
  return s
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

// ─── County cache ────────────────────────────────────────────────────────────

const countyCache = new Map<string, number>();

async function getOrCreateCounty(countyNameRaw: string): Promise<number | null> {
  const upper = countyNameRaw.toUpperCase().trim();
  if (countyCache.has(upper)) return countyCache.get(upper)!;

  const countyName = titleCase(upper);
  const fips = NJ_COUNTY_FIPS[upper];
  if (!fips) {
    console.warn(`  Unknown county: "${countyNameRaw}", skipping`);
    return null;
  }

  // Try existing
  const { data } = await db
    .from("counties")
    .select("id")
    .eq("county_name", countyName)
    .eq("state_code", "NJ")
    .single();

  if (data) {
    countyCache.set(upper, data.id);
    return data.id;
  }

  // Create
  const { data: created, error } = await db
    .from("counties")
    .upsert(
      { county_name: countyName, state_code: "NJ", state_fips: "34", county_fips: fips, active: true },
      { onConflict: "state_fips,county_fips" },
    )
    .select("id")
    .single();

  if (error || !created) {
    console.error(`  Failed to create county ${countyName}: ${error?.message}`);
    return null;
  }

  countyCache.set(upper, created.id);
  return created.id;
}

// ─── Field parsers ───────────────────────────────────────────────────────────

/**
 * DEED_DATE is YYMMDD (same format as SR1A).
 * E.g. "211115" → 2021-11-15, "010716" → 2001-07-16.
 */
function parseDeedDate(raw: any): string | null {
  const s = String(raw ?? "").trim();
  if (!s || s.length < 6 || s === "000000") return null;
  const yy = parseInt(s.substring(0, 2), 10);
  const mm = s.substring(2, 4);
  const dd = s.substring(4, 6);
  const year = yy < 50 ? 2000 + yy : 1900 + yy;
  const month = parseInt(mm, 10);
  const day = parseInt(dd, 10);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return `${year}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

/**
 * Parse city from CITY_STATE field ("PORT NORRIS, NJ" or "TRENTON NJ").
 * Strips the state suffix.
 */
function parseCity(cityState: any): string {
  const s = String(cityState ?? "").trim();
  // Remove trailing ", NJ" or " NJ"
  return s.replace(/,?\s*NJ\s*$/i, "").trim();
}

/**
 * Map NJ PROP_CLASS codes to MXRE property_type enum values.
 *
 * NJ property classes:
 *  1     — Vacant land
 *  2     — Residential (1–4 family)
 *  3A    — Farm (regular)
 *  3B    — Farm (qualified)
 *  4A    — Commercial
 *  4B    — Industrial
 *  4C    — Apartment (5+ units)
 *  5A    — Railroad (Class I)
 *  5B    — Railroad (Class II)
 *  15    — Exempt (various sub-codes: 15A church, 15B school, 15C govt, etc.)
 */
function classifyPropClass(code: any): string {
  const c = String(code ?? "").toUpperCase().trim();
  if (!c) return "other";
  if (c === "1") return "vacant";
  if (c === "2") return "residential";
  if (c.startsWith("3")) return "agricultural";
  if (c === "4A") return "commercial";
  if (c === "4B") return "industrial";
  if (c === "4C") return "residential"; // apartment (5+ units — still residential asset class)
  if (c.startsWith("5")) return "other"; // railroad utility
  if (c.startsWith("15")) return "exempt";
  return "other";
}

// ─── Fetch ───────────────────────────────────────────────────────────────────

async function fetchPage(offset: number, whereClause = "1=1"): Promise<any[]> {
  const params = new URLSearchParams({
    where: whereClause,
    outFields: OUT_FIELDS,
    returnGeometry: "false",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });
  const url = `${SERVICE_URL}/query?${params}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(90_000) });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json: any = await resp.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) ?? [];
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(2000 * Math.pow(2, attempt - 1), 30_000);
      console.log(`  Retry ${attempt}/${MAX_RETRIES} after ${delay}ms: ${err.message}`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  return [];
}

async function getTotalCount(whereClause = "1=1"): Promise<number> {
  const params = new URLSearchParams({ where: whereClause, returnCountOnly: "true", f: "json" });
  const resp = await fetch(`${SERVICE_URL}/query?${params}`, { signal: AbortSignal.timeout(30_000) });
  const json: any = await resp.json();
  return json.count ?? 0;
}

// ─── DB upsert ───────────────────────────────────────────────────────────────

async function upsertBatch(rows: any[]): Promise<{ ok: number; err: number }> {
  let lastErr: any;
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      const { error } = await db
        .from("properties")
        .upsert(rows, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) { lastErr = error; break; }
      return { ok: rows.length, err: 0 };
    } catch (err: any) {
      lastErr = err;
      if (attempt < 5) await new Promise((r) => setTimeout(r, 5000 * attempt));
    }
  }
  console.error(`  DB upsert error: ${lastErr?.message}`);
  return { ok: 0, err: rows.length };
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Ingest NJ MOD-IV Statewide Parcels");
  console.log("Source:", SERVICE_URL);
  console.log();

  // CLI args
  const args = process.argv.slice(2);
  const offsetArg = args.find((a) => a.startsWith("--offset="));
  const countyArg = args.find((a) => a.startsWith("--county="));

  const countyFilter = countyArg ? countyArg.split("=")[1].toUpperCase() : null;
  const whereClause = countyFilter ? `COUNTY='${countyFilter}'` : "1=1";

  if (countyFilter) {
    console.log(`County filter: ${countyFilter}`);
    if (!NJ_COUNTY_FIPS[countyFilter]) {
      console.error(`Unknown county "${countyFilter}". Valid counties: ${Object.keys(NJ_COUNTY_FIPS).join(", ")}`);
      process.exit(1);
    }
  }

  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  const totalCount = await getTotalCount(whereClause);
  console.log(`Total parcels: ${totalCount.toLocaleString()}`);
  if (offset > 0) console.log(`Resuming from offset: ${offset.toLocaleString()}`);
  console.log();

  let totalInserted = 0;
  let totalErrors = 0;
  let totalSkipped = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await fetchPage(offset, whereClause);
    } catch (err: any) {
      console.error(`Fatal fetch error at offset ${offset}: ${err.message}`);
      break;
    }

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, stopping.`);
      break;
    }

    // Build rows
    const rows: any[] = [];
    for (const r of records) {
      const countyRaw = String(r.COUNTY ?? "").trim();
      if (!countyRaw) { totalSkipped++; continue; }

      const countyId = await getOrCreateCounty(countyRaw);
      if (!countyId) { totalSkipped++; continue; }

      const pamsPin = String(r.PAMS_PIN ?? "").trim();
      if (!pamsPin) { totalSkipped++; continue; }

      const address = String(r.PROP_LOC ?? "").trim();
      if (!address) { totalSkipped++; continue; }

      const city = parseCity(r.CITY_STATE);
      const zip = String(r.ZIP5 || r.ZIP_CODE || "").trim().substring(0, 5);

      const landVal = r.LAND_VAL && r.LAND_VAL > 0 ? r.LAND_VAL : null;
      const netVal = r.NET_VALUE && r.NET_VALUE > 0 ? r.NET_VALUE : null;
      const salePrice = r.SALE_PRICE && r.SALE_PRICE > 0 ? r.SALE_PRICE : null;
      const saleDate = parseDeedDate(r.DEED_DATE);
      const yearBuilt = r.YR_CONSTR && r.YR_CONSTR > 0 && r.YR_CONSTR < 2100 ? r.YR_CONSTR : null;
      const ownerName = String(r.OWNER_NAME ?? "").trim() || null;
      const dwellUnits = r.DWELL && r.DWELL > 0 ? r.DWELL : null;

      rows.push({
        county_id: countyId,
        parcel_id: pamsPin,
        address,
        city,
        state_code: "NJ",
        zip,
        owner_name: ownerName,
        land_value: landVal,
        assessed_value: netVal,
        last_sale_price: salePrice,
        last_sale_date: saleDate,
        year_built: yearBuilt,
        property_type: classifyPropClass(r.PROP_CLASS),
        total_units: dwellUnits,
        source: "nj-mod4-parcels",
      });
    }

    // Dedup within page by county_id + parcel_id
    const seen = new Map<string, any>();
    for (const row of rows) {
      seen.set(`${row.county_id}|${row.parcel_id}`, row);
    }
    const dedupedRows = Array.from(seen.values());

    // Upsert in batches
    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      const { ok, err } = await upsertBatch(batch);
      totalInserted += ok;
      totalErrors += err;
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / Math.max(1, (Date.now() - startTime) / 1000);
    const done = offset + records.length;
    const pct = ((done / totalCount) * 100).toFixed(1);
    const eta = rate > 0 ? Math.round((totalCount - done) / rate / 60) : "?";
    process.stdout.write(
      `[${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | ` +
      `inserted=${totalInserted.toLocaleString()} | skipped=${totalSkipped} | ` +
      `errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min\r`,
    );

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log();
  console.log(`\nDone!`);
  console.log(`  Inserted/updated: ${totalInserted.toLocaleString()}`);
  console.log(`  Skipped:          ${totalSkipped.toLocaleString()}`);
  console.log(`  Errors:           ${totalErrors.toLocaleString()}`);
  console.log(`  Elapsed:          ${totalElapsed} minutes`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
