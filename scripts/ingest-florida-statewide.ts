#!/usr/bin/env tsx
/**
 * Ingest Florida Statewide Parcels from FDOR Cadastral ArcGIS FeatureServer.
 * ~10.8M parcels, paginated at 2000 records per request (no geometry).
 *
 * Source: https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/Florida_Statewide_Cadastral/FeatureServer/0
 *
 * Usage:
 *   npx tsx scripts/ingest-florida-statewide.ts
 *   npx tsx scripts/ingest-florida-statewide.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── FL County Number → FIPS (CO_NO 1-67, odd sequential: 001,003,...,133) ─
const FL_COUNTY_FIPS: Record<number, string> = Object.fromEntries(
  Array.from({ length: 67 }, (_, i) => [i + 1, String((i + 1) * 2 - 1).padStart(3, "0")])
);

// ─── FL County Number → Name (DOR CO_NO, 1-67 alphabetical) ────────
const FL_COUNTIES: Record<number, string> = {
  1: "Alachua", 2: "Baker", 3: "Bay", 4: "Bradford", 5: "Brevard",
  6: "Broward", 7: "Calhoun", 8: "Charlotte", 9: "Citrus", 10: "Clay",
  11: "Collier", 12: "Columbia", 13: "Miami-Dade", 14: "DeSoto", 15: "Dixie",
  16: "Duval", 17: "Escambia", 18: "Flagler", 19: "Franklin", 20: "Gadsden",
  21: "Gilchrist", 22: "Glades", 23: "Gulf", 24: "Hamilton", 25: "Hardee",
  26: "Hendry", 27: "Hernando", 28: "Highlands", 29: "Hillsborough", 30: "Holmes",
  31: "Indian River", 32: "Jackson", 33: "Jefferson", 34: "Lafayette", 35: "Lake",
  36: "Lee", 37: "Leon", 38: "Levy", 39: "Liberty", 40: "Madison",
  41: "Manatee", 42: "Marion", 43: "Martin", 44: "Monroe", 45: "Nassau",
  46: "Okaloosa", 47: "Okeechobee", 48: "Orange", 49: "Osceola", 50: "Palm Beach",
  51: "Pasco", 52: "Pinellas", 53: "Polk", 54: "Putnam", 55: "St. Johns",
  56: "St. Lucie", 57: "Santa Rosa", 58: "Sarasota", 59: "Seminole", 60: "Sumter",
  61: "Suwannee", 62: "Taylor", 63: "Union", 64: "Volusia", 65: "Wakulla",
  66: "Walton", 67: "Washington",
};

const SERVICE_URL = "https://services9.arcgis.com/Gh9awoU677aKree0/arcgis/rest/services/Florida_Statewide_Cadastral/FeatureServer/0";
const OUT_FIELDS = "CO_NO,PARCEL_ID,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,OWN_NAME,JV,ACT_YR_BLT,TOT_LVG_AR,NO_RES_UNT,DOR_UC,LND_VAL,SALE_PRC1,SALE_YR1,SALE_MO1,NO_BULDNG,LND_SQFOOT,IMP_QUAL";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

// County ID cache
const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, state: string, countyFips: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "12", county_fips: countyFips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

// DOR Use Code → property type
function classifyDorUC(uc: string): string {
  if (!uc) return "";
  const n = parseInt(uc);
  if (n >= 0 && n <= 9) return "residential";
  if (n >= 10 && n <= 19) return "commercial";
  if (n >= 20 && n <= 29) return "industrial";
  if (n >= 30 && n <= 39) return "agricultural";
  if (n >= 40 && n <= 49) return "institutional";
  if (n >= 50 && n <= 69) return "residential"; // misc residential
  if (n >= 70 && n <= 79) return "government";
  if (n >= 80 && n <= 89) return "commercial"; // misc commercial
  if (n >= 90 && n <= 99) return "exempt";
  return "";
}

async function fetchPage(offset: number): Promise<any[]> {
  const url = `${SERVICE_URL}/query?where=1%3D1&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) ?? [];
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(2000 * Math.pow(2, attempt - 1), 30000);
      console.log(`  Retry ${attempt}/${MAX_RETRIES} after ${delay}ms: ${err.message}`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Florida Statewide Parcels (FDOR Cadastral 2025)");
  console.log("Source:", SERVICE_URL);
  console.log();

  // Get total count
  const countResp = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  // Parse resume offset
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const pageStart = Date.now();
    const records = await fetchPage(offset);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    // Build property rows
    const rows: any[] = [];
    for (const r of records) {
      const coNo = r.CO_NO;
      const countyName = FL_COUNTIES[coNo] || `County ${coNo}`;
      const countyFips = FL_COUNTY_FIPS[coNo] || String(coNo).padStart(3, "0");
      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "FL", countyFips);
      } catch (err: any) {
        if (totalErrors < 5) console.error(`  getOrCreateCounty error (${countyName}): ${err?.message || JSON.stringify(err)}`);
        totalErrors++;
        continue;
      }

      const zip = r.PHY_ZIPCD ? String(Math.round(r.PHY_ZIPCD)).padStart(5, "0") : "";
      const yearBuilt = r.ACT_YR_BLT && r.ACT_YR_BLT > 1700 && r.ACT_YR_BLT < 2030 ? r.ACT_YR_BLT : null;

      let saleDate: string | null = null;
      if (r.SALE_YR1 && r.SALE_YR1 > 1900) {
        const mo = r.SALE_MO1 ? String(r.SALE_MO1).padStart(2, "0") : "01";
        saleDate = `${Math.round(r.SALE_YR1)}-${mo}-01`;
      }

      rows.push({
        county_id: countyId,
        parcel_id: r.PARCEL_ID || "",
        address: r.PHY_ADDR1 || "",
        city: r.PHY_CITY || "",
        state_code: "FL",
        zip,
        owner_name: r.OWN_NAME || "",
        assessed_value: r.JV && r.JV > 0 ? r.JV : null,
        year_built: yearBuilt,
        total_sqft: r.TOT_LVG_AR && r.TOT_LVG_AR > 0 ? r.TOT_LVG_AR : null,
        total_units: r.NO_RES_UNT && r.NO_RES_UNT > 0 ? r.NO_RES_UNT : null,
        property_type: classifyDorUC(r.DOR_UC),
        land_value: r.LND_VAL && r.LND_VAL > 0 ? r.LND_VAL : null,
        land_sqft: r.LND_SQFOOT && r.LND_SQFOOT > 0 ? r.LND_SQFOOT : null,
        last_sale_price: r.SALE_PRC1 && r.SALE_PRC1 > 0 ? r.SALE_PRC1 : null,
        last_sale_date: saleDate,
        total_buildings: r.NO_BULDNG && r.NO_BULDNG > 0 ? r.NO_BULDNG : null,
        improvement_quality: r.IMP_QUAL || null,
        source: "fdor-cadastral-2025",
      });
    }

    // Dedup rows within this page by (county_id, parcel_id) — FL condos share parcel_id
    const seen = new Map<string, any>();
    for (const row of rows) {
      const key = `${row.county_id}|${row.parcel_id}`;
      seen.set(key, row); // Last occurrence wins
    }
    const dedupedRows = Array.from(seen.values());
    totalErrors += rows.length - dedupedRows.length; // Count as "skipped" in errors

    // Insert in sub-batches
    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
      if (error) {
        console.error(`  DB error at offset ${offset}+${i}: ${error.message}`);
        totalErrors += batch.length;
      } else {
        totalInserted += batch.length;
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000);
    const pct = ((offset + records.length) / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsed}s] offset=${offset} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min`);

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone! Inserted ${totalInserted.toLocaleString()} records in ${totalElapsed} minutes. Errors: ${totalErrors}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
