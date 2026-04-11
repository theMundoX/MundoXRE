#!/usr/bin/env tsx
/**
 * Ingest New York State Tax Parcels from NYS GIS FeatureServer.
 * ~3.7M parcels (36 counties), paginated at 1000 records per request (no geometry).
 *
 * Source: https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1
 *
 * Usage:
 *   npx tsx scripts/ingest-ny-statewide.ts
 *   npx tsx scripts/ingest-ny-statewide.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1";
const OUT_FIELDS = "COUNTY_NAME,PRINT_KEY,PARCEL_ADDR,CITYTOWN_NAME,LOC_ZIP,PRIMARY_OWNER,TOTAL_AV,FULL_MARKET_VAL,LAND_AV,YR_BLT,SQFT_LIVING,SQ_FT,ACRES,PROP_CLASS,BLDG_STYLE_DESC,NBR_BEDROOMS,NBR_FULL_BATHS";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// New York county FIPS codes (state FIPS = 36)
const NY_COUNTY_FIPS: Record<string, string> = {
  "ALBANY": "001", "ALLEGANY": "003", "BRONX": "005", "BROOME": "007",
  "CATTARAUGUS": "009", "CAYUGA": "011", "CHAUTAUQUA": "013", "CHEMUNG": "015",
  "CHENANGO": "017", "CLINTON": "019", "COLUMBIA": "021", "CORTLAND": "023",
  "DELAWARE": "025", "DUTCHESS": "027", "ERIE": "029", "ESSEX": "031",
  "FRANKLIN": "033", "FULTON": "035", "GENESEE": "037", "GREENE": "039",
  "HAMILTON": "041", "HERKIMER": "043", "JEFFERSON": "045", "KINGS": "047",
  "LEWIS": "049", "LIVINGSTON": "051", "MADISON": "053", "MONROE": "055",
  "MONTGOMERY": "057", "NASSAU": "059", "NEW YORK": "061", "NIAGARA": "063",
  "ONEIDA": "065", "ONONDAGA": "067", "ONTARIO": "069", "ORANGE": "071",
  "ORLEANS": "073", "OSWEGO": "075", "OTSEGO": "077", "PUTNAM": "079",
  "QUEENS": "081", "RENSSELAER": "083", "RICHMOND": "085", "ROCKLAND": "087",
  "ST. LAWRENCE": "089", "SAINT LAWRENCE": "089", "ST LAWRENCE": "089",
  "SARATOGA": "091", "SCHENECTADY": "093", "SCHOHARIE": "095", "SCHUYLER": "097",
  "SENECA": "099", "STEUBEN": "101", "SUFFOLK": "103", "SULLIVAN": "105",
  "TIOGA": "107", "TOMPKINS": "109", "ULSTER": "111", "WARREN": "113",
  "WASHINGTON": "115", "WAYNE": "117", "WESTCHESTER": "119", "WYOMING": "121",
  "YATES": "123",
};

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  // Case-insensitive lookup to handle GIS data with UPPER CASE county names
  const { data } = await db.from("counties").select("id, county_name").ilike("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  // Normalize to title case before inserting
  const titleName = name.split(" ").map((w: string) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(" ");
  const countyFips = NY_COUNTY_FIPS[name.toUpperCase()] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: titleName, state_code: state, state_fips: "36", county_fips: countyFips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

// NY Property Class → property type
function classifyNYPropClass(code: string): string {
  if (!code) return "";
  const n = parseInt(code);
  if (isNaN(n)) return "";
  const cat = Math.floor(n / 100);
  switch (cat) {
    case 1: return "residential"; // 100-199
    case 2: return "residential"; // 200-299 (residential)
    case 3: return "commercial";  // 300-399 (vacant commercial)
    case 4: return "commercial";  // 400-499 (commercial)
    case 5: return "recreational";
    case 6: return "agricultural";
    case 7: return "industrial";
    case 8: return "government";
    case 9: return "exempt";
    default: return "";
  }
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
  console.log("MXRE — Ingest New York State Tax Parcels (NYS GIS 2024-2025)");
  console.log("Source:", SERVICE_URL);
  console.log();

  const countResp = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const countyName = r.COUNTY_NAME;
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "NY");
      } catch {
        totalErrors++;
        continue;
      }

      const sqft = r.SQFT_LIVING && r.SQFT_LIVING > 0 ? r.SQFT_LIVING : null;
      const lotSqft = r.SQ_FT && r.SQ_FT > 0 ? r.SQ_FT :
        (r.ACRES && r.ACRES > 0 ? Math.round(r.ACRES * 43560) : null);

      rows.push({
        county_id: countyId,
        parcel_id: r.PRINT_KEY || "",
        address: r.PARCEL_ADDR || "",
        city: r.CITYTOWN_NAME || "",
        state_code: "NY",
        zip: r.LOC_ZIP || "",
        owner_name: r.PRIMARY_OWNER || "",
        assessed_value: r.TOTAL_AV && r.TOTAL_AV > 0 ? r.TOTAL_AV : null,
        market_value: r.FULL_MARKET_VAL && r.FULL_MARKET_VAL > 0 ? r.FULL_MARKET_VAL : null,
        land_value: r.LAND_AV && r.LAND_AV > 0 ? r.LAND_AV : null,
        year_built: r.YR_BLT && r.YR_BLT > 1700 && r.YR_BLT < 2030 ? r.YR_BLT : null,
        total_sqft: sqft,
        land_sqft: lotSqft,
        property_type: classifyNYPropClass(r.PROP_CLASS),
        construction_class: r.BLDG_STYLE_DESC || null,
        source: "nys-gis-parcels-2024",
      });
    }

    // Dedup within page by (county_id, parcel_id) to avoid upsert conflict errors
    const seen = new Map<string, any>();
    for (const row of rows) {
      const key = `${row.county_id}|${row.parcel_id}`;
      seen.set(key, row);
    }
    const dedupedRows = Array.from(seen.values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      if (batch.length === 0) continue;
      let lastErr: any;
      let ok = false;
      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
          const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
          if (error) { lastErr = error; break; }
          ok = true; break;
        } catch (err: any) {
          lastErr = err;
          if (attempt < 5) await new Promise(r => setTimeout(r, 5000 * attempt));
        }
      }
      if (!ok) {
        console.error(`  DB error at offset ${offset}+${i}: ${lastErr?.message}`);
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
