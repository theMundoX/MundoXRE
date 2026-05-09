#!/usr/bin/env tsx
/**
 * Ingest Indiana Statewide Parcels from Indiana GIS FeatureServer.
 * Paginated at 2000 records per request (no geometry).
 *
 * Source: https://gisdata.in.gov/server/rest/services/Hosted/Parcel_Boundaries_of_Indiana_Current/FeatureServer/0
 *
 * Usage:
 *   npx tsx scripts/ingest-in-parcels.ts
 *   npx tsx scripts/ingest-in-parcels.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gisdata.in.gov/server/rest/services/Hosted/Parcel_Boundaries_of_Indiana_Current/FeatureServer/0";
const OUT_FIELDS = "state_parcel_id,parcel_id,prop_add,prop_city,prop_state,prop_zip,county_fips,county_id,dlgf_prop_address,dlgf_prop_address_city,dlgf_prop_address_state,dlgf_prop_address_zip,dlgf_prop_class_code,tax_county,tax_township,tax_city,tax_school,tax_library,tax_special";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;
const arg = (name: string) => process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const COUNTY_ARG = arg("county")?.trim().toUpperCase();
const COUNTY_FIPS_ARG = arg("county-fips")?.trim();

const countyCache = new Map<string, number>();

// Indiana county FIPS codes (state FIPS = 18)
const IN_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ALLEN": "003", "BARTHOLOMEW": "005", "BENTON": "007",
  "BLACKFORD": "009", "BOONE": "011", "BROWN": "013", "CARROLL": "015",
  "CASS": "017", "CLARK": "019", "CLAY": "021", "CLINTON": "023",
  "CRAWFORD": "025", "DAVIESS": "027", "DEARBORN": "029", "DECATUR": "031",
  "DEKALB": "033", "DELAWARE": "035", "DUBOIS": "037", "ELKHART": "039",
  "FAYETTE": "041", "FLOYD": "043", "FOUNTAIN": "045", "FRANKLIN": "047",
  "FULTON": "049", "GIBSON": "051", "GRANT": "053", "GREENE": "055",
  "HAMILTON": "057", "HANCOCK": "059", "HARRISON": "061", "HENDRICKS": "063",
  "HENRY": "065", "HOWARD": "067", "HUNTINGTON": "069", "JACKSON": "071",
  "JASPER": "073", "JAY": "075", "JEFFERSON": "077", "JENNINGS": "079",
  "JOHNSON": "081", "KNOX": "083", "KOSCIUSKO": "085", "LAGRANGE": "087",
  "LAKE": "089", "LAPORTE": "091", "LAWRENCE": "093", "MADISON": "095",
  "MARION": "097", "MARSHALL": "099", "MARTIN": "101", "MIAMI": "103",
  "MONROE": "105", "MONTGOMERY": "107", "MORGAN": "109", "NEWTON": "111",
  "NOBLE": "113", "OHIO": "115", "ORANGE": "117", "OWEN": "119",
  "PARKE": "121", "PERRY": "123", "PIKE": "125", "PORTER": "127",
  "POSEY": "129", "PULASKI": "131", "PUTNAM": "133", "RANDOLPH": "135",
  "RIPLEY": "137", "RUSH": "139", "SCOTT": "143", "SHELBY": "145",
  "SPENCER": "147", "ST. JOSEPH": "141", "STARKE": "149", "STEUBEN": "151",
  "SULLIVAN": "153", "SWITZERLAND": "155", "TIPPECANOE": "157", "TIPTON": "159",
  "UNION": "161", "VANDERBURGH": "163", "VERMILLION": "165", "VIGO": "167",
  "WABASH": "169", "WARREN": "171", "WARRICK": "173", "WASHINGTON": "175",
  "WAYNE": "177", "WELLS": "179", "WHITE": "181", "WHITLEY": "183",
};

// Indiana county FIPS number to name mapping
const IN_FIPS_TO_NAME: Record<string, string> = {};
for (const [name, fips] of Object.entries(IN_COUNTY_FIPS)) {
  IN_FIPS_TO_NAME[fips] = name;
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

async function getOrCreateCounty(name: string, state: string, countyFips?: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const fips = countyFips || IN_COUNTY_FIPS[name.toUpperCase()] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "18", county_fips: fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

function classifyPropClass(code: string): string {
  const c = (code || "").trim();
  // Indiana DLGF property class codes
  if (c.startsWith("1") || c.startsWith("5")) return "residential";
  if (c.startsWith("2")) return "commercial";
  if (c.startsWith("3")) return "industrial";
  if (c.startsWith("4")) return "agricultural";
  if (c.startsWith("6") || c.startsWith("7")) return "exempt";
  return "other";
}

function requestedWhere(): string {
  const fips = COUNTY_FIPS_ARG ?? (COUNTY_ARG ? IN_COUNTY_FIPS[COUNTY_ARG] : undefined);
  if (!fips) return "1=1";
  const fullFips = fips.length === 3 ? `18${fips}` : fips;
  return `county_fips='${fullFips.replace(/'/g, "''")}'`;
}

async function fetchPage(offset: number, where: string): Promise<any[]> {
  const url = `${SERVICE_URL}/query?where=${encodeURIComponent(where)}&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(60000) });
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
  console.log("MXRE — Ingest Indiana Statewide Parcels (IN Data Harvest)");
  console.log("Source:", SERVICE_URL);
  console.log();

  const where = requestedWhere();
  console.log(`Filter: ${where}`);
  const countResp = await fetch(`${SERVICE_URL}/query?where=${encodeURIComponent(where)}&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset, where);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      // Derive county name from county_fips (API returns 5-digit full FIPS like "18111")
      const cFips = r.county_fips ? String(r.county_fips).slice(-3).padStart(3, "0") : "";
      const countyName = IN_FIPS_TO_NAME[cFips];
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(titleCase(countyName), "IN", cFips);
      } catch {
        totalErrors++;
        continue;
      }

      const address = r.prop_add || r.dlgf_prop_address || "";
      const city = r.prop_city || r.dlgf_prop_address_city || "";
      const zip = r.prop_zip || r.dlgf_prop_address_zip || "";
      const parcelId = r.state_parcel_id || r.parcel_id || "";
      if (!String(parcelId).trim()) { totalErrors++; continue; }

      rows.push({
        county_id: countyId,
        parcel_id: parcelId,
        address: address.trim(),
        city: city.trim(),
        state_code: "IN",
        zip: String(zip).trim(),
        property_type: classifyPropClass(r.dlgf_prop_class_code),
        source: "in-data-harvest-parcels",
      });
    }

    // Dedup within page
    const seen = new Map<string, any>();
    for (const row of rows) {
      seen.set(`${row.county_id}|${row.parcel_id}`, row);
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
