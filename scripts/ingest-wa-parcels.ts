#!/usr/bin/env tsx
/**
 * Ingest Washington State Statewide Parcels from WA Geospatial FeatureServer.
 * Paginated at 2000 records per request (no geometry).
 *
 * Source: https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/Current_Parcels/FeatureServer/0
 *
 * Fields: FIPS_NR, COUNTY_NM, PARCEL_ID_NR, ORIG_PARCEL_ID, SITUS_ADDRESS, SUB_ADDRESS,
 *         SITUS_CITY_NM, SITUS_ZIP_NR, LANDUSE_CD, VALUE_LAND, VALUE_BLDG, DATA_LINK
 *
 * Usage:
 *   npx tsx scripts/ingest-wa-parcels.ts
 *   npx tsx scripts/ingest-wa-parcels.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/Current_Parcels/FeatureServer/0";
const OUT_FIELDS = "FIPS_NR,COUNTY_NM,PARCEL_ID_NR,ORIG_PARCEL_ID,SITUS_ADDRESS,SUB_ADDRESS,SITUS_CITY_NM,SITUS_ZIP_NR,LANDUSE_CD,VALUE_LAND,VALUE_BLDG";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// Washington county FIPS codes (state FIPS = 53)
const WA_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ASOTIN": "003", "BENTON": "005", "CHELAN": "007",
  "CLALLAM": "009", "CLARK": "011", "COLUMBIA": "013", "COWLITZ": "015",
  "DOUGLAS": "017", "FERRY": "019", "FRANKLIN": "021", "GARFIELD": "023",
  "GRANT": "025", "GRAYS HARBOR": "027", "ISLAND": "029", "JEFFERSON": "031",
  "KING": "033", "KITSAP": "035", "KITTITAS": "037", "KLICKITAT": "039",
  "LEWIS": "041", "LINCOLN": "043", "MASON": "045", "OKANOGAN": "047",
  "PACIFIC": "049", "PEND OREILLE": "051", "PIERCE": "053", "SAN JUAN": "055",
  "SKAGIT": "057", "SKAMANIA": "059", "SNOHOMISH": "061", "SPOKANE": "063",
  "STEVENS": "065", "THURSTON": "067", "WAHKIAKUM": "069", "WALLA WALLA": "071",
  "WHATCOM": "073", "WHITMAN": "075", "YAKIMA": "077",
};

// Reverse mapping: 3-digit FIPS → county name (API returns numeric FIPS in COUNTY_NM/FIPS_NR)
const WA_FIPS_TO_NAME: Record<string, string> = {};
for (const [name, fips] of Object.entries(WA_COUNTY_FIPS)) {
  WA_FIPS_TO_NAME[fips] = name.split(" ").map(w => w.charAt(0) + w.slice(1).toLowerCase()).join(" ");
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

async function getOrCreateCounty(name: string, state: string, countyFips?: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const fips = countyFips || WA_COUNTY_FIPS[name.toUpperCase()] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "53", county_fips: fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

function classifyLandUse(code: any): string {
  const c = String(code ?? "").toUpperCase().trim();
  // Washington state standard land use codes
  if (c.startsWith("1") || c.startsWith("R")) return "residential";
  if (c.startsWith("2") || c.startsWith("C")) return "commercial";
  if (c.startsWith("3") || c.startsWith("I")) return "industrial";
  if (c.startsWith("8") || c.startsWith("A") || c.startsWith("F")) return "agricultural";
  if (c.startsWith("9") || c.startsWith("V")) return "vacant";
  if (c.startsWith("6") || c.startsWith("7") || c.startsWith("E")) return "exempt";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const url = `${SERVICE_URL}/query?where=1%3D1&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

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
  console.log("MXRE — Ingest Washington State Statewide Parcels");
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
      // COUNTY_NM and FIPS_NR both contain the 3-digit county FIPS code (e.g. "057")
      const fipsRaw = r.FIPS_NR ? String(r.FIPS_NR) : (r.COUNTY_NM ? String(r.COUNTY_NM) : "");
      const fips = fipsRaw ? fipsRaw.padStart(3, "0").slice(-3) : "";
      const countyName = WA_FIPS_TO_NAME[fips] || "";
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "WA", fips);
      } catch {
        totalErrors++;
        continue;
      }

      const address = [r.SITUS_ADDRESS, r.SUB_ADDRESS].filter(Boolean).join(" ").trim();

      rows.push({
        county_id: countyId,
        parcel_id: r.PARCEL_ID_NR || r.ORIG_PARCEL_ID || "",
        address: address,
        city: (r.SITUS_CITY_NM || "").trim(),
        state_code: "WA",
        zip: r.SITUS_ZIP_NR ? String(r.SITUS_ZIP_NR).trim() : "",
        land_value: r.VALUE_LAND && r.VALUE_LAND > 0 ? r.VALUE_LAND : null,
        assessed_value: r.VALUE_BLDG && r.VALUE_BLDG > 0
          ? (r.VALUE_LAND || 0) + r.VALUE_BLDG
          : (r.VALUE_LAND && r.VALUE_LAND > 0 ? r.VALUE_LAND : null),
        property_type: classifyLandUse(r.LANDUSE_CD),
        source: "wa-geoservices-parcels",
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
