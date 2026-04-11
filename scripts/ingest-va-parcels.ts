#!/usr/bin/env tsx
/**
 * Ingest Virginia Statewide Parcels from VDEM FeatureServer.
 * Paginated at 1000 records per request (no geometry).
 *
 * Source: https://gismaps.vdem.virginia.gov/arcgis/rest/services/VA_Base_Layers/VA_Parcels/FeatureServer/0
 *
 * Note: This service has limited attribute fields (PARCELID, PTM_ID, LASTUPDATE).
 * We ingest parcel IDs and geometry metadata; richer data must come from county-level sources.
 *
 * Usage:
 *   npx tsx scripts/ingest-va-parcels.ts
 *   npx tsx scripts/ingest-va-parcels.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gismaps.vdem.virginia.gov/arcgis/rest/services/VA_Base_Layers/VA_Parcels/FeatureServer/0";
const OUT_FIELDS = "*";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// Virginia FIPS codes (state FIPS = 51) — independent cities and counties
// The FIPS code is embedded in the parcel IDs in many VA datasets
const VA_FIPS_TO_NAME: Record<string, string> = {
  "001": "Accomack", "003": "Albemarle", "005": "Alleghany", "007": "Amelia",
  "009": "Amherst", "011": "Appomattox", "013": "Arlington", "015": "Augusta",
  "017": "Bath", "019": "Bedford", "021": "Bland", "023": "Botetourt",
  "025": "Brunswick", "027": "Buchanan", "029": "Buckingham", "031": "Campbell",
  "033": "Caroline", "035": "Carroll", "036": "Charles City", "037": "Charlotte",
  "041": "Chesterfield", "043": "Clarke", "045": "Craig", "047": "Culpeper",
  "049": "Cumberland", "051": "Dickenson", "053": "Dinwiddie", "057": "Essex",
  "059": "Fairfax", "061": "Fauquier", "063": "Floyd", "065": "Fluvanna",
  "067": "Franklin", "069": "Frederick", "071": "Giles", "073": "Gloucester",
  "075": "Goochland", "077": "Grayson", "079": "Greene", "081": "Greensville",
  "083": "Halifax", "085": "Hanover", "087": "Henrico", "089": "Henry",
  "091": "Highland", "093": "Isle Of Wight", "095": "James City", "097": "King And Queen",
  "099": "King George", "101": "King William", "103": "Lancaster", "105": "Lee",
  "107": "Loudoun", "109": "Louisa", "111": "Lunenburg", "113": "Madison",
  "115": "Mathews", "117": "Mecklenburg", "119": "Middlesex", "121": "Montgomery",
  "125": "Nelson", "127": "New Kent", "131": "Northampton", "133": "Northumberland",
  "135": "Nottoway", "137": "Orange", "139": "Page", "141": "Patrick",
  "143": "Pittsylvania", "145": "Powhatan", "147": "Prince Edward",
  "149": "Prince George", "153": "Prince William", "155": "Pulaski",
  "157": "Rappahannock", "159": "Richmond", "161": "Roanoke", "163": "Rockbridge",
  "165": "Rockingham", "167": "Russell", "169": "Scott", "171": "Shenandoah",
  "173": "Smyth", "175": "Southampton", "177": "Spotsylvania", "179": "Stafford",
  "181": "Surry", "183": "Sussex", "185": "Tazewell", "187": "Warren",
  "191": "Washington", "193": "Westmoreland", "195": "Wise", "197": "Wythe",
  "199": "York",
  // Independent cities
  "510": "Alexandria", "520": "Bristol", "530": "Buena Vista", "540": "Charlottesville",
  "550": "Chesapeake", "570": "Colonial Heights", "580": "Covington", "590": "Danville",
  "595": "Emporia", "600": "Fairfax City", "610": "Falls Church", "620": "Franklin City",
  "630": "Fredericksburg", "640": "Galax", "650": "Hampton", "660": "Harrisonburg",
  "670": "Hopewell", "678": "Lexington", "680": "Lynchburg", "683": "Manassas",
  "685": "Manassas Park", "690": "Martinsville", "700": "Newport News",
  "710": "Norfolk", "720": "Norton", "730": "Petersburg", "735": "Poquoson",
  "740": "Portsmouth", "750": "Radford", "760": "Richmond City", "770": "Roanoke City",
  "775": "Salem", "790": "Staunton", "800": "Suffolk", "810": "Virginia Beach",
  "820": "Waynesboro", "830": "Williamsburg", "840": "Winchester",
};

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

async function getOrCreateCounty(name: string, fips: string): Promise<number> {
  const key = `${name}_VA`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", "VA").single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: "VA", state_fips: "51", county_fips: fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
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

// Try to extract FIPS from parcel ID (many VA parcels encode FIPS in the ID)
function extractFipsFromParcelId(parcelId: string): string | null {
  if (!parcelId) return null;
  // Some VA parcels start with a 3-digit FIPS
  const match = parcelId.match(/^(\d{3})/);
  if (match && VA_FIPS_TO_NAME[match[1]]) return match[1];
  return null;
}

async function main() {
  console.log("MXRE — Ingest Virginia Statewide Parcels (VDEM)");
  console.log("Source:", SERVICE_URL);
  console.log();

  // First check what fields are available
  const metaResp = await fetch(`${SERVICE_URL}?f=json`);
  const meta = await metaResp.json();
  const fieldNames = meta.fields?.map((f: any) => f.name) || [];
  console.log(`Available fields: ${fieldNames.join(", ")}`);

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
      const parcelId = r.PARCELID || r.PTM_ID || "";
      if (!parcelId) { totalErrors++; continue; }

      // Try to determine county from FIPS embedded in parcel ID
      const fips = extractFipsFromParcelId(parcelId);
      if (!fips) { totalErrors++; continue; }

      const countyName = VA_FIPS_TO_NAME[fips];
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, fips);
      } catch {
        totalErrors++;
        continue;
      }

      rows.push({
        county_id: countyId,
        parcel_id: parcelId,
        address: "",
        city: "",
        state_code: "VA",
        zip: "",
        source: "va-vdem-parcels",
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
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
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
