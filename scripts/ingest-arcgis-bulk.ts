#!/usr/bin/env tsx
/**
 * Bulk ArcGIS FeatureServer Parcel Ingestion
 *
 * Paginate through statewide parcel FeatureServers and upsert into properties table.
 * Supports: NJ, CO, WA (and any other ArcGIS FeatureServer with parcels).
 *
 * Usage:
 *   npx tsx scripts/ingest-arcgis-bulk.ts NJ
 *   npx tsx scripts/ingest-arcgis-bulk.ts CO
 *   npx tsx scripts/ingest-arcgis-bulk.ts WA
 *   npx tsx scripts/ingest-arcgis-bulk.ts ALL
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

// ─── Config ─────────────────────────────────────────────────────────

const BATCH_SIZE = 500;        // DB upsert batch size
const PAGE_SIZE = 2000;        // ArcGIS max per request
const CONCURRENT_REQUESTS = 4; // Parallel ArcGIS fetches
const RETRY_MAX = 5;
const RETRY_BASE_MS = 3000;

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── State Configs ──────────────────────────────────────────────────

interface StateConfig {
  code: string;
  name: string;
  serviceUrl: string;
  layerId: number;
  /** Map feature attributes to our schema */
  mapFeature: (attrs: Record<string, any>, countyIdMap: Map<string, number>) => PropertyRow | null;
}

interface PropertyRow {
  county_id: number;
  parcel_id: string;
  address: string;
  city: string;
  state_code: string;
  zip: string;
  owner_name: string;
  assessed_value: number | null;
  year_built: number | null;
  total_sqft: number | null;
  total_units: number | null;
  property_type: string;
  source: string;
  land_value: number | null;
  last_sale_price: number | null;
  last_sale_date: string | null;
  property_tax: number | null;
  land_sqft: number | null;
}

function classifyLandUse(code: string, desc: string): string {
  const c = (code || "").toLowerCase();
  const d = (desc || "").toLowerCase();
  const combined = `${c} ${d}`;
  if (combined.match(/resid|single.?fam|sfr|dwelling|house|home/)) return "single_family";
  if (combined.match(/multi|apart|duplex|triplex|fourplex|2.?fam|3.?fam|4.?fam/)) return "multifamily";
  if (combined.match(/condo|townho/)) return "condo";
  if (combined.match(/commerc|office|retail|store|shop/)) return "commercial";
  if (combined.match(/industr|warehouse|manufact/)) return "industrial";
  if (combined.match(/vacan|land|lot|agri|farm|ranch|timber|forest|pasture/)) return "land";
  if (combined.match(/exempt|govern|school|church|relig|hospital|park/)) return "exempt";
  return "residential";
}

const NJ_COUNTY_MAP: Record<string, string> = {
  "0100": "Atlantic", "0200": "Bergen", "0300": "Burlington", "0400": "Camden",
  "0500": "Cape May", "0600": "Cumberland", "0700": "Essex", "0800": "Gloucester",
  "0900": "Hudson", "1000": "Hunterdon", "1100": "Mercer", "1200": "Middlesex",
  "1300": "Monmouth", "1400": "Morris", "1500": "Ocean", "1600": "Passaic",
  "1700": "Salem", "1800": "Somerset", "1900": "Sussex", "2000": "Union",
  "2100": "Warren",
};

const WA_FIPS_TO_NAME: Record<string, string> = {
  "001": "Adams", "003": "Asotin", "005": "Benton", "007": "Chelan",
  "009": "Clallam", "011": "Clark", "013": "Columbia", "015": "Cowlitz",
  "017": "Douglas", "019": "Ferry", "021": "Franklin", "023": "Garfield",
  "025": "Grant", "027": "Grays Harbor", "029": "Island", "031": "Jefferson",
  "033": "King", "035": "Kitsap", "037": "Kittitas", "039": "Klickitat",
  "041": "Lewis", "043": "Lincoln", "045": "Mason", "047": "Okanogan",
  "049": "Pacific", "051": "Pend Oreille", "053": "Pierce", "055": "San Juan",
  "057": "Skagit", "059": "Skamania", "061": "Snohomish", "063": "Spokane",
  "065": "Stevens", "067": "Thurston", "069": "Wahkiakum", "071": "Walla Walla",
  "073": "Whatcom", "075": "Whitman", "077": "Yakima",
};

const STATES: StateConfig[] = [
  {
    code: "NJ",
    name: "New Jersey",
    serviceUrl: "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_Composite_NJ_WM/FeatureServer",
    layerId: 0,
    mapFeature: (a, countyMap) => {
      const countyCode = (a.PCL_MUN || "").substring(0, 4);
      const countyName = NJ_COUNTY_MAP[countyCode] || a.COUNTY || "";
      if (!countyName) return null;

      const countyId = countyMap.get(countyName.toUpperCase());
      if (!countyId) return null;

      const propClass = (a.PROP_CLASS || "").trim();
      let propertyType = "residential";
      if (propClass.startsWith("1")) propertyType = "vacant_land";
      else if (propClass.startsWith("2")) propertyType = "single_family";
      else if (propClass.startsWith("4")) propertyType = "commercial";
      else if (propClass.startsWith("5")) propertyType = classifyLandUse(propClass, a.BLDG_DESC || "");

      // Parse deed date (format: MMDDYY)
      let saleDate: string | null = null;
      const dd = (a.DEED_DATE || "").trim();
      if (dd && dd.length === 6) {
        const mm = dd.substring(0, 2);
        const day = dd.substring(2, 4);
        const yy = parseInt(dd.substring(4, 6));
        const year = yy > 50 ? 1900 + yy : 2000 + yy;
        if (parseInt(mm) >= 1 && parseInt(mm) <= 12) {
          saleDate = `${year}-${mm}-${day}`;
        }
      }

      return {
        county_id: countyId,
        parcel_id: a.PAMS_PIN || a.GIS_PIN || "",
        address: (a.PROP_LOC || "").trim().toUpperCase(),
        city: (a.MUN_NAME || "").trim().toUpperCase(),
        state_code: "NJ",
        zip: (a.ZIP5 || a.ZIP_CODE || "").substring(0, 5),
        owner_name: (a.OWNER_NAME || "").trim(),
        assessed_value: a.NET_VALUE && a.NET_VALUE > 0 ? a.NET_VALUE : null,
        year_built: a.YR_CONSTR && a.YR_CONSTR > 1700 && a.YR_CONSTR < 2030 ? a.YR_CONSTR : null,
        total_sqft: null,
        total_units: (a.DWELL || 0) + (a.COMM_DWELL || 0) || null,
        property_type: propertyType,
        source: "njgin-parcels",
        land_value: a.LAND_VAL && a.LAND_VAL > 0 ? a.LAND_VAL : null,
        last_sale_price: a.SALE_PRICE && a.SALE_PRICE > 0 ? a.SALE_PRICE : null,
        last_sale_date: saleDate,
        property_tax: a.LAST_YR_TX && a.LAST_YR_TX > 0 ? Math.round(a.LAST_YR_TX) : null,
        land_sqft: a.CALC_ACRE && a.CALC_ACRE > 0 ? Math.round(a.CALC_ACRE * 43560) : null,
      };
    },
  },
  {
    code: "CO",
    name: "Colorado",
    serviceUrl: "https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer",
    layerId: 0,
    mapFeature: (a, countyMap) => {
      const countyName = (a.countyName || "").trim();
      if (!countyName) return null;
      const countyId = countyMap.get(countyName.toUpperCase());
      if (!countyId) return null;

      const propertyType = classifyLandUse(a.landUseCde || "", a.landUseDsc || "");

      let saleDate: string | null = null;
      if (a.saleDate) {
        // Could be various formats
        const sd = String(a.saleDate).trim();
        if (sd.match(/^\d{4}-\d{2}-\d{2}/)) saleDate = sd.substring(0, 10);
        else if (sd.match(/^\d{1,2}\/\d{1,2}\/\d{4}/)) {
          const parts = sd.split("/");
          saleDate = `${parts[2]}-${parts[0].padStart(2, "0")}-${parts[1].padStart(2, "0")}`;
        }
      }

      const parseNum = (v: any) => {
        if (!v) return null;
        const n = typeof v === "string" ? parseFloat(v.replace(/[,$]/g, "")) : Number(v);
        return isNaN(n) || n <= 0 ? null : n;
      };

      return {
        county_id: countyId,
        parcel_id: (a.parcel_id || a.account || "").trim(),
        address: (a.situsAdd || "").trim().toUpperCase(),
        city: (a.sitAddCty || "").trim().toUpperCase(),
        state_code: "CO",
        zip: (a.sitAddZip || "").substring(0, 5),
        owner_name: [a.owner, a.owner2].filter(Boolean).join(" & ").trim(),
        assessed_value: parseNum(a.asedValTot),
        year_built: null,
        total_sqft: null,
        total_units: null,
        property_type: propertyType,
        source: "colorado-public-parcels",
        land_value: null,
        last_sale_price: parseNum(a.salePrice),
        last_sale_date: saleDate,
        property_tax: null,
        land_sqft: a.landSqft && a.landSqft > 0 ? Math.round(a.landSqft) : (a.landAcres && a.landAcres > 0 ? Math.round(a.landAcres * 43560) : null),
      };
    },
  },
  {
    code: "WA",
    name: "Washington",
    serviceUrl: "https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/Current_Parcels/FeatureServer",
    layerId: 0,
    mapFeature: (a, countyMap) => {
      // WA uses FIPS_NR as number (e.g. "1" = Adams = FIPS 001)
      // COUNTY_NM field also contains the FIPS number, not the name
      const fipsNr = String(a.FIPS_NR || a.COUNTY_NM || "").trim().padStart(3, "0");
      const countyName = WA_FIPS_TO_NAME[fipsNr];
      if (!countyName) return null;
      const countyId = countyMap.get(countyName.toUpperCase());
      if (!countyId) return null;

      const luCode = a.LANDUSE_CD || 0;
      let propertyType = "residential";
      if (luCode === 2 || luCode === 6) propertyType = "commercial";
      else if (luCode === 3) propertyType = "industrial";
      else if (luCode === 4 || luCode === 8) propertyType = "land";
      else if (luCode === 5) propertyType = "exempt";

      return {
        county_id: countyId,
        parcel_id: (a.PARCEL_ID_NR || a.ORIG_PARCEL_ID || "").trim(),
        address: [a.SITUS_ADDRESS, a.SUB_ADDRESS].filter(Boolean).join(" ").trim().toUpperCase(),
        city: (a.SITUS_CITY_NM || "").trim().toUpperCase(),
        state_code: "WA",
        zip: (a.SITUS_ZIP_NR || "").substring(0, 5),
        owner_name: "",
        assessed_value: (a.VALUE_LAND || 0) + (a.VALUE_BLDG || 0) || null,
        year_built: null,
        total_sqft: null,
        total_units: null,
        property_type: propertyType,
        source: "wa-geo-parcels",
        land_value: a.VALUE_LAND && a.VALUE_LAND > 0 ? a.VALUE_LAND : null,
        last_sale_price: null,
        last_sale_date: null,
        property_tax: null,
        land_sqft: null,
      };
    },
  },
];

// ─── ArcGIS Pagination ──────────────────────────────────────────────

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "MXRE-Ingester/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const json = await resp.json();
    if (json.error) throw new Error(`ArcGIS error: ${json.error.message || JSON.stringify(json.error)}`);
    return json;
  } catch (err: any) {
    if (attempt >= RETRY_MAX) throw err;
    const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
    console.log(`    Retry ${attempt}/${RETRY_MAX} in ${(delay / 1000).toFixed(1)}s: ${err.message}`);
    await new Promise(r => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

async function getRecordCount(baseUrl: string, layerId: number): Promise<number> {
  const url = `${baseUrl}/${layerId}/query?where=1%3D1&returnCountOnly=true&f=json`;
  const data = await fetchWithRetry(url);
  return data.count || 0;
}

async function getObjectIdRange(baseUrl: string, layerId: number): Promise<{ min: number; max: number }> {
  // Get min OBJECTID
  const minUrl = `${baseUrl}/${layerId}/query?where=1%3D1&outFields=OBJECTID&orderByFields=${encodeURIComponent("OBJECTID ASC")}&resultRecordCount=1&f=json&returnGeometry=false`;
  const minData = await fetchWithRetry(minUrl);
  const minId = minData.features?.[0]?.attributes?.OBJECTID ?? 1;

  // Get max OBJECTID
  const maxUrl = `${baseUrl}/${layerId}/query?where=1%3D1&outFields=OBJECTID&orderByFields=${encodeURIComponent("OBJECTID DESC")}&resultRecordCount=1&f=json&returnGeometry=false`;
  const maxData = await fetchWithRetry(maxUrl);
  const maxId = maxData.features?.[0]?.attributes?.OBJECTID ?? 0;

  return { min: minId, max: maxId };
}

async function fetchPageByRange(baseUrl: string, layerId: number, minId: number, maxId: number): Promise<any[]> {
  const where = encodeURIComponent(`OBJECTID >= ${minId} AND OBJECTID <= ${maxId}`);
  const url = `${baseUrl}/${layerId}/query?where=${where}&outFields=*&f=json&returnGeometry=false`;
  const data = await fetchWithRetry(url);
  return data.features || [];
}

// ─── FIPS Codes ─────────────────────────────────────────────────────

const STATE_FIPS: Record<string, string> = {
  NJ: "34", CO: "08", WA: "53", MN: "27",
};

// County FIPS codes (partial — populated as encountered)
const COUNTY_FIPS: Record<string, Record<string, string>> = {
  NJ: {
    ATLANTIC: "001", BERGEN: "003", BURLINGTON: "005", CAMDEN: "007",
    "CAPE MAY": "009", CUMBERLAND: "011", ESSEX: "013", GLOUCESTER: "015",
    HUDSON: "017", HUNTERDON: "019", MERCER: "021", MIDDLESEX: "023",
    MONMOUTH: "025", MORRIS: "027", OCEAN: "029", PASSAIC: "031",
    SALEM: "033", SOMERSET: "035", SUSSEX: "037", UNION: "039", WARREN: "041",
  },
  CO: {
    ADAMS: "001", ALAMOSA: "003", ARAPAHOE: "005", ARCHULETA: "007",
    BACA: "009", BENT: "011", BOULDER: "013", BROOMFIELD: "014",
    CHAFFEE: "015", CHEYENNE: "017", "CLEAR CREEK": "019", CONEJOS: "021",
    COSTILLA: "023", CROWLEY: "025", CUSTER: "027", DELTA: "029",
    DENVER: "031", DOLORES: "033", DOUGLAS: "035", EAGLE: "037",
    "EL PASO": "041", ELBERT: "039", FREMONT: "043", GARFIELD: "045",
    GILPIN: "047", GRAND: "049", GUNNISON: "051", HINSDALE: "053",
    HUERFANO: "055", JACKSON: "057", JEFFERSON: "059", KIOWA: "061",
    "KIT CARSON": "063", "LA PLATA": "067", LAKE: "065", LARIMER: "069",
    "LAS ANIMAS": "071", LINCOLN: "073", LOGAN: "075", MESA: "077",
    MINERAL: "079", MOFFAT: "081", MONTEZUMA: "083", MONTROSE: "085",
    MORGAN: "087", OTERO: "089", OURAY: "091", PARK: "093",
    PHILLIPS: "095", PITKIN: "097", PROWERS: "099", PUEBLO: "101",
    "RIO BLANCO": "103", "RIO GRANDE": "105", ROUTT: "107", SAGUACHE: "109",
    "SAN JUAN": "111", "SAN MIGUEL": "113", SEDGWICK: "115", SUMMIT: "117",
    TELLER: "119", WASHINGTON: "121", WELD: "123", YUMA: "125",
  },
  WA: {
    ADAMS: "001", ASOTIN: "003", BENTON: "005", CHELAN: "007",
    CLALLAM: "009", CLARK: "011", COLUMBIA: "013", COWLITZ: "015",
    DOUGLAS: "017", FERRY: "019", FRANKLIN: "021", GARFIELD: "023",
    GRANT: "025", "GRAYS HARBOR": "027", ISLAND: "029", JEFFERSON: "031",
    KING: "033", KITSAP: "035", KITTITAS: "037", KLICKITAT: "039",
    LEWIS: "041", LINCOLN: "043", MASON: "045", OKANOGAN: "047",
    PACIFIC: "049", "PEND OREILLE": "051", PIERCE: "053", "SAN JUAN": "055",
    SKAGIT: "057", SKAMANIA: "059", SNOHOMISH: "061", SPOKANE: "063",
    STEVENS: "065", THURSTON: "067", WAHKIAKUM: "069", "WALLA WALLA": "071",
    WHATCOM: "073", WHITMAN: "075", YAKIMA: "077",
  },
  MN: {},
};

// ─── County Management ──────────────────────────────────────────────

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const cleanName = name.trim();
  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", cleanName)
    .eq("state_code", state)
    .single();
  if (data) return data.id;

  const stateFips = STATE_FIPS[state] || "00";
  const countyFips = COUNTY_FIPS[state]?.[cleanName.toUpperCase()] || "000";

  const { data: created, error } = await db.from("counties")
    .insert({
      county_name: cleanName,
      state_code: state,
      state_fips: stateFips,
      county_fips: countyFips,
      active: true,
    })
    .select("id")
    .single();
  if (error) throw new Error(`Failed to create county ${cleanName}, ${state}: ${error.message}`);
  return created!.id;
}

async function buildCountyMap(stateCode: string, countyNames: string[]): Promise<Map<string, number>> {
  const map = new Map<string, number>();
  // Get existing
  const { data } = await db.from("counties")
    .select("id, county_name")
    .eq("state_code", stateCode);
  for (const c of data || []) {
    map.set(c.county_name.toUpperCase(), c.id);
  }
  // Create missing
  for (const name of countyNames) {
    const upper = name.toUpperCase();
    if (!map.has(upper)) {
      const id = await getOrCreateCounty(name, stateCode);
      map.set(upper, id);
    }
  }
  return map;
}

// ─── DB Batch Insert ────────────────────────────────────────────────

async function batchInsert(rows: PropertyRow[]): Promise<number> {
  let inserted = 0;
  // Dedup rows by (county_id, parcel_id) before upserting to avoid conflict errors
  const seen = new Map<string, any>();
  for (const row of rows) {
    const key = `${row.county_id}|${row.parcel_id}`;
    seen.set(key, row);
  }
  const dedupedRows = Array.from(seen.values());
  for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
    const batch = dedupedRows.slice(i, i + BATCH_SIZE);
    if (batch.length === 0) continue;
    const { error, count } = await db.from("properties").upsert(batch, {
      onConflict: "county_id,parcel_id",
      ignoreDuplicates: false,
    });
    if (error) {
      console.error(`    DB error: ${error.message.substring(0, 120)}`);
    } else {
      inserted += batch.length;
    }
  }
  return inserted;
}

// ─── Main Ingestion ─────────────────────────────────────────────────

async function ingestState(config: StateConfig) {
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  ${config.name} (${config.code})`);
  console.log(`  Service: ${config.serviceUrl}`);
  console.log(`${"═".repeat(60)}`);

  // Step 1: Get total count
  console.log("  Getting record count...");
  const totalCount = await getRecordCount(config.serviceUrl, config.layerId);
  console.log(`  Total records: ${totalCount.toLocaleString()}`);

  // Step 2: Get OBJECTID range for pagination
  console.log("  Getting OBJECTID range...");
  const { min: minId, max: maxId } = await getObjectIdRange(config.serviceUrl, config.layerId);
  console.log(`  OBJECTID range: ${minId} to ${maxId}`);

  // Step 3: First pass — discover county names (sample first 10K IDs)
  console.log("  Discovering counties (sampling first records)...");
  const countyNames = new Set<string>();
  const sampleFeatures = await fetchPageByRange(config.serviceUrl, config.layerId, minId, minId + PAGE_SIZE - 1);
  for (const f of sampleFeatures) {
    const a = f.attributes;
    let name = "";
    if (config.code === "NJ") {
      const code = (a.PCL_MUN || "").substring(0, 4);
      name = NJ_COUNTY_MAP[code] || a.COUNTY || "";
    } else if (config.code === "CO") {
      name = a.countyName || "";
    } else if (config.code === "WA") {
      const fips = String(a.FIPS_NR || a.COUNTY_NM || "").trim().padStart(3, "0");
      name = WA_FIPS_TO_NAME[fips] || "";
    }
    if (name) countyNames.add(name.trim());
  }

  // For NJ, we know all 21 counties; for others, we'll create on-the-fly
  if (config.code === "NJ") {
    for (const name of Object.values(NJ_COUNTY_MAP)) {
      countyNames.add(name);
    }
  }
  console.log(`  Found ${countyNames.size} counties`);

  // Build county ID map
  const countyIdMap = await buildCountyMap(config.code, [...countyNames]);
  console.log(`  County map built (${countyIdMap.size} entries)`);

  // Step 4: Build range-based page chunks
  const rangeChunks: Array<{ start: number; end: number }> = [];
  for (let start = minId; start <= maxId; start += PAGE_SIZE) {
    rangeChunks.push({ start, end: Math.min(start + PAGE_SIZE - 1, maxId) });
  }

  let totalInserted = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let newCountiesCreated = 0;
  const startTime = Date.now();
  const totalPages = rangeChunks.length;

  console.log(`\n  Starting ingestion: ${totalPages} pages of up to ${PAGE_SIZE} records`);
  console.log(`  Concurrency: ${CONCURRENT_REQUESTS} parallel requests\n`);

  // Process in concurrent batches
  for (let ci = 0; ci < totalPages; ci += CONCURRENT_REQUESTS) {
    const batch = rangeChunks.slice(ci, ci + CONCURRENT_REQUESTS);
    const results = await Promise.allSettled(
      batch.map(async (range) => {
        const features = await fetchPageByRange(config.serviceUrl, config.layerId, range.start, range.end);
        const rows: PropertyRow[] = [];
        let skipped = 0;
        for (const f of features) {
          const a = f.attributes;
          // Dynamically discover new counties
          let cName = "";
          if (config.code === "NJ") {
            const code = (a.PCL_MUN || "").substring(0, 4);
            cName = NJ_COUNTY_MAP[code] || a.COUNTY || "";
          } else if (config.code === "CO") {
            cName = a.countyName || "";
          } else if (config.code === "WA") {
            const fips = String(a.FIPS_NR || a.COUNTY_NM || "").trim().padStart(3, "0");
            cName = WA_FIPS_TO_NAME[fips] || "";
          }
          if (cName && !countyIdMap.has(cName.toUpperCase().trim())) {
            const id = await getOrCreateCounty(cName.trim(), config.code);
            countyIdMap.set(cName.toUpperCase().trim(), id);
            newCountiesCreated++;
          }

          const row = config.mapFeature(a, countyIdMap);
          if (row && (row.parcel_id || row.address)) {
            rows.push(row);
          } else {
            skipped++;
          }
        }
        return { rows, skipped };
      })
    );

    const allRows: PropertyRow[] = [];
    for (const r of results) {
      if (r.status === "fulfilled") {
        allRows.push(...r.value.rows);
        totalSkipped += r.value.skipped;
      } else {
        totalErrors++;
        if (totalErrors <= 10) console.error(`    Page error: ${r.reason?.message || r.reason}`);
      }
    }

    if (allRows.length > 0) {
      const n = await batchInsert(allRows);
      totalInserted += n;
    }

    // Progress
    const pagesComplete = Math.min(ci + CONCURRENT_REQUESTS, totalPages);
    const pct = ((pagesComplete / totalPages) * 100).toFixed(1);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted > 0 ? (totalInserted / (parseFloat(elapsed) || 1)).toFixed(0) : "0";
    const remaining = totalInserted > 0
      ? (((totalCount - totalInserted) / (totalInserted / (parseFloat(elapsed) || 1))) / 60).toFixed(1)
      : "?";

    if (pagesComplete % 10 === 0 || pagesComplete === totalPages) {
      console.log(
        `  [${config.code}] ${pagesComplete}/${totalPages} pages (${pct}%) | ` +
        `${totalInserted.toLocaleString()} inserted | ${totalSkipped} skipped | ` +
        `${rate}/sec | ~${remaining} min left | ${elapsed}s`
      );
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n  ── ${config.code} COMPLETE ──`);
  console.log(`  Inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  Skipped: ${totalSkipped.toLocaleString()}`);
  console.log(`  Errors: ${totalErrors}`);
  if (newCountiesCreated > 0) console.log(`  New counties created: ${newCountiesCreated}`);
  console.log(`  Time: ${elapsed}s (${(parseFloat(elapsed) / 60).toFixed(1)} min)`);

  return totalInserted;
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  const target = (process.argv[2] || "ALL").toUpperCase();
  const configs = target === "ALL"
    ? STATES
    : STATES.filter(s => s.code === target);

  if (configs.length === 0) {
    console.error(`Unknown state: ${target}. Available: ${STATES.map(s => s.code).join(", ")}, ALL`);
    process.exit(1);
  }

  console.log(`\nMXRE — Bulk ArcGIS Parcel Ingestion`);
  console.log(`${"═".repeat(60)}`);
  console.log(`Target: ${configs.map(s => `${s.name} (${s.code})`).join(", ")}`);
  console.log(`DB: ${process.env.SUPABASE_URL}`);

  // Show current count
  const { count } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`Current properties in DB: ${(count || 0).toLocaleString()}`);

  let grandTotal = 0;
  for (const config of configs) {
    try {
      const n = await ingestState(config);
      grandTotal += n;
    } catch (err: any) {
      console.error(`\n  FATAL ERROR for ${config.code}: ${err.message}`);
    }
  }

  // Final count
  const { count: finalCount } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`\n${"═".repeat(60)}`);
  console.log(`GRAND TOTAL INSERTED: ${grandTotal.toLocaleString()}`);
  console.log(`Properties in DB: ${(finalCount || 0).toLocaleString()}`);
  console.log(`${"═".repeat(60)}`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
