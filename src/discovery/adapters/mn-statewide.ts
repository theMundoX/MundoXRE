/**
 * Minnesota Statewide Parcel Adapter
 *
 * Imports property data from MnGeo statewide parcels via ArcGIS FeatureServer.
 * Source: https://services2.arcgis.com/36WhZuHs6P46YNMH/arcgis/rest/services/OpenParcels/FeatureServer
 *
 * ~3M parcels across 87 counties.
 * Fields vary by county but commonly include: PIN, PARCEL_ID, OWNER_NAME,
 *   SITUS_ADDR, SITUS_CITY, SITUS_ZIP, EMV_TOTAL, EMV_LAND, YEAR_BUILT,
 *   SALE_VALUE, SALE_DATE, USE_CODE, COUNTYNAME, ACRES
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

const ARCGIS_URL = "https://services2.arcgis.com/36WhZuHs6P46YNMH/arcgis/rest/services/OpenParcels/FeatureServer";
const PAGE_SIZE = 2000;
const MAX_RETRIES = 5;
const RETRY_BASE_MS = 3000;

// MN county FIPS codes (state FIPS = 27)
export const MN_COUNTY_FIPS: Record<string, string> = {
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

function parseNum(v: any): number | undefined {
  if (v === null || v === undefined || v === "") return undefined;
  const n = Number(v);
  return isNaN(n) || n <= 0 ? undefined : n;
}

function mapMNAttributes(a: Record<string, any>): Omit<RawPropertyRecord, "raw"> & { raw: Record<string, unknown> } {
  // MN parcels have various field name conventions depending on county
  const parcelId = a.PIN || a.PARCEL_ID || a.PID || a.PARCEL_NUM || a.TAXPAYER_ID || "";
  const address = a.BLDG_NUM
    ? `${a.BLDG_NUM || ""} ${a.PREFIX_DIR || ""} ${a.PREFIXTYPE || ""} ${a.STREETNAME || ""} ${a.STREETTYPE || ""} ${a.SUFFIX_DIR || ""}`.replace(/\s+/g, " ").trim()
    : (a.SITUS_ADDR || a.SITE_ADDR || a.PROP_ADDR || a.ADDRESS || "").trim();

  const city = (a.SITUS_CITY || a.CITY || a.PLACENAME || "").trim();
  const zip = (a.SITUS_ZIP || a.ZIP || a.ZIPCODE || "").substring(0, 5);
  const owner = (a.OWNER_NAME || a.OWNER || a.TAXPAYER_NAME || "").trim();
  const totalVal = parseNum(a.EMV_TOTAL || a.TOTAL_VALUE || a.MKT_VAL || a.EST_MKT_VAL);
  const landVal = parseNum(a.EMV_LAND || a.LAND_VALUE || a.LAND_MKT);
  const yearBuilt = a.YEAR_BUILT || a.YR_BUILT;
  const sqft = parseNum(a.FIN_SQ_FT || a.BLDG_SQFT || a.TOTAL_SQFT);
  const salePrice = parseNum(a.SALE_VALUE || a.LAST_SALE_PRICE || a.SALE_AMT);
  const useCode = a.USE_CODE || a.LAND_USE || a.USE_DESC || a.USECLASS1 || "";
  const useDesc = a.USE_DESC || a.PROPERTY_TYPE || a.USECLASS1_DESC || "";
  const acres = parseNum(a.ACRES || a.TOTAL_ACRES || a.ACREAGE);

  let saleDate: string | undefined;
  const rawDate = a.SALE_DATE || a.LAST_SALE_DATE || "";
  if (rawDate) {
    const d = String(rawDate);
    if (d.match(/^\d{4}-\d{2}-\d{2}/)) saleDate = d.substring(0, 10);
    else if (d.match(/^\d+$/) && d.length > 8) {
      const dt = new Date(parseInt(d));
      if (dt.getFullYear() > 1970) saleDate = dt.toISOString().substring(0, 10);
    }
  }

  return {
    parcel_id: String(parcelId).trim(),
    address: address.toUpperCase(),
    city: city.toUpperCase(),
    state: "MN",
    zip,
    owner_name: owner || undefined,
    assessed_value: totalVal,
    land_value: landVal,
    year_built: yearBuilt && yearBuilt > 1700 && yearBuilt < 2030 ? yearBuilt : undefined,
    total_sqft: sqft,
    property_type: classifyLandUse(useCode, useDesc),
    last_sale_price: salePrice,
    last_sale_date: saleDate,
    property_tax: parseNum(a.TAX_TOTAL || a.NET_TAX),
    land_sqft: acres ? Math.round(acres * 43560) : undefined,
    raw: {
      countyName: a.COUNTYNAME || a.COUNTY_NAME || a.CO_NAME,
      useCode, useDesc,
      acres,
    },
  };
}

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "MXRE-Adapter/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const json = await resp.json() as any;
    if (json.error) throw new Error(`ArcGIS: ${json.error.message}`);
    return json;
  } catch (err: any) {
    if (attempt >= MAX_RETRIES) throw err;
    const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
    console.log(`    Retry ${attempt}/${MAX_RETRIES}: ${err.message}`);
    await new Promise(r => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

export class MNStatewideAdapter extends AssessorAdapter {
  readonly platform = "mn_statewide";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "mn_statewide";
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    try {
      const countyName = config.name;
      const where = countyName
        ? encodeURIComponent(`COUNTYNAME='${countyName.toUpperCase()}'`)
        : encodeURIComponent("1=1");
      const url = `${ARCGIS_URL}/0/query?where=${where}&returnCountOnly=true&f=json`;
      const data = await fetchWithRetry(url);
      return data.count ?? null;
    } catch {
      return null;
    }
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const countyName = config.name;

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // First get all object IDs for this county
    const countyWhere = countyName
      ? `COUNTYNAME='${countyName.toUpperCase()}'`
      : "1=1";

    const totalCount = await this.estimateCount(config);
    if (totalCount !== null) {
      progress.total_found = totalCount;
      console.log(`  ${config.name}: ${totalCount.toLocaleString()} parcels available`);
    }

    // Get IDs
    console.log(`  Getting object IDs for ${config.name}...`);
    const idsUrl = `${ARCGIS_URL}/0/query?where=${encodeURIComponent(countyWhere)}&returnIdsOnly=true&f=json`;
    const idsData = await fetchWithRetry(idsUrl);
    const allIds: number[] = idsData.objectIds || [];
    allIds.sort((a, b) => a - b);
    console.log(`  IDs retrieved: ${allIds.length.toLocaleString()}`);

    // Process in chunks
    for (let i = 0; i < allIds.length; i += PAGE_SIZE) {
      const idChunk = allIds.slice(i, i + PAGE_SIZE);
      const idsStr = idChunk.join(",");
      const url = `${ARCGIS_URL}/0/query?objectIds=${idsStr}&outFields=*&f=json&returnGeometry=false`;

      let features: any[];
      try {
        const data = await fetchWithRetry(url);
        features = data.features || [];
      } catch (err: any) {
        console.error(`  Chunk error at ${i}: ${err.message}`);
        progress.errors++;
        continue;
      }

      for (const f of features) {
        const a = f.attributes;
        const cntyName = a.COUNTYNAME || a.COUNTY_NAME || a.CO_NAME || "";
        if (!cntyName) continue;

        const mapped = mapMNAttributes(a);
        if (!mapped.parcel_id && !mapped.address) continue;

        const record: RawPropertyRecord = mapped;

        progress.total_processed++;
        if (progress.total_processed % 10000 === 0) {
          console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
          onProgress?.(progress);
        }

        yield record;
      }

      // Polite delay
      if (i % (PAGE_SIZE * 10) === 0 && i > 0) {
        await new Promise(r => setTimeout(r, 500));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
