/**
 * New York State Statewide Parcel Adapter
 *
 * Imports property data from NYS GIS FeatureServer (ArcGIS REST API).
 * Source: https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1
 *
 * ~3.7M parcels across 62 counties, paginated at 1000 records per request.
 * Fields: COUNTY_NAME, PRINT_KEY, PARCEL_ADDR, CITYTOWN_NAME, LOC_ZIP,
 *         PRIMARY_OWNER, TOTAL_AV, FULL_MARKET_VAL, LAND_AV, YR_BLT,
 *         SQFT_LIVING, SQ_FT, ACRES, PROP_CLASS, BLDG_STYLE_DESC
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

const SERVICE_URL = "https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1";
const OUT_FIELDS = "COUNTY_NAME,PRINT_KEY,PARCEL_ADDR,CITYTOWN_NAME,LOC_ZIP,PRIMARY_OWNER,TOTAL_AV,FULL_MARKET_VAL,LAND_AV,YR_BLT,SQFT_LIVING,SQ_FT,ACRES,PROP_CLASS,BLDG_STYLE_DESC,NBR_BEDROOMS,NBR_FULL_BATHS";
const PAGE_SIZE = 1000;
const MAX_RETRIES = 5;

// NY Property Class codes → property type
function classifyNYPropClass(code: string): string {
  if (!code) return "";
  const n = parseInt(code);
  if (isNaN(n)) return "";
  const cat = Math.floor(n / 100);
  switch (cat) {
    case 1: return "residential";
    case 2: return "residential";
    case 3: return "commercial";  // vacant commercial
    case 4: return "commercial";
    case 5: return "recreational";
    case 6: return "agricultural";
    case 7: return "industrial";
    case 8: return "government";
    case 9: return "exempt";
    default: return "";
  }
}

// NY county FIPS codes (state FIPS = 36)
export const NY_COUNTY_FIPS: Record<string, string> = {
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

async function fetchPage(offset: number, countyWhere?: string): Promise<any[]> {
  const where = countyWhere
    ? encodeURIComponent(countyWhere)
    : encodeURIComponent("1=1");
  const url = `${SERVICE_URL}/query?where=${where}&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, {
        headers: { "User-Agent": "MXRE-Adapter/1.0" },
        signal: AbortSignal.timeout(120_000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json() as any;
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

export class NYStatewideAdapter extends AssessorAdapter {
  readonly platform = "ny_statewide";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "ny_statewide";
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    try {
      const countyName = config.name;
      const where = countyName
        ? encodeURIComponent(`COUNTY_NAME='${countyName.toUpperCase()}'`)
        : encodeURIComponent("1=1");
      const url = `${SERVICE_URL}/query?where=${where}&returnCountOnly=true&f=json`;
      const resp = await fetch(url, {
        headers: { "User-Agent": "MXRE-Adapter/1.0" },
        signal: AbortSignal.timeout(30_000),
      });
      if (!resp.ok) return null;
      const json = await resp.json() as any;
      return json.count ?? null;
    } catch {
      return null;
    }
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const countyName = config.name;
    const countyWhere = countyName
      ? `COUNTY_NAME='${countyName.toUpperCase()}'`
      : undefined;

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // Get total count
    const totalCount = await this.estimateCount(config);
    if (totalCount !== null) {
      progress.total_found = totalCount;
      console.log(`  ${config.name}: ${totalCount.toLocaleString()} parcels available`);
    }

    let offset = 0;

    while (true) {
      let records: any[];
      try {
        records = await fetchPage(offset, countyWhere);
      } catch (err: any) {
        console.error(`  Page error at offset ${offset}: ${err.message}`);
        progress.errors++;
        offset += PAGE_SIZE;
        if (totalCount !== null && offset >= totalCount) break;
        continue;
      }

      if (records.length === 0) break;

      for (const r of records) {
        const address = (r.PARCEL_ADDR || "").trim();
        if (!address) continue;

        const sqft = r.SQFT_LIVING && r.SQFT_LIVING > 0 ? r.SQFT_LIVING : undefined;
        const lotSqft = r.SQ_FT && r.SQ_FT > 0
          ? r.SQ_FT
          : (r.ACRES && r.ACRES > 0 ? Math.round(r.ACRES * 43560) : undefined);

        const record: RawPropertyRecord = {
          parcel_id: r.PRINT_KEY || "",
          address,
          city: r.CITYTOWN_NAME || "",
          state: "NY",
          zip: r.LOC_ZIP || "",
          owner_name: r.PRIMARY_OWNER || undefined,
          property_type: classifyNYPropClass(r.PROP_CLASS) || undefined,
          assessed_value: r.TOTAL_AV && r.TOTAL_AV > 0 ? r.TOTAL_AV : undefined,
          market_value: r.FULL_MARKET_VAL && r.FULL_MARKET_VAL > 0 ? r.FULL_MARKET_VAL : undefined,
          land_value: r.LAND_AV && r.LAND_AV > 0 ? r.LAND_AV : undefined,
          year_built: r.YR_BLT && r.YR_BLT > 1700 && r.YR_BLT < 2030 ? r.YR_BLT : undefined,
          total_sqft: sqft,
          land_sqft: lotSqft,
          construction_class: r.BLDG_STYLE_DESC || undefined,
          raw: {
            countyName: r.COUNTY_NAME,
            propClass: r.PROP_CLASS,
            bedrooms: r.NBR_BEDROOMS,
            bathrooms: r.NBR_FULL_BATHS,
            acres: r.ACRES,
          },
        };

        progress.total_processed++;
        if (progress.total_processed % 10000 === 0) {
          console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
          onProgress?.(progress);
        }

        yield record;
      }

      offset += records.length;

      // Small delay to be polite to the server
      if (offset % 50000 === 0) {
        await new Promise(r => setTimeout(r, 500));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
