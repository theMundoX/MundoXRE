/**
 * Pennsylvania Statewide Parcel Adapter
 *
 * Imports property data from PA DEP MapServer (ArcGIS REST API).
 * Source: https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0
 *
 * ~4.7M parcels across 67 counties, paginated at 1000 records per request.
 * Fields: PARCEL_ID, OWNER_NAME, OWNER_LAST_NAME, OWNER_FIRST_NAME,
 *         PROPERTY_ADDRESS_1, PROPERTY_ADDRESS_2, CITY, STATE, ZIP,
 *         COUNTY_NAME, COUNTY_CODE, DISTRICT, ACREAGE, ACCOUNT
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

const SERVICE_URL = "https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0";
const OUT_FIELDS = "PARCEL_ID,OWNER_NAME,OWNER_LAST_NAME,OWNER_FIRST_NAME,PROPERTY_ADDRESS_1,PROPERTY_ADDRESS_2,CITY,STATE,ZIP,COUNTY_NAME,COUNTY_CODE,DISTRICT,ACREAGE,ACCOUNT";
const PAGE_SIZE = 1000;
const MAX_RETRIES = 5;

// PA county FIPS codes (state FIPS = 42)
export const PA_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ALLEGHENY": "003", "ARMSTRONG": "005", "BEAVER": "007",
  "BEDFORD": "009", "BERKS": "011", "BLAIR": "013", "BRADFORD": "015",
  "BUCKS": "017", "BUTLER": "019", "CAMBRIA": "021", "CAMERON": "023",
  "CARBON": "025", "CENTRE": "027", "CHESTER": "029", "CLARION": "031",
  "CLEARFIELD": "033", "CLINTON": "035", "COLUMBIA": "037", "CRAWFORD": "039",
  "CUMBERLAND": "041", "DAUPHIN": "043", "DELAWARE": "045", "ELK": "047",
  "ERIE": "049", "FAYETTE": "051", "FOREST": "053", "FRANKLIN": "055",
  "FULTON": "057", "GREENE": "059", "HUNTINGDON": "061", "INDIANA": "063",
  "JEFFERSON": "065", "JUNIATA": "067", "LACKAWANNA": "069", "LANCASTER": "071",
  "LAWRENCE": "073", "LEBANON": "075", "LEHIGH": "077", "LUZERNE": "079",
  "LYCOMING": "081", "MCKEAN": "083", "MERCER": "085", "MIFFLIN": "087",
  "MONROE": "089", "MONTGOMERY": "091", "MONTOUR": "093", "NORTHAMPTON": "095",
  "NORTHUMBERLAND": "097", "PERRY": "099", "PHILADELPHIA": "101", "PIKE": "103",
  "POTTER": "105", "SCHUYLKILL": "107", "SNYDER": "109", "SOMERSET": "111",
  "SULLIVAN": "113", "SUSQUEHANNA": "115", "TIOGA": "117", "UNION": "119",
  "VENANGO": "121", "WARREN": "123", "WASHINGTON": "125", "WAYNE": "127",
  "WESTMORELAND": "129", "WYOMING": "131", "YORK": "133",
};

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

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

export class PAStatewideAdapter extends AssessorAdapter {
  readonly platform = "pa_statewide";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "pa_statewide";
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
        const address = [r.PROPERTY_ADDRESS_1, r.PROPERTY_ADDRESS_2]
          .filter(Boolean)
          .join(" ")
          .trim();
        if (!address) continue;

        const ownerName = r.OWNER_NAME ||
          [r.OWNER_LAST_NAME, r.OWNER_FIRST_NAME].filter(Boolean).join(", ") || undefined;

        const acreage = r.ACREAGE ? parseFloat(r.ACREAGE) : undefined;
        const landSqft = acreage && acreage > 0 ? Math.round(acreage * 43560) : undefined;

        const record: RawPropertyRecord = {
          parcel_id: r.PARCEL_ID || r.ACCOUNT || "",
          address,
          city: r.CITY || "",
          state: "PA",
          zip: r.ZIP || "",
          owner_name: ownerName,
          land_sqft: landSqft,
          raw: {
            countyName: r.COUNTY_NAME ? titleCase(r.COUNTY_NAME.trim()) : undefined,
            countyCode: r.COUNTY_CODE,
            district: r.DISTRICT,
            acreage: acreage,
            account: r.ACCOUNT,
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

      if (offset % 50000 === 0) {
        await new Promise(r => setTimeout(r, 500));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
