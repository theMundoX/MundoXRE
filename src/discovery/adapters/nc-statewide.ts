/**
 * North Carolina Statewide Parcel Adapter
 *
 * Imports property data from NC OneMap FeatureServer (ArcGIS REST API).
 * Source: https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0
 *
 * ~5.9M parcels across 100 counties, paginated at 5000 records per request.
 * Fields: parno, altparno, ownname, ownname2, improvval, landval, parval,
 *         siteadd, scity, szip, sstate, gisacres, parusedesc, parusecode,
 *         cntyname, cntyfips, stfips, structyear, structno, presentval
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

const BASE_URL = "https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0/query";
const OUT_FIELDS = "parno,altparno,ownname,ownname2,improvval,landval,parval,siteadd,scity,szip,sstate,gisacres,parusedesc,parusecode,cntyname,cntyfips,stfips,structyear,structno,presentval";
const PAGE_SIZE = 5000;
const MAX_RETRIES = 3;

// NC county FIPS codes (state FIPS = 37)
export const NC_COUNTY_FIPS: Record<string, string> = {
  "ALAMANCE": "001", "ALEXANDER": "003", "ALLEGHANY": "005", "ANSON": "007",
  "ASHE": "009", "AVERY": "011", "BEAUFORT": "013", "BERTIE": "015",
  "BLADEN": "017", "BRUNSWICK": "019", "BUNCOMBE": "021", "BURKE": "023",
  "CABARRUS": "025", "CALDWELL": "027", "CAMDEN": "029", "CARTERET": "031",
  "CASWELL": "033", "CATAWBA": "035", "CHATHAM": "037", "CHEROKEE": "039",
  "CHOWAN": "041", "CLAY": "043", "CLEVELAND": "045", "COLUMBUS": "047",
  "CRAVEN": "049", "CUMBERLAND": "051", "CURRITUCK": "053", "DARE": "055",
  "DAVIDSON": "057", "DAVIE": "059", "DUPLIN": "061", "DURHAM": "063",
  "EDGECOMBE": "065", "FORSYTH": "067", "FRANKLIN": "069", "GASTON": "071",
  "GATES": "073", "GRAHAM": "075", "GRANVILLE": "077", "GREENE": "079",
  "GUILFORD": "081", "HALIFAX": "083", "HARNETT": "085", "HAYWOOD": "087",
  "HENDERSON": "089", "HERTFORD": "091", "HOKE": "093", "HYDE": "095",
  "IREDELL": "097", "JACKSON": "099", "JOHNSTON": "101", "JONES": "103",
  "LEE": "105", "LENOIR": "107", "LINCOLN": "109", "MACON": "113",
  "MADISON": "115", "MARTIN": "117", "MCDOWELL": "111", "MECKLENBURG": "119",
  "MITCHELL": "121", "MONTGOMERY": "123", "MOORE": "125", "NASH": "127",
  "NEW HANOVER": "129", "NORTHAMPTON": "131", "ONSLOW": "133", "ORANGE": "135",
  "PAMLICO": "137", "PASQUOTANK": "139", "PENDER": "141", "PERQUIMANS": "143",
  "PERSON": "145", "PITT": "147", "POLK": "149", "RANDOLPH": "151",
  "RICHMOND": "153", "ROBESON": "155", "ROCKINGHAM": "157", "ROWAN": "159",
  "RUTHERFORD": "161", "SAMPSON": "163", "SCOTLAND": "165", "STANLY": "167",
  "STOKES": "169", "SURRY": "171", "SWAIN": "173", "TRANSYLVANIA": "175",
  "TYRRELL": "177", "UNION": "179", "VANCE": "181", "WAKE": "183",
  "WARREN": "185", "WASHINGTON": "187", "WATAUGA": "189", "WAYNE": "191",
  "WILKES": "193", "WILSON": "195", "YADKIN": "197", "YANCEY": "199",
};

function classifyPropertyType(useCode: string, useDesc: string): string {
  const code = (useCode || "").toUpperCase().trim();
  const desc = (useDesc || "").toUpperCase().trim();
  if (desc.includes("RESIDENTIAL") || desc.includes("SINGLE FAM") || code === "R") return "residential";
  if (desc.includes("COMMERCIAL") || code === "C") return "commercial";
  if (desc.includes("INDUSTRIAL") || code === "I") return "industrial";
  if (desc.includes("AGRICULTUR") || desc.includes("FARM") || code === "A") return "agricultural";
  if (desc.includes("APARTMENT") || desc.includes("MULTI") || code === "M") return "apartment";
  if (desc.includes("VACANT") || code === "V") return "vacant";
  if (desc.includes("EXEMPT") || code === "E") return "exempt";
  return "other";
}

async function fetchPage(offset: number, countyWhere?: string): Promise<any[]> {
  const params = new URLSearchParams({
    where: countyWhere || "1=1",
    outFields: OUT_FIELDS,
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}?${params}`, {
        headers: { "User-Agent": "MXRE-Adapter/1.0" },
        signal: AbortSignal.timeout(60000),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json() as any;
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) || [];
    } catch (err: any) {
      if (attempt === MAX_RETRIES - 1) throw err;
      console.error(`  Retry ${attempt + 1}: ${err.message}`);
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

export class NCStatewideAdapter extends AssessorAdapter {
  readonly platform = "nc_statewide";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "nc_statewide";
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    try {
      const countyName = config.name;
      const where = countyName
        ? `cntyname='${countyName.toUpperCase()}'`
        : "1=1";
      const params = new URLSearchParams({ where, returnCountOnly: "true", f: "json" });
      const res = await fetch(`${BASE_URL}?${params}`, {
        headers: { "User-Agent": "MXRE-Adapter/1.0" },
        signal: AbortSignal.timeout(30_000),
      });
      if (!res.ok) return null;
      const json = await res.json() as any;
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
      ? `cntyname='${countyName.toUpperCase()}'`
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

      for (const p of records) {
        const address = (p.siteadd || "").trim();
        if (!address) continue;

        const parcelId = (p.parno || p.altparno || "").trim();
        const totalVal = p.parval || p.presentval || undefined;
        const yearBuilt = p.structyear && p.structyear > 1700 && p.structyear < 2030
          ? p.structyear : undefined;
        const acres = p.gisacres && p.gisacres > 0 ? p.gisacres : undefined;

        const record: RawPropertyRecord = {
          parcel_id: parcelId,
          address,
          city: (p.scity || "").trim(),
          state: "NC",
          zip: (p.szip || "").trim(),
          owner_name: (p.ownname || "").trim() || undefined,
          property_type: classifyPropertyType(p.parusecode, p.parusedesc) || undefined,
          assessed_value: totalVal && totalVal > 0 ? totalVal : undefined,
          land_value: p.landval && p.landval > 0 ? p.landval : undefined,
          year_built: yearBuilt,
          total_buildings: p.structno && p.structno > 0 ? p.structno : undefined,
          land_sqft: acres ? Math.round(acres * 43560) : undefined,
          raw: {
            countyName: (p.cntyname || "").trim(),
            countyFips: p.cntyfips,
            stateFips: p.stfips,
            improvVal: p.improvval && p.improvval > 0 ? p.improvval : undefined,
            paruseCode: p.parusecode,
            paruseDesc: p.parusedesc,
            ownerName2: (p.ownname2 || "").trim() || undefined,
            altParno: (p.altparno || "").trim() || undefined,
            acres,
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
