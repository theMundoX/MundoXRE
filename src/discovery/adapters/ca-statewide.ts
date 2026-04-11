/**
 * California Statewide Parcel Adapter
 *
 * California has no single statewide parcel API. This adapter handles multiple
 * county-level ArcGIS FeatureServer and Socrata endpoints.
 *
 * Confirmed data sources:
 *  - Los Angeles County (ArcGIS, 2.4M parcels, 2025 roll year)
 *  - Alameda County (ArcGIS, 490K parcels)
 *  - Sacramento County (ArcGIS, 501K parcels)
 *  - San Francisco County (Socrata, 212K parcels)
 *  - Napa County (ArcGIS, 51K parcels)
 *  - Ventura County (ArcGIS, 268K parcels)
 *  - Kern County (ArcGIS, 422K parcels)
 *
 * Each county config's search_params.county_source selects which source to use.
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

const PAGE_SIZE = 2000;
const MAX_RETRIES = 5;

// CA county FIPS codes (state FIPS = 06)
export const CA_COUNTY_FIPS: Record<string, string> = {
  "Alameda": "001", "Alpine": "003", "Amador": "005", "Butte": "007",
  "Calaveras": "009", "Colusa": "011", "Contra Costa": "013", "Del Norte": "015",
  "El Dorado": "017", "Fresno": "019", "Glenn": "021", "Humboldt": "023",
  "Imperial": "025", "Inyo": "027", "Kern": "029", "Kings": "031",
  "Lake": "033", "Lassen": "035", "Los Angeles": "037", "Madera": "039",
  "Marin": "041", "Mariposa": "043", "Mendocino": "045", "Merced": "047",
  "Modoc": "049", "Mono": "051", "Monterey": "053", "Napa": "055",
  "Nevada": "057", "Orange": "059", "Placer": "061", "Plumas": "063",
  "Riverside": "065", "Sacramento": "067", "San Benito": "069",
  "San Bernardino": "071", "San Diego": "073", "San Francisco": "075",
  "San Joaquin": "077", "San Luis Obispo": "079", "San Mateo": "081",
  "Santa Barbara": "083", "Santa Clara": "085", "Santa Cruz": "087",
  "Shasta": "089", "Sierra": "091", "Siskiyou": "093", "Solano": "095",
  "Sonoma": "097", "Stanislaus": "099", "Sutter": "101", "Tehama": "103",
  "Trinity": "105", "Tulare": "107", "Tuolumne": "109", "Ventura": "111",
  "Yolo": "113", "Yuba": "115",
};

// ─── County source configs ──────────────────────────────────────────

interface CACountySource {
  type: "arcgis" | "socrata";
  serviceUrl: string;
  layerId?: number;
  outFields: string;
  extraWhere?: string;
  mapRecord: (r: any, countyName: string) => RawPropertyRecord | null;
}

function classifyLandUse(code: string, desc: string): string {
  const c = (code || "").toLowerCase();
  const d = (desc || "").toLowerCase();
  const combined = `${c} ${d}`;
  if (combined.match(/single.?fam|sfr|resid.*01|^0100|single family/)) return "single_family";
  if (combined.match(/multi|apart|duplex|triplex|fourplex|2.?fam|3.?fam|4.?fam/)) return "multifamily";
  if (combined.match(/condo|townho/)) return "condo";
  if (combined.match(/commerc|office|retail|store|shop/)) return "commercial";
  if (combined.match(/industr|warehouse|manufact/)) return "industrial";
  if (combined.match(/vacan|land|lot|agri|farm|ranch|timber|forest|pasture/)) return "land";
  if (combined.match(/exempt|govern|school|church|relig|hospital|park/)) return "exempt";
  return "residential";
}

const CA_COUNTY_SOURCES: Record<string, CACountySource> = {
  "Los Angeles": {
    type: "arcgis",
    serviceUrl: "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Parcel_Data_2021_Table/FeatureServer",
    layerId: 0,
    outFields: "AIN,PropertyLocation,SitusHouseNo,SitusFraction,SitusDirection,SitusStreet,SitusUnit,SitusCity,SitusZIP5,UseType,UseCode,YearBuilt,SQFTmain,Units,Roll_TotalValue,Roll_LandValue,Roll_ImpValue,netTaxableValue,RecordingDate,totBuildingDataLines,CENTER_LAT,CENTER_LON",
    extraWhere: "RollYear='2025'",
    mapRecord: (r, _county) => {
      const houseNo = r.SitusHouseNo ? String(r.SitusHouseNo) : "";
      const fraction = r.SitusFraction ? ` ${r.SitusFraction}` : "";
      const direction = r.SitusDirection ? ` ${r.SitusDirection}` : "";
      const street = r.SitusStreet ? ` ${r.SitusStreet}` : "";
      const unit = r.SitusUnit ? ` ${r.SitusUnit}` : "";
      const address = houseNo
        ? `${houseNo}${fraction}${direction}${street}${unit}`.trim()
        : (r.PropertyLocation || "").split("  ")[0].trim();
      if (!address) return null;

      const city = (r.SitusCity || "").replace(/\s+CA\s*$/, "").trim();
      let yearBuilt: number | undefined;
      if (r.YearBuilt) {
        const y = parseInt(String(r.YearBuilt));
        if (y > 1700 && y <= 2030) yearBuilt = y;
      }
      let saleDate: string | undefined;
      if (r.RecordingDate) {
        try {
          const d = new Date(r.RecordingDate);
          if (!isNaN(d.getTime()) && d.getFullYear() > 1900) saleDate = d.toISOString().substring(0, 10);
        } catch { /* ignore */ }
      }

      return {
        parcel_id: r.AIN || "",
        address: address || r.PropertyLocation || "",
        city: city || "LOS ANGELES",
        state: "CA",
        zip: (r.SitusZIP5 || "").substring(0, 5),
        property_type: classifyLandUse(r.UseCode || "", r.UseType || ""),
        year_built: yearBuilt,
        total_sqft: r.SQFTmain && r.SQFTmain > 0 ? r.SQFTmain : undefined,
        total_units: r.Units && r.Units > 0 ? r.Units : undefined,
        assessed_value: r.Roll_TotalValue && r.Roll_TotalValue > 0 ? r.Roll_TotalValue : undefined,
        land_value: r.Roll_LandValue && r.Roll_LandValue > 0 ? r.Roll_LandValue : undefined,
        taxable_value: r.netTaxableValue && r.netTaxableValue > 0 ? r.netTaxableValue : undefined,
        total_buildings: r.totBuildingDataLines && r.totBuildingDataLines > 0 ? r.totBuildingDataLines : undefined,
        last_sale_date: saleDate,
        raw: {
          useCode: r.UseCode, useType: r.UseType,
          lat: r.CENTER_LAT, lng: r.CENTER_LON,
          impValue: r.Roll_ImpValue,
        },
      };
    },
  },
  "Alameda": {
    type: "arcgis",
    serviceUrl: "https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/Parcels/FeatureServer",
    layerId: 0,
    outFields: "APN,SitusAddress,SitusCity,SitusZip,Land,Imps,TotalNetValue,UseCode,EconomicUnit",
    mapRecord: (r, _county) => {
      const address = (r.SitusAddress || "").trim();
      if (!address) return null;
      return {
        parcel_id: r.APN || "",
        address,
        city: (r.SitusCity || "").trim() || "ALAMEDA COUNTY",
        state: "CA",
        zip: (r.SitusZip || "").substring(0, 5),
        property_type: classifyLandUse(r.UseCode || "", ""),
        assessed_value: r.TotalNetValue && r.TotalNetValue > 0 ? r.TotalNetValue : undefined,
        land_value: r.Land && r.Land > 0 ? r.Land : undefined,
        market_value: (r.Land || 0) + (r.Imps || 0) > 0 ? (r.Land || 0) + (r.Imps || 0) : undefined,
        raw: { useCode: r.UseCode, economicUnit: r.EconomicUnit },
      };
    },
  },
  "Sacramento": {
    type: "arcgis",
    serviceUrl: "https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer",
    layerId: 0,
    outFields: "APN,STREET_NBR,STREET_NAM,CITY,ZIP,LU_GENERAL,LU_SPECIF,LANDUSE",
    mapRecord: (r, _county) => {
      const streetNbr = r.STREET_NBR ? String(r.STREET_NBR).trim() : "";
      const streetNam = (r.STREET_NAM || "").trim();
      const address = streetNbr && streetNam ? `${streetNbr} ${streetNam}` : (streetNbr || streetNam);
      if (!address) return null;
      return {
        parcel_id: r.APN || "",
        address,
        city: (r.CITY || "SACRAMENTO").trim(),
        state: "CA",
        zip: r.ZIP ? String(r.ZIP).substring(0, 5).padStart(5, "0") : "",
        property_type: classifyLandUse(r.LANDUSE || "", r.LU_GENERAL || ""),
        raw: { luGeneral: r.LU_GENERAL, luSpecific: r.LU_SPECIF, landuse: r.LANDUSE },
      };
    },
  },
  "San Francisco": {
    type: "socrata",
    serviceUrl: "https://data.sfgov.org/resource/wv5m-vpq2.json",
    outFields: "parcel_number,property_location,use_code,use_definition,year_property_built,number_of_units,property_area,lot_area,assessed_improvement_value,assessed_land_value,the_geom,closed_roll_year",
    extraWhere: "closed_roll_year='2024'",
    mapRecord: (r, _county) => {
      const address = (r.property_location || "").trim();
      if (!address) return null;
      let yearBuilt: number | undefined;
      if (r.year_property_built) {
        const y = parseInt(r.year_property_built);
        if (y > 1700 && y <= 2030) yearBuilt = y;
      }
      const assessedLand = parseFloat(r.assessed_land_value) || undefined;
      const assessedImpr = parseFloat(r.assessed_improvement_value) || undefined;
      const totalAssessed = (assessedLand || 0) + (assessedImpr || 0);
      return {
        parcel_id: r.parcel_number || "",
        address,
        city: "SAN FRANCISCO",
        state: "CA",
        zip: "",
        property_type: classifyLandUse(r.use_code || "", r.use_definition || ""),
        year_built: yearBuilt,
        total_units: r.number_of_units ? parseInt(r.number_of_units) || undefined : undefined,
        total_sqft: r.property_area ? parseInt(r.property_area) || undefined : undefined,
        assessed_value: totalAssessed > 0 ? totalAssessed : undefined,
        land_value: assessedLand,
        land_sqft: r.lot_area ? parseInt(r.lot_area) || undefined : undefined,
        raw: {
          useCode: r.use_code, useDefinition: r.use_definition,
          lat: r.the_geom?.coordinates?.[1],
          lng: r.the_geom?.coordinates?.[0],
        },
      };
    },
  },
  "Napa": {
    type: "arcgis",
    serviceUrl: "https://services1.arcgis.com/Uw4rsm9qOgyMMPnh/arcgis/rest/services/Parcels/FeatureServer",
    layerId: 0,
    outFields: "APN,SITUS,CITY,ZIP,OWNER,LANDUSE,LANDVAL,IMPVAL,TOTALVAL",
    mapRecord: (r, _county) => {
      const address = (r.SITUS || "").trim();
      if (!address) return null;
      return {
        parcel_id: r.APN || "",
        address,
        city: (r.CITY || "NAPA").trim(),
        state: "CA",
        zip: (r.ZIP || "").substring(0, 5),
        owner_name: (r.OWNER || "").trim() || undefined,
        property_type: classifyLandUse(r.LANDUSE || "", ""),
        assessed_value: r.TOTALVAL && r.TOTALVAL > 0 ? r.TOTALVAL : undefined,
        land_value: r.LANDVAL && r.LANDVAL > 0 ? r.LANDVAL : undefined,
        raw: { landuse: r.LANDUSE, impVal: r.IMPVAL },
      };
    },
  },
  "Ventura": {
    type: "arcgis",
    serviceUrl: "https://maps.ventura.org/arcgis/rest/services/Parcels/MapServer",
    layerId: 0,
    outFields: "APN,SITUS,APN_LABEL",
    mapRecord: (r, _county) => {
      const address = (r.SITUS || "").trim();
      if (!address) return null;
      return {
        parcel_id: r.APN || r.APN_LABEL || "",
        address,
        city: "VENTURA COUNTY",
        state: "CA",
        zip: "",
        raw: { apnLabel: r.APN_LABEL },
      };
    },
  },
  "Kern": {
    type: "arcgis",
    serviceUrl: "https://maps.kerncounty.com/kcgis/rest/services/ParcelsPublic/MapServer",
    layerId: 0,
    outFields: "APN,SITUS_ADDR,SITUS_CITY,SITUS_ZIP",
    mapRecord: (r, _county) => {
      const address = (r.SITUS_ADDR || "").trim();
      if (!address) return null;
      return {
        parcel_id: r.APN || "",
        address,
        city: (r.SITUS_CITY || "KERN COUNTY").trim(),
        state: "CA",
        zip: (r.SITUS_ZIP || "").substring(0, 5),
        raw: {},
      };
    },
  },
};

// ─── ArcGIS fetch helpers ───────────────────────────────────────────

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "MXRE-Adapter/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const json = await resp.json();
    if ((json as any).error) throw new Error((json as any).error.message || JSON.stringify((json as any).error));
    return json;
  } catch (err: any) {
    if (attempt >= MAX_RETRIES) throw err;
    const delay = Math.min(3000 * Math.pow(2, attempt - 1), 60_000) + Math.random() * 2000;
    console.log(`    Retry ${attempt}/${MAX_RETRIES}: ${err.message}`);
    await new Promise(r => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

async function arcgisFetchPage(serviceUrl: string, layerId: number, offset: number, outFields: string, extraWhere = "1=1"): Promise<any[]> {
  const where = encodeURIComponent(extraWhere);
  const fields = encodeURIComponent(outFields);
  const url = `${serviceUrl}/${layerId}/query?where=${where}&outFields=${fields}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;
  const data = await fetchWithRetry(url);
  return data.features?.map((f: any) => f.attributes) ?? [];
}

async function arcgisGetCount(serviceUrl: string, layerId: number, where = "1=1"): Promise<number> {
  const url = `${serviceUrl}/${layerId}/query?where=${encodeURIComponent(where)}&returnCountOnly=true&f=json`;
  const data = await fetchWithRetry(url);
  return data.count || 0;
}

// ─── Socrata fetch ──────────────────────────────────────────────────

async function socrataFetchPage(apiUrl: string, fields: string, where: string, offset: number): Promise<any[]> {
  const url = `${apiUrl}?$select=${encodeURIComponent(fields)}&$where=${encodeURIComponent(where)}&$limit=${PAGE_SIZE}&$offset=${offset}&$order=parcel_number`;
  const resp = await fetch(url, {
    headers: { "User-Agent": "MXRE-Adapter/1.0" },
    signal: AbortSignal.timeout(120_000),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return await resp.json() as any[];
}

async function socrataGetCount(apiUrl: string, where: string): Promise<number> {
  const resp = await fetchWithRetry(`${apiUrl}?$select=count(*)&$where=${encodeURIComponent(where)}`);
  return parseInt(resp[0]?.count || "0");
}

// ─── Adapter ────────────────────────────────────────────────────────

export class CAStatewideAdapter extends AssessorAdapter {
  readonly platform = "ca_statewide";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "ca_statewide";
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    const countyName = config.name;
    const source = CA_COUNTY_SOURCES[countyName];
    if (!source) return null;

    try {
      if (source.type === "arcgis") {
        return await arcgisGetCount(source.serviceUrl, source.layerId ?? 0, source.extraWhere || "1=1");
      } else {
        return await socrataGetCount(source.serviceUrl, source.extraWhere || "1=1");
      }
    } catch {
      return null;
    }
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const countyName = config.name;
    // If search_params.county_source is specified, use that; otherwise use config.name
    const sourceName = config.search_params?.county_source || countyName;
    const source = CA_COUNTY_SOURCES[sourceName];

    if (!source) {
      console.error(`  CA adapter: no source configured for county "${sourceName}"`);
      console.error(`  Available: ${Object.keys(CA_COUNTY_SOURCES).join(", ")}`);
      return;
    }

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
        if (source.type === "arcgis") {
          records = await arcgisFetchPage(
            source.serviceUrl, source.layerId ?? 0, offset,
            source.outFields, source.extraWhere || "1=1",
          );
        } else {
          records = await socrataFetchPage(
            source.serviceUrl, source.outFields,
            source.extraWhere || "1=1", offset,
          );
        }
      } catch (err: any) {
        console.error(`  Page error at offset ${offset}: ${err.message}`);
        progress.errors++;
        offset += PAGE_SIZE;
        if (totalCount !== null && offset >= totalCount) break;
        continue;
      }

      if (records.length === 0) break;

      for (const r of records) {
        const mapped = source.mapRecord(r, countyName);
        if (!mapped) continue;

        progress.total_processed++;
        if (progress.total_processed % 10000 === 0) {
          console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
          onProgress?.(progress);
        }

        yield mapped;
      }

      offset += records.length;

      if (offset % 20000 === 0) {
        await new Promise(r => setTimeout(r, 300));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
