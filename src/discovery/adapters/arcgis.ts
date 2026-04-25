/**
 * Generic ArcGIS REST API Adapter
 *
 * Scrapes parcel/property data from ANY county that exposes an ArcGIS REST
 * FeatureServer or MapServer endpoint. Hundreds of counties nationwide use this
 * pattern, so one adapter covers them all.
 *
 * ArcGIS REST query pattern:
 *   {base_url}/query?where=1=1&outFields=*&resultOffset={offset}&resultRecordCount=1000&f=json
 *
 * Pagination via resultOffset; server signals more pages with `exceededTransferLimit: true`.
 *
 * Per-county field name variations are handled via:
 *   1. A built-in map of common ArcGIS field name aliases
 *   2. An optional `field_map` in CountyConfig for county-specific overrides
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";
import { waitForSlot, backoffDomain, resetDomainRate } from "../../utils/rate-limiter.js";

const PAGE_SIZE = 1000;
const MAX_PAGES = 5000; // 5M records max safety limit
const RETRY_MAX = 5;
const RETRY_BASE_MS = 3000;

// ─── Common ArcGIS Field Name Aliases ────────────────────────────────

/** Maps our internal field name to an array of common ArcGIS attribute names (checked in order). */
const DEFAULT_FIELD_ALIASES: Record<string, string[]> = {
  address: [
    "SITEADDR", "SITE_ADDR", "SITUS", "ADDRESS", "PROP_ADDR", "PROPERTY_ADDRESS",
    "PROP_LOC", "PHYSICAL_ADDRESS", "SITUS_ADDRESS", "SITUS_ADDR", "FULL_ADDRESS",
    "STREET_ADDRESS", "LOCATION", "LOC_ADDR", "SITUSADDRESS", "SITE_ADDRESS",
    "situsAdd", "HOUSE_ADDR", "ADDR", "PROPADDR",
  ],
  owner_name: [
    "OWNER", "OWNER_NAME", "OWNERNAME", "OWNER1", "OWN_NAME", "OWNER_NAME1",
    "PRIMARY_OWNER", "GRANTEE", "TAXPAYER", "TAX_NAME", "Owner",
  ],
  parcel_id: [
    "PARCELID", "PARCEL_ID", "PIN", "APN", "PARCEL_NO", "PARCEL_NUM",
    "PARCELNUMBER", "PARCEL_NUMBER", "PARCEL", "PARID", "PID", "ACCT",
    "ACCOUNT", "AcctNumb", "PAMS_PIN", "GIS_PIN", "PARCEL_ID_NR", "PropID",
    "TAX_ID", "TAXID", "PCL_ID",
  ],
  city: [
    "SITUS_CITY", "CITY", "PROP_CITY", "PROPERTY_CITY", "PHYSICAL_CITY",
    "SITUS_CTY", "SITE_CITY", "sitAddCty", "MAIL_CITY", "MUNI",
    "MUNICIPALITY", "TOWN", "TOWNSHIP",
  ],
  zip: [
    "SITUS_ZIP", "ZIP", "ZIPCODE", "ZIP_CODE", "ZIP5", "PROP_ZIP",
    "PROPERTY_ZIP", "PHYSICAL_ZIP", "SITUS_ZP", "SITE_ZIP", "sitAddZip",
    "MAIL_ZIP", "POSTAL",
  ],
  market_value: [
    "TOTALVAL", "TOTAL_VALUE", "MKT_VALUE", "MARKET_VALUE", "TOT_VALUE",
    "TOTAL_MKT_VALUE", "APPRAISED_VALUE", "TOTAL_APPR", "TotVal", "FCV_CUR",
    "FULL_MARKET_VALUE", "TOTAL_VAL", "FAIR_MKT_VAL", "JUST_VALUE",
  ],
  assessed_value: [
    "ASSESSED", "ASSESSED_VALUE", "ASSD_VALUE", "ASSESSED_VAL", "ASDTOTAL",
    "TOTAL_ASSESSED", "ASSD_TOTAL", "NET_VALUE", "ASSESSED_TOTAL",
    "asedValTot", "TOTAL_ASSESSED_VALUE",
  ],
  year_built: [
    "YEARBUILT", "YEAR_BUILT", "YR_BLT", "YR_BUILT", "CONST_YEAR",
    "YEAR_CONST", "YR_CONSTR", "BUILT_YEAR", "EFFYR", "EFF_YR",
    "YrBlt", "BLDG_YEAR",
  ],
  total_sqft: [
    "SQFT", "TOTAL_SQFT", "BLDG_SQFT", "LIVING_AREA", "LIVING_SPACE",
    "HEATED_SQFT", "BLDG_SQ_FT", "GROSS_AREA", "GBA", "TOT_GBA",
    "BUILDING_SQFT", "BUILDING_SQUARE_FOOTAGE", "TOTAL_AREA", "FINISH_AREA",
    "HEATED_AREA",
  ],
  property_type: [
    "LANDUSE", "LAND_USE", "PROP_TYPE", "USE_CODE", "PROPERTY_TYPE",
    "LAND_USE_CODE", "CLASS", "PROP_CLASS", "ZONING", "PUC", "PropUse",
    "USE_TYPE", "PROPERTY_CLASS", "PROP_USE",
  ],
  taxable_value: [
    "TAXABLE", "TAXABLE_VALUE", "TAXABLE_VAL", "TAX_VALUE", "TAX_VAL",
    "NET_TAXABLE",
  ],
  last_sale_price: [
    "LAST_SALE_PRICE", "SALE_PRICE", "CONSIDAMT", "SALE_AMT", "SALEAMT",
    "RECENT_SALE_PRICE", "CONSIDERATION", "SALES_PRICE", "TRANSFER_PRICE",
    "salePrice", "SALE_PRICE1",
  ],
  last_sale_date: [
    "LAST_SALE_DATE", "SALE_DATE", "DEED_DATE", "TRANSFER_DATE",
    "RECENT_SALE_DATE", "SOLD_DATE", "DATE_SOLD", "SALE_DT", "saleDate",
  ],
  total_units: [
    "UNITS", "NUM_UNITS", "TOTAL_UNITS", "UNIT_CNT", "NO_UNITS",
    "DWELL_UNITS", "RES_UNITS",
  ],
  stories: [
    "STORIES", "NUM_STORIES", "NO_STORIES", "Stories", "STORY",
    "NUM_FLOORS", "FLOORS",
  ],
  land_value: [
    "LAND_VALUE", "LAND_VAL", "LANDVAL", "LandVal", "LAND_APPR",
    "LPV_CUR", "LAND_MKT",
  ],
  land_sqft: [
    "LAND_SQFT", "LAND_AREA", "LOT_SIZE", "LOT_SQFT", "LAND_SIZE",
    "ACREAGE", "ACRES", "LOT_AREA", "LAND_SF", "LglAcres",
  ],
  legal_description: [
    "LEGAL", "LEGAL_DESC", "LEGAL_DESCRIPTION", "LEGALDESC", "LglDesc",
    "LEGAL1",
  ],
};

// ─── Helpers ─────────────────────────────────────────────────────────

interface ArcGISResponse {
  features?: Array<{ attributes: Record<string, unknown> }>;
  exceededTransferLimit?: boolean;
  error?: { message?: string; code?: number; details?: string[] };
}

/**
 * Resolve a logical field name to the actual ArcGIS attribute name present
 * in the feature record.  Priority:
 *   1. Explicit field_map from CountyConfig
 *   2. DEFAULT_FIELD_ALIASES (first match wins)
 */
function resolveField(
  logicalName: string,
  attrs: Record<string, unknown>,
  fieldMap?: Record<string, string>,
): unknown {
  // 1. Check explicit field_map
  if (fieldMap?.[logicalName]) {
    const mapped = fieldMap[logicalName];
    if (mapped in attrs) return attrs[mapped];
  }

  // 2. Walk the default aliases
  const aliases = DEFAULT_FIELD_ALIASES[logicalName];
  if (aliases) {
    for (const alias of aliases) {
      if (alias in attrs) return attrs[alias];
    }
    // Case-insensitive fallback (ArcGIS field names vary in casing)
    const upperKeys = new Map<string, string>();
    for (const key of Object.keys(attrs)) {
      upperKeys.set(key.toUpperCase(), key);
    }
    for (const alias of aliases) {
      const realKey = upperKeys.get(alias.toUpperCase());
      if (realKey !== undefined) return attrs[realKey];
    }
  }

  return undefined;
}

function toStr(val: unknown): string {
  if (val === undefined || val === null) return "";
  return String(val).trim();
}

function toNum(val: unknown): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === "number" ? val : parseFloat(String(val).replace(/[$,\s]/g, ""));
  return isNaN(n) || n <= 0 ? undefined : Math.round(n);
}

function toInt(val: unknown): number | undefined {
  if (val === undefined || val === null) return undefined;
  const n = typeof val === "number" ? Math.round(val) : parseInt(String(val).replace(/[,\s]/g, ""), 10);
  return isNaN(n) || n <= 0 ? undefined : n;
}

/** Convert epoch ms, ISO strings, or MM/DD/YYYY to YYYY-MM-DD */
function toDateStr(val: unknown): string | undefined {
  if (val === undefined || val === null) return undefined;
  let d: Date;
  if (typeof val === "number") {
    // ArcGIS often stores dates as epoch milliseconds
    d = new Date(val);
  } else {
    const s = String(val).trim();
    if (!s) return undefined;
    d = new Date(s);
  }
  if (isNaN(d.getTime())) return undefined;
  return d.toISOString().split("T")[0];
}

/** Handle acres -> sqft conversion for land area */
function toLandSqft(val: unknown): number | undefined {
  const n = toNum(val);
  if (n === undefined) return undefined;
  // If value looks like acres (< 5000), convert to sqft
  if (n < 5000) return Math.round(n * 43560);
  return n;
}

async function fetchWithRetry(url: string, attempt = 1): Promise<ArcGISResponse> {
  try {
    const resp = await fetch(url, {
      headers: {
        "User-Agent": "MXRE-Ingester/1.0",
        Accept: "application/json",
      },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const json = (await resp.json()) as ArcGISResponse;
    if (json.error) {
      throw new Error(
        `ArcGIS error ${json.error.code || ""}: ${json.error.message || JSON.stringify(json.error)}`,
      );
    }
    return json;
  } catch (err: unknown) {
    if (attempt >= RETRY_MAX) throw err;
    const delay = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.random() * 2000;
    const msg = err instanceof Error ? err.message : "Unknown";
    console.log(`    Retry ${attempt}/${RETRY_MAX} in ${(delay / 1000).toFixed(1)}s: ${msg}`);
    await new Promise((r) => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

// ─── Adapter ─────────────────────────────────────────────────────────

export class ArcGISAdapter extends AssessorAdapter {
  readonly platform = "arcgis";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "arcgis";
  }

  private getWhereClause(config: CountyConfig): string {
    return config.search_params?.where_clause || "1=1";
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    try {
      const baseUrl = config.base_url.replace(/\/$/, "");
      const where = this.getWhereClause(config);
      const url = `${baseUrl}/query?where=${encodeURIComponent(where)}&returnCountOnly=true&f=json`;
      const data = await fetchWithRetry(url);
      return (data as unknown as { count?: number }).count ?? null;
    } catch {
      return null;
    }
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    const baseUrl = config.base_url.replace(/\/$/, "");
    const fieldMap = config.field_map;
    const whereClause = this.getWhereClause(config);

    // Get total count for progress reporting
    let totalCount: number | null = null;
    try {
      totalCount = await this.estimateCount(config);
      if (totalCount !== null) {
        console.log(`  ${config.name}: ~${totalCount.toLocaleString()} records estimated`);
      }
    } catch {
      // Non-fatal
    }

    let offset = 0;
    let hasMore = true;
    let consecutiveErrors = 0;

    console.log(`  Fetching parcels from ArcGIS REST: ${baseUrl}`);

    while (hasMore && offset / PAGE_SIZE < MAX_PAGES) {
      await waitForSlot(baseUrl);

      try {
        const queryUrl =
          `${baseUrl}/query?where=${encodeURIComponent(whereClause)}` +
          `&outFields=*` +
          `&returnGeometry=false` +
          `&resultOffset=${offset}` +
          `&resultRecordCount=${PAGE_SIZE}` +
          `&f=json`;

        const data = await fetchWithRetry(queryUrl);
        const features = data.features || [];

        if (features.length === 0) {
          hasMore = false;
          break;
        }

        consecutiveErrors = 0;
        resetDomainRate(baseUrl);

        for (const feature of features) {
          const attrs = feature.attributes;
          progress.total_found++;

          const address = toStr(resolveField("address", attrs, fieldMap));
          const parcelId = toStr(resolveField("parcel_id", attrs, fieldMap));

          // Skip records without an address or parcel ID
          if (!address && !parcelId) continue;

          const record: RawPropertyRecord = {
            parcel_id: parcelId,
            address: address,
            city: toStr(resolveField("city", attrs, fieldMap)).toUpperCase(),
            state: config.state,
            zip: toStr(resolveField("zip", attrs, fieldMap)).substring(0, 5),
            owner_name: toStr(resolveField("owner_name", attrs, fieldMap)) || undefined,
            property_type: toStr(resolveField("property_type", attrs, fieldMap)) || undefined,
            assessed_value: toNum(resolveField("assessed_value", attrs, fieldMap)),
            market_value: toNum(resolveField("market_value", attrs, fieldMap)),
            taxable_value: toNum(resolveField("taxable_value", attrs, fieldMap)),
            land_value: toNum(resolveField("land_value", attrs, fieldMap)),
            year_built: toInt(resolveField("year_built", attrs, fieldMap)),
            total_sqft: toInt(resolveField("total_sqft", attrs, fieldMap)),
            total_units: toInt(resolveField("total_units", attrs, fieldMap)),
            stories: toInt(resolveField("stories", attrs, fieldMap)),
            last_sale_price: toNum(resolveField("last_sale_price", attrs, fieldMap)),
            last_sale_date: toDateStr(resolveField("last_sale_date", attrs, fieldMap)),
            land_sqft: toLandSqft(resolveField("land_sqft", attrs, fieldMap)),
            lot_acres: resolveField("lot_acres", attrs, fieldMap) as string | number | undefined,
            legal_description: toStr(resolveField("legal_description", attrs, fieldMap)) || undefined,
            subdivision: toStr(resolveField("subdivision", attrs, fieldMap)) || undefined,
            neighborhood_code: toStr(resolveField("neighborhood_code", attrs, fieldMap)) || undefined,
            // Mailing address — absentee owner detection
            mailing_address: toStr(resolveField("mailing_address", attrs, fieldMap)) || undefined,
            mailing_city: toStr(resolveField("mailing_city", attrs, fieldMap)) || undefined,
            mailing_state: toStr(resolveField("mailing_state", attrs, fieldMap)) || undefined,
            mailing_zip: toStr(resolveField("mailing_zip", attrs, fieldMap)).substring(0, 5) || undefined,
            // Asset class signals
            property_class: toStr(resolveField("property_class", attrs, fieldMap)) || undefined,
            property_use: toStr(resolveField("property_use", attrs, fieldMap)) || undefined,
            appraised_land: toNum(resolveField("appraised_land", attrs, fieldMap)),
            appraised_building: toNum(resolveField("appraised_building", attrs, fieldMap)),
            assessor_url: undefined,
            raw: attrs as Record<string, unknown>,
          };

          progress.total_processed++;
          if (progress.total_processed % 10000 === 0) {
            const pct = totalCount ? ` (${((progress.total_processed / totalCount) * 100).toFixed(1)}%)` : "";
            console.log(
              `  Progress: ${progress.total_processed.toLocaleString()} processed${pct}`,
            );
            onProgress?.(progress);
          }

          yield record;
        }

        // Check if more pages exist
        hasMore = data.exceededTransferLimit === true || features.length === PAGE_SIZE;
        offset += PAGE_SIZE;
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Unknown";
        console.error(`  ArcGIS fetch error at offset ${offset}: ${msg}`);
        progress.errors++;
        consecutiveErrors++;
        backoffDomain(baseUrl);

        if (consecutiveErrors >= 5) {
          console.error(`  Too many consecutive errors, stopping ${config.name}`);
          break;
        }

        // Wait and retry the same offset
        await new Promise((r) => setTimeout(r, 5000));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_found.toLocaleString()} found, ` +
        `${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
