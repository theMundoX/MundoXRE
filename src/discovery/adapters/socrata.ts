/**
 * Generic Socrata SODA API Adapter
 *
 * Handles ANY county/city that publishes property data on a Socrata portal
 * (data.{city/county}.gov). Uses the SODA v2 API for pagination and filtering.
 *
 * Socrata API docs: https://dev.socrata.com/docs/endpoints.html
 *
 * No authentication needed for most public datasets.
 * Pagination via $offset with configurable page size (default 50K rows).
 * Rate limit: polite 1 req/sec (handled by the shared rate limiter).
 */

import {
  AssessorAdapter,
  type CountyConfig,
  type RawPropertyRecord,
  type AdapterProgress,
} from "./base.js";
import { waitForSlot, backoffDomain } from "../../utils/rate-limiter.js";

// ─── Types ───────────────────────────────────────────────────────────

/** Extended config with Socrata-specific fields stored in search_params / field_map */
interface SocrataConfig extends CountyConfig {
  /** search_params must contain at least: dataset_id */
  search_params: {
    dataset_id: string;
    /** Optional secondary dataset to join (e.g., assessed values) */
    secondary_dataset_id?: string;
    /** Optional $order column for deterministic pagination */
    order_by?: string;
    /** Optional $where filter (e.g., "year = '2024'") */
    where?: string;
    /** Rows per page (default: 50000) */
    page_size?: string;
    /** Max pages to fetch (default: 200 = 10M rows) */
    max_pages?: string;
  };
  /**
   * Maps source field names to our canonical RawPropertyRecord fields.
   * Key = source column, Value = canonical field name.
   * If not provided, COMMON_FIELD_MAPPINGS is used as a fallback.
   */
  field_map?: Record<string, string>;
}

/**
 * Common Socrata property field name variations mapped to our canonical names.
 * Applied as a fallback when a county config doesn't specify its own field_map.
 * The adapter checks each source field against this map.
 */
const COMMON_FIELD_MAPPINGS: Record<string, string> = {
  // Parcel ID
  pin: "parcel_id",
  pin10: "parcel_id",
  parcel_number: "parcel_id",
  parcel_id: "parcel_id",
  parcel_num: "parcel_id",
  parid: "parcel_id",
  bbl: "parcel_id",
  apn: "parcel_id",
  ssl: "parcel_id",
  tax_id: "parcel_id",

  // Address
  address: "address",
  property_address: "address",
  location: "address",
  property_location: "address",
  situs_address: "address",
  premise_address: "address",
  full_address: "address",

  // City
  city: "city",
  property_city: "city",
  municipality: "city",
  township_name: "city",
  cook_municipality_name: "city",

  // Zip
  zip: "zip",
  zip_code: "zip",
  zipcode: "zip",
  property_zip: "zip",
  postal_code: "zip",

  // Owner
  owner: "owner_name",
  owner_name: "owner_name",
  ownername: "owner_name",
  owner1: "owner_name",

  // Assessed value
  assessed_value: "assessed_value",
  total_assessed: "assessed_value",
  assesstot: "assessed_value",
  certified_tot: "assessed_value",
  assessed_total_value: "assessed_value",
  assessed_improvement_value: "assessed_improvement_value",
  assessed_land_value: "assessed_land_value",
  assessland: "land_value",

  // Market value
  market_value: "market_value",
  total_market: "market_value",
  full_market_value: "market_value",
  current_total: "market_value",

  // Year built
  year_built: "year_built",
  yearbuilt: "year_built",
  yr_built: "year_built",
  year_property_built: "year_built",

  // Square feet
  square_feet: "total_sqft",
  building_sq_ft: "total_sqft",
  building_square_footage: "total_sqft",
  bldgarea: "total_sqft",
  gross_sqft: "total_sqft",
  property_area: "total_sqft",
  total_area: "total_sqft",
  living_area: "total_sqft",

  // Land sqft
  lot_area: "land_sqft",
  lotarea: "land_sqft",
  land_square_footage: "land_sqft",
  land_area: "land_sqft",
  lot_size: "land_sqft",

  // Property type / class
  property_class: "property_type",
  property_type: "property_type",
  class: "property_type",
  bldgclass: "property_type",
  use_code: "property_type",
  land_use: "property_type",
  landuse: "property_type",
  property_class_code: "property_type",
  use_definition: "property_type_desc",

  // Sale info
  sale_price: "last_sale_price",
  last_sale_price: "last_sale_price",
  recent_sale_price: "last_sale_price",
  sale_date: "last_sale_date",
  last_sale_date: "last_sale_date",
  recent_sale_date: "last_sale_date",

  // Units / stories
  units_total: "total_units",
  unitstotal: "total_units",
  unitsres: "residential_units",
  num_units: "total_units",
  number_of_units: "total_units",
  num_stories: "stories",
  numfloors: "stories",
  number_of_stories: "stories",
  stories: "stories",
  numbldgs: "total_buildings",

  // Exemptions / tax
  exempttot: "exempt_value",
  property_tax: "property_tax",
  tax_amount: "property_tax",

  // Legal
  legal_description: "legal_description",

  // Coordinates
  latitude: "latitude",
  longitude: "longitude",
  lat: "latitude",
  lon: "longitude",
};

/** Numeric fields that should be parsed from strings */
const NUMERIC_FIELDS = new Set([
  "assessed_value",
  "assessed_improvement_value",
  "assessed_land_value",
  "market_value",
  "land_value",
  "year_built",
  "total_sqft",
  "land_sqft",
  "total_units",
  "residential_units",
  "stories",
  "total_buildings",
  "last_sale_price",
  "exempt_value",
  "property_tax",
  "taxable_value",
  "latitude",
  "longitude",
]);

// ─── Adapter ─────────────────────────────────────────────────────────

export class SocrataAdapter extends AssessorAdapter {
  readonly platform = "socrata";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "socrata";
  }

  private getSocrataConfig(config: CountyConfig): SocrataConfig {
    if (!config.search_params?.dataset_id) {
      throw new Error(`Socrata config for ${config.name} missing search_params.dataset_id`);
    }
    return config as SocrataConfig;
  }

  private buildApiUrl(baseUrl: string, datasetId: string): string {
    // Normalize: strip trailing slash, build resource URL
    const base = baseUrl.replace(/\/$/, "");
    return `${base}/resource/${datasetId}.json`;
  }

  /**
   * Map a raw Socrata record to our canonical fields using the config's field_map
   * or falling back to COMMON_FIELD_MAPPINGS.
   */
  private mapRecord(
    raw: Record<string, unknown>,
    fieldMap: Record<string, string> | undefined,
    config: SocrataConfig,
  ): RawPropertyRecord | null {
    // Build effective mapping: config overrides > common fallback
    const effectiveMap = fieldMap ?? COMMON_FIELD_MAPPINGS;

    const mapped: Record<string, unknown> = {};

    for (const [sourceField, value] of Object.entries(raw)) {
      const canonicalField = effectiveMap[sourceField.toLowerCase()];
      if (canonicalField && value != null && value !== "") {
        // Only set if not already set (first match wins for multi-mapping)
        if (mapped[canonicalField] === undefined) {
          mapped[canonicalField] = value;
        }
      }
    }

    // parcel_id and address are required minimums
    const parcelId = String(mapped.parcel_id ?? "").trim();
    const address = String(mapped.address ?? "").trim();

    if (!parcelId && !address) return null;

    // Parse numeric fields
    const num = (key: string): number | undefined => {
      const v = mapped[key];
      if (v == null) return undefined;
      const n = typeof v === "number" ? v : parseFloat(String(v));
      return isNaN(n) ? undefined : n;
    };

    const str = (key: string): string | undefined => {
      const v = mapped[key];
      if (v == null || v === "") return undefined;
      return String(v).trim();
    };

    return {
      parcel_id: parcelId,
      address: address,
      city: str("city")?.toUpperCase() ?? "",
      state: config.state,
      zip: str("zip") ?? "",
      owner_name: str("owner_name"),
      property_type: str("property_type") ?? str("property_type_desc"),
      assessed_value: num("assessed_value"),
      market_value: num("market_value"),
      taxable_value: num("taxable_value"),
      land_value: num("land_value") ?? num("assessed_land_value"),
      property_tax: num("property_tax"),
      year_built: num("year_built"),
      total_sqft: num("total_sqft"),
      total_units: num("total_units"),
      total_buildings: num("total_buildings"),
      stories: num("stories"),
      last_sale_price: num("last_sale_price"),
      last_sale_date: str("last_sale_date"),
      land_sqft: num("land_sqft"),
      legal_description: str("legal_description"),
      assessor_url: undefined, // Can be overridden per-county in post-processing
      raw,
    };
  }

  async estimateCount(config: CountyConfig): Promise<number | null> {
    const sc = this.getSocrataConfig(config);
    const apiUrl = this.buildApiUrl(sc.base_url, sc.search_params.dataset_id);

    try {
      await waitForSlot(apiUrl);

      const countUrl = `${apiUrl}?$select=count(*)`;
      const response = await fetch(countUrl, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(30_000),
      });

      if (!response.ok) return null;

      const data = (await response.json()) as Record<string, string>[];
      if (data.length > 0) {
        // Socrata returns count as count_* or count
        const countVal = Object.values(data[0])[0];
        return parseInt(countVal) || null;
      }
    } catch {
      // Silently fail — estimation is optional
    }

    return null;
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const sc = this.getSocrataConfig(config);
    const params = sc.search_params;

    const apiUrl = this.buildApiUrl(sc.base_url, params.dataset_id);
    const pageSize = parseInt(params.page_size ?? "50000") || 50000;
    const maxPages = parseInt(params.max_pages ?? "200") || 200;
    const orderBy = params.order_by;
    const whereClause = params.where;

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    let offset = 0;
    let hasMore = true;
    let consecutiveErrors = 0;

    console.log(`  [socrata] ${config.name}, ${config.state}: fetching from ${sc.base_url}`);
    console.log(`  [socrata] Dataset: ${params.dataset_id}, page size: ${pageSize}`);

    while (hasMore && offset / pageSize < maxPages) {
      await waitForSlot(apiUrl);

      try {
        // Build query URL
        const qp = new URLSearchParams();
        qp.set("$limit", String(pageSize));
        qp.set("$offset", String(offset));
        if (orderBy) qp.set("$order", orderBy);
        if (whereClause) qp.set("$where", whereClause);

        const url = `${apiUrl}?${qp.toString()}`;

        const response = await fetch(url, {
          headers: { Accept: "application/json" },
          signal: AbortSignal.timeout(120_000), // 2 min for large pages
        });

        if (!response.ok) {
          console.error(`  [socrata] API error ${response.status} at offset ${offset}`);
          progress.errors++;
          consecutiveErrors++;
          backoffDomain(apiUrl);

          if (consecutiveErrors >= 3) {
            console.error(`  [socrata] Too many consecutive errors, stopping`);
            break;
          }

          // Wait and retry
          await new Promise((r) => setTimeout(r, 5000 * consecutiveErrors));
          continue;
        }

        consecutiveErrors = 0;
        const rows = (await response.json()) as Record<string, unknown>[];

        if (!rows || rows.length === 0) {
          hasMore = false;
          break;
        }

        for (const row of rows) {
          progress.total_found++;

          const record = this.mapRecord(row, sc.field_map, sc);
          if (!record) continue;

          progress.total_processed++;

          if (progress.total_processed % 25000 === 0) {
            console.log(
              `  [socrata] ${config.name}: ${progress.total_processed.toLocaleString()} processed (offset ${offset.toLocaleString()})`,
            );
            onProgress?.(progress);
          }

          yield record;
        }

        // If we got fewer rows than page size, we've reached the end
        if (rows.length < pageSize) {
          hasMore = false;
        }

        offset += pageSize;
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Unknown error";
        console.error(`  [socrata] Fetch error at offset ${offset}: ${msg}`);
        progress.errors++;
        consecutiveErrors++;
        backoffDomain(apiUrl);

        if (consecutiveErrors >= 3) {
          console.error(`  [socrata] Too many consecutive errors, stopping`);
          break;
        }

        await new Promise((r) => setTimeout(r, 5000 * consecutiveErrors));
      }
    }

    console.log(
      `  [socrata] ${config.name}: done — ${progress.total_found.toLocaleString()} found, ` +
        `${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
