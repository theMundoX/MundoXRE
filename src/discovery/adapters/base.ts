/**
 * Base adapter interface for county assessor data sources.
 * Each platform adapter handles one website platform that may serve many counties.
 */

export interface CountyConfig {
  state_fips: string;
  county_fips: string;
  name: string;
  state: string;
  platform: string;
  base_url: string;
  alt_platform?: string;
  alt_url?: string;
  search_params?: Record<string, string>;
  field_map?: Record<string, string>;
}

export interface RawPropertyRecord {
  parcel_id: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  owner_name?: string;
  property_type?: string;
  assessed_value?: number;
  market_value?: number;
  taxable_value?: number;
  land_value?: number;
  property_tax?: number;
  year_built?: number;
  total_sqft?: number;
  total_units?: number;
  total_buildings?: number;
  stories?: number;
  last_sale_price?: number;
  last_sale_date?: string;
  construction_class?: string;
  improvement_quality?: string;
  land_sqft?: number;
  legal_description?: string;
  assessor_url?: string;
  raw: Record<string, unknown>;
}

export interface AdapterProgress {
  county: string;
  total_found: number;
  total_processed: number;
  errors: number;
  started_at: Date;
}

export abstract class AssessorAdapter {
  abstract readonly platform: string;

  abstract canHandle(config: CountyConfig): boolean;

  /**
   * Yields property records from the county assessor.
   * Implementations must handle pagination, rate limiting, and caching internally.
   */
  abstract fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord>;

  /**
   * Optional: estimate total property count before full scrape.
   */
  async estimateCount(_config: CountyConfig): Promise<number | null> {
    return null;
  }
}
