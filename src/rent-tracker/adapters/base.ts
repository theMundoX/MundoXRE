/**
 * Rent Tracker — base types and abstract class for on-market listing adapters.
 * Parallel pipeline to the assessor adapter — separate data model, same infrastructure.
 *
 * Legal constraints enforced at the adapter level:
 *   - Public pages only (no login, no CAPTCHA bypass)
 *   - No copyrighted content (photos, descriptions, marketing copy)
 *   - Agent business contact fields come only from public listing, broker/profile,
 *     or state license sources and must keep source/confidence provenance
 */

// ─── Types ───────────────────────────────────────────────────────────

export interface OnMarketRecord {
  address: string;
  city: string;
  state: string;
  zip: string;
  is_on_market: boolean;
  mls_list_price?: number;
  listing_agent_name?: string;
  listing_agent_first_name?: string;
  listing_agent_last_name?: string;
  listing_agent_email?: string;
  listing_agent_phone?: string;
  listing_brokerage?: string;
  listing_source: "zillow" | "redfin" | "realtor" | "movoto";
  listing_url?: string;
  days_on_market?: number;
  property_type?: string;
  beds?: number;
  baths?: number;
  sqft?: number;
  lot_sqft?: number;
  year_built?: number;
  observed_at: string; // ISO timestamp
  raw: Record<string, unknown>;
}

export interface AgentLicenseRecord {
  agent_name: string;
  license_number: string;
  license_state: string;
  license_status: "active" | "inactive" | "expired";
  brokerage_name?: string;
  phone?: string;
  email?: string;
  license_type?: string; // "salesperson" | "broker" | "associate_broker"
  source_url: string;
  observed_at: string;
}

export interface ListingSearchArea {
  city?: string;
  state: string;
  zip?: string;
  bounds?: GeoBounds;
}

export interface GeoBounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

export interface ListingProgress {
  source: string;
  area: string;
  total_found: number;
  total_processed: number;
  errors: number;
  started_at: Date;
}

// ─── Abstract Adapter ───────────────────────────────────────────────

export abstract class ListingAdapter {
  abstract readonly source: "zillow" | "redfin" | "realtor" | "movoto";

  abstract canHandle(area: ListingSearchArea): boolean;

  /**
   * Yields on-market property records from public search pages.
   * Implementations MUST:
   *   - Only access publicly visible pages (no login)
   *   - Never bypass CAPTCHA or rate limits
   *   - Never store photos, descriptions, or marketing copy
   *   - Respect robots.txt via the robots-checker utility
   *   - Use the listing rate limiter (8s+ between requests)
   */
  abstract fetchListings(
    area: ListingSearchArea,
    onProgress?: (progress: ListingProgress) => void,
  ): AsyncGenerator<OnMarketRecord>;

  /**
   * Optional: estimate total listing count before full scrape.
   */
  async estimateCount(_area: ListingSearchArea): Promise<number | null> {
    return null;
  }
}

// ─── State License Adapter ──────────────────────────────────────────

export abstract class StateLicenseAdapter {
  abstract readonly state: string;

  /**
   * Look up a real estate agent's license and contact info
   * from a state government database. These are public records.
   */
  abstract lookupAgent(
    agentName: string,
    brokerage?: string,
  ): Promise<AgentLicenseRecord | null>;

  /**
   * Check if this adapter handles the given state.
   */
  abstract canHandle(stateCode: string): boolean;
}
