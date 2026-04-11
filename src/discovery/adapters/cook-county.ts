/**
 * Cook County IL (Chicago) Socrata API Adapter
 *
 * Imports property data from Cook County's open data portal via REST API.
 * Source: https://datacatalog.cookcountyil.gov/
 *
 * Datasets:
 *   Parcel Universe: https://datacatalog.cookcountyil.gov/resource/nj4t-kc8j.json
 *   Assessed Values: https://datacatalog.cookcountyil.gov/resource/uzyt-m557.json
 *
 * Socrata API — no auth needed, 50K row limit per request, supports $offset pagination.
 */

import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";
import { waitForSlot } from "../../utils/rate-limiter.js";

const PARCEL_API = "https://datacatalog.cookcountyil.gov/resource/nj4t-kc8j.json";
const VALUES_API = "https://datacatalog.cookcountyil.gov/resource/uzyt-m557.json";
const PAGE_SIZE = 1000;
const MAX_PAGES = 2000; // 2M records max

interface SocrataParcel {
  pin: string;
  class: string;
  township_name: string;
  nbhd: string;
  tax_code: string;
  property_address: string;
  property_city: string;
  property_zip: string;
  land_square_footage: string;
  building_square_footage: string;
  num_stories: string;
  num_units: string;
  num_bedrooms: string;
  year_built: string;
  recent_sale_date: string;
  recent_sale_price: string;
  certified_tot: string;
}

export class CookCountyAdapter extends AssessorAdapter {
  readonly platform = "cook_county";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "cook_county";
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

    let offset = 0;
    let hasMore = true;

    console.log("  Fetching parcels from Socrata API...");

    while (hasMore && offset / PAGE_SIZE < MAX_PAGES) {
      await waitForSlot(PARCEL_API);

      try {
        const url = `${PARCEL_API}?$limit=${PAGE_SIZE}&$offset=${offset}&$order=pin`;
        const response = await fetch(url, {
          headers: { Accept: "application/json" },
          signal: AbortSignal.timeout(60_000),
        });

        if (!response.ok) {
          console.log(`  API error: ${response.status}`);
          progress.errors++;
          break;
        }

        const parcels = (await response.json()) as SocrataParcel[];

        if (parcels.length === 0) {
          hasMore = false;
          break;
        }

        for (const p of parcels) {
          if (!p.property_address) continue;

          progress.total_found++;

          // Classify property type from Cook County class codes
          // 2xx = residential, 3xx = multifamily, 5xx = commercial, 6xx = industrial
          const classCode = parseInt(p.class) || 0;
          let propertyType = "residential";
          let isApartment = false;
          let isSfr = true;
          if (classCode >= 300 && classCode < 400) {
            propertyType = "multifamily";
            isApartment = true;
            isSfr = false;
          } else if (classCode >= 500 && classCode < 600) {
            propertyType = "commercial";
            isSfr = false;
          } else if (classCode >= 600 && classCode < 700) {
            propertyType = "industrial";
            isSfr = false;
          }

          const record: RawPropertyRecord = {
            parcel_id: p.pin || "",
            address: p.property_address.trim(),
            city: (p.property_city || p.township_name || "CHICAGO").trim().toUpperCase(),
            state: "IL",
            zip: (p.property_zip || "").trim(),
            owner_name: undefined, // Not in parcel universe dataset
            property_type: propertyType,
            assessed_value: p.certified_tot ? parseInt(p.certified_tot) || undefined : undefined,
            year_built: p.year_built ? parseInt(p.year_built) || undefined : undefined,
            total_sqft: p.building_square_footage ? parseInt(p.building_square_footage) || undefined : undefined,
            total_units: p.num_units ? parseInt(p.num_units) || undefined : undefined,
            stories: p.num_stories ? parseInt(p.num_stories) || undefined : undefined,
            last_sale_price: p.recent_sale_price ? parseInt(p.recent_sale_price) || undefined : undefined,
            last_sale_date: p.recent_sale_date || undefined,
            assessor_url: `https://www.cookcountyassessor.com/pin/${p.pin}`,
            raw: {
              classCode: p.class,
              township: p.township_name,
              neighborhood: p.nbhd,
              landSqft: p.land_square_footage ? parseInt(p.land_square_footage) || undefined : undefined,
              bedrooms: p.num_bedrooms ? parseInt(p.num_bedrooms) || undefined : undefined,
              isApartment,
              isSfr,
            },
          };

          progress.total_processed++;
          if (progress.total_processed % 10000 === 0) {
            console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
            onProgress?.(progress);
          }

          yield record;
        }

        offset += PAGE_SIZE;
      } catch (err) {
        console.error(`  API fetch error at offset ${offset}:`, err instanceof Error ? err.message : "Unknown");
        progress.errors++;
        // Retry after delay
        await new Promise((r) => setTimeout(r, 5000));
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_found.toLocaleString()} found, ${progress.total_processed.toLocaleString()} processed, ${progress.errors} errors`,
    );
  }
}
