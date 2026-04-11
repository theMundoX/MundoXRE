/**
 * Phase 2: Property Website Discovery
 *
 * Finds leasing/management websites for properties already in the database.
 * Runs as a worker that pulls properties without websites and discovers them.
 *
 * Discovery methods:
 * 1. Management company lookup — known companies have predictable URL patterns
 * 2. RentCafe/Entrata subdomain patterns — {property-name}.rentcafe.com
 * 3. Google Places API — search for property name + city
 *
 * Prioritizes multifamily (is_apartment=true) since those have rental data.
 */

import { getDb, getWriteDb } from "../db/client.js";

// ─── Known Management Company Websites ───────────────────────────────

const MGMT_COMPANY_PATTERNS: Record<string, string> = {
  "GREYSTAR": "greystar.com",
  "MAA": "maac.com",
  "CAMDEN": "camdenliving.com",
  "EQUITY RESIDENTIAL": "equityapartments.com",
  "AVALONBAY": "avaloncommunities.com",
  "UDR": "udr.com",
  "ESSEX": "essexapartmenthomes.com",
  "MID-AMERICA": "maac.com",
  "NRP GROUP": "nrpgroup.com",
  "CORTLAND": "cortland.com",
  "LINCOLN": "lincolnapts.com",
  "PINNACLE": "pinnacleliving.com",
  "WATERTON": "watertonresidential.com",
};

// ─── RentCafe URL Pattern Discovery ──────────────────────────────────

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .trim();
}

/**
 * Generate candidate RentCafe URLs for a property name.
 * RentCafe sites follow: {slug}.rentcafe.com
 */
function generateRentCafeUrls(propertyName: string, city: string): string[] {
  const urls: string[] = [];
  const slug = slugify(propertyName);
  if (slug) {
    urls.push(`https://${slug}.rentcafe.com`);
    // Try with city
    const citySlug = slugify(city);
    if (citySlug) {
      urls.push(`https://${slug}-${citySlug}.rentcafe.com`);
    }
  }
  return urls;
}

// ─── Database Queries ────────────────────────────────────────────────

interface PropertyForDiscovery {
  id: number;
  address: string;
  city: string;
  state_code: string;
  zip: string;
  owner_name: string | null;
  mgmt_company: string | null;
  property_type: string | null;
  total_units: number | null;
  is_apartment: boolean;
}

/**
 * Get properties that need website discovery.
 * Prioritizes: apartments > multifamily > SFR with units > 1
 */
async function getPropertiesNeedingWebsites(
  limit = 100,
  offset = 0,
): Promise<PropertyForDiscovery[]> {
  const db = getDb();
  const { data, error } = await db
    .from("properties")
    .select("id, address, city, state_code, zip, owner_name, mgmt_company, property_type, total_units, is_apartment")
    .is("website", null)
    .eq("is_apartment", true)
    .order("total_units", { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (error) throw new Error("Failed to query properties for website discovery.");
  return (data ?? []) as PropertyForDiscovery[];
}

/**
 * Save a discovered website for a property.
 */
async function savePropertyWebsite(
  propertyId: number,
  url: string,
  platform: string,
  discoveryMethod: string,
): Promise<void> {
  const db = getWriteDb();

  // Insert into property_websites table
  await db.from("property_websites").upsert(
    {
      property_id: propertyId,
      url,
      platform,
      discovery_method: discoveryMethod,
      active: true,
    },
    { onConflict: "property_id,url" },
  );

  // Also update the property's website field
  await db
    .from("properties")
    .update({ website: url, updated_at: new Date().toISOString() })
    .eq("id", propertyId);
}

// ─── Discovery Worker ────────────────────────────────────────────────

export interface DiscoveryStats {
  processed: number;
  found: number;
  errors: number;
}

/**
 * Run website discovery for a batch of properties.
 * Returns stats on what was found.
 */
export async function discoverWebsites(
  batchSize = 50,
  offset = 0,
): Promise<DiscoveryStats> {
  const stats: DiscoveryStats = { processed: 0, found: 0, errors: 0 };

  const properties = await getPropertiesNeedingWebsites(batchSize, offset);
  if (properties.length === 0) return stats;

  for (const prop of properties) {
    stats.processed++;

    try {
      // Method 1: Check management company patterns
      if (prop.owner_name) {
        const ownerUpper = prop.owner_name.toUpperCase();
        for (const [pattern, domain] of Object.entries(MGMT_COMPANY_PATTERNS)) {
          if (ownerUpper.includes(pattern)) {
            const url = `https://www.${domain}`;
            await savePropertyWebsite(prop.id, url, domain, "mgmt_company_match");
            stats.found++;
            console.log(`  Found: ${prop.address} → ${url} (mgmt match)`);
            break;
          }
        }
      }

      // Method 2: Try RentCafe URL patterns for apartment properties
      if (prop.is_apartment && prop.address) {
        // Extract property name from address (heuristic — many apartments have names)
        // For now, generate candidates from the address
        const candidates = generateRentCafeUrls(
          prop.address.replace(/^\d+\s+/, ""), // strip house number
          prop.city,
        );

        for (const url of candidates) {
          try {
            // Quick HEAD request to check if URL exists
            const response = await fetch(url, {
              method: "HEAD",
              redirect: "follow",
              signal: AbortSignal.timeout(5000),
            });

            if (response.ok) {
              await savePropertyWebsite(prop.id, url, "rentcafe", "url_pattern");
              stats.found++;
              console.log(`  Found: ${prop.address} → ${url} (RentCafe)`);
              break;
            }
          } catch {
            // URL doesn't exist — try next candidate
          }
        }
      }
    } catch (err) {
      stats.errors++;
    }
  }

  return stats;
}

/**
 * Count properties still needing website discovery.
 */
export async function countPropertiesNeedingWebsites(): Promise<number> {
  const db = getDb();
  const { count, error } = await db
    .from("properties")
    .select("id", { count: "exact", head: true })
    .is("website", null)
    .eq("is_apartment", true);

  if (error) return 0;
  return count ?? 0;
}
