#!/usr/bin/env tsx
/**
 * Property Website Discovery
 *
 * Uses Google Places API (New) to find apartment complexes and their websites.
 * Detects platform (RentCafe, AppFolio, Entrata, etc.) and stores in property_websites.
 *
 * Usage:
 *   npx tsx scripts/discover-websites.ts --city=Indianapolis --state=IN --county_id=797583
 *   npx tsx scripts/discover-websites.ts --city=Indianapolis --state=IN --county_id=797583 --dry-run
 */

import "dotenv/config";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

// ─── Config ──────────────────────────────────────────────────────────

const GOOGLE_API_KEY = process.env.GOOGLE_PLACES_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY!;

const args = process.argv.slice(2);
const city = args.find((a) => a.startsWith("--city="))?.split("=")[1] || "Lawton";
const state = args.find((a) => a.startsWith("--state="))?.split("=")[1] || "OK";
const countyId = parseInt(args.find((a) => a.startsWith("--county_id="))?.split("=")[1] || "3");
const dryRun = args.includes("--dry-run");

// ─── Platform Detection ──────────────────────────────────────────────

interface PlatformMatch {
  platform: string;
  pattern: RegExp;
}

const PLATFORM_PATTERNS: PlatformMatch[] = [
  { platform: "rentcafe", pattern: /rentcafe\.com/i },
  { platform: "appfolio", pattern: /appfolio\.com/i },
  { platform: "entrata", pattern: /entrata\.com/i },
  { platform: "realpage", pattern: /realpage\.com/i },
  { platform: "buildium", pattern: /buildium\.com/i },
  { platform: "resman", pattern: /myresman\.com/i },
  { platform: "yardi", pattern: /yardi\.com/i },
  { platform: "rent_manager", pattern: /rentmanager\.com/i },
  { platform: "apartments_com", pattern: /apartments\.com/i },
  { platform: "zillow", pattern: /zillow\.com/i },
];

function detectPlatform(url: string): string {
  for (const { platform, pattern } of PLATFORM_PATTERNS) {
    if (pattern.test(url)) return platform;
  }
  return "direct";
}

// Domains we should NOT store as property websites (aggregators, not property sites)
const SKIP_DOMAINS = new Set([
  "apartments.com",
  "zillow.com",
  "trulia.com",
  "realtor.com",
  "redfin.com",
  "hotpads.com",
  "rent.com",
  "zumper.com",
  "apartmentlist.com",
  "padmapper.com",
  "apartmentguide.com",
  "apartmentfinder.com",
  "facebook.com",
  "yelp.com",
  "google.com",
  "yellowpages.com",
  "bbb.org",
]);

function shouldSkipUrl(url: string): boolean {
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    for (const domain of SKIP_DOMAINS) {
      if (hostname === domain || hostname.endsWith(`.${domain}`)) return true;
    }
    return false;
  } catch {
    return true;
  }
}

// ─── Google Places API ───────────────────────────────────────────────

interface PlaceResult {
  id: string;
  displayName: { text: string };
  formattedAddress: string;
  location: { latitude: number; longitude: number };
  websiteUri?: string;
  googleMapsUri?: string;
  types?: string[];
  primaryType?: string;
  regularOpeningHours?: unknown;
  nationalPhoneNumber?: string;
}

interface TextSearchResponse {
  places?: PlaceResult[];
  nextPageToken?: string;
}

/**
 * Search Google Places (New) Text Search API for apartment complexes.
 */
async function searchPlaces(query: string, pageToken?: string): Promise<TextSearchResponse> {
  const url = "https://places.googleapis.com/v1/places:searchText";

  const body: Record<string, unknown> = {
    textQuery: query,
    maxResultCount: 20,
    languageCode: "en",
  };

  if (pageToken) {
    body.pageToken = pageToken;
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": GOOGLE_API_KEY!,
    "X-Goog-FieldMask":
      "places.id,places.displayName,places.formattedAddress,places.location,places.websiteUri,places.googleMapsUri,places.types,places.primaryType,places.nationalPhoneNumber,nextPageToken",
  };

  const resp = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Places API error ${resp.status}: ${text}`);
  }

  return (await resp.json()) as TextSearchResponse;
}

/**
 * Search for apartment complexes with multiple queries for better coverage.
 */
async function discoverApartments(
  city: string,
  state: string,
): Promise<PlaceResult[]> {
  const queries = [
    `apartments for rent in ${city}, ${state}`,
    `apartment complex ${city}, ${state}`,
    `apartment homes ${city}, ${state}`,
    `multifamily housing ${city}, ${state}`,
    `senior living apartments ${city}, ${state}`,
    `townhomes for rent ${city}, ${state}`,
  ];

  const allPlaces = new Map<string, PlaceResult>();

  for (const query of queries) {
    console.log(`  Searching: "${query}"`);

    try {
      let response = await searchPlaces(query);
      let pageNum = 1;

      while (response.places && response.places.length > 0) {
        for (const place of response.places) {
          if (!allPlaces.has(place.id)) {
            allPlaces.set(place.id, place);
          }
        }

        console.log(`    Page ${pageNum}: ${response.places.length} results (total unique: ${allPlaces.size})`);

        // Follow pagination
        if (response.nextPageToken) {
          await new Promise((r) => setTimeout(r, 2000)); // Required delay for pagination
          response = await searchPlaces(query, response.nextPageToken);
          pageNum++;
        } else {
          break;
        }
      }
    } catch (err) {
      console.error(`    Error: ${err instanceof Error ? err.message : "Unknown"}`);
    }

    // Rate limit between queries
    await new Promise((r) => setTimeout(r, 1000));
  }

  return Array.from(allPlaces.values());
}

// ─── Address Matching ────────────────────────────────────────────────

function normalizeAddress(addr: string): string {
  return addr
    .toUpperCase()
    .replace(/[.,#]/g, "")
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bPARKWAY\b/g, "PKWY")
    .replace(/\bNORTHWEST\b/g, "NW")
    .replace(/\bNORTHEAST\b/g, "NE")
    .replace(/\bSOUTHWEST\b/g, "SW")
    .replace(/\bSOUTHEAST\b/g, "SE")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Try to extract a street address from a Google Places formatted address.
 * Google format: "123 Main St, Lawton, OK 73501, USA"
 */
function extractStreetAddress(formattedAddress: string): string {
  const parts = formattedAddress.split(",");
  return parts[0]?.trim() || "";
}

// ─── Database Operations ─────────────────────────────────────────────

async function findExistingProperty(
  db: SupabaseClient,
  streetAddress: string,
  countyId: number,
): Promise<{ id: number } | null> {
  const normalized = normalizeAddress(streetAddress);
  if (!normalized || normalized === "N/A") return null;

  // Try exact match on normalized address
  const { data } = await db
    .from("properties")
    .select("id, address")
    .eq("county_id", countyId)
    .limit(100);

  if (!data) return null;

  for (const prop of data) {
    if (normalizeAddress(prop.address) === normalized) {
      return { id: prop.id };
    }
  }

  // Try partial match — match on house number + street name
  const houseNumMatch = normalized.match(/^(\d+)\s+(.+)/);
  if (houseNumMatch) {
    const [, houseNum, street] = houseNumMatch;
    for (const prop of data) {
      const propNorm = normalizeAddress(prop.address);
      if (propNorm.startsWith(houseNum) && propNorm.includes(street.split(" ")[0])) {
        return { id: prop.id };
      }
    }
  }

  return null;
}

async function upsertPropertyFromPlace(
  db: SupabaseClient,
  place: PlaceResult,
  countyId: number,
): Promise<number> {
  const streetAddress = extractStreetAddress(place.formattedAddress);
  const addrParts = place.formattedAddress.split(",").map((s) => s.trim());

  // Try to find existing property
  const existing = await findExistingProperty(db, streetAddress, countyId);
  if (existing) {
    // Update existing property with discovered data
    await db
      .from("properties")
      .update({
        is_apartment: true,
        website: place.websiteUri || null,
        lat: place.location.latitude,
        lng: place.location.longitude,
        updated_at: new Date().toISOString(),
      })
      .eq("id", existing.id);

    return existing.id;
  }

  // Parse city/state/zip from formatted address
  // Format: "123 Main St, Lawton, OK 73501, USA"
  let placeCity = city.toUpperCase();
  let placeState = state.toUpperCase();
  let placeZip = "";

  if (addrParts.length >= 3) {
    placeCity = addrParts[1]?.toUpperCase() || placeCity;
    const stateZip = addrParts[2]?.trim() || "";
    const szMatch = stateZip.match(/([A-Z]{2})\s+(\d{5})/);
    if (szMatch) {
      placeState = szMatch[1];
      placeZip = szMatch[2];
    }
  }

  // Insert new property
  const { data, error } = await db
    .from("properties")
    .insert({
      county_id: countyId,
      address: streetAddress,
      city: placeCity,
      state_code: placeState,
      zip: placeZip,
      lat: place.location.latitude,
      lng: place.location.longitude,
      property_type: "multifamily",
      is_apartment: true,
      website: place.websiteUri || null,
      source: "google_places",
    })
    .select("id")
    .single();

  if (error) {
    // Might be a duplicate — try to find by address
    console.log(`    Insert failed for "${streetAddress}": ${error.message}`);
    const fallback = await findExistingProperty(db, streetAddress, countyId);
    if (fallback) return fallback.id;
    throw error;
  }

  return data.id;
}

async function upsertPropertyWebsite(
  db: SupabaseClient,
  propertyId: number,
  url: string,
  platform: string,
): Promise<void> {
  await db.from("property_websites").upsert(
    {
      property_id: propertyId,
      url,
      platform,
      discovery_method: "google_places",
      active: true,
    },
    { onConflict: "property_id,url" },
  );
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE: Property Website Discovery");
  console.log("=".repeat(50));
  console.log(`City: ${city}, ${state}`);
  console.log(`County ID: ${countyId}`);
  console.log(`Dry run: ${dryRun}`);

  if (!GOOGLE_API_KEY) {
    console.error("\nERROR: GOOGLE_PLACES_API_KEY not set in .env");
    if (city.toUpperCase() !== "LAWTON" || state.toUpperCase() !== "OK") {
      console.log("\nWebsite discovery needs GOOGLE_PLACES_API_KEY for this market.");
      console.log("Skipping discovery safely. No database writes were performed.");
      return;
    }
    console.log("\nFalling back to known Lawton RentCafe apartment discovery...\n");
    await discoverKnownRentCafe();
    return;
  }

  console.log(`Google Places API key: ${GOOGLE_API_KEY.substring(0, 8)}...`);
  console.log();

  const db = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

  // Step 1: Discover apartments via Google Places
  console.log("Step 1: Searching Google Places for apartments...\n");
  const places = await discoverApartments(city, state);
  console.log(`\nTotal unique places found: ${places.length}\n`);

  // Step 2: Process each place
  console.log("Step 2: Processing discoveries...\n");

  let stats = {
    total: places.length,
    withWebsite: 0,
    rentcafe: 0,
    otherPlatform: 0,
    directSite: 0,
    noWebsite: 0,
    saved: 0,
    skipped: 0,
    errors: 0,
  };

  for (const place of places) {
    const name = place.displayName?.text || "Unknown";
    const addr = place.formattedAddress || "";
    const website = place.websiteUri || "";

    if (!website) {
      console.log(`  [no-web] ${name} — ${addr}`);
      stats.noWebsite++;
      continue;
    }

    if (shouldSkipUrl(website)) {
      console.log(`  [skip]   ${name} — ${website} (aggregator)`);
      stats.skipped++;
      continue;
    }

    const platform = detectPlatform(website);
    if (platform === "rentcafe") stats.rentcafe++;
    else if (platform !== "direct") stats.otherPlatform++;
    else stats.directSite++;
    stats.withWebsite++;

    console.log(`  [${platform.padEnd(10)}] ${name} — ${website}`);

    if (dryRun) continue;

    try {
      const propertyId = await upsertPropertyFromPlace(db, place, countyId);
      await upsertPropertyWebsite(db, propertyId, website, platform);
      stats.saved++;
    } catch (err) {
      console.error(`    Error saving: ${err instanceof Error ? err.message : "Unknown"}`);
      stats.errors++;
    }
  }

  if (city.toUpperCase() === "LAWTON" && state.toUpperCase() === "OK") {
    console.log("\nStep 3: Trying known RentCafe URL patterns...\n");
    const extraFound = await tryKnownRentCafePatterns(db, countyId);
    stats.saved += extraFound;
  }

  console.log("\n" + "=".repeat(50));
  console.log("Discovery Summary");
  console.log("=".repeat(50));
  console.log(`  Total places found:    ${stats.total}`);
  console.log(`  With website:          ${stats.withWebsite}`);
  console.log(`    RentCafe:            ${stats.rentcafe}`);
  console.log(`    Other platforms:     ${stats.otherPlatform}`);
  console.log(`    Direct sites:        ${stats.directSite}`);
  console.log(`  No website:            ${stats.noWebsite}`);
  console.log(`  Skipped (aggregators): ${stats.skipped}`);
  console.log(`  Saved to DB:           ${stats.saved}`);
  console.log(`  Errors:                ${stats.errors}`);
}

// ─── Known RentCafe Patterns ─────────────────────────────────────────

/**
 * Try known RentCafe URL patterns for Lawton OK apartments.
 * These are common apartment complex names that often have RentCafe pages.
 */
const KNOWN_LAWTON_APARTMENTS = [
  // Known large complexes in Lawton, OK
  "sheridan-village",
  "ashley-park-apartments",
  "comanche-hills",
  "heritage-park",
  "cameron-crossing",
  "quail-ridge",
  "sherwood-forest",
  "prairie-west",
  "pecan-place",
  "summit-park",
  "patriot-place",
  "westwood-park",
  "falcon-crest",
  "eagle-creek",
  "bellaire-apartments",
  "village-green",
  "park-terrace",
  "lake-elmer-thomas",
  "creekside-village",
  "parkview-terrace",
];

async function tryKnownRentCafePatterns(
  db: SupabaseClient,
  countyId: number,
): Promise<number> {
  let found = 0;

  for (const slug of KNOWN_LAWTON_APARTMENTS) {
    const urls = [
      `https://${slug}.rentcafe.com`,
      `https://www.rentcafe.com/apartments-for-rent/lawton-ok/${slug}/`,
    ];

    for (const url of urls) {
      try {
        const resp = await fetch(url, {
          method: "HEAD",
          redirect: "follow",
          signal: AbortSignal.timeout(5000),
        });

        if (resp.ok) {
          console.log(`  Found: ${url}`);

          // Check if already in DB
          const { data: existing } = await db
            .from("property_websites")
            .select("id")
            .eq("url", url)
            .limit(1);

          if (!existing || existing.length === 0) {
            // Create a placeholder property if needed
            const name = slug
              .split("-")
              .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
              .join(" ");

            const { data: prop } = await db
              .from("properties")
              .insert({
                county_id: countyId,
                address: name,
                city: city.toUpperCase(),
                state_code: state.toUpperCase(),
                zip: "",
                property_type: "multifamily",
                is_apartment: true,
                website: url,
                source: "rentcafe_pattern",
              })
              .select("id")
              .single();

            if (prop) {
              await db.from("property_websites").upsert(
                {
                  property_id: prop.id,
                  url,
                  platform: "rentcafe",
                  discovery_method: "url_pattern",
                  active: true,
                },
                { onConflict: "property_id,url" },
              );
              found++;
            }
          }
          break; // Found a working URL for this slug, move on
        }
      } catch {
        // URL doesn't exist — try next
      }
    }
  }

  return found;
}

/**
 * Fallback when no Google API key: try known patterns and direct RentCafe search.
 */
async function discoverKnownRentCafe() {
  const db = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

  console.log("Trying known RentCafe URL patterns for Lawton, OK...\n");
  const found = await tryKnownRentCafePatterns(db, countyId);

  // Also try the RentCafe search/city page
  console.log("\nTrying RentCafe city search page...\n");
  try {
    const searchCity = city.toLowerCase().replace(/\s+/g, "-");
    const searchUrl = `https://www.rentcafe.com/apartments-for-rent/us/${state.toLowerCase()}/${searchCity}/`;
    const resp = await fetch(searchUrl, {
      redirect: "follow",
      signal: AbortSignal.timeout(10000),
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
      },
    });

    if (resp.ok) {
      const html = await resp.text();
      // Extract property URLs from the search results page
      const urlMatches = html.matchAll(/href="(https?:\/\/[^"]*\.rentcafe\.com[^"]*)"/g);
      const urls = new Set<string>();
      for (const m of urlMatches) {
        const u = m[1];
        if (
          u &&
          !u.includes("/login") &&
          !u.includes("/register") &&
          !u.includes("/about") &&
          u !== searchUrl
        ) {
          urls.add(u);
        }
      }

      console.log(`  Found ${urls.size} RentCafe property URLs from search page`);
      for (const url of urls) {
        console.log(`    ${url}`);

        // Extract property name from URL
        const nameMatch = url.match(
          /\/([^/]+)\/?$/,
        );
        const slug = nameMatch?.[1] || "";
        const name = slug
          .split("-")
          .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
          .join(" ");

        if (!name) continue;

        // Check if already in DB
        const { data: existing } = await db
          .from("property_websites")
          .select("id")
          .eq("url", url)
          .limit(1);

        if (!existing || existing.length === 0) {
          const { data: prop } = await db
            .from("properties")
            .insert({
              county_id: countyId,
              address: name,
              city: city.toUpperCase(),
              state_code: state.toUpperCase(),
              zip: "",
              property_type: "multifamily",
              is_apartment: true,
              website: url,
              source: "rentcafe_search",
            })
            .select("id")
            .single();

          if (prop) {
            await db.from("property_websites").upsert(
              {
                property_id: prop.id,
                url,
                platform: "rentcafe",
                discovery_method: "rentcafe_search",
                active: true,
              },
              { onConflict: "property_id,url" },
            );
          }
        }
      }
    }
  } catch (err) {
    console.error(`  Error: ${err instanceof Error ? err.message : "Unknown"}`);
  }

  console.log(`\nDone. Found ${found} apartments via URL patterns.`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
