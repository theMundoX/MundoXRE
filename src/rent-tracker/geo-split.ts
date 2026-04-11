/**
 * Rent Tracker — Geographic bounding-box splitting algorithm.
 * When a search returns more results than a platform allows (typically 500),
 * recursively split the bounding box into quadrants and re-search each.
 *
 * Inspired by actor-zillow-api-scraper's map-splitting approach.
 */

import type { GeoBounds } from "./adapters/base.js";

const MAX_SPLIT_DEPTH = 6; // 4^6 = 4096 quadrants max — more than enough

/**
 * Split a bounding box into 4 equal quadrants: NW, NE, SW, SE.
 */
export function splitBounds(bounds: GeoBounds): [GeoBounds, GeoBounds, GeoBounds, GeoBounds] {
  const midLat = (bounds.north + bounds.south) / 2;
  const midLng = (bounds.east + bounds.west) / 2;

  const nw: GeoBounds = { north: bounds.north, south: midLat, east: midLng, west: bounds.west };
  const ne: GeoBounds = { north: bounds.north, south: midLat, east: bounds.east, west: midLng };
  const sw: GeoBounds = { north: midLat, south: bounds.south, east: midLng, west: bounds.west };
  const se: GeoBounds = { north: midLat, south: bounds.south, east: bounds.east, west: midLng };

  return [nw, ne, sw, se];
}

/**
 * Calculate the approximate area of a bounding box in square miles.
 * Used to decide when to stop splitting (avoid infinitely small quadrants).
 */
export function boundsAreaSqMiles(bounds: GeoBounds): number {
  const latDiff = Math.abs(bounds.north - bounds.south);
  const lngDiff = Math.abs(bounds.east - bounds.west);
  // Approximate: 1 degree lat ≈ 69 miles, 1 degree lng ≈ 55 miles (at ~40° lat)
  return latDiff * 69 * lngDiff * 55;
}

/**
 * Recursively search a geographic area, splitting into quadrants when
 * the result count exceeds the platform's max.
 *
 * @param bounds - The bounding box to search
 * @param searchFn - Platform-specific search function that returns result count
 * @param maxResults - Maximum results the platform returns per search (e.g., 500)
 * @param depth - Current recursion depth (for internal tracking)
 *
 * @yields GeoBounds for each quadrant that needs to be fully scraped
 */
export async function* recursiveSplit(
  bounds: GeoBounds,
  searchFn: (bounds: GeoBounds) => Promise<{ count: number; capped: boolean }>,
  maxResults: number,
  depth = 0,
): AsyncGenerator<GeoBounds> {
  // Safety: don't recurse too deep or into tiny areas
  if (depth >= MAX_SPLIT_DEPTH) {
    yield bounds;
    return;
  }

  const area = boundsAreaSqMiles(bounds);
  if (area < 0.1) {
    // Less than 0.1 sq miles — just scrape what we can
    yield bounds;
    return;
  }

  const result = await searchFn(bounds);

  if (!result.capped || result.count <= maxResults) {
    // Results fit in this bounding box — yield for scraping
    yield bounds;
  } else {
    // Too many results — split into quadrants and recurse
    const [nw, ne, sw, se] = splitBounds(bounds);

    yield* recursiveSplit(nw, searchFn, maxResults, depth + 1);
    yield* recursiveSplit(ne, searchFn, maxResults, depth + 1);
    yield* recursiveSplit(sw, searchFn, maxResults, depth + 1);
    yield* recursiveSplit(se, searchFn, maxResults, depth + 1);
  }
}

// ─── Known City Bounding Boxes ──────────────────────────────────────

/**
 * Approximate bounding boxes for cities we target.
 * These are starting points for the recursive split algorithm.
 */
export const CITY_BOUNDS: Record<string, GeoBounds> = {
  // ─── Texas ──────────────────────────────────────────────────────────
  "DALLAS,TX": { north: 33.02, south: 32.62, east: -96.46, west: -96.99 },
  "FORT WORTH,TX": { north: 32.97, south: 32.55, east: -97.07, west: -97.58 },
  "HOUSTON,TX": { north: 30.11, south: 29.52, east: -95.01, west: -95.79 },
  "SAN ANTONIO,TX": { north: 29.70, south: 29.26, east: -98.29, west: -98.73 },
  "AUSTIN,TX": { north: 30.52, south: 30.10, east: -97.56, west: -97.94 },
  "ARLINGTON,TX": { north: 32.80, south: 32.63, east: -97.04, west: -97.23 },
  "PLANO,TX": { north: 33.13, south: 33.00, east: -96.61, west: -96.77 },
  "IRVING,TX": { north: 32.93, south: 32.81, east: -96.89, west: -97.03 },
  "GARLAND,TX": { north: 32.97, south: 32.85, east: -96.58, west: -96.71 },
  "FRISCO,TX": { north: 33.24, south: 33.10, east: -96.75, west: -96.90 },
  "MCKINNEY,TX": { north: 33.26, south: 33.14, east: -96.58, west: -96.75 },
  "DENTON,TX": { north: 33.28, south: 33.16, east: -97.08, west: -97.20 },
  "EL PASO,TX": { north: 31.97, south: 31.69, east: -106.29, west: -106.63 },
  "CORPUS CHRISTI,TX": { north: 27.86, south: 27.63, east: -97.28, west: -97.54 },

  // ─── Oklahoma ───────────────────────────────────────────────────────
  "OKLAHOMA CITY,OK": { north: 35.65, south: 35.32, east: -97.23, west: -97.68 },
  "TULSA,OK": { north: 36.28, south: 35.98, east: -95.75, west: -96.11 },
  "LAWTON,OK": { north: 34.67, south: 34.55, east: -98.35, west: -98.50 },
  "NORMAN,OK": { north: 35.27, south: 35.16, east: -97.36, west: -97.52 },
  "BROKEN ARROW,OK": { north: 36.10, south: 35.98, east: -95.72, west: -95.85 },
  "EDMOND,OK": { north: 35.70, south: 35.60, east: -97.39, west: -97.53 },
  "MOORE,OK": { north: 35.37, south: 35.30, east: -97.44, west: -97.53 },

  // ─── Florida ────────────────────────────────────────────────────────
  "MIAMI,FL": { north: 25.86, south: 25.71, east: -80.12, west: -80.32 },
  "ORLANDO,FL": { north: 28.62, south: 28.37, east: -81.23, west: -81.51 },
  "TAMPA,FL": { north: 28.07, south: 27.87, east: -82.37, west: -82.58 },
  "JACKSONVILLE,FL": { north: 30.59, south: 30.10, east: -81.39, west: -81.77 },
  "FORT LAUDERDALE,FL": { north: 26.22, south: 26.08, east: -80.09, west: -80.21 },
  "ST. PETERSBURG,FL": { north: 27.84, south: 27.71, east: -82.60, west: -82.77 },
  "TALLAHASSEE,FL": { north: 30.55, south: 30.38, east: -84.20, west: -84.37 },
  "CAPE CORAL,FL": { north: 26.72, south: 26.56, east: -81.90, west: -82.06 },
  "FORT MYERS,FL": { north: 26.69, south: 26.57, east: -81.82, west: -81.95 },
  "SARASOTA,FL": { north: 27.40, south: 27.28, east: -82.47, west: -82.60 },
  "GAINESVILLE,FL": { north: 29.72, south: 29.60, east: -82.26, west: -82.42 },
  "PENSACOLA,FL": { north: 30.52, south: 30.39, east: -87.14, west: -87.32 },
  "NAPLES,FL": { north: 26.25, south: 26.11, east: -81.72, west: -81.85 },
  "PALM BEACH,FL": { north: 26.78, south: 26.60, east: -80.03, west: -80.13 },

  // ─── Illinois ───────────────────────────────────────────────────────
  "CHICAGO,IL": { north: 42.02, south: 41.64, east: -87.52, west: -87.94 },
  "AURORA,IL": { north: 41.80, south: 41.72, east: -88.25, west: -88.38 },
  "NAPERVILLE,IL": { north: 41.80, south: 41.70, east: -88.10, west: -88.24 },
  "ROCKFORD,IL": { north: 42.32, south: 42.20, east: -88.95, west: -89.14 },
  "JOLIET,IL": { north: 41.57, south: 41.48, east: -88.05, west: -88.18 },
  "SPRINGFIELD,IL": { north: 39.85, south: 39.73, east: -89.58, west: -89.72 },

  // ─── Ohio ───────────────────────────────────────────────────────────
  "COLUMBUS,OH": { north: 40.13, south: 39.87, east: -82.81, west: -83.13 },
  "CLEVELAND,OH": { north: 41.54, south: 41.39, east: -81.54, west: -81.88 },
  "CINCINNATI,OH": { north: 39.21, south: 39.05, east: -84.37, west: -84.62 },
  "AKRON,OH": { north: 41.12, south: 41.01, east: -81.44, west: -81.58 },
  "DAYTON,OH": { north: 39.82, south: 39.71, east: -84.11, west: -84.26 },
  "TOLEDO,OH": { north: 41.72, south: 41.61, east: -83.48, west: -83.65 },

  // ─── Georgia ────────────────────────────────────────────────────────
  "ATLANTA,GA": { north: 33.89, south: 33.65, east: -84.29, west: -84.55 },
  "SAVANNAH,GA": { north: 32.11, south: 31.97, east: -81.05, west: -81.20 },
  "AUGUSTA,GA": { north: 33.54, south: 33.40, east: -81.90, west: -82.10 },

  // ─── North Carolina ─────────────────────────────────────────────────
  "CHARLOTTE,NC": { north: 35.39, south: 35.13, east: -80.72, west: -80.97 },
  "RALEIGH,NC": { north: 35.87, south: 35.71, east: -78.54, west: -78.78 },
  "DURHAM,NC": { north: 36.08, south: 35.93, east: -78.82, west: -79.01 },
  "GREENSBORO,NC": { north: 36.14, south: 36.01, east: -79.72, west: -79.88 },

  // ─── Tennessee ──────────────────────────────────────────────────────
  "NASHVILLE,TN": { north: 36.28, south: 36.04, east: -86.52, west: -86.97 },
  "MEMPHIS,TN": { north: 35.23, south: 35.00, east: -89.81, west: -90.11 },
  "KNOXVILLE,TN": { north: 36.02, south: 35.90, east: -83.82, west: -84.00 },
  "CHATTANOOGA,TN": { north: 35.10, south: 34.98, east: -85.22, west: -85.39 },

  // ─── Arizona ────────────────────────────────────────────────────────
  "PHOENIX,AZ": { north: 33.67, south: 33.29, east: -111.82, west: -112.32 },
  "SCOTTSDALE,AZ": { north: 33.80, south: 33.46, east: -111.75, west: -111.95 },
  "TUCSON,AZ": { north: 32.32, south: 32.08, east: -110.80, west: -111.07 },
  "MESA,AZ": { north: 33.50, south: 33.35, east: -111.59, west: -111.87 },
  "CHANDLER,AZ": { north: 33.36, south: 33.23, east: -111.78, west: -111.88 },

  // ─── Colorado ───────────────────────────────────────────────────────
  "DENVER,CO": { north: 39.80, south: 39.61, east: -104.60, west: -105.11 },
  "COLORADO SPRINGS,CO": { north: 38.93, south: 38.74, east: -104.70, west: -104.88 },
  "AURORA,CO": { north: 39.75, south: 39.62, east: -104.64, west: -104.83 },
  "FORT COLLINS,CO": { north: 40.62, south: 40.49, east: -105.02, west: -105.14 },

  // ─── Nevada ─────────────────────────────────────────────────────────
  "LAS VEGAS,NV": { north: 36.33, south: 36.00, east: -115.04, west: -115.38 },
  "HENDERSON,NV": { north: 36.10, south: 35.93, east: -114.90, west: -115.12 },
  "RENO,NV": { north: 39.58, south: 39.46, east: -119.72, west: -119.90 },

  // ─── Michigan ───────────────────────────────────────────────────────
  "DETROIT,MI": { north: 42.45, south: 42.28, east: -82.91, west: -83.29 },
  "GRAND RAPIDS,MI": { north: 43.01, south: 42.89, east: -85.59, west: -85.75 },
  "ANN ARBOR,MI": { north: 42.33, south: 42.22, east: -83.68, west: -83.80 },

  // ─── New York ───────────────────────────────────────────────────────
  "NEW YORK CITY,NY": { north: 40.92, south: 40.49, east: -73.70, west: -74.26 },
  "BUFFALO,NY": { north: 42.97, south: 42.83, east: -78.79, west: -78.92 },
  "ROCHESTER,NY": { north: 43.21, south: 43.11, east: -77.53, west: -77.70 },
  "SYRACUSE,NY": { north: 43.08, south: 42.99, east: -76.08, west: -76.22 },
};

/**
 * Get bounding box for a city/state combo, or generate from ZIP if not known.
 */
export function getBoundsForArea(city: string, state: string): GeoBounds | null {
  const key = `${city.toUpperCase()},${state.toUpperCase()}`;
  return CITY_BOUNDS[key] ?? null;
}
