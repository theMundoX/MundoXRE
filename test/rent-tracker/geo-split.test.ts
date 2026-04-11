import { describe, it, expect } from "vitest";
import {
  splitBounds,
  boundsAreaSqMiles,
  getBoundsForArea,
  CITY_BOUNDS,
} from "../../src/rent-tracker/geo-split.js";
import type { GeoBounds } from "../../src/rent-tracker/adapters/base.js";

const DALLAS_BOUNDS: GeoBounds = { north: 33.02, south: 32.62, east: -96.46, west: -96.99 };

// ─── splitBounds ────────────────────────────────────────────────────

describe("splitBounds", () => {
  it("produces exactly 4 quadrants", () => {
    const quads = splitBounds(DALLAS_BOUNDS);
    expect(quads).toHaveLength(4);
  });

  it("quadrants cover the original bounding box (no gaps)", () => {
    const [nw, ne, sw, se] = splitBounds(DALLAS_BOUNDS);

    // The union of all quadrants should reproduce the original north/south/east/west
    const allNorth = Math.max(nw.north, ne.north, sw.north, se.north);
    const allSouth = Math.min(nw.south, ne.south, sw.south, se.south);
    const allEast = Math.max(nw.east, ne.east, sw.east, se.east);
    const allWest = Math.min(nw.west, ne.west, sw.west, se.west);

    expect(allNorth).toBeCloseTo(DALLAS_BOUNDS.north, 5);
    expect(allSouth).toBeCloseTo(DALLAS_BOUNDS.south, 5);
    expect(allEast).toBeCloseTo(DALLAS_BOUNDS.east, 5);
    expect(allWest).toBeCloseTo(DALLAS_BOUNDS.west, 5);
  });

  it("midpoints are correct", () => {
    const [nw, ne, sw, se] = splitBounds(DALLAS_BOUNDS);
    const midLat = (DALLAS_BOUNDS.north + DALLAS_BOUNDS.south) / 2;
    const midLng = (DALLAS_BOUNDS.east + DALLAS_BOUNDS.west) / 2;

    // NW south edge = midLat
    expect(nw.south).toBeCloseTo(midLat, 5);
    // SE north edge = midLat
    expect(se.north).toBeCloseTo(midLat, 5);
    // NW east edge = midLng
    expect(nw.east).toBeCloseTo(midLng, 5);
    // NE west edge = midLng
    expect(ne.west).toBeCloseTo(midLng, 5);
  });

  it("each quadrant has a smaller area than the original", () => {
    const originalArea = boundsAreaSqMiles(DALLAS_BOUNDS);
    const quads = splitBounds(DALLAS_BOUNDS);
    for (const q of quads) {
      expect(boundsAreaSqMiles(q)).toBeLessThan(originalArea);
    }
  });
});

// ─── boundsAreaSqMiles ──────────────────────────────────────────────

describe("boundsAreaSqMiles", () => {
  it("calculates a reasonable value for Dallas bounds", () => {
    const area = boundsAreaSqMiles(DALLAS_BOUNDS);
    // Dallas metro: roughly 0.4 lat * 69 * 0.53 lng * 55 ≈ ~800 sq miles
    expect(area).toBeGreaterThan(100);
    expect(area).toBeLessThan(2000);
  });

  it("returns 0 for a point (zero area)", () => {
    const point: GeoBounds = { north: 33.0, south: 33.0, east: -96.5, west: -96.5 };
    expect(boundsAreaSqMiles(point)).toBe(0);
  });

  it("returns a positive value for valid bounds", () => {
    const bounds: GeoBounds = { north: 30.0, south: 29.0, east: -95.0, west: -96.0 };
    expect(boundsAreaSqMiles(bounds)).toBeGreaterThan(0);
  });
});

// ─── getBoundsForArea ───────────────────────────────────────────────

describe("getBoundsForArea", () => {
  it("returns known bounds for Dallas, TX", () => {
    const bounds = getBoundsForArea("Dallas", "TX");
    expect(bounds).not.toBeNull();
    expect(bounds!.north).toBeCloseTo(33.02, 1);
  });

  it("returns known bounds for Houston, TX", () => {
    const bounds = getBoundsForArea("Houston", "TX");
    expect(bounds).not.toBeNull();
  });

  it("is case-insensitive", () => {
    const lower = getBoundsForArea("dallas", "tx");
    const upper = getBoundsForArea("DALLAS", "TX");
    expect(lower).toEqual(upper);
  });

  it("returns null for unknown city", () => {
    const bounds = getBoundsForArea("Nowheresville", "ZZ");
    expect(bounds).toBeNull();
  });
});

// ─── CITY_BOUNDS keys ───────────────────────────────────────────────

describe("CITY_BOUNDS", () => {
  it.each([
    "DALLAS,TX",
    "HOUSTON,TX",
    "OKLAHOMA CITY,OK",
    "CHICAGO,IL",
    "MIAMI,FL",
  ])("contains entry for %s", (key) => {
    expect(CITY_BOUNDS[key]).toBeDefined();
    expect(CITY_BOUNDS[key].north).toBeGreaterThan(CITY_BOUNDS[key].south);
  });
});
