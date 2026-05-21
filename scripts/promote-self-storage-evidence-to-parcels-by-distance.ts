#!/usr/bin/env tsx
import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const DRY_RUN = process.argv.includes("--dry-run");
const ONLY = new Set(
  process.argv
    .filter((arg) => arg.startsWith("--market="))
    .flatMap((arg) => arg.replace("--market=", "").split(",").map((item) => item.trim()).filter(Boolean)),
);

type Market = {
  key: string;
  state: string;
  countyId: number;
};

const markets: Market[] = [
  { key: "dallas", state: "TX", countyId: 7 },
  { key: "indianapolis", state: "IN", countyId: 797583 },
  { key: "columbus", state: "OH", countyId: 1698985 },
  { key: "dayton", state: "OH", countyId: 1698991 },
  { key: "toledo", state: "OH", countyId: 2338836 },
  { key: "san-antonio", state: "TX", countyId: 1741238 },
  { key: "memphis", state: "TN", countyId: 1741244 },
  { key: "cleveland", state: "OH", countyId: 1698988 },
  { key: "akron", state: "OH", countyId: 1698989 },
  { key: "fort-wayne", state: "IN", countyId: 797481 },
  { key: "south-bend", state: "IN", countyId: 797737 },
  { key: "peoria", state: "IL", countyId: 2338837 },
  { key: "west-chester", state: "PA", countyId: 817175 },
  { key: "birmingham", state: "AL", countyId: 1973348 },
  { key: "detroit", state: "MI", countyId: 1973412 },
].filter((market) => ONLY.size === 0 || ONLY.has(market.key));

type Evidence = {
  id: number;
  source: string;
  title: string | null;
  address: string | null;
  raw: Record<string, unknown> | null;
};

type Property = {
  id: number;
  address: string | null;
  asset_type: string | null;
  lat: number | string | null;
  lng: number | string | null;
  latitude: number | string | null;
  longitude: number | string | null;
};

function numberOrNull(value: unknown) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function miles(lat1: number, lon1: number, lat2: number, lon2: number) {
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  return 3958.7613 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function normalize(value: string | null | undefined) {
  return String(value ?? "").toUpperCase().replace(/[^A-Z0-9]+/g, "");
}

function firstNumber(value: string | null | undefined) {
  return String(value ?? "").match(/\b\d+\b/)?.[0] ?? "";
}

function plausibleMatch(evidence: Evidence, property: Property, distance: number) {
  if (distance > 0.025) return false;
  const prior = String(property.asset_type ?? "").toLowerCase();
  if (["residential", "single_family", "apartment"].includes(prior)) return false;

  const evidenceNumber = firstNumber(evidence.address);
  const propertyNumber = firstNumber(property.address);
  if (evidenceNumber && propertyNumber && evidenceNumber === propertyNumber) return true;
  if (!evidenceNumber && /storage|u-haul|cubesmart|public storage|extra space|life storage/i.test(`${evidence.title ?? ""} ${evidence.address ?? ""}`)) return true;
  return distance <= 0.01 && normalize(property.address).length > 0;
}

async function loadEvidence(market: Market) {
  const rows: Evidence[] = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from("external_market_listings")
      .select("id,source,title,address,raw")
      .eq("asset_class", "self_storage")
      .eq("market", market.key)
      .eq("source", "osm_openstreetmap_facility")
      .range(from, from + pageSize - 1);
    if (error) throw error;
    rows.push(...((data ?? []) as Evidence[]).filter((row) => numberOrNull(row.raw?.lat) !== null && numberOrNull(row.raw?.lon) !== null));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

async function loadProperties(market: Market, evidence: Evidence[]) {
  const coords = evidence
    .map((row) => ({ lat: numberOrNull(row.raw?.lat), lon: numberOrNull(row.raw?.lon) }))
    .filter((coord): coord is { lat: number; lon: number } => coord.lat !== null && coord.lon !== null);
  if (coords.length === 0) return [];

  const minLat = Math.min(...coords.map((coord) => coord.lat)) - 0.002;
  const maxLat = Math.max(...coords.map((coord) => coord.lat)) + 0.002;
  const minLon = Math.min(...coords.map((coord) => coord.lon)) - 0.002;
  const maxLon = Math.max(...coords.map((coord) => coord.lon)) + 0.002;

  const rows: Property[] = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from("properties")
      .select("id,address,asset_type,lat,lng,latitude,longitude")
      .eq("county_id", market.countyId)
      .eq("state_code", market.state)
      .gte("latitude", minLat)
      .lte("latitude", maxLat)
      .gte("longitude", minLon)
      .lte("longitude", maxLon)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    rows.push(...((data ?? []) as Property[]));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

async function promote(propertyId: number, confidence: "medium" | "high") {
  if (DRY_RUN) return;
  const { error } = await db
    .from("properties")
    .update({
      asset_type: "self_storage",
      asset_subtype: "self_storage",
      asset_confidence: confidence,
      total_units: null,
      unit_count_source: "not_applicable",
      is_sfr: false,
      is_apartment: false,
      updated_at: new Date().toISOString(),
    })
    .eq("id", propertyId);
  if (error) throw error;
}

async function countSelfStorage(market: Market) {
  const { count, error } = await db
    .from("properties")
    .select("id", { count: "exact", head: true })
    .eq("county_id", market.countyId)
    .eq("state_code", market.state)
    .eq("asset_type", "self_storage");
  if (error) throw error;
  return count ?? 0;
}

async function main() {
  console.log("MXRE - promote self-storage facility evidence by coordinate proximity");
  console.log(`Dry run: ${DRY_RUN}`);

  const summary: unknown[] = [];
  for (const market of markets) {
    const before = await countSelfStorage(market);
    const evidence = await loadEvidence(market);
    const properties = await loadProperties(market, evidence);
    const propertyCoords = properties
      .map((property) => ({
        property,
        lat: numberOrNull(property.lat ?? property.latitude),
        lon: numberOrNull(property.lng ?? property.longitude),
      }))
      .filter((row): row is { property: Property; lat: number; lon: number } => row.lat !== null && row.lon !== null);

    const promoted = new Set<number>();
    const samples: unknown[] = [];

    for (const row of evidence) {
      const lat = numberOrNull(row.raw?.lat);
      const lon = numberOrNull(row.raw?.lon);
      if (lat === null || lon === null) continue;

      const nearest = propertyCoords
        .map((property) => ({ ...property, distance: miles(lat, lon, property.lat, property.lon) }))
        .filter((property) => plausibleMatch(row, property.property, property.distance))
        .sort((a, b) => a.distance - b.distance)[0];
      if (!nearest) continue;
      if (promoted.has(nearest.property.id)) continue;
      promoted.add(nearest.property.id);
      if (samples.length < 5) {
        samples.push({
          evidenceId: row.id,
          evidenceAddress: row.address,
          propertyId: nearest.property.id,
          propertyAddress: nearest.property.address,
          distanceMiles: Number(nearest.distance.toFixed(4)),
          previousAssetType: nearest.property.asset_type,
        });
      }
      await promote(nearest.property.id, "medium");
    }

    const after = DRY_RUN ? before : await countSelfStorage(market);
    const item = {
      market: market.key,
      evidenceRows: evidence.length,
      candidateProperties: properties.length,
      promotedUniqueProperties: promoted.size,
      before,
      after,
      samples,
    };
    summary.push(item);
    console.log(JSON.stringify(item));
  }

  console.log(JSON.stringify({ summary }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
