#!/usr/bin/env tsx
import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const DRY_RUN = process.argv.includes("--dry-run");

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
];

type Evidence = {
  id: number;
  market: string;
  source: string;
  title: string | null;
  address: string | null;
  city: string | null;
  state_code: string | null;
  zip: string | null;
  raw: Record<string, unknown> | null;
};

type PropertyRow = {
  id: number;
  address: string | null;
  city: string | null;
  state_code: string | null;
  zip: string | null;
  asset_type: string | null;
};

function normalizeAddress(value: string | null | undefined) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/\b(STREET|ST)\b/g, "ST")
    .replace(/\b(AVENUE|AVE)\b/g, "AVE")
    .replace(/\b(ROAD|RD)\b/g, "RD")
    .replace(/\b(DRIVE|DR)\b/g, "DR")
    .replace(/\b(BOULEVARD|BLVD)\b/g, "BLVD")
    .replace(/\b(PARKWAY|PKWY|PKY)\b/g, "PKWY")
    .replace(/\b(HIGHWAY|HWY)\b/g, "HWY")
    .replace(/\b(NORTH)\b/g, "N")
    .replace(/\b(SOUTH)\b/g, "S")
    .replace(/\b(EAST)\b/g, "E")
    .replace(/\b(WEST)\b/g, "W")
    .replace(/[^A-Z0-9]+/g, "");
}

function zip5(value: string | null | undefined) {
  return String(value ?? "").slice(0, 5);
}

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

function usableEvidence(evidence: Evidence) {
  const evidenceType = String(evidence.raw?.evidence_type ?? "");
  if (evidenceType === "former_self_storage_development_land") return false;
  const key = normalizeAddress(evidence.address);
  if (key.length < 6) return false;
  if (key.startsWith("OSM")) return false;
  return true;
}

async function loadEvidence(market: Market) {
  const rows: Evidence[] = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from("external_market_listings")
      .select("id,market,source,title,address,city,state_code,zip,raw")
      .eq("asset_class", "self_storage")
      .eq("market", market.key)
      .in("source", ["osm_openstreetmap_facility", "free_web_loopnet_detail", "loopnet_search_snapshot"])
      .range(from, from + pageSize - 1);
    if (error) throw error;
    rows.push(...((data ?? []) as Evidence[]).filter(usableEvidence));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

async function findProperty(market: Market, evidence: Evidence): Promise<PropertyRow | null> {
  const targetAddress = normalizeAddress(evidence.address);
  const targetZip = zip5(evidence.zip);
  const city = String(evidence.city ?? "").toUpperCase();

  let query = db
    .from("properties")
    .select("id,address,city,state_code,zip,asset_type")
    .eq("county_id", market.countyId)
    .eq("state_code", market.state)
    .limit(25);

  if (targetZip) {
    query = query.eq("zip", targetZip);
  } else if (city) {
    query = query.ilike("city", city);
  }

  const streetNumber = String(evidence.address ?? "").match(/\b\d+\b/)?.[0];
  if (streetNumber) query = query.ilike("address", `${streetNumber}%`);

  const { data, error } = await query;
  if (error) throw error;

  const candidates = ((data ?? []) as PropertyRow[])
    .map((property) => ({ property, key: normalizeAddress(property.address) }))
    .filter(({ key }) => key === targetAddress);

  return candidates[0]?.property ?? null;
}

async function findNearestProperty(market: Market, evidence: Evidence): Promise<PropertyRow | null> {
  const lat = numberOrNull(evidence.raw?.lat);
  const lon = numberOrNull(evidence.raw?.lon);
  if (lat === null || lon === null) return null;

  const delta = 0.0015;
  const { data, error } = await db
    .from("properties")
    .select("id,address,city,state_code,zip,asset_type,lat,lng,latitude,longitude")
    .eq("county_id", market.countyId)
    .eq("state_code", market.state)
    .gte("lat", lat - delta)
    .lte("lat", lat + delta)
    .gte("lng", lon - delta)
    .lte("lng", lon + delta)
    .limit(20);
  if (error) throw error;

  const candidates = ((data ?? []) as Array<PropertyRow & {
    lat?: number | string | null;
    lng?: number | string | null;
    latitude?: number | string | null;
    longitude?: number | string | null;
  }>)
    .map((property) => {
      const pLat = numberOrNull(property.lat ?? property.latitude);
      const pLon = numberOrNull(property.lng ?? property.longitude);
      if (pLat === null || pLon === null) return null;
      return { property, distanceMiles: miles(lat, lon, pLat, pLon) };
    })
    .filter((item): item is { property: PropertyRow; distanceMiles: number } => item !== null)
    .filter((item) => item.distanceMiles <= 0.05)
    .sort((a, b) => a.distanceMiles - b.distanceMiles);

  return candidates[0]?.property ?? null;
}

async function promoteProperty(propertyId: number, confidence: "high" | "medium") {
  if (DRY_RUN) return false;
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
  return true;
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
  console.log("MXRE - promote self-storage evidence to parcel-backed asset_type");
  console.log(`Dry run: ${DRY_RUN}`);

  const summary: Record<string, unknown>[] = [];
  const seenPropertyIds = new Set<number>();

  for (const market of markets) {
    const before = await countSelfStorage(market);
    const evidenceRows = await loadEvidence(market);
    let matched = 0;
    let updated = 0;
    const samples: unknown[] = [];

    for (const evidence of evidenceRows) {
      const property = await findProperty(market, evidence);
      if (!property) continue;
      matched++;
      if (samples.length < 5) {
        samples.push({
          evidenceId: evidence.id,
          source: evidence.source,
          evidenceAddress: evidence.address,
          propertyId: property.id,
          propertyAddress: property.address,
          previousAssetType: property.asset_type,
        });
      }
      if (seenPropertyIds.has(property.id)) continue;
      seenPropertyIds.add(property.id);
      const confidence = evidence.source === "osm_openstreetmap_facility" ? "medium" : "high";
      const didUpdate = await promoteProperty(property.id, confidence);
      if (didUpdate || DRY_RUN) updated++;
    }

    const after = DRY_RUN ? before : await countSelfStorage(market);
    const item = {
      market: market.key,
      evidenceRows: evidenceRows.length,
      matchedEvidence: matched,
      promotedUniqueProperties: updated,
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
