import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const observedAt = new Date().toISOString();
const OVERPASS_URL = "https://overpass-api.de/api/interpreter";
const USER_AGENT = "MXRE self-storage facility evidence refresh (contact: https://mundox.ai)";

type Market = {
  key: string;
  label: string;
  bbox: [number, number, number, number];
};

const markets: Market[] = [
  { key: "indianapolis", label: "Indianapolis, IN", bbox: [39.58, -86.38, 39.96, -85.93] },
  { key: "dallas", label: "Dallas, TX", bbox: [32.55, -97.05, 33.05, -96.55] },
  { key: "columbus", label: "Columbus, OH", bbox: [39.78, -83.22, 40.16, -82.75] },
  { key: "dayton", label: "Dayton, OH", bbox: [39.60, -84.35, 39.90, -83.98] },
  { key: "toledo", label: "Toledo, OH", bbox: [41.50, -83.75, 41.78, -83.40] },
  { key: "cleveland", label: "Cleveland, OH", bbox: [41.35, -81.90, 41.62, -81.45] },
  { key: "akron", label: "Akron, OH", bbox: [40.95, -81.65, 41.20, -81.35] },
  { key: "fort-wayne", label: "Fort Wayne, IN", bbox: [40.95, -85.35, 41.25, -84.95] },
  { key: "south-bend", label: "South Bend, IN", bbox: [41.55, -86.40, 41.85, -86.10] },
  { key: "peoria", label: "Peoria, IL", bbox: [40.58, -89.75, 40.82, -89.45] },
  { key: "san-antonio", label: "San Antonio, TX", bbox: [29.20, -98.75, 29.65, -98.25] },
  { key: "birmingham", label: "Birmingham, AL", bbox: [33.35, -87.05, 33.70, -86.55] },
  { key: "memphis", label: "Memphis, TN", bbox: [34.95, -90.20, 35.30, -89.70] },
  { key: "detroit", label: "Detroit, MI", bbox: [42.20, -83.35, 42.50, -82.90] },
  { key: "west-chester", label: "West Chester, PA", bbox: [39.82, -75.78, 40.08, -75.45] },
];

type OsmElement = {
  type: "node" | "way" | "relation";
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  tags?: Record<string, string>;
};

function overpassQuery([south, west, north, east]: [number, number, number, number]) {
  const bbox = `${south},${west},${north},${east}`;
  return `
[out:json][timeout:60];
(
  node["shop"="storage_rental"](${bbox});
  way["shop"="storage_rental"](${bbox});
  relation["shop"="storage_rental"](${bbox});
  node["amenity"="self_storage"](${bbox});
  way["amenity"="self_storage"](${bbox});
  relation["amenity"="self_storage"](${bbox});
);
out center tags 1000;
`;
}

function tag(element: OsmElement, key: string) {
  return element.tags?.[key]?.trim() || "";
}

function coordinates(element: OsmElement) {
  return {
    lat: element.lat ?? element.center?.lat ?? null,
    lon: element.lon ?? element.center?.lon ?? null,
  };
}

function address(element: OsmElement) {
  const house = tag(element, "addr:housenumber");
  const street = tag(element, "addr:street");
  const name = tag(element, "name");
  if (house && street) return `${house} ${street}`;
  return name || `OSM ${element.type}/${element.id}`;
}

function city(element: OsmElement, market: Market) {
  return tag(element, "addr:city") || market.label.replace(/, .*/, "");
}

function normalizeKey(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function isAcceptedSelfStorage(element: OsmElement) {
  const tags = element.tags ?? {};
  const name = normalizeKey(tags.name ?? "");
  const shop = tags.shop;
  const amenity = tags.amenity;
  const note = normalizeKey(tags.note ?? "");
  const website = normalizeKey(tags.website ?? "");

  if (shop === "storage_rental" || amenity === "self_storage") return true;

  const text = `${name} ${note} ${website}`;
  return /\bself storage\b/.test(text)
    || /\bmini storage\b/.test(text)
    || /\bpublic storage\b/.test(text)
    || /\bextra space storage\b/.test(text)
    || /\bcubesmart\b/.test(text)
    || /\blife storage\b/.test(text)
    || /\bu haul moving storage\b/.test(text)
    || /\bstoragemart\b/.test(text)
    || /\blockaway storage\b/.test(text)
    || /\block away storage\b/.test(text);
}

function confidence(element: OsmElement) {
  const tags = element.tags ?? {};
  if (tags.shop === "storage_rental" || tags.amenity === "self_storage") return "high";
  return "medium";
}

async function fetchMarket(market: Market): Promise<OsmElement[]> {
  const response = await fetch(OVERPASS_URL, {
    method: "POST",
    headers: { "user-agent": USER_AGENT },
    body: new URLSearchParams({ data: overpassQuery(market.bbox) }),
    signal: AbortSignal.timeout(90_000),
  });
  if (!response.ok) throw new Error(`Overpass ${response.status} for ${market.key}: ${await response.text()}`);
  const json = await response.json() as { elements?: OsmElement[] };
  const seen = new Set<string>();
  const elements: OsmElement[] = [];
  for (const element of json.elements ?? []) {
    if (!isAcceptedSelfStorage(element)) continue;
    const coords = coordinates(element);
    const key = [
      normalizeKey(tag(element, "name")),
      normalizeKey(address(element)),
      coords.lat?.toFixed(5),
      coords.lon?.toFixed(5),
    ].join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    elements.push(element);
  }
  return elements;
}

await db
  .from("external_market_listings")
  .delete()
  .eq("asset_class", "self_storage")
  .eq("source", "osm_openstreetmap_facility");

const summary: Record<string, number> = {};
let inserted = 0;

for (const market of markets) {
  const elements = await fetchMarket(market);
  summary[market.key] = elements.length;
  for (const element of elements) {
    const coords = coordinates(element);
    const tags = element.tags ?? {};
    const { error } = await db.from("external_market_listings").insert({
      market: market.key,
      asset_class: "self_storage",
      source: "osm_openstreetmap_facility",
      source_url: `https://www.openstreetmap.org/${element.type}/${element.id}`,
      title: tag(element, "name") || `Self-storage facility ${element.type}/${element.id}`,
      address: address(element),
      city: city(element, market),
      state_code: tag(element, "addr:state") || market.label.slice(-2),
      zip: tag(element, "addr:postcode") || null,
      units: null,
      list_price: null,
      price_per_unit: null,
      cap_rate: null,
      noi: null,
      status: "off_market",
      confidence: confidence(element),
      observed_at: observedAt,
      first_seen_at: observedAt,
      last_seen_at: observedAt,
      raw: {
        evidence_type: "operating_self_storage_facility",
        operating_status: "operating_or_mapped_facility",
        source_summary: "OpenStreetMap public facility evidence tagged as storage_rental/self_storage or matched by explicit self-storage brand/name.",
        verification: "openstreetmap_facility_tags",
        osm_type: element.type,
        osm_id: element.id,
        lat: coords.lat,
        lon: coords.lon,
        tags,
        accepted_reason: "Facility evidence only. This does not mean the property is listed for sale.",
      },
    });
    if (error) throw error;
    inserted++;
  }
  console.log(JSON.stringify({ market: market.key, facilities: elements.length }));
}

console.log(JSON.stringify({
  inserted,
  summary,
  source: "osm_openstreetmap_facility",
  license: "OpenStreetMap data is available under the Open Database License (ODbL).",
}, null, 2));
