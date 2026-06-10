#!/usr/bin/env tsx
/**
 * MXRE - Bentonville, AR public parcel backfill.
 *
 * Source: City of Bentonville/Benton County ArcGIS parcels.
 * https://services6.arcgis.com/gyy0H6zBnh9CbWoj/arcgis/rest/services/Parcels_Bentonville/FeatureServer/0
 *
 * This updates already-linked active Bentonville properties by exact physical
 * address match. It does not relink listings or overwrite verified paid data.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { firstEnv, hydrateWindowsUserEnv, preflightOutboundNetwork } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(firstEnv("SUPABASE_URL")!, firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY")!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://services6.arcgis.com/gyy0H6zBnh9CbWoj/arcgis/rest/services/Parcels_Bentonville/FeatureServer/0";
const STATE_CODE = "AR";
const CITY = "BENTONVILLE";
const MARKET_ZIPS = ["72712", "72713"];
const COUNTY_ID = 649708;
const PAGE_SIZE = 2000;
const DRY_RUN = process.argv.includes("--dry-run");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="))?.split("=")[1];
const LIMIT = limitArg ? Math.max(0, Number.parseInt(limitArg, 10) || 0) : 0;

type ArcgisFeature = {
  attributes: Record<string, unknown>;
  geometry?: { rings?: number[][][] };
};

type ActiveProperty = {
  id: number;
  address: string | null;
  owner_name: string | null;
  mailing_address: string | null;
  latitude: number | string | null;
  longitude: number | string | null;
  parcel_id: string | null;
  apn_formatted: string | null;
};

function clean(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function cleanUpper(value: unknown): string {
  return clean(value).toUpperCase();
}

function normalizeAddress(value: unknown): string {
  return cleanUpper(value)
    .replace(/\b(?:APT|APARTMENT|UNIT|STE|SUITE|#)\s+[A-Z0-9-]+$/g, "")
    .replace(/\bLOT\s+[A-Z0-9-]+$/g, "")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bHIGHWAY\b/g, "HWY")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bPARKWAY\b/g, "PKWY")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bTERRACE\b/g, "TER")
    .replace(/\b(AVE|BLVD|CIR|CT|DR|HWY|LN|PKWY|PL|RD|ST|TER)\s+\1\b/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

function parseNum(value: unknown): number | null {
  if (value == null) return null;
  const n = Number.parseFloat(String(value).replace(/,/g, "").trim());
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.round(n);
}

function parseFloatPositive(value: unknown): number | null {
  if (value == null) return null;
  const n = Number.parseFloat(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) && n > 0 ? n : null;
}

function centroid(rings: number[][][] | undefined): { lng: number | null; lat: number | null } {
  const points = rings?.flat() ?? [];
  let x = 0;
  let y = 0;
  let count = 0;
  for (const point of points) {
    if (point.length < 2 || !Number.isFinite(point[0]) || !Number.isFinite(point[1])) continue;
    x += point[0];
    y += point[1];
    count++;
  }
  return count ? { lng: x / count, lat: y / count } : { lng: null, lat: null };
}

function classifyType(value: unknown): string {
  const code = cleanUpper(value);
  if (!code) return "unknown";
  if (code.startsWith("R")) return "residential";
  if (code.startsWith("C")) return "commercial";
  if (code.startsWith("I")) return "industrial";
  if (code.startsWith("A")) return "agricultural";
  if (code.startsWith("E")) return "exempt";
  return code.toLowerCase();
}

async function fetchPage(offset: number): Promise<ArcgisFeature[]> {
  const params = new URLSearchParams({
    where: "PH_ADD IS NOT NULL AND PH_ADD <> ' ' AND OW_NAME IS NOT NULL AND OW_NAME <> ' '",
    outFields: "OBJECTID,PARCELID,OW_NAME,OW_ADD,PH_ADD,TYPE_,ASSESS_VAL,IMP_VAL,LAND_VAL,TOTAL_VAL,ACRE_AREA,GIS_EST_AC",
    returnGeometry: "true",
    outSR: "4326",
    orderByFields: "OBJECTID ASC",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });
  const response = await fetch(`${ARCGIS_URL}/query?${params.toString()}`, { signal: AbortSignal.timeout(45_000) });
  if (!response.ok) throw new Error(`ArcGIS HTTP ${response.status}: ${await response.text()}`);
  const json = (await response.json()) as { error?: unknown; features?: ArcgisFeature[] };
  if (json.error) throw new Error(JSON.stringify(json.error));
  return json.features ?? [];
}

async function fetchActiveProperties(): Promise<ActiveProperty[]> {
  const ids = new Set<number>();
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await db
      .from("listing_signals")
      .select("property_id")
      .eq("is_on_market", true)
      .eq("state_code", STATE_CODE)
      .or(`city.eq.${CITY},zip.in.(${MARKET_ZIPS.join(",")})`)
      .not("property_id", "is", null)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    for (const row of data ?? []) ids.add(Number(row.property_id));
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  const rows: ActiveProperty[] = [];
  const idList = [...ids];
  for (let i = 0; i < idList.length; i += 500) {
    const { data, error } = await db
      .from("properties")
      .select("id,address,owner_name,mailing_address,latitude,longitude,parcel_id,apn_formatted")
      .in("id", idList.slice(i, i + 500));
    if (error) throw error;
    rows.push(...((data ?? []) as ActiveProperty[]));
  }
  return rows;
}

async function fetchExistingParcelIds(): Promise<Set<string>> {
  const parcelIds = new Set<string>();
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await db
      .from("properties")
      .select("parcel_id")
      .eq("county_id", COUNTY_ID)
      .not("parcel_id", "is", null)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    for (const row of data ?? []) {
      const parcelId = clean((row as { parcel_id?: unknown }).parcel_id);
      if (parcelId) parcelIds.add(parcelId);
    }
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return parcelIds;
}

async function main() {
  console.log("MXRE - Bentonville, AR public parcel backfill");
  console.log("=".repeat(58));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Limit: ${LIMIT || "none"}`);

  await preflightOutboundNetwork("Bentonville AR parcel source", ARCGIS_URL);

  const parcelByAddress = new Map<string, ArcgisFeature>();
  let fetched = 0;
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const features = await fetchPage(offset);
    if (features.length === 0) break;
    for (const feature of features) {
      const key = normalizeAddress(feature.attributes.PH_ADD);
      if (!key || parcelByAddress.has(key)) continue;
      parcelByAddress.set(key, feature);
      fetched++;
      if (LIMIT && fetched >= LIMIT) break;
    }
    if (LIMIT && fetched >= LIMIT) break;
    if (features.length < PAGE_SIZE) break;
  }

  const activeProperties = await fetchActiveProperties();
  const existingParcelIds = await fetchExistingParcelIds();
  let matched = 0;
  let updated = 0;
  let skippedNoChange = 0;
  const samples: Array<Record<string, unknown>> = [];

  for (const property of activeProperties) {
    const parcel = parcelByAddress.get(normalizeAddress(property.address));
    if (!parcel) continue;
    matched++;
    const row = parcel.attributes;
    const center = centroid(parcel.geometry?.rings);
    const ownerName = cleanUpper(row.OW_NAME) || null;
    const mailingAddress = cleanUpper(row.OW_ADD) || null;
    const parcelId = clean(row.PARCELID) || null;
    const totalValue = parseNum(row.TOTAL_VAL);
    const landValue = parseNum(row.LAND_VAL);
    const improvementValue = parseNum(row.IMP_VAL);
    const acres = parseFloatPositive(row.ACRE_AREA) ?? parseFloatPositive(row.GIS_EST_AC);

    const patch: Record<string, unknown> = {
      source: "bentonville-arcgis-parcels",
      updated_at: new Date().toISOString(),
    };
    if (!property.owner_name && ownerName) patch.owner_name = ownerName;
    if (!property.mailing_address && mailingAddress) {
      patch.mailing_address = mailingAddress;
      patch.mail_address = mailingAddress;
    }
    if (!property.parcel_id && parcelId && !existingParcelIds.has(parcelId)) patch.parcel_id = parcelId;
    if (!property.apn_formatted && parcelId) patch.apn_formatted = parcelId;
    if (parcelId) patch.county_id = COUNTY_ID;
    if (property.latitude == null && center.lat != null) {
      patch.latitude = center.lat;
      patch.lat = center.lat;
    }
    if (property.longitude == null && center.lng != null) {
      patch.longitude = center.lng;
      patch.lng = center.lng;
    }
    if (totalValue) {
      patch.market_value = totalValue;
      patch.assessed_value = totalValue;
    }
    if (landValue) {
      patch.land_value = landValue;
      patch.appraised_land = landValue;
    }
    if (improvementValue) patch.appraised_building = improvementValue;
    if (acres) {
      patch.lot_acres = acres;
      patch.land_sqft = Math.round(acres * 43560);
    }
    const propertyType = classifyType(row.TYPE_);
    if (propertyType !== "unknown") {
      patch.property_type = propertyType;
      patch.land_use = cleanUpper(row.TYPE_);
      patch.property_use = cleanUpper(row.TYPE_);
    }

    if (Object.keys(patch).length <= 2) {
      skippedNoChange++;
      continue;
    }

    if (samples.length < 5) samples.push({ id: property.id, address: property.address, patch });
    if (!DRY_RUN) {
      const { error } = await db.from("properties").update(patch).eq("id", property.id);
      if (error) throw error;
    }
    updated++;
  }

  console.log(JSON.stringify({ fetched, active_properties: activeProperties.length, matched, updated, skipped_no_change: skippedNoChange, samples }, null, 2));
}

main().catch((error) => {
  console.error("Fatal Bentonville AR parcel backfill error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
