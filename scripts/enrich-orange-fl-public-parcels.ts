#!/usr/bin/env tsx
/**
 * MXRE - Orange County, FL public parcel backfill for Orlando.
 *
 * Source: Orange County Property Appraiser DynamicForJs/PARCEL MapServer.
 * https://vgispublic.ocpafl.org/server/rest/services/DynamicForJs/PARCEL/MapServer/1
 */
import "dotenv/config";
import { firstEnv, hydrateWindowsUserEnv, preflightOutboundNetwork } from "./lib/env.ts";
import { makeDbClient, type DbClient } from "./lib/db.ts";

hydrateWindowsUserEnv();

const ARCGIS_URL = "https://vgispublic.ocpafl.org/server/rest/services/DynamicForJs/PARCEL/MapServer/1";
const STATE_CODE = "FL";
const CITY = "ORLANDO";
const TARGET_ZIPS = [
  "32801", "32803", "32804", "32805", "32806", "32807", "32808", "32809", "32810", "32811",
  "32812", "32814", "32817", "32819", "32821", "32822", "32824", "32827", "32829", "32832",
  "32835", "32836", "32837", "32839",
];
const PAGE_SIZE = 100;
const DRY_RUN = process.argv.includes("--dry-run");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="))?.split("=")[1];
const LIMIT = limitArg ? Math.max(0, Number.parseInt(limitArg, 10) || 0) : 0;
const propertyLimitArg = process.argv.find((arg) => arg.startsWith("--property-limit="))?.split("=")[1];
const PROPERTY_LIMIT = propertyLimitArg ? Math.max(0, Number.parseInt(propertyLimitArg, 10) || 0) : LIMIT;

type ArcgisFeature = { attributes: Record<string, unknown> };
type ActiveProperty = {
  id: number;
  address: string | null;
  owner_name: string | null;
  mailing_address: string | null;
  latitude: number | string | null;
  longitude: number | string | null;
};

function clean(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function cleanUpper(value: unknown): string {
  return clean(value).toUpperCase();
}

function normalizeAddress(value: unknown): string {
  return cleanUpper(value)
    .replace(/#/g, " UNIT ")
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
    .replace(/\bTRAIL\b/g, "TRL")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/\bAPARTMENT\b/g, "APT")
    .replace(/\bSUITE\b/g, "STE")
    .replace(/\bUNIT\s+0+(\d+[A-Z]?)\b/g, "UNIT $1")
    .replace(/\bAPT\s+0+(\d+[A-Z]?)\b/g, "APT $1")
    .replace(/\s+/g, " ")
    .trim();
}

function addressKeys(value: unknown): string[] {
  const full = normalizeAddress(value);
  if (!full) return [];
  const keys = new Set<string>([full]);
  for (const unitWord of ["UNIT", "APT", "STE"]) {
    const noMarker = full.replace(new RegExp(`\\s+${unitWord}\\s+([A-Z0-9-]+)$`, "g"), " $1").trim();
    if (noMarker !== full) keys.add(noMarker);
  }
  return [...keys].filter((key) => key.length >= 5);
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

function parseDate(value: unknown): string | null {
  if (value == null) return null;
  if (typeof value === "number" && value > 0) {
    const date = new Date(value);
    const year = date.getUTCFullYear();
    if (year > 1900 && year < 2100) return date.toISOString().slice(0, 10);
  }
  const parsed = new Date(clean(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
}

function mailingAddress(row: Record<string, unknown>) {
  const address = [row.ADD1, row.ADD2, row.ADD3, row.ADD4].map(cleanUpper).filter(Boolean).join(" ");
  return {
    address: address || null,
    city: cleanUpper(row.CITY) || null,
    state: cleanUpper(row.STATE) || null,
    zip: clean(row.ZIP).match(/\d{5}/)?.[0] ?? null,
  };
}

function classifyDor(row: Record<string, unknown>): string {
  const code = clean(row.DOR_CODE).replace(/\D/g, "").padStart(4, "0");
  if (code === "0300" || code === "0800") return "multifamily";
  if (code === "0400") return "condo";
  if (code === "0200") return "mobile_home";
  if (code === "0100" || code === "0104" || code === "0900") return "residential";
  if (code === "0000" || code === "1000") return "land";
  const n = Number.parseInt(code.slice(0, 2), 10);
  if (Number.isFinite(n) && n >= 11 && n <= 39) return "commercial";
  if (Number.isFinite(n) && n >= 40 && n <= 49) return "industrial";
  return "unknown";
}

const BASE_WHERE = `ZIP_SITUS in (${TARGET_ZIPS.map((zip) => `'${zip}'`).join(",")}) and SITUS is not null`;
const OUT_FIELDS = [
  "OBJECTID", "PARCEL", "NAME1", "NAME2", "SITUS", "SITUS_CITY", "SITUS_ZIP", "ZIP_SITUS",
  "ADD1", "ADD2", "ADD3", "ADD4", "CITY", "STATE", "ZIP", "TOTAL_MKT", "TOTAL_ASSD",
  "LAND_MKT", "BLDG_MKT", "TOTAL_LAND", "ACREAGE", "DOR_CODE", "LAND_DOR_CODE", "ZONING_CODE",
  "BEDS", "BATH", "LIVING_AREA", "GROSS_AREA", "AYB", "EYB", "SALE_DATE", "SALE_ADJ_VALUE",
  "LATITUDE", "LONGITUDE", "PARCEL_CATEGORY",
].join(",");

function streetNumber(value: unknown): number | null {
  const match = normalizeAddress(value).match(/^(\d{1,6})\b/);
  if (!match) return null;
  const n = Number.parseInt(match[1], 10);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function arcgisSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

async function fetchObjectIds(addresses: string[]): Promise<number[]> {
  const ids = new Set<number>();
  const chunkSize = 20;
  for (let i = 0; i < addresses.length; i += chunkSize) {
    const chunk = addresses.slice(i, i + chunkSize);
    console.log(`Fetching OCPA object IDs for address chunk ${Math.floor(i / chunkSize) + 1}/${Math.ceil(addresses.length / chunkSize)} (${chunk.length} addresses)`);
    const where = `${BASE_WHERE} and SITUS in (${chunk.map(arcgisSqlString).join(",")})`;
    const response = await fetch(`${ARCGIS_URL}/query?${new URLSearchParams({ where, returnIdsOnly: "true", f: "json" })}`, { signal: AbortSignal.timeout(60_000) });
    if (!response.ok) throw new Error(`OCPA ArcGIS ID HTTP ${response.status}: ${await response.text()}`);
    const json = (await response.json()) as { error?: unknown; objectIds?: number[] };
    if (json.error) throw new Error(JSON.stringify(json.error));
    for (const id of json.objectIds ?? []) {
      if (Number.isFinite(id)) ids.add(id);
    }
  }
  return [...ids].sort((a, b) => a - b);
}

async function fetchObjectChunk(ids: number[]): Promise<ArcgisFeature[]> {
  const response = await fetch(`${ARCGIS_URL}/query?${new URLSearchParams({ objectIds: ids.join(","), outFields: OUT_FIELDS, returnGeometry: "false", f: "json" })}`, { signal: AbortSignal.timeout(45_000) });
  if (!response.ok) throw new Error(`OCPA ArcGIS HTTP ${response.status}: ${await response.text()}`);
  const json = (await response.json()) as { error?: unknown; features?: ArcgisFeature[] };
  if (json.error) throw new Error(JSON.stringify(json.error));
  return json.features ?? [];
}

async function fetchObjectChunkSafe(ids: number[]): Promise<{ features: ArcgisFeature[]; failedIds: number[] }> {
  try {
    return { features: await fetchObjectChunk(ids), failedIds: [] };
  } catch {
    if (ids.length === 1) return { features: [], failedIds: ids };
    const features: ArcgisFeature[] = [];
    const failedIds: number[] = [];
    for (const id of ids) {
      const single = await fetchObjectChunkSafe([id]);
      features.push(...single.features);
      failedIds.push(...single.failedIds);
    }
    return { features, failedIds };
  }
}

async function fetchActiveProperties(db: DbClient): Promise<ActiveProperty[]> {
  const { rows } = await db.query<ActiveProperty>(`
    with active_ids as (
      select distinct property_id::bigint as property_id
      from listing_signals
      where is_on_market = true
        and state_code = $1
        and (upper(coalesce(city, '')) = $2 or zip = any($3::text[]))
        and property_id is not null
    )
    select p.id, p.address, p.owner_name, p.mailing_address, p.latitude, p.longitude
    from properties p
    join active_ids a on a.property_id = p.id
    where nullif(p.address, '') is not null
    order by p.id
  `, [STATE_CODE, CITY, TARGET_ZIPS]);
  return rows;
}

async function updateProperty(db: DbClient, id: number, patch: Record<string, unknown>): Promise<void> {
  const entries = Object.entries(patch);
  const assignments = entries.map(([key, value]) => `${key} = ${sqlLiteral(value)}`);
  await db.query(`update properties set ${assignments.join(", ")} where id = ${id}`);
}

function sqlLiteral(value: unknown): string {
  if (value == null) return "null";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function main() {
  console.log("MXRE - Orange County, FL public parcel backfill");
  console.log("=".repeat(59));
  console.log(`Market: ${CITY}, ${STATE_CODE}`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Limit: ${LIMIT || "none"}`);
  console.log(`Property limit: ${PROPERTY_LIMIT || "none"}`);

  await preflightOutboundNetwork({ url: ARCGIS_URL });

  const db = await makeDbClient();
  try {
  const allActiveProperties = await fetchActiveProperties(db);
  const activeProperties = PROPERTY_LIMIT ? allActiveProperties.slice(0, PROPERTY_LIMIT) : allActiveProperties;
  const activeAddresses = [...new Set(activeProperties.map((property) => normalizeAddress(property.address)).filter(Boolean))];
  const activeStreetNumbers = [...new Set(activeProperties.map((property) => streetNumber(property.address)).filter((value): value is number => value != null))];
  console.log(`Loaded ${allActiveProperties.length} active Orlando properties; using ${activeProperties.length} for this run with ${activeAddresses.length} unique addresses and ${activeStreetNumbers.length} unique street numbers.`);
  const parcelByAddress = new Map<string, ArcgisFeature>();
  let fetched = 0;
  let failedObjectIds = 0;
  const objectIds = await fetchObjectIds(activeAddresses);
  console.log(`OCPA returned ${objectIds.length} candidate parcel object IDs.`);
  for (let offset = 0; offset < objectIds.length; offset += PAGE_SIZE) {
    console.log(`Fetching OCPA parcel detail chunk ${Math.floor(offset / PAGE_SIZE) + 1}/${Math.ceil(objectIds.length / PAGE_SIZE)}`);
    const chunk = await fetchObjectChunkSafe(objectIds.slice(offset, offset + PAGE_SIZE));
    failedObjectIds += chunk.failedIds.length;
    for (const feature of chunk.features) {
      for (const key of addressKeys(feature.attributes.SITUS)) {
        if (!parcelByAddress.has(key)) parcelByAddress.set(key, feature);
      }
      fetched++;
      if (LIMIT && fetched >= LIMIT) break;
    }
    if (LIMIT && fetched >= LIMIT) break;
  }

  let matched = 0;
  let updated = 0;
  let skippedNoChange = 0;
  const samples: Array<Record<string, unknown>> = [];

  for (const property of activeProperties) {
    const parcel = addressKeys(property.address).map((key) => parcelByAddress.get(key)).find((value): value is ArcgisFeature => Boolean(value));
    if (!parcel) continue;
    matched++;
    const row = parcel.attributes;
    const mail = mailingAddress(row);
    const lat = parseFloatPositive(row.LATITUDE);
    const lngRaw = Number.parseFloat(clean(row.LONGITUDE));
    const lng = Number.isFinite(lngRaw) && lngRaw < 0 ? lngRaw : null;
    const acres = parseFloatPositive(row.ACREAGE);
    const patch: Record<string, unknown> = { source: "orange-county-fl-ocpa-parcels", updated_at: new Date().toISOString() };

    const ownerName = [row.NAME1, row.NAME2].map(cleanUpper).filter(Boolean).join("; ") || null;
    if (!property.owner_name && ownerName) patch.owner_name = ownerName;
    if (!property.mailing_address && mail.address) {
      patch.mailing_address = mail.address;
      patch.mailing_city = mail.city;
      patch.mailing_state = mail.state;
      patch.mailing_zip = mail.zip;
      patch.mail_address = mail.address;
      patch.mail_city = mail.city;
      patch.mail_state = mail.state;
      patch.mail_zip = mail.zip;
    }
    if (property.latitude == null && lat != null) {
      patch.latitude = lat;
      patch.lat = lat;
    }
    if (property.longitude == null && lng != null) {
      patch.longitude = lng;
      patch.lng = lng;
    }
    if (parseNum(row.TOTAL_MKT)) patch.market_value = parseNum(row.TOTAL_MKT);
    if (parseNum(row.TOTAL_ASSD)) patch.assessed_value = parseNum(row.TOTAL_ASSD);
    if (parseNum(row.LAND_MKT)) {
      patch.land_value = parseNum(row.LAND_MKT);
      patch.appraised_land = parseNum(row.LAND_MKT);
    }
    if (parseNum(row.BLDG_MKT)) patch.appraised_building = parseNum(row.BLDG_MKT);
    if (acres) patch.lot_acres = acres;
    if (parseNum(row.LIVING_AREA)) patch.total_sqft = parseNum(row.LIVING_AREA);
    if (parseNum(row.BEDS)) patch.bedrooms = parseNum(row.BEDS);
    if (parseFloatPositive(row.BATH)) patch.bathrooms = parseFloatPositive(row.BATH);
    if (parseNum(row.AYB)) patch.year_built = parseNum(row.AYB);
    if (parseNum(row.SALE_ADJ_VALUE)) patch.last_sale_price = parseNum(row.SALE_ADJ_VALUE);
    if (parseDate(row.SALE_DATE)) patch.last_sale_date = parseDate(row.SALE_DATE);
    const propertyType = classifyDor(row);
    if (propertyType !== "unknown") {
      patch.property_type = propertyType;
      patch.land_use = clean(row.DOR_CODE) || null;
      patch.property_use = patch.land_use;
    }
    if (clean(row.ZONING_CODE)) patch.zoning = cleanUpper(row.ZONING_CODE);

    if (Object.keys(patch).length <= 2) {
      skippedNoChange++;
      continue;
    }
    if (samples.length < 5) samples.push({ id: property.id, address: property.address, patch });
    if (!DRY_RUN) {
      await updateProperty(db, property.id, patch);
    }
    updated++;
  }

  console.log(JSON.stringify({ fetched, failed_object_ids: failedObjectIds, active_properties: activeProperties.length, active_street_numbers: activeStreetNumbers.length, object_ids: objectIds.length, matched, updated, skipped_no_change: skippedNoChange, samples }, null, 2));
  } finally {
    await db.end();
  }
}

main().catch((error) => {
  console.error("Fatal Orange County FL parcel backfill error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
