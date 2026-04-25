/**
 * Normalizes raw assessor data into our Property interface.
 * Handles address formatting, property classification, and value normalization.
 */

import type { RawPropertyRecord } from "./adapters/base.js";
import type { Property } from "../db/queries.js";

// ─── Property Type Classification ───────────────────────────────────

const APARTMENT_KEYWORDS = [
  "apartment", "apt", "multi-family", "multifamily", "multi family",
  "duplex", "triplex", "fourplex", "quadplex", "4-plex",
  "garden apt", "high rise", "mid rise", "low rise",
];

const COMMERCIAL_KEYWORDS = [
  "commercial", "office", "retail", "warehouse", "industrial",
  "hotel", "motel", "restaurant", "store", "shop",
];

const LAND_KEYWORDS = [
  "vacant", "land", "lot", "acreage", "farm", "ranch",
  "agricultural", "pasture", "timber",
];

const CONDO_KEYWORDS = ["condo", "condominium", "townhouse", "townhome"];

function classifyPropertyType(raw: RawPropertyRecord): {
  property_type: string;
  is_apartment: boolean;
  is_sfr: boolean;
  is_condo: boolean;
} {
  // Check adapter-provided classification first (from raw field)
  if (raw.raw?.isApartment === true) {
    return { property_type: raw.property_type || "multifamily", is_apartment: true, is_sfr: false, is_condo: false };
  }
  if (raw.raw?.isCondo === true) {
    return { property_type: raw.property_type || "condo", is_apartment: false, is_sfr: false, is_condo: true };
  }

  const typeStr = (raw.property_type ?? "").toLowerCase();
  const addrStr = (raw.address ?? "").toLowerCase();
  const combined = `${typeStr} ${addrStr}`;

  // Check for multifamily first
  if (
    APARTMENT_KEYWORDS.some((k) => combined.includes(k)) ||
    (raw.total_units && raw.total_units >= 5)
  ) {
    return { property_type: "multifamily", is_apartment: true, is_sfr: false, is_condo: false };
  }

  if (CONDO_KEYWORDS.some((k) => combined.includes(k))) {
    return { property_type: "condo", is_apartment: false, is_sfr: false, is_condo: true };
  }

  if (COMMERCIAL_KEYWORDS.some((k) => combined.includes(k))) {
    return { property_type: "commercial", is_apartment: false, is_sfr: false, is_condo: false };
  }

  if (LAND_KEYWORDS.some((k) => combined.includes(k))) {
    return { property_type: "land", is_apartment: false, is_sfr: false, is_condo: false };
  }

  // Check adapter-provided SFR flag
  if (raw.raw?.isSfr === true) {
    return { property_type: raw.property_type || "single_family", is_apartment: false, is_sfr: true, is_condo: false };
  }

  // Default: single-family residential
  return { property_type: "single_family", is_apartment: false, is_sfr: true, is_condo: false };
}

// ─── Address Normalization ──────────────────────────────────────────

function normalizeAddress(address: string): string {
  return address
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(\d+)\s+/, "$1 ") // normalize spacing after house number
    .toUpperCase();
}

function extractZip(zip: string | undefined): string {
  if (!zip) return "";
  // Take first 5 digits
  const match = zip.match(/\d{5}/);
  return match ? match[0] : zip.trim();
}

/**
 * Infer city from subdivision names commonly used by ActDataScout.
 * Falls back to the raw city value if no match.
 */
function inferCity(rawCity: string, subdivision?: string): string {
  const sub = (subdivision ?? "").toUpperCase().trim();
  const city = rawCity.toUpperCase().trim();

  // ActDataScout uses subdivisions that are often the city name
  // Map common subdivision prefixes to proper city names
  if (sub.startsWith("LAWTON") || sub === "BISHOP" || sub === "FLOWER MOUND") return "LAWTON";
  if (sub.startsWith("CACHE")) return "CACHE";
  if (sub.startsWith("ELGIN")) return "ELGIN";
  if (sub.startsWith("GERONIMO") || sub.startsWith("GREATER GERONIMO")) return "GERONIMO";
  if (sub.startsWith("FLETCHER")) return "FLETCHER";
  if (sub.startsWith("STERLING")) return "STERLING";
  if (sub.startsWith("INDIAHOMA")) return "INDIAHOMA";
  if (sub.startsWith("MEDICINE PARK")) return "MEDICINE PARK";
  if (sub.startsWith("CHATTANOOGA")) return "CHATTANOOGA";
  if (sub.startsWith("FAXON")) return "FAXON";

  return city || sub;
}

// ─── Value Normalization ────────────────────────────────────────────

function parseDollarAmount(val: string | number | undefined): number | undefined {
  if (val === undefined || val === null) return undefined;
  if (typeof val === "number") return val;
  const cleaned = val.replace(/[$,\s]/g, "");
  const num = parseFloat(cleaned);
  return isNaN(num) ? undefined : Math.round(num);
}

function parseInteger(val: string | number | undefined): number | undefined {
  if (val === undefined || val === null) return undefined;
  if (typeof val === "number") return Math.round(val);
  const cleaned = val.replace(/[,\s]/g, "");
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? undefined : num;
}

function parseDate(val: string | undefined): string | undefined {
  if (!val) return undefined;
  // Try to parse various date formats
  const d = new Date(val);
  if (isNaN(d.getTime())) return undefined;
  return d.toISOString().split("T")[0]; // YYYY-MM-DD
}

// ─── Main Normalizer ────────────────────────────────────────────────

export function normalizeProperty(
  raw: RawPropertyRecord,
  countyId: number,
): Property {
  const classification = classifyPropertyType(raw);

  return {
    county_id: countyId,
    parcel_id: raw.parcel_id?.trim() || undefined,
    address: normalizeAddress(raw.address),
    city: inferCity(raw.city ?? "", raw.raw?.subdivision as string | undefined),
    state_code: (raw.state ?? "").trim().toUpperCase().slice(0, 2),
    zip: extractZip(raw.zip),
    property_type: classification.property_type,
    is_apartment: classification.is_apartment,
    is_sfr: classification.is_sfr,
    is_condo: classification.is_condo,
    owner_name: raw.owner_name?.trim() || undefined,
    assessed_value: parseDollarAmount(raw.assessed_value),
    market_value: parseDollarAmount(raw.market_value),
    taxable_value: parseDollarAmount(raw.taxable_value),
    land_value: parseDollarAmount(raw.land_value),
    property_tax: parseDollarAmount(raw.property_tax),
    last_sale_price: parseDollarAmount(raw.last_sale_price),
    last_sale_date: parseDate(raw.last_sale_date),
    year_built: parseInteger(raw.year_built),
    total_sqft: parseInteger(raw.total_sqft),
    total_units: parseInteger(raw.total_units),
    total_buildings: parseInteger(raw.total_buildings),
    stories: parseInteger(raw.stories),
    construction_class: raw.construction_class?.trim() || undefined,
    improvement_quality: raw.improvement_quality?.trim() || undefined,
    land_sqft: parseInteger(raw.land_sqft),
    lot_acres: parseAcreage(raw.lot_acres),
    legal_description: raw.legal_description?.trim() || undefined,
    subdivision: raw.subdivision?.trim() || undefined,
    neighborhood_code: raw.neighborhood_code?.trim() || undefined,
    // Owner mailing — absentee owner = mailing_address differs from property address.
    mailing_address: raw.mailing_address?.trim() || undefined,
    mailing_city: raw.mailing_city?.trim()?.toUpperCase() || undefined,
    mailing_state: raw.mailing_state?.trim()?.toUpperCase().slice(0, 2) || undefined,
    mailing_zip: raw.mailing_zip ? extractZip(raw.mailing_zip) : undefined,
    absentee_owner: detectAbsenteeOwner(raw),
    corporate_owned: detectCorporateOwner(raw.owner_name),
    // Asset class
    property_class: raw.property_class?.trim() || undefined,
    property_use: raw.property_use?.trim() || undefined,
    appraised_land: parseDollarAmount(raw.appraised_land),
    appraised_building: parseDollarAmount(raw.appraised_building),
    assessor_url: raw.assessor_url,
    source: "assessor",
  };
}

// ─── New helpers for the expanded normalizer ───────────────────────────────

function parseAcreage(v: unknown): number | undefined {
  if (v == null) return undefined;
  const n = parseFloat(String(v).replace(/[^\d.\-]/g, ""));
  return isFinite(n) && n > 0 ? n : undefined;
}

function detectAbsenteeOwner(raw: RawPropertyRecord): boolean | undefined {
  // Absentee = owner's mailing address differs from the property address.
  // We compare on (mailing_zip, mailing_city) vs (zip, city) — if mailing_zip
  // exists and either differs, the owner doesn't live there.
  if (!raw.mailing_zip && !raw.mailing_city) return undefined;
  const propZip = String(raw.zip ?? "").trim().slice(0, 5);
  const mailZip = String(raw.mailing_zip ?? "").trim().slice(0, 5);
  if (mailZip && propZip && mailZip !== propZip) return true;
  const propCity = String(raw.city ?? "").trim().toUpperCase();
  const mailCity = String(raw.mailing_city ?? "").trim().toUpperCase();
  if (mailCity && propCity && mailCity !== propCity) return true;
  return false;
}

function detectCorporateOwner(name?: string): boolean | undefined {
  if (!name) return undefined;
  const n = name.toUpperCase();
  // Common corp/entity tokens. False positives are fine — used as a weak signal.
  return /\b(LLC|L\.L\.C\.|INC|CORP|CO\.|COMPANY|TRUST|LP|LLP|LTD|HOLDINGS|PARTNERS|GROUP|LLC\.|FOUNDATION|ESTATE OF|FAMILY TRUST)\b/.test(n);
}

/**
 * Normalize a batch of raw records.
 */
export function normalizeProperties(
  records: RawPropertyRecord[],
  countyId: number,
): Property[] {
  return records
    .map((r) => normalizeProperty(r, countyId))
    .filter((p) => p.address && p.city); // skip records missing critical fields
}
