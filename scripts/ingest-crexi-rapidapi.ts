#!/usr/bin/env tsx
import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { detectCreativeFinanceSignals } from "./lib/creative-finance-signals.ts";
import { normalizeCrexiBuildingClass } from "./lib/crexi-listing-class.ts";
import { parseCrexiMultifamilyStats } from "./lib/crexi-multifamily-stats.ts";
import { parseCrexiTextStats } from "./lib/crexi-text-stats.ts";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const RAPIDAPI_HOST = firstEnv("CREXI_RAPIDAPI_HOST") ?? "unofficial-crexi-data.p.rapidapi.com";
const RAPIDAPI_KEY = firstEnv("CREXI_RAPIDAPI_KEY", "RAPIDAPI_KEY");
const DRY_RUN = process.argv.includes("--dry-run");

const arg = (name: string, fallback?: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const MARKET = (arg("market", "indianapolis") ?? "indianapolis").toLowerCase();
const CITY = arg("city", "Indianapolis") ?? "Indianapolis";
const STATE = (arg("state", "IN") ?? "IN").toUpperCase();
const LAT = arg("lat", "39.76909") ?? "39.76909";
const LNG = arg("lng", "-86.158018") ?? "-86.158018";
const ASSET_CLASS = (arg("asset-class", "all") ?? "all").toLowerCase();
const LIMIT = Math.max(1, Number.parseInt(arg("limit", "60") ?? "60", 10));
const DETAIL_LIMIT = Math.max(0, Number.parseInt(arg("detail-limit", "10") ?? "10", 10));
const MAX_UPSERT = Math.max(1, Number.parseInt(arg("max-upsert", String(Number.MAX_SAFE_INTEGER)) ?? String(Number.MAX_SAFE_INTEGER), 10));
const PAGE = Math.max(1, Number.parseInt(arg("page", "1") ?? "1", 10));
const NATIONWIDE = process.argv.includes("--nationwide");
const CREXI_PUBLIC_RV_SEARCH = process.argv.includes("--crexi-public-rv-search");
const CREXI_PUBLIC_TYPE = arg("crexi-public-type");
const CREXI_PUBLIC_SUBTYPE = arg("crexi-public-subtype");
const CREXI_PUBLIC_SCOPE = arg("crexi-public-scope");
const SWEEP_STARTED_AT = arg("sweep-started-at");
const CREXI_PUBLIC_IDS = (arg("crexi-public-ids") ?? "")
  .split(",")
  .map(id => id.trim().replace(/^sales-/, ""))
  .filter(Boolean);

type CrexiLocation = {
  address?: string;
  city?: string;
  state?: { code?: string; name?: string };
  zip?: string;
  fullAddress?: string;
  latitude?: number;
  longitude?: number;
};

type CrexiSearchRow = {
  id?: number | string;
  name?: string;
  description?: string;
  urlSlug?: string;
  brokerageName?: string;
  activatedOn?: string;
  updatedOn?: string;
  askingPrice?: number;
  squareFootage?: number;
  types?: string[];
  status?: string;
  locations?: CrexiLocation[];
};

type CrexiUniversalSearchItem = {
  id?: string;
  propertyName?: string;
  urlSlug?: string;
  propertyPrice?: { total?: number };
  financials?: { netOperatingIncome?: number };
  propertyAttributes?: { type?: string; subType?: string };
  listingAttributes?: { status?: string; dateActivated?: string; dateUpdated?: string };
  address?: Array<{
    fullAddress?: string;
    streetAddress?: string;
    city?: string;
    stateCode?: string;
    stateName?: string;
    zip?: string;
    county?: string;
    slug?: string;
    location?: { lat?: number; lon?: number };
  }>;
};

type ClassifiedRow = {
  assetClass: "multifamily" | "self_storage" | "mobile_home_rv";
  confidence: "low" | "medium";
  row: CrexiSearchRow;
  detail?: Record<string, unknown>;
};

function requireRapidApiKey(): string {
  if (!RAPIDAPI_KEY) throw new Error("Missing CREXI_RAPIDAPI_KEY or RAPIDAPI_KEY.");
  return RAPIDAPI_KEY;
}

async function crexiGet(path: string, params: Record<string, string | number | undefined>) {
  const url = new URL(`https://${RAPIDAPI_HOST}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") url.searchParams.set(key, String(value));
  }
  const response = await fetch(url, {
    headers: {
      "x-rapidapi-key": requireRapidApiKey(),
      "x-rapidapi-host": RAPIDAPI_HOST,
    },
    signal: AbortSignal.timeout(45_000),
  });
  const body = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) throw new Error(`${path} ${response.status}: ${JSON.stringify(body).slice(0, 500)}`);
  return body;
}

async function crexiPublicUniversalSearch(from: number, size: number) {
  const typeValues = CREXI_PUBLIC_TYPE
    ? CREXI_PUBLIC_TYPE.split(",").map(part => part.trim()).filter(Boolean)
    : ["Multifamily"];
  const subTypeValues = CREXI_PUBLIC_SUBTYPE
    ? CREXI_PUBLIC_SUBTYPE.split(",").map(part => part.trim()).filter(Boolean)
    : CREXI_PUBLIC_TYPE
      ? []
      : ["RV Park"];
  const filters: Record<string, unknown> = {
    "searchAttributes.status": {
      mode: "Include",
      structuredValues: ["On-Market", "Auction", "Highest & Best", "Call For Offers"],
      type: "Plain",
      values: [],
    },
  };
  if (typeValues.length > 0) {
    filters["propertyAttributes.type"] = {
      mode: "Include",
      structuredValues: typeValues,
      type: "Plain",
      values: [],
    };
  }
  if (subTypeValues.length > 0) {
    filters["propertyAttributes.subType"] = {
      mode: "Include",
      structuredValues: subTypeValues,
      type: "Plain",
      values: [],
    };
  }
  const body = {
    boundingBox: {
      latitudeMax: 61.13444375228813,
      latitudeMin: 9.170889762862968,
      longitudeMax: -78.45254687499998,
      longitudeMin: -118.70645312499998,
    },
    excludeFilters: [],
    excludeSort: [],
    filters,
    from,
    ids: CREXI_PUBLIC_IDS.map(id => `sales-${id}`),
    searchTypes: ["Sales"],
    size,
    sorting: { "searchAttributes.crexiSearchRank": "Descending" },
  };
  const response = await fetch("https://api.crexi.com/universal-search/v2/search", {
    method: "POST",
    headers: {
      "accept": "application/json",
      "content-type": "application/json",
      "origin": "https://www.crexi.com",
      "referer": CREXI_PUBLIC_TYPE === "Mobile Home Park"
        ? "https://www.crexi.com/properties/Mobile-Home-Park"
        : "https://www.crexi.com/search/rv-parks-properties-for-sale",
      "user-agent": "Mozilla/5.0",
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(45_000),
  });
  const parsed = await response.json().catch(async () => ({ error: await response.text() }));
  if (!response.ok) throw new Error(`universal-search ${response.status}: ${JSON.stringify(parsed).slice(0, 500)}`);
  return parsed as { items?: CrexiUniversalSearchItem[]; totalCount?: number };
}

function crexiUniversalItemToSearchRow(item: CrexiUniversalSearchItem): CrexiSearchRow | null {
  const rawId = String(item.id ?? "").replace(/^sales-/, "");
  if (!rawId) return null;
  const address = item.address?.[0];
  return {
    id: rawId,
    name: item.propertyName,
    description: compactText([
      item.propertyName,
      item.propertyAttributes?.type,
      item.propertyAttributes?.subType,
      address?.fullAddress,
    ]),
    urlSlug: item.urlSlug ?? address?.slug,
    askingPrice: item.propertyPrice?.total,
    types: compactText([item.propertyAttributes?.type]).split(",").map(part => part.trim()).filter(Boolean),
    status: item.listingAttributes?.status,
    activatedOn: item.listingAttributes?.dateActivated,
    updatedOn: item.listingAttributes?.dateUpdated,
    locations: address ? [{
      fullAddress: address.fullAddress,
      address: address.streetAddress,
      city: address.city,
      state: { code: address.stateCode, name: address.stateName },
      zip: address.zip,
      latitude: address.location?.lat,
      longitude: address.location?.lon,
    }] : [],
  };
}

function compactText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(compactText).join(" ");
  if (typeof value === "object") return Object.values(value as Record<string, unknown>).map(compactText).join(" ");
  return String(value);
}

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function marketFromLocation(location: CrexiLocation | Record<string, unknown> | undefined): string {
  const city = typeof location?.city === "string" ? location.city : CITY;
  const stateValue = (location as CrexiLocation | undefined)?.state;
  const state = typeof stateValue === "object" && stateValue && "code" in stateValue
    ? String((stateValue as { code?: string }).code ?? STATE)
    : typeof (location as Record<string, unknown> | undefined)?.state === "string"
      ? String((location as Record<string, unknown>).state)
      : STATE;
  return city && state ? `${slug(city)}-${state.toLowerCase()}` : MARKET;
}

function classify(row: CrexiSearchRow, detail?: Record<string, unknown>): ClassifiedRow | null {
  const rowText = compactText([
    row.name,
    row.description,
    row.types,
    row.locations?.map(l => l.fullAddress),
  ]).toLowerCase();
  const detailTypeText = compactText([
    detail?.subtypes,
    (detail?.details as Record<string, unknown> | undefined)?.["Property Type"],
    (detail?.details as Record<string, unknown> | undefined)?.["Sub Type"],
    (Array.isArray(detail?.summaryDetails) ? detail?.summaryDetails as Record<string, unknown>[] : [])
      .filter(item => /property type|subtype/i.test(String(item.label ?? item.key ?? "")))
      .map(item => item.display ?? item.value),
  ]).toLowerCase();
  const fullText = compactText([
    rowText,
    detailTypeText,
    detail?.details,
    detail?.summaryDetails,
    detail?.investmentHighlights,
  ]).toLowerCase();
  const propertyTypeText = compactText([
    detail?.details,
    detail?.summaryDetails,
    row.types,
  ]).toLowerCase();

  const selfStoragePattern = /\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage units?\b|\bmini[-\s]?warehouse\b/;
  const actualSelfStorageType = selfStoragePattern.test(detailTypeText);
  const explicitSelfStorageListing =
    selfStoragePattern.test(rowText) &&
    !/\bredevelopment\b|\bpotential uses?\b|\bmany potential uses?\b|\bland opportunity\b|\bauto shop\b|\bwarehouse\b|\boffice\b|\bretail\b/.test(rowText);
  const isMultifamily =
    /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(detailTypeText) ||
    /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(rowText);
  const hasMobileRvSignal = /\brv\b|\bmobile home\b|\bmanufactured housing\b|\bmhp\b|\bmhc\b|\btrailer park\b|\bcampground\b|\bcampsite\b/.test(fullText);
  const incompatibleMobileRvListing =
    (/\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(fullText) && !hasMobileRvSignal) ||
    (/\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(fullText) && !hasMobileRvSignal);
  const rvParkPattern = /\brv\s*parks?\b|\brv\s*resorts?\b|\brecreational vehicle\s*parks?\b|\bcampgrounds?\b|\bkoa\b|\bglamping\b|\bmobile home\s*(?:park|community)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b/;
  const actualRvParkType = rvParkPattern.test(detailTypeText);
  const rvParkLotFalsePositive =
    /\brv\s*park\s*lot\b|\brv\s*lot\b|\bdeeded\s*rv\s*lot\b|\bindividual\s*lot\b/.test(fullText)
    && !/\bmobile home park\b|\bmanufactured housing\b|\b\d+\s*(?:sites|pads|spaces)\b/.test(propertyTypeText);
  const rvDevelopmentOnly =
    /\bproposed\s+rv\b|\bfor\s+(?:an?\s+)?rv\s+park\b|\bpotential\s+(?:rv|mobile home)\b|\bpossible\s+(?:rv|mobile home)\b|\bideal\s+for\s+(?:an?\s+)?rv\b|\brv\s+park\s+development\b|\b(?:land\/home|land-home)\s+sites?\b|\bmhp\s+or\b|\bdevelopment\s+(?:land|opportunity)\b|\bunrestricted\s+acreage\b/.test(fullText)
    && !/\b(?:existing|established|operating|stabilized|income[-\s]?producing)\b|\bnoi\b|\bcap\s*rate\b/.test(fullText);
  const landUseMenuOnly =
    /\bland\b/.test(propertyTypeText) &&
    /\bhotel\b/.test(fullText) &&
    /\brv\s*parks?\b/.test(fullText) &&
    /\bcommercial\b|\bretail\b|\bmedical\b|\bsports venue\b/.test(fullText) &&
    !/\b(?:existing|established|operating|stabilized|income[-\s]?producing)\b|\bnoi\b|\bcap\s*rate\b/.test(fullText);
  const explicitRvParkListing =
    rvParkPattern.test(fullText) &&
    !rvParkLotFalsePositive &&
    !rvDevelopmentOnly &&
    !landUseMenuOnly &&
    !/\bnearby\b|\badjacent\b|\bminutes from\b|\bpotential uses?\b|\bpossible use\b|\bideal for\b|\bdevelopment opportunity\b/.test(fullText);

  if ((actualSelfStorageType || explicitSelfStorageListing) && (ASSET_CLASS === "all" || ASSET_CLASS === "self_storage")) {
    return { assetClass: "self_storage", confidence: detail ? "medium" : "low", row, detail };
  }
  if ((actualRvParkType || explicitRvParkListing) && !rvParkLotFalsePositive && !rvDevelopmentOnly && !landUseMenuOnly && !incompatibleMobileRvListing && (ASSET_CLASS === "all" || ASSET_CLASS === "mobile_home_rv" || ASSET_CLASS === "rv_park")) {
    return { assetClass: "mobile_home_rv", confidence: detail ? "medium" : "low", row, detail };
  }
  if (isMultifamily && !hasMobileRvSignal && !actualRvParkType && (ASSET_CLASS === "all" || ASSET_CLASS === "multifamily")) {
    return { assetClass: "multifamily", confidence: detail ? "medium" : "low", row, detail };
  }
  return null;
}

function numberFromDisplay(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);
  const text = String(value ?? "");
  const match = text.replace(/,/g, "").match(/\d+/);
  return match ? Number.parseInt(match[0], 10) : null;
}

function positiveNumber(value: unknown): boolean {
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value.replace(/[%,$,\s]/g, ""));
    return Number.isFinite(parsed) && parsed > 0;
  }
  return false;
}

function withStandardUnitFields(
  raw: Record<string, unknown>,
  units: number | null,
  unitSource: string | null,
): Record<string, unknown> {
  return {
    ...raw,
    total_units: units,
    unit_count: units,
    unit_count_status: units !== null ? "explicit_value" : "no_data",
    unit_count_source: units !== null ? unitSource : "no_data",
  };
}

function withApartmentUnits(
  unitMix: Record<string, unknown>,
  apartmentUnits: number | null,
): Record<string, unknown> {
  return {
    ...unitMix,
    apartmentUnits: apartmentUnits ?? unitMix.apartmentUnits ?? null,
  };
}

function detailValue(detail: Record<string, unknown> | undefined, label: RegExp): unknown {
  const summary = Array.isArray(detail?.summaryDetails) ? detail?.summaryDetails as Record<string, unknown>[] : [];
  const match = summary.find(item => label.test(String(item.label ?? item.key ?? "")));
  if (match) return match.value ?? match.display;
  const details = detail?.details && typeof detail.details === "object" ? detail.details as Record<string, unknown> : {};
  const detailsMatch = Object.entries(details).find(([key]) => label.test(key));
  if (detailsMatch) return detailsMatch[1];
  const directMatch = Object.entries(detail ?? {}).find(([key]) => label.test(key));
  return directMatch?.[1];
}

function inferUnits(row: CrexiSearchRow, detail?: Record<string, unknown>): number | null {
  const unitValue = detailValue(detail, /^units?$|^sites?$|^spaces?$/i);
  const fromDetail = numberFromDisplay(unitValue);
  if (fromDetail !== null && fromDetail > 0 && fromDetail < 10_000) return fromDetail;
  const text = `${row.name ?? ""} ${row.description ?? ""}`;
  const match = text.match(/\b(\d{1,5})\s*(?:rv\s*)?(?:sites?|pads?|spaces?|units?)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1], 10);
  return parsed > 0 && parsed < 10_000 ? parsed : null;
}

function inferPads(detail?: Record<string, unknown>): number | null {
  const pads = numberFromDisplay(detailValue(detail, /^pads?$/i));
  return pads !== null && pads > 0 ? pads : null;
}

function stringFromDisplay(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).replace(/\s+/g, " ").trim();
  return text || null;
}

function inferDetailString(detail: Record<string, unknown> | undefined, label: RegExp): string | null {
  return stringFromDisplay(detailValue(detail, label));
}

function crexiBuildingClass(detail: Record<string, unknown> | undefined) {
  return normalizeCrexiBuildingClass(inferDetailString(detail, /^class$/i));
}

function inferListPrice(row: CrexiSearchRow, detail?: Record<string, unknown>): number | null {
  if (typeof row.askingPrice === "number" && Number.isFinite(row.askingPrice)) return Math.round(row.askingPrice);
  return numberFromDisplay(detailValue(detail, /asking|list price|price/i));
}

function inferNoi(detail?: Record<string, unknown>): number | null {
  if (typeof detail?.netOperatingIncome === "number" && Number.isFinite(detail.netOperatingIncome)) {
    return Math.round(detail.netOperatingIncome);
  }
  return numberFromDisplay(detailValue(detail, /\bnoi\b|net\s*operating\s*income|netOperatingIncome/i));
}

function inferCapRate(detail?: Record<string, unknown>): number | null {
  if (typeof detail?.capRate === "number" && Number.isFinite(detail.capRate)) return detail.capRate;
  const value = detailValue(detail, /cap\s*rate|capRate|capitalization/i);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value ?? "");
  const match = text.match(/\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function inferProFormaNoi(detail?: Record<string, unknown>): number | null {
  return numberFromDisplay(detailValue(detail, /pro[\s-]?forma\s*noi|proFormaNoi/i));
}

function inferProFormaCapRate(detail?: Record<string, unknown>): number | null {
  const value = detailValue(detail, /pro[\s-]?forma\s*cap\s*rate|proFormaCapRate/i);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value ?? "").match(/\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function inferLotSizeAcres(detail?: Record<string, unknown>): number | null {
  const value = detailValue(detail, /lot\s*size|acreage|acres/i);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value ?? "").match(/\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function inferPricePerItem(row: CrexiSearchRow, detail?: Record<string, unknown>, units?: number | null): number | null {
  if (typeof detail?.pricePerItem === "number" && Number.isFinite(detail.pricePerItem)) return Math.round(detail.pricePerItem);
  const fromDetail = numberFromDisplay(detailValue(detail, /price\s*\/\s*(pad|site|unit)|pricePerItem/i));
  if (fromDetail !== null) return fromDetail;
  const listPrice = inferListPrice(row, detail);
  return listPrice && units ? Math.round(listPrice / units) : null;
}

function stripHtml(value: unknown): string | null {
  const text = String(value ?? "")
    .replace(/<\/p>\s*<p>/gi, "\n\n")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text || null;
}

function marketingDescription(detail?: Record<string, unknown>): string | null {
  return stripHtml(detail?.marketingDescription);
}

function investmentHighlights(detail?: Record<string, unknown>): string | null {
  return stripHtml(detail?.investmentHighlights);
}

function listingDescription(row: CrexiSearchRow, detail?: Record<string, unknown>): string | null {
  const parts = [
    stripHtml(row.description),
    marketingDescription(detail),
    investmentHighlights(detail),
  ].filter((part): part is string => Boolean(part));
  return parts.length > 0 ? [...new Set(parts)].join("\n\n") : null;
}

function parsedTextStats(row: CrexiSearchRow, detail?: Record<string, unknown>) {
  return parseCrexiTextStats(
    row.description,
    marketingDescription(detail),
    investmentHighlights(detail),
    detail?.details,
    detail?.summaryDetails,
  );
}

function inferSubAssetType(row: CrexiSearchRow, detail?: Record<string, unknown>): "rv_park" | "mobile_home_park" | "mixed_rv_mhp" | "campground" | "unknown" {
  const parsed = parsedTextStats(row, detail);
  const explicitRvSiteEvidence =
    positiveNumber(parsed.unitMix.rvSites) ||
    positiveNumber(parsed.unitMix.pullThroughSites);
  const explicitMobileHomeSiteEvidence =
    positiveNumber(parsed.unitMix.mobileHomeSites);
  const titleText = compactText([row.name, row.description]).toLowerCase();
  const typeText = compactText([
    row.types,
    detail?.subtypes,
    detail?.customSubtypes,
    (detail?.details as Record<string, unknown> | undefined)?.["Property Type"],
    (detail?.details as Record<string, unknown> | undefined)?.["Sub Type"],
    (Array.isArray(detail?.summaryDetails) ? detail?.summaryDetails as Record<string, unknown>[] : [])
      .filter(item => /property type|subtype/i.test(String(item.label ?? item.key ?? "")))
      .map(item => item.display ?? item.value),
  ]).toLowerCase();
  const text = compactText([
    row.name,
    row.description,
    row.types,
    detail?.details,
    detail?.summaryDetails,
    detail?.marketingDescription,
    detail?.investmentHighlights,
    detail?.subtypes,
    detail?.customSubtypes,
  ]).toLowerCase();
  const evidenceText = compactText([
    row.name,
    row.description,
    detail?.details,
    detail?.summaryDetails,
    detail?.marketingDescription,
    detail?.investmentHighlights,
  ]).toLowerCase();
  const strongRvMhpTitle =
    /\brv\s*parks?\b|\brv\s*resorts?\b|\bcampgrounds?\b|\bmobile home\s*(?:park|community)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b|\btrailer park\b/.test(titleText);
  const strongRvMhpType =
    /\brv\s*parks?\b|\brv\s*resorts?\b|\bcampgrounds?\b|\bmobile home\s*(?:park|community)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b|\btrailer park\b/.test(typeText);
  const incompatibleTitle =
    (/\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(titleText) ||
      /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b|\b[0-9]+[-\s]?unit\b/.test(titleText)) &&
    !strongRvMhpTitle &&
    !strongRvMhpType;
  const storageSignal =
    /\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(titleText) ||
    /\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(typeText);
  if (incompatibleTitle) return "unknown";
  const operatingEvidence =
    positiveNumber(detail?.netOperatingIncome) ||
    positiveNumber(detail?.capRate) ||
    positiveNumber(detailValue(detail, /\bnoi\b|net\s*operating\s*income|netOperatingIncome/i)) ||
    positiveNumber(detailValue(detail, /cap\s*rate|capRate|capitalization/i)) ||
    /\b(?:existing|established|operating|stabilized|income[-\s]?producing)\b|\bnoi\b|\bcap\s*rate\b/.test(text);
  const financialEvidence =
    positiveNumber(detail?.netOperatingIncome) ||
    positiveNumber(detail?.capRate) ||
    positiveNumber(detailValue(detail, /\bnoi\b|net\s*operating\s*income|netOperatingIncome/i)) ||
    positiveNumber(detailValue(detail, /cap\s*rate|capRate|capitalization/i));
  const directListingEvidence =
    /\brv\s*parks?\b|\brv\s*resorts?\b|\bcampgrounds?\b|\bmobile home\s*(?:park|community)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b|\btrailer park\b/.test(evidenceText);
  const entitledRvLotEvidence =
    /\b(?:fully\s+entitled|entitled|platted|approved|permitted)\b/.test(text) &&
    /\b[0-9]{1,5}\s*rv\s*(?:lots?|sites?|spaces?|pads?)\b/.test(text) &&
    /\brv\s*(?:parks?|resorts?)\b|\brecreational vehicle\s+park\b/.test(text);
  const developmentListingWithoutSiteEvidence =
    /\bdevelopment\b|\bdev\s+site\b|\bland\b/.test(typeText) &&
    /\bdevelopment\b|\bdevelop(?:ment|er|ers|able)?\b|\bshovel[-\s]?ready\b|\bentitled\b|\bplanned\b|\bproposed\b/.test(text) &&
    !explicitRvSiteEvidence &&
    !explicitMobileHomeSiteEvidence;
  if (developmentListingWithoutSiteEvidence) return "unknown";
  if ((strongRvMhpTitle || strongRvMhpType) && explicitRvSiteEvidence && !storageSignal) {
    return /\bmobile home\s*(?:park|community)\b|\bmhp\b|\bmhc\b/.test(text) ? "mixed_rv_mhp" : "rv_park";
  }
  if ((strongRvMhpTitle || strongRvMhpType) && explicitMobileHomeSiteEvidence && !explicitRvSiteEvidence && !storageSignal) {
    return "mobile_home_park";
  }
  if (strongRvMhpType && financialEvidence && !storageSignal) {
    const hasRvSignal = /\brv\s*parks?\b|\brv\s*resorts?\b|\brv\s*sites?\b|\brv\s*pads?\b|\brv\s*spaces?\b|\brecreational vehicle\b|\bcampgrounds?\b/.test(text);
    const hasMhpSignal = /\bmobile home\s*(?:park|community|sites?|pads?|spaces?)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b/.test(text);
    if (hasRvSignal && hasMhpSignal) return "mixed_rv_mhp";
    if (hasRvSignal) return "rv_park";
    if (hasMhpSignal) return "mobile_home_park";
  }
  if (/\bcampgrounds?\b/.test(titleText) && !/\bcampground\s+(?:road|rd)\b/.test(titleText)) return "campground";
  if (strongRvMhpType && !directListingEvidence && !financialEvidence && !explicitRvSiteEvidence) return "unknown";
  if (storageSignal && !explicitRvSiteEvidence && !/\brv\/mh\s+parks?\b|\brv\s+sites?\b|\bmobile home\s+(?:park|community|sites?)\b/.test(evidenceText)) return "unknown";
  if (/\bhospitality\b/.test(typeText) && /\bresort\b/.test(titleText) && !explicitRvSiteEvidence && !/\brv\s+(?:park|resort|sites?)\b/.test(titleText)) return "unknown";
  if (/\bland\b/.test(typeText) && !strongRvMhpTitle && !explicitRvSiteEvidence && /\buses?\s+allowed\b|\buse\s+allowed\b/.test(text)) return "unknown";
  if (/\bland\b/.test(typeText) && !financialEvidence && /\b(?:development opportunity|development potential|future development|cleared land|zoned\s+mh[-\s]?\d*|mobile home park zoning|rezoning|developers?|builders?|gross acres?|buildable|floodplain|owner carry|joint venture development|endless possibilities|wide range of possibilities|custom home|barndominium|equestrian facility)\b/.test(text)) return "unknown";
  if (!financialEvidence && !explicitRvSiteEvidence && /\b(?:development site|future development|rv design|mobile home or rv design|unrestricted[-\s,].*(?:development|rv|mobile home)|potentially\s+be\s+tied\s+in)\b/.test(text)) return "unknown";
  if (!financialEvidence && !explicitRvSiteEvidence && /\brv\s*(?:parks?|resorts?)\?/.test(text)) return "unknown";
  const hardDevelopmentOrFormerUse =
    /\bprospective\s+rv\b|\bproposed\s+rv\b|\bpotential\s+(?:rv|mobile home|mobil home)\b|\bpossible\s+(?:rv|mobile home|mobil home)\b|\bwell\s+suited\s+for\s+(?:an?\s+)?(?:rv|mobile home|mobil home|mhp)\b|\bzoned\s+(?:mh[-\s]?\d*|mobile home|mhp)\b|\bshovel[-\s]?ready\s+mobile home park\b|\ball\s+mobile homes?\s+removed\b|\b(?:land\/home|land-home)\s+sites?\b|\bmhp\s+or\b|\bpreviously\s+utilized\s+as\s+a\s+mobile\s+home\s+park\b/.test(text);
  const softLandSignal =
    /\bundeveloped\s+land\b|\brezoning\b|\bdesired\s+use\b|\bfrontage\b|\blots?\s+[0-9-]+\b|\bbuild\s+anything\b|\bbuild\s+(?:a\s+)?single\s+home\b|\bsubdivide\b|\bresidential\s+development\b|\bdevelopment\s+site\b|\bpoised\s+for\s+commercial\b|\bflex\/warehouse\b|\bmetal\s+(?:buildings?|outbuildings?)\b/.test(text);
  const landUseMenuOnly =
    /\bland\b/.test(typeText) &&
    /\bhotel\b/.test(text) &&
    /\brv\s*parks?\b/.test(text) &&
    /\bcommercial\b|\bretail\b|\bmedical\b|\bsports venue\b/.test(text);
  const nonOperatingCommercialShell =
    /\b(?:industrial|retail|mixed use)\b/.test(typeText) &&
    /\b(?:flexible building|industrial\/retail|retail box|variety of uses|multi[-\s]?purpose site)\b/.test(text);
  if ((hardDevelopmentOrFormerUse || landUseMenuOnly || nonOperatingCommercialShell || (softLandSignal && !strongRvMhpTitle)) && !financialEvidence && !strongRvMhpTitle) return "unknown";
  const developmentOnly =
    /\bprospective\s+rv\b|\bproposed\s+rv\b|\bfor\s+(?:an?\s+)?rv\s+park\b|\bpotential\s+(?:rv|mobile home)\b|\bpossible\s+(?:rv|mobile home)\b|\bideal\s+for\s+(?:an?\s+)?rv\b|\brv\s+park\s+development\b|\b(?:land\/home|land-home)\s+sites?\b|\bmhp\s+or\b|\bdevelopment\s+(?:land|opportunity)\b|\bunrestricted\s+acreage\b/.test(text)
    && !operatingEvidence && !entitledRvLotEvidence;
  if (developmentOnly) return "unknown";
  const hasMobileRvSignal = /\brv\b|\bmobile home\b|\bmanufactured housing\b|\bmhp\b|\bmhc\b|\btrailer park\b|\bcampground\b|\bcampsite\b/.test(text);
  if ((/\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(typeText) && !strongRvMhpTitle && !strongRvMhpType) ||
      (/\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(typeText) && !strongRvMhpTitle && !strongRvMhpType) ||
      ((/\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage\s+(?:facility|units?)\b/.test(text) ||
        /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(text)) && !hasMobileRvSignal)) {
    return "unknown";
  }
  const hasRv = /\brv\s*parks?\b|\brv\s*resorts?\b|\brv\s*sites?\b|\brv\s*pads?\b|\brv\s*spaces?\b|\brecreational vehicle\b/.test(text);
  const hasMhp = /\bmobile home\s*(?:park|community|sites?|pads?|spaces?)\b|\bmanufactured housing\s*(?:park|community)\b|\bmhp\b|\bmhc\b/.test(text);
  const campgroundRoadOnly = /\bcampground\s+(?:road|rd)\b/.test(titleText) || (/^\s*(?:0\s+w\s+)?campground\b/.test(titleText) && /\bland\b/.test(typeText) && !financialEvidence);
  const hasCampground = !campgroundRoadOnly && (/\bcampgrounds?\b|\bcampsites?\b|\bcamping\b|\bkoa\b|\bglamping\b/.test(typeText) || /\b(?:campground|camping|glamping)\b/.test(titleText));
  if (campgroundRoadOnly && !financialEvidence) return "unknown";
  if (!strongRvMhpTitle && !financialEvidence && !hasRv && /\bland\b/.test(typeText) && /\b(?:industrial|warehouse|flex|mixed use)\b/.test(typeText)) return "unknown";
  if (/\brv\s*(?:parks?|resorts?)\b/.test(titleText) && !developmentOnly && !landUseMenuOnly && !campgroundRoadOnly) {
    return hasMhp || /\bmobile home\s*(?:park|community)\b|\bmhp\b|\bmhc\b/.test(typeText) ? "mixed_rv_mhp" : "rv_park";
  }
  if (hasRv && hasMhp) return "mixed_rv_mhp";
  if (hasRv) return "rv_park";
  if (hasCampground) return "campground";
  if (hasMhp) return "mobile_home_park";
  return "unknown";
}

async function main() {
  console.log("MXRE - CREXI RapidAPI ingest");
  console.log("=".repeat(40));
  console.log(JSON.stringify({ market: MARKET, city: CITY, state: STATE, assetClass: ASSET_CLASS, limit: LIMIT, detailLimit: DETAIL_LIMIT, page: PAGE, nationwide: NATIONWIDE, crexiPublicRvSearch: CREXI_PUBLIC_RV_SEARCH, crexiPublicType: CREXI_PUBLIC_TYPE ?? null, crexiPublicSubtype: CREXI_PUBLIC_SUBTYPE ?? null, crexiPublicScope: CREXI_PUBLIC_SCOPE ?? null, sweepStartedAt: SWEEP_STARTED_AT ?? null, crexiPublicIds: CREXI_PUBLIC_IDS.length, dryRun: DRY_RUN }, null, 2));

  let search: Record<string, unknown>;
  try {
    if (CREXI_PUBLIC_RV_SEARCH || CREXI_PUBLIC_TYPE || CREXI_PUBLIC_SUBTYPE) {
      const from = (PAGE - 1) * LIMIT;
      const publicSearch = await crexiPublicUniversalSearch(from, LIMIT);
      search = {
        data: (publicSearch.items ?? []).map(crexiUniversalItemToSearchRow).filter(Boolean),
        totalCount: publicSearch.totalCount,
        from,
      };
      console.log(JSON.stringify({ publicSearchTotal: publicSearch.totalCount ?? null, from }, null, 2));
    } else {
      search = await crexiGet("/search", NATIONWIDE
        ? { page: PAGE }
        : {
            city: CITY,
            code: STATE,
            lat: LAT,
            lng: LNG,
            page: PAGE,
          });
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`search lookup skipped for ${MARKET}: ${message.slice(0, 220)}`);
    console.log("Search rows: 0; scanning: 0");
    console.log(JSON.stringify({ detailCalls: 0, matched: 0, byClass: {}, skipped: true, reason: "search_lookup_failed" }, null, 2));
    console.log(JSON.stringify({ upserted: 0, source: "crexi_rapidapi", skipped: true }, null, 2));
    return;
  }
  const searchRows = (Array.isArray(search.data) ? search.data : []) as CrexiSearchRow[];
  const rows = searchRows.slice(0, LIMIT);
  console.log(`Search rows: ${searchRows.length}; scanning: ${rows.length}`);

  const classified: ClassifiedRow[] = [];
  let detailCalls = 0;

  for (const row of rows) {
    const summaryClassified = classify(row) ?? (CREXI_PUBLIC_RV_SEARCH || CREXI_PUBLIC_TYPE === "Mobile Home Park" ? {
      assetClass: "mobile_home_rv" as const,
      confidence: "medium" as const,
      row,
    } : null);
    let detail: Record<string, unknown> | undefined;

    if (summaryClassified && detailCalls < DETAIL_LIMIT && row.id && row.urlSlug) {
      detailCalls++;
      try {
        const detailBody = await crexiGet("/listingdetails", { id: row.id, urlSlug: row.urlSlug });
        detail = (detailBody.data ?? detailBody) as Record<string, unknown>;
      } catch (error) {
        console.warn(`detail lookup skipped for ${row.id}: ${error instanceof Error ? error.message.slice(0, 180) : String(error)}`);
      }
    }

    const fullClassified = classify(row, detail) ?? summaryClassified;
    if (fullClassified) classified.push(fullClassified);
    if (!DRY_RUN && classified.length >= MAX_UPSERT) break;
  }

  const byClass = classified.reduce<Record<string, number>>((acc, item) => {
    acc[item.assetClass] = (acc[item.assetClass] ?? 0) + 1;
    return acc;
  }, {});
  console.log(JSON.stringify({ detailCalls, matched: classified.length, byClass }, null, 2));

  if (DRY_RUN) {
    console.log(JSON.stringify(classified.slice(0, 10).map(item => ({
      ...(() => {
        const parsed = parsedTextStats(item.row, item.detail);
        const units = inferUnits(item.row, item.detail) ?? parsed.units;
        const location = item.detail?.locations && Array.isArray(item.detail.locations)
          ? item.detail.locations[0] as Record<string, unknown> | undefined
          : item.row.locations?.[0] as Record<string, unknown> | undefined;
        return {
          units,
          multifamily: parseCrexiMultifamilyStats(item.detail, inferListPrice(item.row, item.detail)),
          pads: inferPads(item.detail) ?? parsed.pads,
          pricePerItem: inferPricePerItem(item.row, item.detail, units),
          lotSizeAcres: inferLotSizeAcres(item.detail),
          grossIncome: parsed.grossIncome,
          cashOnCashReturn: parsed.cashOnCashReturn,
          nightlyRent: parsed.nightlyRent,
          weeklyRent: parsed.weeklyRent,
          monthlyRent: parsed.monthlyRent,
          unitMix: parsed.unitMix,
          county: location?.county ?? null,
          latitude: location?.latitude ?? null,
          longitude: location?.longitude ?? null,
        };
      })(),
      assetClass: item.assetClass,
      confidence: item.confidence,
      id: item.row.id,
      title: item.row.name,
      types: item.row.types,
      address: item.row.locations?.[0]?.fullAddress,
      sourceUrl: item.row.id && item.row.urlSlug ? `https://www.crexi.com/properties/${item.row.id}/${item.row.urlSlug}` : null,
      listPrice: inferListPrice(item.row, item.detail),
      noi: inferNoi(item.detail) ?? parsedTextStats(item.row, item.detail).noi,
      capRate: inferCapRate(item.detail) ?? parsedTextStats(item.row, item.detail).capRate,
      proFormaNoi: inferProFormaNoi(item.detail) ?? parsedTextStats(item.row, item.detail).proFormaNoi,
      proFormaCapRate: inferProFormaCapRate(item.detail) ?? parsedTextStats(item.row, item.detail).proFormaCapRate,
      propertyType: inferDetailString(item.detail, /^property type$/i),
      subType: inferDetailString(item.detail, /^sub type$/i),
      brokerCoOp: inferDetailString(item.detail, /^broker co-op$/i),
      class: crexiBuildingClass(item.detail).sourceClass,
      buildingClass: crexiBuildingClass(item.detail).buildingClass,
      buildingClassStatus: crexiBuildingClass(item.detail).status,
      buildingClassEvidence: crexiBuildingClass(item.detail).evidence,
      acreage: inferDetailString(item.detail, /lot\s*size|acreage|acres/i),
      createdOn: item.detail?.createdOn ?? null,
      activatedOn: item.row.activatedOn ?? null,
      updatedOn: item.row.updatedOn ?? null,
      status: item.detail?.status ?? item.row.status ?? null,
      description: listingDescription(item.row, item.detail),
      marketingDescription: marketingDescription(item.detail),
      investmentHighlights: investmentHighlights(item.detail),
      hasOM: item.detail?.hasOM ?? null,
      hasFlyer: item.detail?.hasFlyer ?? null,
      hasVault: item.detail?.hasVault ?? null,
      vaultAccessStatus: item.detail?.vaultAccessStatus ?? null,
      userCanDownloadMarketingDoc: item.detail?.userCanDownloadMd ?? null,
      isInOpportunityZone: item.detail?.isInOpportunityZone ?? null,
      isUnpriced: item.detail?.isUnpriced ?? null,
      isSold: item.detail?.isSold ?? null,
      isPaused: item.detail?.isPaused ?? null,
      isOutdated: item.detail?.isOutdated ?? null,
      isPrivate: item.detail?.isPrivate ?? null,
      thumbnailUrl: item.detail?.thumbnailUrl ?? null,
      brokerage: item.row.brokerageName,
    })), null, 2));
    return;
  }

  const db = getWriteDb();
  let upserted = 0;
  const observedAt = new Date().toISOString();

  for (const item of classified) {
    if (upserted >= MAX_UPSERT) break;
    const row = item.row;
    const location = row.locations?.[0] ?? {};
    const parsed = parsedTextStats(row, item.detail);
    const creativeFinance = detectCreativeFinanceSignals(
      row.description,
      marketingDescription(item.detail),
      investmentHighlights(item.detail),
      item.detail?.details,
      item.detail?.summaryDetails,
    );
    const subAssetType = inferSubAssetType(row, item.detail);
    const buildingClass = crexiBuildingClass(item.detail);
    const listPrice = inferListPrice(row, item.detail);
    const multifamily = parseCrexiMultifamilyStats(item.detail, listPrice);
    const units = item.assetClass === "multifamily"
      ? multifamily.units ?? inferUnits(row, item.detail) ?? parsed.units
      : inferUnits(row, item.detail) ?? parsed.units;
    const detailLocation = item.detail?.locations && Array.isArray(item.detail.locations)
      ? item.detail.locations[0] as Record<string, unknown> | undefined
      : undefined;
    const sourceUrl = row.id && row.urlSlug ? `https://www.crexi.com/properties/${row.id}/${row.urlSlug}` : "https://www.crexi.com/";
    const { data: existingRows, error: existingError } = await db
      .from("external_market_listings")
      .select("id, first_seen_at, observed_at, raw")
      .eq("asset_class", item.assetClass)
      .eq("source", "crexi_rapidapi")
      .or(`source_url.eq.${sourceUrl},source_url.like.https://www.crexi.com/properties/${row.id}/%`)
      .limit(1);
    if (existingError) throw existingError;
    const existing = existingRows?.[0] as { id: number; first_seen_at?: string | null; observed_at?: string | null; raw?: Record<string, unknown> | null } | undefined;
    const previousRaw = existing?.raw ?? {};
    const publicFeedMarket = NATIONWIDE || CREXI_PUBLIC_RV_SEARCH || CREXI_PUBLIC_TYPE || CREXI_PUBLIC_SUBTYPE;
    const unitSource = item.assetClass === "multifamily" && multifamily.units !== null
      ? "crexi_details.units"
      : units !== null
        ? "crexi_text_stats"
        : null;
    const unitMix = item.assetClass === "multifamily"
      ? withApartmentUnits(parsed.unitMix, multifamily.apartmentUnits)
      : parsed.unitMix;
    const payload = {
      market: publicFeedMarket ? marketFromLocation(location) : MARKET,
      asset_class: item.assetClass,
      source: "crexi_rapidapi",
      source_url: sourceUrl,
      title: row.name ?? null,
      address: location.fullAddress ?? location.address ?? row.description ?? null,
      city: location.city ?? CITY,
      state_code: location.state?.code ?? STATE,
      zip: location.zip ?? null,
      units,
      list_price: listPrice,
      price_per_unit: listPrice && units ? Math.round(listPrice / units) : null,
      cap_rate: inferCapRate(item.detail) ?? parsed.capRate,
      noi: inferNoi(item.detail) ?? parsed.noi,
      status: String(row.status ?? "active").toLowerCase().includes("market") || String(row.status ?? "active").toLowerCase().includes("active") ? "active" : String(row.status ?? "active").toLowerCase(),
      confidence: item.confidence,
      observed_at: observedAt,
      first_seen_at: existing?.first_seen_at ?? existing?.observed_at ?? observedAt,
      last_seen_at: observedAt,
      raw: withStandardUnitFields({
        title: row.name ?? null,
        previous_updated_on: previousRaw.updated_on ?? null,
        previous_status: previousRaw.source_status ?? null,
        crexi_public_scope: CREXI_PUBLIC_SCOPE ?? previousRaw.crexi_public_scope ?? null,
        crexi_public_type: CREXI_PUBLIC_TYPE ?? previousRaw.crexi_public_type ?? null,
        crexi_public_subtype: CREXI_PUBLIC_SUBTYPE ?? previousRaw.crexi_public_subtype ?? null,
        crexi_last_sweep_started_at: SWEEP_STARTED_AT ?? previousRaw.crexi_last_sweep_started_at ?? null,
        removed_candidate_at: null,
        removed_confirmed_at: null,
        crexi_id: row.id,
        url_slug: row.urlSlug,
        listing_description: listingDescription(row, item.detail),
        short_description: stripHtml(row.description),
        marketing_description: marketingDescription(item.detail),
        investment_highlights: investmentHighlights(item.detail),
        types: row.types ?? [],
        custom_subtypes: item.detail?.customSubtypes ?? [],
        square_footage: row.squareFootage ?? null,
        brokerage_name: row.brokerageName ?? null,
        created_on: item.detail?.createdOn ?? null,
        activated_on: row.activatedOn ?? null,
        updated_on: row.updatedOn ?? null,
        source_status: item.detail?.status ?? row.status ?? null,
        sub_asset_type: subAssetType,
        multifamily: item.assetClass === "multifamily" ? multifamily : null,
        apartment_units: item.assetClass === "multifamily" ? multifamily.apartmentUnits : null,
        building_count: item.assetClass === "multifamily" ? multifamily.buildings : null,
        story_count: item.assetClass === "multifamily" ? multifamily.stories : null,
        year_built: item.assetClass === "multifamily" ? multifamily.yearBuilt : null,
        years_built: item.assetClass === "multifamily" ? multifamily.yearsBuilt : [],
        occupancy_pct: item.assetClass === "multifamily" ? multifamily.occupancyPct : null,
        occupancy_status: item.assetClass === "multifamily" ? multifamily.occupancyStatus : null,
        tenancy: item.assetClass === "multifamily" ? multifamily.tenancy : null,
        zoning: item.assetClass === "multifamily" ? multifamily.zoning : null,
        keys: item.assetClass === "multifamily" ? multifamily.keys : null,
        price_per_sqft: item.assetClass === "multifamily" ? multifamily.pricePerSqft : null,
        avg_unit_sqft: item.assetClass === "multifamily" ? multifamily.avgUnitSqft : null,
        creative_finance: creativeFinance,
        pro_forma_noi: inferProFormaNoi(item.detail) ?? parsed.proFormaNoi,
        pro_forma_cap_rate: inferProFormaCapRate(item.detail) ?? parsed.proFormaCapRate,
        cash_on_cash_return: parsed.cashOnCashReturn,
        gross_income: parsed.grossIncome,
        nightly_rent: parsed.nightlyRent,
        weekly_rent: parsed.weeklyRent,
        monthly_rent: parsed.monthlyRent,
        unit_mix: unitMix,
        pads: inferPads(item.detail) ?? parsed.pads,
        price_per_item: inferPricePerItem(row, item.detail, units),
        price_per_item_type: item.detail?.pricePerItemType ?? null,
        lot_size_acres: inferLotSizeAcres(item.detail),
        property_type: inferDetailString(item.detail, /^property type$/i),
        sub_type: inferDetailString(item.detail, /^sub type$/i),
        broker_co_op: inferDetailString(item.detail, /^broker co-op$/i),
        class: buildingClass.sourceClass,
        building_class: buildingClass.buildingClass,
        building_class_status: buildingClass.status,
        building_class_source: buildingClass.sourceClass ? "crexi_details.class" : "no_data",
        building_class_evidence: buildingClass.evidence,
        acreage: inferDetailString(item.detail, /lot\s*size|acreage|acres/i),
        county: detailLocation?.county ?? (location as Record<string, unknown>).county ?? null,
        latitude: detailLocation?.latitude ?? location.latitude ?? null,
        longitude: detailLocation?.longitude ?? location.longitude ?? null,
        thumbnail_url: item.detail?.thumbnailUrl ?? null,
        has_om: item.detail?.hasOM ?? null,
        has_flyer: item.detail?.hasFlyer ?? null,
        has_vault: item.detail?.hasVault ?? null,
        has_private_vault: item.detail?.hasPrivateVault ?? null,
        vault_access_status: item.detail?.vaultAccessStatus ?? null,
        user_can_download_marketing_doc: item.detail?.userCanDownloadMd ?? null,
        is_in_opportunity_zone: item.detail?.isInOpportunityZone ?? null,
        is_unpriced: item.detail?.isUnpriced ?? null,
        is_sold: item.detail?.isSold ?? null,
        is_paused: item.detail?.isPaused ?? null,
        is_outdated: item.detail?.isOutdated ?? null,
        is_private: item.detail?.isPrivate ?? null,
        is_note_loan: item.detail?.isNoteLoan ?? null,
        has_virtual_tour: item.detail?.hasVirtualTour ?? null,
        broker_products: item.detail?.brokerProducts ?? [],
        ca_principal_info_type: item.detail?.caPrincipalInfoType ?? null,
        detail: item.detail ?? null,
      }, units, unitSource),
    };

    const write = existing
      ? await db.from("external_market_listings").update(payload).eq("id", existing.id)
      : await db.from("external_market_listings").insert(payload);
    if (write.error) throw write.error;
    upserted++;
  }

  console.log(JSON.stringify({ upserted, source: "crexi_rapidapi" }, null, 2));
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
