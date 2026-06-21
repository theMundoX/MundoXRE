#!/usr/bin/env tsx
import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { detectCreativeFinanceSignals } from "./lib/creative-finance-signals.ts";
import { parseCrexiTextStats } from "./lib/crexi-text-stats.ts";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const DRY_RUN = process.argv.includes("--dry-run");
const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const LIMIT = Math.max(1, Number.parseInt(arg("limit", "500") ?? "500", 10));
const PAGE_SIZE = Math.min(1000, Math.max(1, Number.parseInt(arg("page-size", "1000") ?? "1000", 10)));

type ExternalRow = {
  id: number;
  title: string | null;
  units: number | null;
  noi: number | null;
  cap_rate: number | string | null;
  raw: Record<string, unknown> | null;
};

function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value) && value > 0) return Math.round(value);
    if (typeof value === "string" && value.trim() && Number.isFinite(Number(value)) && Number(value) > 0) return Math.round(Number(value));
  }
  return null;
}

function compactText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(compactText).join(" ");
  if (typeof value === "object") return Object.values(value as Record<string, unknown>).map(compactText).join(" ");
  return String(value);
}

function positiveNumber(value: unknown): boolean {
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value.replace(/[%,$,\s]/g, ""));
    return Number.isFinite(parsed) && parsed > 0;
  }
  return false;
}

function objectValue(value: unknown, key: string): unknown {
  return value && typeof value === "object" ? (value as Record<string, unknown>)[key] : undefined;
}

function inferSubAssetType(raw: Record<string, unknown>): "rv_park" | "mobile_home_park" | "mixed_rv_mhp" | "campground" | "unknown" {
  const unitMix = raw.unit_mix as Record<string, unknown> | undefined;
  const textParsedStats = raw.text_parsed_stats as Record<string, unknown> | undefined;
  const textParsedUnitMix = objectValue(textParsedStats, "unitMix");
  const explicitRvSiteEvidence =
    positiveNumber(unitMix?.rvSites) ||
    positiveNumber(objectValue(textParsedUnitMix, "rvSites"));
  const explicitMobileHomeSiteEvidence =
    positiveNumber(unitMix?.mobileHomeSites) ||
    positiveNumber(objectValue(textParsedUnitMix, "mobileHomeSites"));
  const titleText = compactText([
    raw.title,
    raw.short_description,
  ]).toLowerCase();
  const typeText = compactText([
    raw.types,
    raw.custom_subtypes,
    raw.property_type,
    raw.sub_type,
  ]).toLowerCase();
  const text = compactText([
    raw.title,
    raw.short_description,
    raw.marketing_description,
    raw.investment_highlights,
    raw.listing_description,
    raw.types,
    raw.custom_subtypes,
    raw.property_type,
    raw.sub_type,
  ]).toLowerCase();
  const evidenceText = compactText([
    raw.title,
    raw.short_description,
    raw.marketing_description,
    raw.investment_highlights,
    raw.listing_description,
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
    positiveNumber(raw.noi) ||
    positiveNumber(raw.cap_rate) ||
    positiveNumber((raw.text_parsed_stats as Record<string, unknown> | undefined)?.noi) ||
    positiveNumber((raw.text_parsed_stats as Record<string, unknown> | undefined)?.capRate) ||
    /\b(?:existing|established|operating|stabilized|income[-\s]?producing)\b|\bnoi\b|\bcap\s*rate\b/.test(text);
  const financialEvidence =
    positiveNumber(raw.noi) ||
    positiveNumber(raw.cap_rate) ||
    positiveNumber((raw.text_parsed_stats as Record<string, unknown> | undefined)?.noi) ||
    positiveNumber((raw.text_parsed_stats as Record<string, unknown> | undefined)?.capRate);
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
  const db = getWriteDb();
  let scanned = 0;
  let updated = 0;
  let parsedNoi = 0;
  let parsedCapRate = 0;
  let parsedUnitMix = 0;

  for (let offset = 0; offset < LIMIT; offset += PAGE_SIZE) {
    const pageEnd = Math.min(LIMIT, offset + PAGE_SIZE) - 1;
    const { data, error } = await db
      .from("external_market_listings")
      .select("id, title, units, noi, cap_rate, raw")
      .eq("source", "crexi_rapidapi")
      .eq("asset_class", "mobile_home_rv")
      .order("id", { ascending: true })
      .range(offset, pageEnd);
    if (error) throw error;
    const rows = (data ?? []) as ExternalRow[];
    if (rows.length === 0) break;

  for (const row of rows) {
    scanned++;
    const raw = row.raw ?? {};
    const rawWithTitle = {
      ...raw,
      title: raw.title ?? row.title,
      noi: raw.noi ?? row.noi,
      cap_rate: raw.cap_rate ?? row.cap_rate,
    };
    const parsed = parseCrexiTextStats(
      rawWithTitle.short_description,
      rawWithTitle.marketing_description,
      rawWithTitle.investment_highlights,
      rawWithTitle.listing_description,
      rawWithTitle.detail,
    );
    const rawForClassification = {
      ...rawWithTitle,
      text_parsed_stats: parsed,
    };
    const nextRaw = {
      ...rawForClassification,
      sub_asset_type: inferSubAssetType(rawForClassification),
      creative_finance: raw.creative_finance ?? detectCreativeFinanceSignals(
        rawWithTitle.short_description,
        rawWithTitle.marketing_description,
        rawWithTitle.investment_highlights,
        rawWithTitle.listing_description,
        rawWithTitle.detail,
      ),
      unit_mix: parsed.unitMix,
      cash_on_cash_return: raw.cash_on_cash_return ?? parsed.cashOnCashReturn,
      gross_income: raw.gross_income ?? parsed.grossIncome,
      nightly_rent: raw.nightly_rent ?? parsed.nightlyRent,
      weekly_rent: raw.weekly_rent ?? parsed.weeklyRent,
      monthly_rent: raw.monthly_rent ?? parsed.monthlyRent,
      pro_forma_noi: raw.pro_forma_noi ?? parsed.proFormaNoi,
      pro_forma_cap_rate: raw.pro_forma_cap_rate ?? parsed.proFormaCapRate,
      pads: raw.pads ?? parsed.pads,
    };
    const patch: Record<string, unknown> = { raw: nextRaw };
    const noi = firstNumber(row.noi, parsed.noi);
    const capRate = row.cap_rate ?? parsed.capRate;
    const units = firstNumber(row.units, parsed.units);
    nextRaw.total_units = units;
    nextRaw.unit_count = units;
    nextRaw.unit_count_status = units !== null ? "explicit_value" : "no_data";
    nextRaw.unit_count_source = units !== null
      ? row.units !== null ? "external_market_listings.units" : "crexi_text_stats"
      : "no_data";
    if (noi !== row.noi) {
      patch.noi = noi;
      if (noi !== null) parsedNoi++;
    }
    if (capRate !== row.cap_rate) {
      patch.cap_rate = capRate;
      if (capRate !== null) parsedCapRate++;
    }
    if (units !== row.units) {
      patch.units = units;
    }
    if (Object.values(parsed.unitMix).some((value) => value !== null)) parsedUnitMix++;

    if (!DRY_RUN) {
      const { error: updateError } = await db
        .from("external_market_listings")
        .update(patch)
        .eq("id", row.id);
      if (updateError) throw updateError;
    }
    updated++;
  }
    if (rows.length < PAGE_SIZE) break;
  }

  console.log(JSON.stringify({
    dryRun: DRY_RUN,
    scanned,
    updated,
    parsedNoi,
    parsedCapRate,
    parsedUnitMix,
  }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
