#!/usr/bin/env tsx
import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
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

type ClassifiedRow = {
  assetClass: "multifamily" | "self_storage";
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

function compactText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(compactText).join(" ");
  if (typeof value === "object") return Object.values(value as Record<string, unknown>).map(compactText).join(" ");
  return String(value);
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

  const isSelfStorage = /\bself[-\s]?storage\b|\bmini[-\s]?storage\b|\bstorage units?\b|\bmini[-\s]?warehouse\b/.test(fullText);
  const isMultifamily =
    /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(detailTypeText) ||
    /\bmultifamily\b|\bmulti[-\s]?family\b|\bapartment\b|\bapartments\b|\bduplex\b|\btriplex\b|\bfourplex\b/.test(rowText);

  if (isSelfStorage && (ASSET_CLASS === "all" || ASSET_CLASS === "self_storage")) {
    return { assetClass: "self_storage", confidence: detail ? "medium" : "low", row, detail };
  }
  if (isMultifamily && (ASSET_CLASS === "all" || ASSET_CLASS === "multifamily")) {
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

function detailValue(detail: Record<string, unknown> | undefined, label: RegExp): unknown {
  const summary = Array.isArray(detail?.summaryDetails) ? detail?.summaryDetails as Record<string, unknown>[] : [];
  const match = summary.find(item => label.test(String(item.label ?? item.key ?? "")));
  return match?.value ?? match?.display;
}

function inferUnits(row: CrexiSearchRow, detail?: Record<string, unknown>): number | null {
  const unitValue = detailValue(detail, /unit/i);
  const fromDetail = numberFromDisplay(unitValue);
  if (fromDetail !== null) return fromDetail;
  return numberFromDisplay(`${row.name ?? ""} ${row.description ?? ""}`);
}

async function main() {
  console.log("MXRE - CREXI RapidAPI ingest");
  console.log("=".repeat(40));
  console.log(JSON.stringify({ market: MARKET, city: CITY, state: STATE, assetClass: ASSET_CLASS, limit: LIMIT, detailLimit: DETAIL_LIMIT, dryRun: DRY_RUN }, null, 2));

  const search = await crexiGet("/search", {
    city: CITY,
    code: STATE,
    lat: LAT,
    lng: LNG,
  });
  const searchRows = (Array.isArray(search.data) ? search.data : []) as CrexiSearchRow[];
  const rows = searchRows.slice(0, LIMIT);
  console.log(`Search rows: ${searchRows.length}; scanning: ${rows.length}`);

  const classified: ClassifiedRow[] = [];
  let detailCalls = 0;

  for (const row of rows) {
    const summaryClassified = classify(row);
    let detail: Record<string, unknown> | undefined;

    if (detailCalls < DETAIL_LIMIT && row.id && row.urlSlug) {
      detailCalls++;
      try {
        const detailBody = await crexiGet("/listingdetails", { id: row.id, urlSlug: row.urlSlug });
        detail = (detailBody.data ?? detailBody) as Record<string, unknown>;
      } catch (error) {
        console.warn(`detail lookup skipped for ${row.id}: ${error instanceof Error ? error.message.slice(0, 180) : String(error)}`);
        break;
      }
    }

    const fullClassified = classify(row, detail) ?? summaryClassified;
    if (fullClassified) classified.push(fullClassified);
  }

  const byClass = classified.reduce<Record<string, number>>((acc, item) => {
    acc[item.assetClass] = (acc[item.assetClass] ?? 0) + 1;
    return acc;
  }, {});
  console.log(JSON.stringify({ detailCalls, matched: classified.length, byClass }, null, 2));

  if (DRY_RUN) {
    console.log(JSON.stringify(classified.slice(0, 10).map(item => ({
      assetClass: item.assetClass,
      confidence: item.confidence,
      id: item.row.id,
      title: item.row.name,
      types: item.row.types,
      address: item.row.locations?.[0]?.fullAddress,
      brokerage: item.row.brokerageName,
    })), null, 2));
    return;
  }

  const db = getWriteDb();
  let upserted = 0;
  const observedAt = new Date().toISOString();

  for (const item of classified) {
    const row = item.row;
    const location = row.locations?.[0] ?? {};
    const units = inferUnits(row, item.detail);
    const listPrice = typeof row.askingPrice === "number" ? Math.round(row.askingPrice) : null;
    const sourceUrl = row.id && row.urlSlug ? `https://www.crexi.com/properties/${row.id}/${row.urlSlug}` : "https://www.crexi.com/";

    const { error: deleteError } = await db
      .from("external_market_listings")
      .delete()
      .eq("market", MARKET)
      .eq("asset_class", item.assetClass)
      .eq("source", "crexi_rapidapi")
      .eq("title", row.name ?? "");
    if (deleteError) throw deleteError;

    const { error: insertError } = await db.from("external_market_listings").insert({
      market: MARKET,
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
      status: String(row.status ?? "active").toLowerCase().includes("market") || String(row.status ?? "active").toLowerCase().includes("active") ? "active" : String(row.status ?? "active").toLowerCase(),
      confidence: item.confidence,
      observed_at: observedAt,
      last_seen_at: observedAt,
      raw: {
        crexi_id: row.id,
        url_slug: row.urlSlug,
        types: row.types ?? [],
        square_footage: row.squareFootage ?? null,
        brokerage_name: row.brokerageName ?? null,
        activated_on: row.activatedOn ?? null,
        updated_on: row.updatedOn ?? null,
        detail: item.detail ?? null,
      },
    });
    if (insertError) throw insertError;
    upserted++;
  }

  console.log(JSON.stringify({ upserted, source: "crexi_rapidapi" }, null, 2));
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
