#!/usr/bin/env tsx
/**
 * Ingest Lucas County, OH parcel data for Toledo coverage.
 *
 * Public source: Lucas County Tax Parcels ArcGIS FeatureServer
 * https://services3.arcgis.com/T8dczfwPixv79EgZ/ArcGIS/rest/services/Lucas_County_TaxParcels/FeatureServer/0
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://services3.arcgis.com/T8dczfwPixv79EgZ/ArcGIS/rest/services/Lucas_County_TaxParcels/FeatureServer/0";
const PAGE_SIZE = 1000;
const INT_MAX = 2_147_483_647;
const FIELDS = [
  "PARCELID",
  "ASSESSORNUM",
  "SITEADDRESS",
  "SITECITY",
  "SITEZIP",
  "OWNERNME1",
  "OWNERNME2",
  "USEDC",
  "USEDSCRP",
  "CLASSCD",
  "CLASSDSCRP",
  "RESYRBLT",
  "TLA",
  "BED_COUNT",
  "TOTBATHS",
  "STATEDAREA",
  "ACREAGE",
  "ACREAGE_CALC",
  "DEEDACRES",
  "LNDVALUE",
  "BLDGVALUE",
  "TOTALVALUE",
].join(",");

const offsetArg = process.argv.find(a => a.startsWith("--offset="));
const maxPagesArg = process.argv.find(a => a.startsWith("--max-pages="));
const cityArg = process.argv.find(a => a.startsWith("--city="))?.split("=").slice(1).join("=").toUpperCase();
let offset = offsetArg ? Number(offsetArg.split("=")[1]) : 0;
const maxPages = maxPagesArg ? Math.max(Number(maxPagesArg.split("=")[1]), 1) : null;

function parseNum(value: unknown): number | null {
  const n = Number(String(value ?? "").replace(/[^0-9.-]/g, ""));
  if (!Number.isFinite(n) || n <= 0) return null;
  const rounded = Math.round(n);
  return rounded <= INT_MAX ? rounded : null;
}

function parseYear(value: unknown): number | null {
  const y = Number(value);
  return Number.isInteger(y) && y > 1700 && y < 2030 ? y : null;
}

function parseBaths(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function classifyAsset(row: Record<string, unknown>): string {
  const used = String(row.USEDSCRP ?? "").toLowerCase();
  const classDesc = String(row.CLASSDSCRP ?? "").toLowerCase();
  const usedCode = String(row.USEDC ?? "").trim();
  if (used.includes("apartment") || used.includes("multi")) return "small_multifamily";
  if (used.includes("condo")) return "residential";
  if (used.includes("single family") || usedCode === "510") return "residential";
  if (classDesc.includes("commercial")) return "commercial";
  if (classDesc.includes("industrial")) return "industrial";
  if (classDesc.includes("agricultural")) return "agricultural";
  if (classDesc.includes("exempt")) return "exempt";
  return "residential";
}

function buildWhere(): string {
  if (!cityArg) return "1=1";
  return `SITECITY='${cityArg.replace(/'/g, "''")}'`;
}

async function fetchPage(pageOffset: number): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({
    where: buildWhere(),
    outFields: FIELDS,
    returnGeometry: "false",
    resultOffset: String(pageOffset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });
  const url = `${ARCGIS_URL}/query?${params.toString()}`;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(30_000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json() as { error?: unknown; features?: Array<{ attributes: Record<string, unknown> }> };
      if (body.error) throw new Error(JSON.stringify(body.error));
      return (body.features ?? []).map(feature => feature.attributes);
    } catch (error) {
      if (attempt === 4) throw error;
      await new Promise(resolve => setTimeout(resolve, 1500 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE - Ingest Lucas County OH parcels");
  console.log(`Source: ${ARCGIS_URL}`);
  console.log(`City filter: ${cityArg ?? "none"}`);
  console.log(`Starting offset: ${offset}`);

  let { data: county } = await db.from("counties").select("id").eq("county_name", "Lucas").eq("state_code", "OH").single();
  if (!county) {
    const { data, error } = await db
      .from("counties")
      .insert({ county_name: "Lucas", state_code: "OH", state_fips: "39", county_fips: "095", active: true })
      .select("id")
      .single();
    if (error) throw new Error(`County insert failed: ${error.message}`);
    county = data;
  }
  console.log(`County ID: ${county!.id}`);

  let inserted = 0;
  let pages = 0;
  let emptyPages = 0;

  while (true) {
    if (maxPages != null && pages >= maxPages) break;
    const rows = await fetchPage(offset);
    pages++;
    if (rows.length === 0) {
      if (++emptyPages >= 3) break;
      offset += PAGE_SIZE;
      continue;
    }
    emptyPages = 0;

    const batch = rows.map(row => {
      const parcelId = String(row.PARCELID ?? row.ASSESSORNUM ?? "").trim();
      const owner = [row.OWNERNME1, row.OWNERNME2].map(v => String(v ?? "").trim()).filter(Boolean).join(" ");
      const acres = parseNum(row.STATEDAREA) ?? parseNum(row.ACREAGE) ?? parseNum(row.ACREAGE_CALC) ?? parseNum(row.DEEDACRES);
      return {
        county_id: county!.id,
        parcel_id: parcelId,
        address: String(row.SITEADDRESS ?? "").trim().toUpperCase(),
        city: String(row.SITECITY ?? "").trim().toUpperCase(),
        state_code: "OH",
        zip: String(row.SITEZIP ?? "").trim().slice(0, 5),
        owner_name: owner || null,
        property_type: String(row.USEDSCRP ?? row.CLASSDSCRP ?? "").trim().toLowerCase() || null,
        property_use: String(row.USEDSCRP ?? "").trim() || null,
        asset_type: classifyAsset(row),
        asset_confidence: "assessor_public",
        year_built: parseYear(row.RESYRBLT),
        total_sqft: parseNum(row.TLA),
        bedrooms: parseNum(row.BED_COUNT),
        bathrooms: parseBaths(row.TOTBATHS),
        land_sqft: acres ? Math.round(acres * 43560) : null,
        land_value: parseNum(row.LNDVALUE),
        assessed_value: parseNum(row.TOTALVALUE),
        market_value: parseNum(row.TOTALVALUE),
        source: "lucas-oh-tax-parcels-arcgis",
        updated_at: new Date().toISOString(),
      };
    }).filter(row => row.parcel_id);

    const seen = new Map<string, Record<string, unknown>>();
    for (const row of batch) seen.set(String(row.parcel_id), row);
    const deduped = Array.from(seen.values());

    if (deduped.length > 0) {
      const { error } = await db.from("properties").upsert(deduped, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) console.error(`\nUpsert error at offset ${offset}: ${error.message.slice(0, 200)}`);
      else inserted += deduped.length;
    }

    offset += PAGE_SIZE;
    process.stdout.write(`\rUpserted ${inserted.toLocaleString()} | offset ${offset.toLocaleString()} | pages ${pages}`);
    if (rows.length < PAGE_SIZE) break;
  }

  console.log(`\nDone: ${inserted.toLocaleString()} Lucas County parcels upserted`);
  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`Lucas County total in DB: ${count?.toLocaleString()}`);
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
