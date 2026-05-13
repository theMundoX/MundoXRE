#!/usr/bin/env tsx
/**
 * Ingest City of Detroit, MI public parcel data from the city ArcGIS layer.
 *
 * Source:
 * https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Parcels_Current/FeatureServer/0
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL =
  "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/Parcels_Current/FeatureServer/0";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const COUNTY_NAME = "Wayne";
const STATE_CODE = "MI";
const STATE_FIPS = "26";
const COUNTY_FIPS = "163";
const OUT_FIELDS = [
  "object_id",
  "parcel_number",
  "address",
  "zip_code",
  "taxpayer_1",
  "taxpayer_2",
  "taxpayer_street",
  "taxpayer_city",
  "taxpayer_state",
  "taxpayer_zip",
  "property_class",
  "property_class_desc",
  "use_code",
  "use_code_desc",
  "tax_status",
  "tax_status_description",
  "total_square_footage",
  "total_acreage",
  "num_bldgs",
  "total_floor_area",
  "style",
  "year_built",
  "sale_price",
  "sale_date",
  "assessed_value",
  "taxable_value",
  "zoning",
  "legal_description",
].join(",");

function num(value: unknown): number | null {
  if (value == null || value === "") return null;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

function text(value: unknown): string | null {
  const clean = String(value ?? "").replace(/\s+/g, " ").trim();
  return clean || null;
}

function classify(desc: unknown): string {
  const value = String(desc ?? "").toUpperCase();
  if (/APART|MULTI|DUPLEX|TWO|THREE|FOUR|FLAT/.test(value)) return "residential";
  if (/RES|SINGLE|CONDO|DWELL/.test(value)) return "residential";
  if (/COMM|RETAIL|OFFICE|BUSINESS/.test(value)) return "commercial";
  if (/IND|WAREHOUSE|MANUF/.test(value)) return "industrial";
  if (/VAC|LAND|LOT/.test(value)) return "vacant";
  if (/EXEMPT|PUBLIC|GOV/.test(value)) return "exempt";
  return "other";
}

async function getOrCreateCounty(): Promise<number> {
  const { data } = await db
    .from("counties")
    .select("id")
    .eq("state_code", STATE_CODE)
    .eq("county_fips", COUNTY_FIPS)
    .single();
  if (data) return data.id;

  const { data: created, error } = await db
    .from("counties")
    .insert({
      county_name: COUNTY_NAME,
      state_code: STATE_CODE,
      state_fips: STATE_FIPS,
      county_fips: COUNTY_FIPS,
      active: true,
    })
    .select("id")
    .single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  return created.id;
}

async function fetchCount(): Promise<number> {
  const response = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`, {
    signal: AbortSignal.timeout(60_000),
  });
  if (!response.ok) throw new Error(`Count failed: ${response.status}`);
  const json = await response.json();
  return Number(json.count ?? 0);
}

async function fetchPage(offset: number): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    returnGeometry: "false",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields: "object_id",
    f: "json",
  });
  const response = await fetch(`${SERVICE_URL}/query?${params}`, {
    signal: AbortSignal.timeout(90_000),
  });
  if (!response.ok) throw new Error(`Page ${offset} failed: ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(json.error.message ?? JSON.stringify(json.error));
  return json.features?.map((feature: { attributes: Record<string, unknown> }) => feature.attributes) ?? [];
}

async function main() {
  console.log("MXRE - Ingest City of Detroit, MI public parcels");
  console.log(`Source: ${SERVICE_URL}`);

  const countyId = await getOrCreateCounty();
  const totalCount = await fetchCount();
  const offsetArg = process.argv.find((arg) => arg.startsWith("--offset="));
  let offset = offsetArg ? Number(offsetArg.split("=")[1]) : 0;
  let totalInserted = 0;
  let totalErrors = 0;
  const startedAt = Date.now();

  console.log(`County ID: ${countyId}`);
  console.log(`Total parcels: ${totalCount.toLocaleString()}`);

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) break;

    const rows = records.flatMap((record) => {
      const parcelId = text(record.parcel_number);
      if (!parcelId) {
        totalErrors += 1;
        return [];
      }
      const address = text(record.address)?.toUpperCase() ?? null;
      const zip = text(record.zip_code)?.slice(0, 5) ?? null;
      if (!address || !zip) {
        totalErrors += 1;
        return [];
      }
      const taxpayer = [record.taxpayer_1, record.taxpayer_2].map(text).filter(Boolean).join(" ");
      const assessedValue = num(record.assessed_value);
      const acreage = Number(record.total_acreage ?? 0);
      return [{
        county_id: countyId,
        parcel_id: parcelId,
        address,
        city: "DETROIT",
        state_code: STATE_CODE,
        zip,
        owner_name: taxpayer || null,
        mailing_address: text(record.taxpayer_street),
        mailing_city: text(record.taxpayer_city),
        mailing_state: text(record.taxpayer_state)?.slice(0, 2),
        mailing_zip: text(record.taxpayer_zip)?.slice(0, 10),
        property_use: text(record.property_class_desc) ?? text(record.use_code_desc) ?? text(record.property_class),
        property_type: classify(record.property_class_desc ?? record.use_code_desc),
        year_built: num(record.year_built),
        total_sqft: num(record.total_floor_area) ?? num(record.total_square_footage),
        assessed_value: assessedValue,
        market_value: assessedValue === null ? null : assessedValue * 2,
        lot_sqft: Number.isFinite(acreage) && acreage > 0 ? Math.round(acreage * 43560) : null,
        source: "detroit-mi-parcels-current",
      }];
    });

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(batch, {
        onConflict: "county_id,parcel_id",
        ignoreDuplicates: false,
      });
      if (error) {
        console.error(`  DB error at offset ${offset}+${i}: ${error.message}`);
        totalErrors += batch.length;
      } else {
        totalInserted += batch.length;
      }
    }

    offset += records.length;
    const elapsedSeconds = Math.max(1, (Date.now() - startedAt) / 1000);
    const rate = totalInserted / elapsedSeconds;
    const pct = (offset / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsedSeconds.toFixed(0)}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors.toLocaleString()} | ${rate.toFixed(0)}/s | ETA ${eta}min`);
  }

  const elapsedMinutes = ((Date.now() - startedAt) / 1000 / 60).toFixed(1);
  console.log(`Done! Inserted ${totalInserted.toLocaleString()} Detroit parcel records in ${elapsedMinutes} minutes. Errors: ${totalErrors.toLocaleString()}`);
}

main().catch((error) => {
  console.error("Fatal Detroit parcel ingest error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
