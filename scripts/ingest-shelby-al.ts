#!/usr/bin/env tsx
/**
 * Ingest Shelby County, AL public cadastral parcels for Birmingham.
 *
 * Source:
 * https://maps.shelbyal.com/gisserver/rest/services/LegacyServices/Cadastral_2025/MapServer/91
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://maps.shelbyal.com/gisserver/rest/services/LegacyServices/Cadastral_2025/MapServer/91";
const WHERE = "Upper(CITY) = 'BIRMINGHAM'";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const COUNTY_NAME = "Shelby";
const STATE_CODE = "AL";
const STATE_FIPS = "01";
const COUNTY_FIPS = "117";
const OUT_FIELDS = [
  "OBJECTID",
  "Assess_Num",
  "NAM1",
  "NAM2",
  "ADR1",
  "CITY",
  "STATE",
  "ZIP",
  "ACRES",
  "SQUARE_FEET",
  "SUBD_NAME_1",
  "LOT_DIM_1",
  "LOT_DIM_2",
  "PARCEL_YR",
].join(",");

const text = (value: unknown): string | null => {
  const clean = String(value ?? "").replace(/\s+/g, " ").trim();
  return clean || null;
};

const num = (value: unknown): number | null => {
  if (value == null || value === "") return null;
  const parsed = Number(String(value).replace(/,/g, ""));
  return Number.isFinite(parsed) ? Math.round(parsed) : null;
};

async function getOrCreateCounty(): Promise<number> {
  const { data } = await db.from("counties")
    .select("id")
    .eq("state_code", STATE_CODE)
    .eq("county_fips", COUNTY_FIPS)
    .single();
  if (data) return data.id;

  const { data: created, error } = await db.from("counties").insert({
    county_name: COUNTY_NAME,
    state_code: STATE_CODE,
    state_fips: STATE_FIPS,
    county_fips: COUNTY_FIPS,
    active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  return created.id;
}

async function fetchCount(): Promise<number> {
  const params = new URLSearchParams({ where: WHERE, returnCountOnly: "true", f: "json" });
  const response = await fetch(`${SERVICE_URL}/query?${params}`, { signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`Count failed: ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(json.error.message ?? JSON.stringify(json.error));
  return Number(json.count ?? 0);
}

async function fetchPage(offset: number): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({
    where: WHERE,
    outFields: OUT_FIELDS,
    returnGeometry: "false",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields: "OBJECTID",
    f: "json",
  });
  const response = await fetch(`${SERVICE_URL}/query?${params}`, { signal: AbortSignal.timeout(90_000) });
  if (!response.ok) throw new Error(`Page ${offset} failed: ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(json.error.message ?? JSON.stringify(json.error));
  return json.features?.map((feature: { attributes: Record<string, unknown> }) => feature.attributes) ?? [];
}

async function main() {
  console.log("MXRE - Ingest Shelby County, AL public parcels for Birmingham");
  console.log(`Source: ${SERVICE_URL}`);

  const countyId = await getOrCreateCounty();
  const totalCount = await fetchCount();
  let offset = Number(process.argv.find(arg => arg.startsWith("--offset="))?.split("=")[1] ?? 0);
  let totalInserted = 0;
  let totalErrors = 0;
  let dbErrors = 0;
  const startedAt = Date.now();

  console.log(`County ID: ${countyId}`);
  console.log(`Total Birmingham/Shelby parcels: ${totalCount.toLocaleString()}`);

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) break;

    const rows = records.flatMap(record => {
      const parcelId = text(record.Assess_Num);
      const address = text(record.ADR1)?.toUpperCase() ?? null;
      const zip = text(record.ZIP)?.slice(0, 5) ?? null;
      if (!parcelId || !address || !zip) {
        totalErrors += 1;
        return [];
      }

      const ownerName = [record.NAM1, record.NAM2].map(text).filter(Boolean).join(" ");
      const acres = Number(record.ACRES ?? 0);
      return [{
        county_id: countyId,
        parcel_id: parcelId,
        address,
        city: "BIRMINGHAM",
        state_code: STATE_CODE,
        zip,
        owner_name: ownerName || null,
        property_use: text(record.SUBD_NAME_1),
        property_type: "unknown",
        total_sqft: num(record.SQUARE_FEET),
        lot_sqft: Number.isFinite(acres) && acres > 0 ? Math.round(acres * 43560) : num(record.SQUARE_FEET),
        source: "shelby-al-cadastral-2025",
      }];
    });

    const dedupedRows = Array.from(
      new Map(rows.map(row => [`${row.county_id}|${row.parcel_id}`, row])).values(),
    );

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(batch, {
        onConflict: "county_id,parcel_id",
        ignoreDuplicates: false,
      });
      if (error) {
        console.error(`  DB error at offset ${offset}+${i}: ${error.message}`);
        totalErrors += batch.length;
        dbErrors += 1;
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
  console.log(`Done! Inserted ${totalInserted.toLocaleString()} Birmingham/Shelby parcel records in ${elapsedMinutes} minutes. Errors: ${totalErrors.toLocaleString()}`);
  if (dbErrors > 0) {
    throw new Error(`Shelby AL ingest had ${dbErrors} database batch errors`);
  }
}

main().catch((error) => {
  console.error("Fatal Shelby AL parcel ingest error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
