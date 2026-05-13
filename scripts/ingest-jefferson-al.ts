#!/usr/bin/env tsx
/**
 * Ingest Jefferson County, AL public parcel data for Birmingham.
 *
 * Source:
 * https://jccgis.jccal.org/server/rest/services/Basemap/Parcels/MapServer/0
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://jccgis.jccal.org/server/rest/services/Basemap/Parcels/MapServer/0";
const WHERE = "Property_City='BIRMINGHAM'";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const COUNTY_NAME = "Jefferson";
const STATE_CODE = "AL";
const STATE_FIPS = "01";
const COUNTY_FIPS = "073";
const OUT_FIELDS = [
  "OBJECTID", "PID", "PARCELID", "APP_PID", "ParcelNo", "ADDR_APR", "ZIP",
  "OWNERNAME", "Name2", "PROP_MAIL", "CITYMAIL", "STATE_Mail", "ZIP_MAIL",
  "Property_City", "Property_State", "Legal_Desc", "Cls", "ZONING_BOE",
  "AssdValue", "PrevParcelLand", "PrevParcelImp", "PrevParcelTotal",
  "TotalMHValue", "ACRES_APR", "GIS_ACRES", "Sqft", "Bldg_Number",
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

function classify(value: unknown): string {
  const textValue = String(value ?? "").toUpperCase();
  if (/APART|MULTI|DUPLEX|TRIPLEX|FOUR|2|3|4/.test(textValue)) return "residential";
  if (/RES|SINGLE|CONDO|DWELL/.test(textValue)) return "residential";
  if (/COMM|RETAIL|OFFICE|BUSINESS/.test(textValue)) return "commercial";
  if (/IND|WAREHOUSE|MANUF/.test(textValue)) return "industrial";
  if (/VAC|LAND|LOT/.test(textValue)) return "vacant";
  if (/EXEMPT|PUBLIC|GOV/.test(textValue)) return "exempt";
  return "other";
}

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
  console.log("MXRE - Ingest Jefferson County, AL public parcels for Birmingham");
  console.log(`Source: ${SERVICE_URL}`);

  const countyId = await getOrCreateCounty();
  const totalCount = await fetchCount();
  let offset = Number(process.argv.find(arg => arg.startsWith("--offset="))?.split("=")[1] ?? 0);
  let totalInserted = 0;
  let totalErrors = 0;
  const startedAt = Date.now();

  console.log(`County ID: ${countyId}`);
  console.log(`Total Birmingham parcels: ${totalCount.toLocaleString()}`);

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) break;
    const rows = records.flatMap(record => {
      const parcelId = text(record.PID) ?? text(record.PARCELID) ?? text(record.APP_PID) ?? text(record.ParcelNo);
      const address = text(record.ADDR_APR)?.toUpperCase() ?? null;
      const zip = text(record.ZIP)?.slice(0, 5) ?? null;
      if (!parcelId || !address || !zip) {
        totalErrors += 1;
        return [];
      }
      const ownerName = [record.OWNERNAME, record.Name2].map(text).filter(Boolean).join(" ");
      const assessedValue = num(record.AssdValue) ?? num(record.PrevParcelTotal);
      const acres = Number(record.ACRES_APR ?? record.GIS_ACRES ?? 0);
      return [{
        county_id: countyId,
        parcel_id: parcelId,
        address,
        city: "BIRMINGHAM",
        state_code: STATE_CODE,
        zip,
        owner_name: ownerName || null,
        mailing_address: text(record.PROP_MAIL),
        mailing_city: text(record.CITYMAIL),
        mailing_state: text(record.STATE_Mail)?.slice(0, 2),
        mailing_zip: text(record.ZIP_MAIL)?.slice(0, 10),
        property_use: text(record.Cls) ?? text(record.ZONING_BOE),
        property_type: classify(record.Cls ?? record.ZONING_BOE),
        total_sqft: num(record.Sqft),
        assessed_value: assessedValue,
        market_value: assessedValue,
        lot_sqft: Number.isFinite(acres) && acres > 0 ? Math.round(acres * 43560) : null,
        source: "jefferson-al-parcels",
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
  console.log(`Done! Inserted ${totalInserted.toLocaleString()} Birmingham parcel records in ${elapsedMinutes} minutes. Errors: ${totalErrors.toLocaleString()}`);
}

main().catch((error) => {
  console.error("Fatal Jefferson AL parcel ingest error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
