#!/usr/bin/env tsx
/**
 * Ingest Peoria County, IL tax parcels from the public Peoria County ArcGIS layer.
 *
 * Source:
 * https://services.arcgis.com/iPiPjILCMYxPZWTc/ArcGIS/rest/services/Tax_Parcels/FeatureServer/5
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://services.arcgis.com/iPiPjILCMYxPZWTc/ArcGIS/rest/services/Tax_Parcels/FeatureServer/5";
const OUT_FIELDS = [
  "OBJECTID", "PIN", "owner_name", "ADDR1", "ADDR2", "OWNCTY", "OWNSTE", "OWZIP",
  "PropClass", "prop_street", "CITY", "STATE", "PZIP", "LEGAL", "year_built",
  "total_living_area", "n_bedrooms", "full_baths", "half_baths", "land_lot_value",
  "improvements_value", "total_assessed_value", "Acres",
].join(",");
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const COUNTY_FIPS = "143";
const COUNTY_NAME = "Peoria";

const numberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

function classifyPropClass(value: unknown): string {
  const text = String(value ?? "").toUpperCase();
  if (!text) return "unknown";
  if (/FARM|AGRIC/.test(text)) return "agricultural";
  if (/APT|APART|MULTI|DUPLEX|TRIPLEX|FOUR|FLAT|2[- ]?FAM|3[- ]?FAM|4[- ]?FAM/.test(text)) return "residential";
  if (/RES|CONDO|DWELL|SINGLE/.test(text)) return "residential";
  if (/COMM|RETAIL|OFFICE|BUSINESS/.test(text)) return "commercial";
  if (/IND|WAREHOUSE|MANUF/.test(text)) return "industrial";
  if (/VAC|LAND|LOT/.test(text)) return "vacant";
  if (/EXEMPT|PUBLIC|GOV/.test(text)) return "exempt";
  return "other";
}

async function getOrCreateCounty(): Promise<number> {
  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", COUNTY_NAME)
    .eq("state_code", "IL")
    .single();
  if (data) return data.id;

  const { data: created, error } = await db.from("counties").insert({
    county_name: COUNTY_NAME,
    state_code: "IL",
    state_fips: "17",
    county_fips: COUNTY_FIPS,
    active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  return created.id;
}

async function fetchCount(): Promise<number> {
  const url = `${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`;
  const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
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
  console.log("MXRE - Ingest Peoria County, IL public tax parcels");
  console.log(`Source: ${SERVICE_URL}`);

  const countyId = await getOrCreateCounty();
  const totalCount = await fetchCount();
  console.log(`County ID: ${countyId}`);
  console.log(`Total parcels: ${totalCount.toLocaleString()}`);

  const offsetArg = process.argv.find(arg => arg.startsWith("--offset="));
  let offset = offsetArg ? Number(offsetArg.split("=")[1]) : 0;
  let totalInserted = 0;
  let totalErrors = 0;
  const startedAt = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) break;

    const rows = records.flatMap((record) => {
      const parcelId = String(record.PIN ?? "").trim();
      if (!parcelId) {
        totalErrors += 1;
        return [];
      }

      const streetAddress = String(record.prop_street ?? "").trim();
      const mailingAddress = [record.ADDR1, record.ADDR2].map(v => String(v ?? "").trim()).filter(Boolean).join(" ");
      const halfBaths = numberOrNull(record.half_baths);
      const fullBaths = numberOrNull(record.full_baths);
      const assessedValue = numberOrNull(record.total_assessed_value);

      return [{
        county_id: countyId,
        parcel_id: parcelId,
        address: streetAddress.toUpperCase(),
        city: String(record.CITY ?? "").trim().toUpperCase(),
        state_code: "IL",
        zip: String(record.PZIP ?? "").trim().slice(0, 5),
        owner_name: String(record.owner_name ?? "").trim(),
        mailing_address: mailingAddress || null,
        mailing_city: String(record.OWNCTY ?? "").trim() || null,
        mailing_state: String(record.OWNSTE ?? "").trim().slice(0, 2) || null,
        mailing_zip: String(record.OWZIP ?? "").trim().slice(0, 10) || null,
        property_use: String(record.PropClass ?? "").trim() || null,
        property_type: classifyPropClass(record.PropClass),
        year_built: numberOrNull(record.year_built),
        total_sqft: numberOrNull(record.total_living_area),
        bedrooms: numberOrNull(record.n_bedrooms),
        bathrooms: fullBaths === null && halfBaths === null ? null : (fullBaths ?? 0) + (halfBaths ?? 0) * 0.5,
        assessed_value: assessedValue,
        market_value: assessedValue === null ? null : Math.round(assessedValue * 3.3333),
        lot_sqft: numberOrNull(record.Acres) === null ? null : Math.round(numberOrNull(record.Acres)! * 43560),
        source: "peoria-county-il-tax-parcels",
      }];
    });

    const dedupedRows = Array.from(new Map(rows.map(row => [`${row.county_id}|${row.parcel_id}`, row])).values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
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
  console.log(`Done! Inserted ${totalInserted.toLocaleString()} Peoria County records in ${elapsedMinutes} minutes. Errors: ${totalErrors.toLocaleString()}`);
}

main().catch((error) => {
  console.error("Fatal Peoria parcel ingest error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
