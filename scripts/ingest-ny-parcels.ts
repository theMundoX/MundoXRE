#!/usr/bin/env tsx
/**
 * Ingest New York State tax parcels from NYS GIS FeatureServer.
 * ~3.7M parcels (36 counties with public data), paginated at 1000 records.
 * Source: https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL = "https://gisservices.its.ny.gov/arcgis/rest/services/NYS_Tax_Parcels_Public/FeatureServer/1/query";
const PAGE_SIZE = 1000;
const DB_BATCH = 500;
const OUT_FIELDS = "COUNTY_NAME,MUNI_NAME,PRINT_KEY,SBL,PARCEL_ADDR,LOC_ST_NBR,LOC_STREET,LOC_UNIT,LOC_ZIP,PROP_CLASS,LAND_AV,TOTAL_AV,FULL_MARKET_VAL,YR_BLT,SQ_FT,SQFT_LIVING,ACRES,PRIMARY_OWNER,ADD_OWNER,NBR_BEDROOMS,NBR_FULL_BATHS,BLDG_STYLE_DESC,USED_AS_DESC,CITYTOWN_NAME";

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, state: string, stateFips = "36", countyFips = "000"): Promise<number> {
  const key = `${name}|${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }
  const { data: created, error } = await db.from("counties").insert({
    county_name: name, state_code: state, state_fips: stateFips,
    county_fips: countyFips, active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  countyCache.set(key, created.id);
  return created.id;
}

function classifyPropertyType(propClass: string, usedAs: string): string {
  const cls = (propClass || "").trim();
  const desc = (usedAs || "").toUpperCase();
  // NYS property class codes: 1xx=agricultural, 2xx=residential, 3xx=vacant, 4xx=commercial, 5xx=recreation, 6xx=community services, 7xx=industrial, 8xx=public services, 9xx=wild/forest
  if (cls.startsWith("2") || desc.includes("RESIDENTIAL") || desc.includes("SINGLE FAMILY")) return "residential";
  if (cls.startsWith("4") || desc.includes("COMMERCIAL") || desc.includes("OFFICE")) return "commercial";
  if (cls.startsWith("7") || desc.includes("INDUSTRIAL") || desc.includes("MANUFACTUR")) return "industrial";
  if (cls.startsWith("1") || desc.includes("AGRICULTUR") || desc.includes("FARM")) return "agricultural";
  if (cls.startsWith("3") || desc.includes("VACANT")) return "vacant";
  if (desc.includes("APARTMENT") || desc.includes("MULTI")) return "apartment";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}?${params}`, { signal: AbortSignal.timeout(60000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) || [];
    } catch (err: any) {
      if (attempt === 2) throw err;
      console.error(`  Retry ${attempt + 1}: ${err.message}`);
      await new Promise(r => setTimeout(r, 3000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest NY State Tax Parcels\n");

  const countRes = await fetch(`${BASE_URL}?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countRes.json();
  console.log(`Total parcels available: ${totalCount?.toLocaleString()}\n`);

  const startOffset = parseInt(process.argv[2] || "0");
  if (startOffset > 0) console.log(`Resuming from offset ${startOffset.toLocaleString()}\n`);

  let inserted = 0, skipped = 0, dbErrors = 0;
  let offset = startOffset;

  while (true) {
    const features = await fetchPage(offset);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const p of features) {
      const countyName = (p.COUNTY_NAME || "").trim();
      if (!countyName) { skipped++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "NY");
      } catch { skipped++; continue; }

      const address = (p.PARCEL_ADDR || "").trim() ||
        [p.LOC_ST_NBR, p.LOC_STREET, p.LOC_UNIT].filter(Boolean).join(" ").trim();
      const city = (p.CITYTOWN_NAME || p.MUNI_NAME || "").trim();
      const sqft = p.SQFT_LIVING || p.SQ_FT || null;
      const totalVal = p.FULL_MARKET_VAL || p.TOTAL_AV || null;
      const yearBuilt = p.YR_BLT && p.YR_BLT > 1700 && p.YR_BLT < 2030 ? p.YR_BLT : null;

      batch.push({
        county_id: countyId,
        parcel_id: (p.PRINT_KEY || p.SBL || "").trim(),
        address: address || "",
        city: city || "",
        state_code: "NY",
        zip: (p.LOC_ZIP || "").trim(),
        owner_name: (p.PRIMARY_OWNER || "").trim(),
        assessed_value: totalVal && totalVal > 0 ? totalVal : null,
        year_built: yearBuilt,
        total_sqft: sqft && sqft > 0 ? sqft : null,
        total_units: 1,
        property_type: classifyPropertyType(p.PROP_CLASS, p.USED_AS_DESC),
        source: "nys-tax-parcels",
      });

      if (batch.length >= DB_BATCH) {
        const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
        if (error) {
          dbErrors++;
          if (dbErrors <= 5) console.error(`\n  DB error: ${error.message.slice(0, 120)}`);
        } else {
          inserted += batch.length;
        }
        batch.length = 0;
      }
    }

    if (batch.length > 0) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
      if (error) dbErrors++;
      else inserted += batch.length;
    }

    offset += features.length;
    const pct = totalCount ? ((offset / totalCount) * 100).toFixed(1) : "?";
    process.stdout.write(`\r  Offset: ${offset.toLocaleString()} / ${totalCount?.toLocaleString()} (${pct}%) | Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped} | Errors: ${dbErrors}   `);

    if (offset % 20000 === 0) await new Promise(r => setTimeout(r, 500));
  }

  const { count } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`\n\n══════════════════════════════════════════════`);
  console.log(`  NY parcels inserted: ${inserted.toLocaleString()}`);
  console.log(`  Skipped: ${skipped} | DB errors: ${dbErrors}`);
  console.log(`  Total properties in DB: ${count?.toLocaleString()}`);
  console.log(`══════════════════════════════════════════════\n`);
}

main().catch(console.error);
