#!/usr/bin/env tsx
/**
 * Ingest Wyandot County OH from GeoJSON (40K parcels).
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "node:fs";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created, error } = await db.from("counties").insert({ county_name: name, state_code: state, active: true }).select("id").single();
  if (error || !created) {
    // Race or already exists — retry select
    const { data: retry } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
    if (!retry) throw new Error(`Could not get or create county ${name}, ${state}: ${error?.message}`);
    return retry.id;
  }
  return created.id;
}

async function main() {
  console.log("MXRE — Ingest Wyandot County OH (GeoJSON)\n");

  const countyId = await getOrCreateCounty("Wyandot", "OH");
  console.log("County ID:", countyId);

  const geojson = JSON.parse(readFileSync("C:/Users/msanc/mxre/data/wyandot-oh-parcels.geojson", "utf-8"));
  console.log(`Features: ${geojson.features.length}\n`);

  let inserted = 0, skipped = 0, errors = 0;
  const BATCH_SIZE = 200;
  let batch: any[] = [];

  for (const feature of geojson.features) {
    const p = feature.properties;
    const r = (key: string) => (p[key] || "").toString().trim();
    const n = (key: string) => parseFloat(p[key]) || null;
    const i = (key: string) => parseInt(p[key]) || null;

    const parcelId = r("Tax_Parcel_GIS_Parcel_Number") || r("Tax_Parcel_TSC_Parcel_Number");
    const address = r("Tax_Parcel_TSC_Site_Address");
    const owner = r("Tax_Parcel_TSC_Owner1");

    if (!parcelId && !address) { skipped++; continue; }

    // Values from the rexu_ExportTable1 fields
    const totalVal = n("rexu_ExportTable1_TOTAL");
    const landVal = n("rexu_ExportTable1_CLAND") || n("rexu_ExportTable1_LAND");
    const bldgVal = n("rexu_ExportTable1_CBLDG");
    const yearBuilt = i("rexu_ExportTable1_DWYB");
    const rooms = i("rexu_ExportTable1_ROOMS");
    const beds = i("rexu_ExportTable1_BED");
    const baths = i("rexu_ExportTable1_PLUM");
    const sqft = i("rexu_ExportTable1_FIATSQ") || i("rexu_ExportTable1_STORY1");
    const units = i("rexu_ExportTable1_LIVUNT");
    const salePrice = n("rexu_ExportTable1_PURPRI");
    // Date might be YYMMDD (e.g. 70523 = 2007-05-23) or empty
    const rawDate = r("rexu_ExportTable1_PURDAT");
    let saleDate: string | null = null;
    if (rawDate && rawDate.length >= 5 && rawDate.length <= 6) {
      const padded = rawDate.padStart(6, "0");
      const yy = parseInt(padded.slice(0, 2));
      const mm = padded.slice(2, 4);
      const dd = padded.slice(4, 6);
      const year = yy > 50 ? 1900 + yy : 2000 + yy;
      if (parseInt(mm) >= 1 && parseInt(mm) <= 12 && parseInt(dd) >= 1 && parseInt(dd) <= 31) {
        saleDate = `${year}-${mm}-${dd}`;
      }
    }
    const acres = n("rexu_ExportTable1_ACRES");
    const legal = [r("rexu_ExportTable1_LEGAL1"), r("rexu_ExportTable1_LEGAL2"), r("rexu_ExportTable1_LEGAL3")].filter(Boolean).join(" ");

    // Classify
    const classCode = r("rexu_ExportTable1_CLASS");
    let propertyType = "residential";
    if (classCode.startsWith("5")) propertyType = "commercial";
    else if (classCode.startsWith("3")) propertyType = "industrial";
    else if (classCode.startsWith("6")) propertyType = "agricultural";

    batch.push({
      county_id: countyId,
      parcel_id: parcelId || "",
      address: address || "",
      city: "",
      state_code: "OH",
      zip: "",
      owner_name: owner || "",
      assessed_value: totalVal && totalVal > 0 ? totalVal : null,
      year_built: yearBuilt && yearBuilt > 1700 && yearBuilt < 2030 ? yearBuilt : null,
      total_sqft: sqft && sqft > 0 ? sqft : null,
      total_units: units && units > 0 ? units : 1,
      property_type: propertyType,
      last_sale_date: saleDate,
      last_sale_price: salePrice && salePrice > 0 ? salePrice : null,
      land_sqft: acres && acres > 0 ? Math.round(acres * 43560) : null,
      source: "ohio-parcels-geohio",
    });

    if (batch.length >= BATCH_SIZE) {
      const { error } = await db.from("properties").insert(batch);
      if (error) {
        errors++;
        if (errors <= 3) console.error(`Insert error: ${error.message.slice(0, 100)}`);
      } else {
        inserted += batch.length;
      }
      batch = [];
      if (inserted % 5000 === 0) {
        process.stdout.write(`\r  Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped}`);
      }
    }
  }

  if (batch.length > 0) {
    const { error } = await db.from("properties").insert(batch);
    if (error) errors++;
    else inserted += batch.length;
  }

  console.log(`\n\nDone: ${inserted.toLocaleString()} inserted | ${skipped} skipped | ${errors} errors`);
}

main().catch(console.error);
