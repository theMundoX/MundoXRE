#!/usr/bin/env tsx
/**
 * Ingest Black Hawk County IA (Waterloo) property data from Iowa Statewide Parcels (2017).
 * Source: Iowa_Parcels_2017 FeatureServer filtered by COUNTYNAME='BLACK HAWK'.
 * Limited fields: parcel_id, property_type (class), owner_name (deed holder).
 * Black Hawk County's own GIS server is not publicly accessible, and the Beacon
 * portal has Cloudflare protection. The statewide data provides ~68K parcels with
 * basic information.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const STATEWIDE_URL = "https://services3.arcgis.com/kd9gaiUExYqUbnoq/arcgis/rest/services/Iowa_Parcels_2017/FeatureServer/0/query";

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "19", county_fips: "013", active: true })
    .select("id").single();
  return created!.id;
}

async function queryArcGIS(offset: number, limit: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "COUNTYNAME='BLACK HAWK'",
    outFields: "PARCELNUMB,STATEPARID,DEEDHOLDER,PARCELCLAS",
    resultRecordCount: String(limit),
    resultOffset: String(offset),
    f: "json",
  });
  const resp = await fetch(`${STATEWIDE_URL}?${params}`, {
    headers: { "User-Agent": "Mozilla/5.0" },
  });
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.features || [];
}

function classifyProperty(parcelClass: string): string {
  const cls = (parcelClass || "").toUpperCase().trim();
  if (cls.includes("COMMERCIAL")) return "commercial";
  if (cls.includes("INDUSTRIAL")) return "industrial";
  if (cls.includes("AGRIC")) return "agricultural";
  if (cls.includes("RESID")) return "residential";
  if (cls.includes("EXEMPT")) return "exempt";
  if (cls.includes("MULTI")) return "multifamily";
  return "other";
}

async function main() {
  console.log("MXRE — Ingest Black Hawk County IA Property Data\n");
  console.log("  Source: Iowa Statewide Parcels (2017) — limited fields (parcel_id, class, owner)");
  console.log("  Note: Black Hawk County GIS is not publicly accessible; using statewide data.\n");

  const countyId = await getOrCreateCounty("Black Hawk", "IA");
  console.log("  County ID:", countyId);

  const { count: existing } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log("  Existing properties:", existing);

  const BATCH_SIZE = 2000;
  let offset = 0;
  let inserted = 0, skipped = 0, duplicates = 0, errors = 0;

  while (true) {
    const features = await queryArcGIS(offset, BATCH_SIZE);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const f of features) {
      const a = f.attributes;
      const parcelId = (a.PARCELNUMB || a.STATEPARID || "").trim();
      const owner = (a.DEEDHOLDER || "").trim();

      if (!parcelId) { skipped++; continue; }

      batch.push({
        county_id: countyId,
        parcel_id: parcelId,
        address: "",
        city: "",
        state_code: "IA",
        zip: "",
        owner_name: owner,
        assessed_value: null,
        taxable_value: null,
        market_value: null,
        land_value: null,
        year_built: null,
        total_sqft: null,
        property_type: classifyProperty(a.PARCELCLAS),
        last_sale_date: null,
        last_sale_price: null,
        source: "iowa-statewide-parcels-2017",
      });
    }

    if (batch.length > 0) {
      const SUB_BATCH = 200;
      for (let i = 0; i < batch.length; i += SUB_BATCH) {
        const chunk = batch.slice(i, i + SUB_BATCH);
        const { error } = await db.from("properties").insert(chunk);
        if (error) {
          if (error.message.includes("duplicate") || error.code === "23505") {
            for (const row of chunk) {
              const { error: e2 } = await db.from("properties").insert(row);
              if (e2) {
                if (e2.message.includes("duplicate") || e2.code === "23505") duplicates++;
                else {
                  errors++;
                  if (errors <= 3) console.error(`\n  Insert error: ${e2.message.slice(0, 100)}`);
                }
              } else {
                inserted++;
              }
            }
          } else {
            errors++;
            if (errors <= 3) console.error(`\n  Insert error: ${error.message.slice(0, 100)}`);
          }
        } else {
          inserted += chunk.length;
        }
      }
    }

    offset += features.length;
    process.stdout.write(`\r  Progress: ${offset.toLocaleString()} fetched | ${inserted.toLocaleString()} inserted | ${duplicates} dups | ${skipped} skipped | ${errors} errors`);

    if (features.length < BATCH_SIZE) break;
  }

  console.log(`\n\n  Done: ${inserted.toLocaleString()} inserted | ${duplicates} duplicates | ${skipped} skipped | ${errors} errors`);

  const { count: total } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log(`  Black Hawk County IA now has ${total?.toLocaleString()} properties`);
}

main().catch(console.error);
