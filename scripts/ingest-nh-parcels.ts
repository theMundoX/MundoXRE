#!/usr/bin/env tsx
/**
 * Ingest NH statewide parcel data from GRANIT shapefile.
 *
 * Data source: NH GRANIT Parcel Mosaic Polygons
 * Fields: PID (parcel ID), TOWN, CountyID, SLU/SLUC (state land use)
 * Note: No owner names, addresses, or assessed values in this GIS dataset.
 *
 * Target counties (8 of 10):
 *   01=Belknap, 02=Carroll, 03=Cheshire, 05=Grafton,
 *   06=Hillsborough, 08=Rockingham, 09=Strafford, 10=Sullivan
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// NH state FIPS = 33. County FIPS codes & names (target counties only).
const NH_STATE_FIPS = "33";
const COUNTY_MAP: Record<string, { name: string; fips: string }> = {
  "01": { name: "Belknap",      fips: "001" },
  "02": { name: "Carroll",      fips: "003" },
  "03": { name: "Cheshire",     fips: "005" },
  "05": { name: "Grafton",      fips: "009" },
  "06": { name: "Hillsborough", fips: "011" },
  "08": { name: "Rockingham",   fips: "015" },
  "09": { name: "Strafford",    fips: "017" },
  "10": { name: "Sullivan",     fips: "019" },
};

async function getOrCreateCounty(name: string, state: string, stateFips: string, countyFips: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: stateFips, county_fips: countyFips, active: true })
    .select("id").single();
  if (error) throw new Error(`Failed to create county ${name}: ${error.message}`);
  return created!.id;
}

/**
 * Map NH State Land Use (SLU) codes to property types.
 * NH DRA codes: 1x=residential, 2x=open space/farm, 3x=commercial, 4x=industrial, 5x=exempt
 */
function classifyPropertyType(slu: string): string {
  if (!slu) return "residential";
  const primary = slu.split("-")[0];
  const code = parseInt(primary);
  if (isNaN(code)) return "residential";

  if (code >= 10 && code <= 19) return "residential";
  if (code >= 20 && code <= 29) {
    if (code === 27) return "vacant_land";
    return "agricultural";
  }
  if (code >= 30 && code <= 39) return "commercial";
  if (code >= 40 && code <= 49) return "industrial";
  if (code >= 50 && code <= 59) return "exempt";
  if (code >= 70 && code <= 79) return "vacant_land";
  return "residential";
}

async function main() {
  console.log("MXRE — Ingest NH Statewide Parcels (GRANIT Shapefile)\n");
  console.log("Target counties:", Object.values(COUNTY_MAP).map(c => c.name).join(", "), "\n");

  // Resolve county IDs upfront
  const countyIds: Record<string, number> = {};
  for (const [code, info] of Object.entries(COUNTY_MAP)) {
    countyIds[code] = await getOrCreateCounty(info.name, "NH", NH_STATE_FIPS, info.fips);
    console.log(`  ${info.name} (${code}) => county_id ${countyIds[code]}`);
  }
  console.log();

  // Check existing counts
  for (const [code, info] of Object.entries(COUNTY_MAP)) {
    const { count } = await db.from("properties")
      .select("*", { count: "exact", head: true })
      .eq("county_id", countyIds[code]);
    if (count && count > 0) {
      console.log(`  ${info.name}: ${count.toLocaleString()} existing properties`);
    }
  }

  const shpPath = "C:/Users/msanc/mxre/data/nh-parcels/NH_Parcel_Mosaic_-_Polygons.shp";
  const source = await shapefile.open(shpPath);

  // Phase 1: Read all records into memory, deduplicating by (county_id, parcel_id)
  console.log("\nPhase 1: Reading and deduplicating shapefile...");
  const seen = new Map<string, any>(); // key = "countyId:parcelId"
  let total = 0, skipped = 0;
  const countyStats: Record<string, number> = {};

  while (true) {
    const result = await source.read();
    if (result.done) break;
    total++;

    const p = result.value.properties;
    const countyCode = (p.CountyID || "").toString().padStart(2, "0");

    if (!COUNTY_MAP[countyCode]) {
      skipped++;
      continue;
    }

    const parcelId = (p.PID || "").toString().trim();
    const town = (p.TOWN || "").toString().trim();
    const slu = (p.SLU || "").toString().trim();
    const nhGisId = (p.NH_GIS_ID || "").toString().trim();

    if (!parcelId && !nhGisId) {
      skipped++;
      continue;
    }

    const pid = parcelId || nhGisId || "";
    const cid = countyIds[countyCode];
    const key = `${cid}:${pid}`;

    if (seen.has(key)) continue; // deduplicate

    const propertyType = classifyPropertyType(slu);

    seen.set(key, {
      county_id: cid,
      parcel_id: pid,
      address: "",
      city: town || "",
      state_code: "NH",
      zip: "",
      owner_name: "",
      assessed_value: null,
      year_built: null,
      total_sqft: null,
      total_units: null,
      property_type: propertyType,
      source: "nh-granit-parcels",
    });

    countyStats[countyCode] = (countyStats[countyCode] || 0) + 1;

    if (total % 100000 === 0) {
      process.stdout.write(`\r  Read ${total.toLocaleString()} records, ${seen.size.toLocaleString()} unique`);
    }
  }

  console.log(`\r  Read ${total.toLocaleString()} records, ${seen.size.toLocaleString()} unique parcels, ${skipped.toLocaleString()} skipped`);

  // Phase 2: Upsert in batches
  console.log("\nPhase 2: Upserting into database...");
  const rows = Array.from(seen.values());
  const BATCH_SIZE = 500;
  let inserted = 0, errors = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);

    const { error } = await db.from("properties")
      .upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });

    if (error) {
      errors++;
      if (errors <= 5) console.error(`\n  Upsert error at batch ${Math.floor(i / BATCH_SIZE)}: ${error.message.slice(0, 150)}`);
    } else {
      inserted += batch.length;
    }

    if (inserted % 10000 < BATCH_SIZE) {
      process.stdout.write(`\r  Upserted: ${inserted.toLocaleString()} / ${rows.length.toLocaleString()} | Errors: ${errors}`);
    }
  }

  console.log(`\n\nDone!`);
  console.log(`  Total records scanned: ${total.toLocaleString()}`);
  console.log(`  Unique parcels: ${seen.size.toLocaleString()}`);
  console.log(`  Upserted: ${inserted.toLocaleString()}`);
  console.log(`  Skipped (non-target county or no ID): ${skipped.toLocaleString()}`);
  console.log(`  Errors: ${errors}`);

  console.log("\nPer-county breakdown:");
  for (const [code, info] of Object.entries(COUNTY_MAP)) {
    const parsed = countyStats[code] || 0;
    const { count } = await db.from("properties")
      .select("*", { count: "exact", head: true })
      .eq("county_id", countyIds[code]);
    console.log(`  ${info.name}: ${parsed.toLocaleString()} unique parcels, ${(count || 0).toLocaleString()} total in DB`);
  }
}

main().catch(console.error);
