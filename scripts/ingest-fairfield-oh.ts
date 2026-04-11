#!/usr/bin/env tsx
/**
 * Ingest Fairfield County OH property data from downloaded shapefile.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import * as shapefile from "shapefile";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Ingest Fairfield County OH Properties\n");

  // Create county
  let { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();

  if (!county) {
    const { data: newCounty, error } = await db.from("counties")
      .insert({ county_name: "Fairfield", state_code: "OH", state_fips: "39", county_fips: "045", active: true })
      .select("id").single();
    if (error) { console.error("County insert error:", error.message); return; }
    county = newCounty;
  }
  console.log(`  County ID: ${county!.id}`);

  // Read shapefile
  const source = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");

  // Read first record to see field names
  const first = await source.read();
  if (first.done) { console.log("No records"); return; }

  const fields = Object.keys(first.value.properties || {});
  console.log(`  Fields: ${fields.join(", ")}`);
  console.log(`  Sample: ${JSON.stringify(first.value.properties).slice(0, 500)}`);

  // Reset and read all
  const source2 = await shapefile.open("data/fairfield-oh-parcels/parcels.shp");
  let inserted = 0, batch: Array<Record<string, unknown>> = [];

  while (true) {
    const result = await source2.read();
    if (result.done) break;

    const p = result.value.properties || {};

    // Fairfield County OH shapefile field mapping (confirmed from actual .dbf)
    // PARID/PIN=parcel, OWN1=owner, PADDR1=site address, MCITYNAME=city, MZIP1=zip
    // APPRVAL=total appraised, APRLAND=land value, APRBLDG=bldg value
    // SFLA=sq ft living area, YRBLT=year built, LUC=land use code
    // PRICE/SALEVAL=sale price, SALEDT=sale date, RMBED=bedrooms, FIXBATH=full baths
    const lucMap: Record<string, string> = {
      "100":"single_family","101":"single_family","102":"single_family","110":"single_family",
      "111":"single_family","120":"single_family","200":"multifamily","210":"multifamily",
      "300":"condo","400":"commercial","500":"industrial","600":"land","700":"exempt","800":"exempt",
    };
    const luc = String(p.LUC || "").trim();
    const propType = lucMap[luc] || (luc.startsWith("1") ? "single_family" : luc.startsWith("2") ? "multifamily" : luc.startsWith("4") ? "commercial" : "residential");

    const saleDateRaw = String(p.SALEDT || "").trim();
    let saleDate: string | null = null;
    if (saleDateRaw && saleDateRaw.length >= 8) {
      // Format: YYYYMMDD or epoch ms
      if (/^\d{8}$/.test(saleDateRaw)) {
        saleDate = `${saleDateRaw.slice(0,4)}-${saleDateRaw.slice(4,6)}-${saleDateRaw.slice(6,8)}`;
      } else if (/^\d{12,}$/.test(saleDateRaw)) {
        const dt = new Date(parseInt(saleDateRaw));
        if (dt.getFullYear() > 1970) saleDate = dt.toISOString().slice(0, 10);
      }
    }

    const record: Record<string, unknown> = {
      county_id: county!.id,
      parcel_id: String(p.PARID || p.PIN || "").trim(),
      owner_name: String(p.OWN1 || "").trim(),
      address: String(p.PADDR1 || "").trim().toUpperCase(),
      city: String(p.MCITYNAME || "").trim().toUpperCase(),
      state_code: "OH",
      zip: String(p.MZIP1 || "").trim().slice(0, 5),
      assessed_value: parseFloat(p.APPRVAL) > 0 ? Math.round(parseFloat(p.APPRVAL)) : null,
      land_value: parseFloat(p.APRLAND) > 0 ? Math.round(parseFloat(p.APRLAND)) : null,
      property_type: propType,
      total_sqft: parseFloat(p.SFLA) > 0 ? Math.round(parseFloat(p.SFLA)) : null,
      year_built: parseInt(p.YRBLT) > 1700 && parseInt(p.YRBLT) < 2030 ? parseInt(p.YRBLT) : null,
      bedrooms: parseInt(p.RMBED) > 0 ? parseInt(p.RMBED) : null,
      bathrooms_full: parseInt(p.FIXBATH) > 0 ? parseInt(p.FIXBATH) : null,
      last_sale_price: parseFloat(p.PRICE || p.SALEVAL) > 0 ? Math.round(parseFloat(p.PRICE || p.SALEVAL)) : null,
      last_sale_date: saleDate,
      land_sqft: parseFloat(p.ACRES) > 0 ? Math.round(parseFloat(p.ACRES) * 43560) : null,
      source: "fairfield-oh-auditor",
    };

    if (!record.parcel_id) continue;
    batch.push(record);
    if (batch.length >= 500) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) console.error(`  Upsert error: ${error.message.slice(0, 80)}`);
      else inserted += batch.length;
      batch = [];
      if (inserted % 5000 === 0) process.stdout.write(`\r  Updated: ${inserted.toLocaleString()}`);
    }
  }

  if (batch.length > 0) {
    const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
    if (!error) inserted += batch.length;
  }

  console.log(`\n  Done: ${inserted.toLocaleString()} properties upserted for Fairfield County, OH`);
}

main().catch(console.error);
