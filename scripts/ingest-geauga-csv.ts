#!/usr/bin/env tsx
/**
 * Ingest Geauga County OH from the full auditor CSV (149K parcels).
 * Rich data: owner, address, values, year built, sqft, sale history, legal desc.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { createReadStream } from "node:fs";
import { parse } from "csv-parse";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created, error } = await db.from("counties").insert({ county_name: name, state_code: state, active: true }).select("id").single();
  if (error || !created) {
    const { data: retry } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
    if (!retry) throw new Error(`Could not get or create county ${name}, ${state}: ${error?.message}`);
    return retry.id;
  }
  return created.id;
}

async function main() {
  console.log("MXRE — Ingest Geauga County OH (Full Auditor CSV)\n");

  const countyId = await getOrCreateCounty("Geauga", "OH");
  console.log("County ID:", countyId);

  const { count: existing } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log("Existing properties:", existing);
  // Skip delete — just upsert/add new records

  const csvPath = "C:/Users/msanc/mxre/data/geauga-oh-parcels.csv";
  const parser = createReadStream(csvPath).pipe(parse({
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
  }));

  let inserted = 0, skipped = 0, errors = 0;
  const batch: any[] = [];
  const BATCH_SIZE = 200;

  for await (const row of parser) {
    const address = (row.LOCATION_A || "").trim();
    const city = (row.LOCATION_C || "").trim();
    const zip = (row.LOCATION_Z || "").trim();
    const owner = (row.Oname1 || "").trim();
    const parcelId = (row.PARCEL_ID || row.PARCEL_ID2 || "").trim();

    if (!parcelId && !address) { skipped++; continue; }

    const mktTotal = parseFloat(row.MktTotVal) || null;
    const taxTotal = parseFloat(row.TaxTotVal) || null;
    const yearBuilt = parseInt(row.YearBuilt) || null;
    const sqft = parseInt(row.LivingArea) || null;
    const saleAmt = parseFloat(row.Sale_Amt) || null;
    const saleDate = (row.Sale_Date || "").trim() || null;

    // Classify property type from PropClass
    const propClass = (row.PropClass || "").trim().toUpperCase();
    let propertyType = "residential";
    if (propClass.includes("COM")) propertyType = "commercial";
    else if (propClass.includes("IND")) propertyType = "industrial";
    else if (propClass.includes("AGR") || propClass.includes("AG")) propertyType = "agricultural";
    else if (propClass.includes("VAC")) propertyType = "vacant_land";

    batch.push({
      county_id: countyId,
      parcel_id: parcelId || "",
      address: address || "",
      city: city || "",
      state_code: "OH",
      zip: zip || "",
      owner_name: owner || "",
      assessed_value: mktTotal,
      taxable_value: taxTotal,
      market_value: mktTotal,
      land_value: parseFloat(row.MktLandVal) || null,
      year_built: yearBuilt && yearBuilt > 1700 && yearBuilt < 2030 ? yearBuilt : null,
      total_sqft: sqft && sqft > 0 ? sqft : null,
      property_type: propertyType,
      last_sale_date: saleDate,
      last_sale_price: saleAmt && saleAmt > 0 ? saleAmt : null,
      source: "geauga-oh-auditor",
    });

    if (batch.length >= BATCH_SIZE) {
      const { error } = await db.from("properties").insert(batch);
      if (error) {
        errors++;
        if (errors <= 3) console.error(`\n  Insert error: ${error.message.slice(0, 100)}`);
      } else {
        inserted += batch.length;
      }
      batch.length = 0;
      if (inserted % 10000 === 0) {
        process.stdout.write(`\r  Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped} | Errors: ${errors}`);
      }
    }
  }

  if (batch.length > 0) {
    const { error } = await db.from("properties").insert(batch);
    if (error) errors++;
    else inserted += batch.length;
  }

  console.log(`\n\nDone: ${inserted.toLocaleString()} inserted | ${skipped} skipped | ${errors} errors`);

  const { count: total } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log(`Geauga County now has ${total?.toLocaleString()} properties`);
}

main().catch(console.error);
