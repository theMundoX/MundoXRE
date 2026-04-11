#!/usr/bin/env tsx
/**
 * Ingest Geauga County OH property data from CSV.
 * Source: data/geauga-oh-parcels.csv (~149K rows)
 * State FIPS: 39, County FIPS: 055
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { createReadStream } from "fs";
import { createInterface } from "readline";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current.trim());
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current.trim());
  return fields;
}

function parseNumber(val: string | undefined): number | null {
  if (!val) return null;
  const cleaned = val.replace(/[,$"]/g, "");
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

function parseInt2(val: string | undefined): number | null {
  if (!val) return null;
  const cleaned = val.replace(/[,$"]/g, "");
  const n = parseInt(cleaned, 10);
  return isNaN(n) ? null : n;
}

async function main() {
  console.log("MXRE — Ingest Geauga County OH Properties\n");

  // Ensure county record exists
  let { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Geauga")
    .eq("state_code", "OH")
    .single();

  if (!county) {
    // Try insert, if duplicate just re-select
    const { data: newCounty, error } = await db
      .from("counties")
      .insert({
        county_name: "Geauga",
        state_code: "OH",
        state_fips: "39",
        county_fips: "055",
        active: true,
      })
      .select("id")
      .single();
    if (error) {
      // Retry select — might have been a timeout on first try
      const { data: retry } = await db.from("counties").select("id")
        .eq("county_name", "Geauga").eq("state_code", "OH").single();
      if (!retry) { console.error("Cannot find or create county"); return; }
      county = retry;
    } else {
      county = newCounty;
    }
  }
  console.log(`  County ID: ${county!.id}`);

  // Read CSV
  const rl = createInterface({
    input: createReadStream("data/geauga-oh-parcels.csv", "utf-8"),
    crlfDelay: Infinity,
  });

  let headers: string[] = [];
  let lineNum = 0;
  let inserted = 0;
  let skipped = 0;
  let batch: Array<Record<string, unknown>> = [];
  const BATCH_SIZE = 500;

  for await (const line of rl) {
    lineNum++;
    if (lineNum === 1) {
      headers = parseCSVLine(line);
      console.log(`  Headers (${headers.length}): ${headers.slice(0, 10).join(", ")}...`);
      continue;
    }

    const fields = parseCSVLine(line);
    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]] = fields[i] || "";
    }

    const parcelId = row["PARCEL_ID"];
    if (!parcelId) {
      skipped++;
      continue;
    }

    const saleAmt = parseNumber(row["Sale_Amt"]);
    const mktTotVal = parseNumber(row["MktTotVal"]);
    const taxTotVal = parseNumber(row["TaxTotVal"]);
    const livingArea = parseInt2(row["LivingArea"]);
    const yearBuilt = parseInt2(row["YearBuilt"]);

    const address = row["LOCATION_A"] || "";
    const city = row["LOCATION_C"] || "";
    if (!address && !city) {
      skipped++;
      continue;
    }

    const record: Record<string, unknown> = {
      county_id: county!.id,
      parcel_id: parcelId,
      owner_name: row["Oname1"] || null,
      address: address || "UNKNOWN",
      city: city || "UNKNOWN",
      state_code: "OH",
      zip: row["LOCATION_Z"] || "00000",
      assessed_value: mktTotVal,
      taxable_value: taxTotVal,
      property_type: row["PropClass"] || null,
      total_sqft: livingArea,
      year_built: yearBuilt,
      total_units: 1,
    };

    batch.push(record);
    if (batch.length >= BATCH_SIZE) {
      // Deduplicate by parcel_id within batch (keep last)
      const deduped = [...new Map(batch.map(r => [r.parcel_id, r])).values()];
      const { error } = await db
        .from("properties")
        .upsert(deduped, { onConflict: "county_id,parcel_id" });
      if (error) {
        console.error(`  Insert error at line ${lineNum}: ${error.message.slice(0, 120)}`);
      } else {
        inserted += deduped.length;
      }
      batch = [];
      if (inserted % 5000 === 0) {
        process.stdout.write(`\r  Inserted: ${inserted.toLocaleString()}`);
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    const deduped = [...new Map(batch.map(r => [r.parcel_id, r])).values()];
    const { error } = await db
      .from("properties")
      .upsert(deduped, { onConflict: "county_id,parcel_id" });
    if (error) {
      console.error(`  Final batch error: ${error.message.slice(0, 120)}`);
    } else {
      inserted += deduped.length;
    }
  }

  console.log(`\n\n  Done: ${inserted.toLocaleString()} properties upserted for Geauga County, OH`);
  console.log(`  Skipped: ${skipped} (no parcel ID)`);
  console.log(`  Total lines: ${(lineNum - 1).toLocaleString()}`);

  // Sample records
  const { data: samples } = await db
    .from("properties")
    .select("parcel_id, owner_name, address, city, zip, assessed_value, year_built, total_sqft")
    .eq("county_id", county!.id)
    .limit(5);

  console.log("\n  Sample records:");
  for (const s of samples ?? []) {
    console.log(`    ${s.parcel_id} | ${s.owner_name ?? "(none)"} | ${s.address}, ${s.city} ${s.zip} | val=$${s.assessed_value != null ? s.assessed_value.toLocaleString() : "n/a"} | yr=${s.year_built ?? "n/a"} | sqft=${s.total_sqft ?? "n/a"}`);
  }
}

main().catch(console.error);
