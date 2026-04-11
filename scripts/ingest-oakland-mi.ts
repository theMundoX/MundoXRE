#!/usr/bin/env tsx
/**
 * Ingest Oakland County MI property data from CSV.
 * Source: data/oakland-mi-parcels.csv (~10K rows)
 * State FIPS: 26, County FIPS: 125
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
  return isNaN(n) || n === 0 ? null : n;
}

async function main() {
  console.log("MXRE — Ingest Oakland County MI Properties\n");

  // Ensure county record exists
  let { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Oakland")
    .eq("state_code", "MI")
    .single();

  if (!county) {
    const { data: newCounty, error } = await db
      .from("counties")
      .insert({
        county_name: "Oakland",
        state_code: "MI",
        state_fips: "26",
        county_fips: "125",
        active: true,
      })
      .select("id")
      .single();
    if (error) {
      console.error("County insert error:", error.message);
      return;
    }
    county = newCounty;
  }
  console.log(`  County ID: ${county!.id}`);

  // Read CSV
  const rl = createInterface({
    input: createReadStream("data/oakland-mi-parcels.csv", "utf-8"),
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
      console.log(`  Headers (${headers.length}): ${headers.join(", ")}`);
      continue;
    }

    const fields = parseCSVLine(line);
    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]] = fields[i] || "";
    }

    const parcelId = row["KEYPIN"];
    if (!parcelId) {
      skipped++;
      continue;
    }

    const assessedValue = parseNumber(row["ASSESSEDVALUE"]);
    const livingArea = parseInt2(row["LIVING_AREA_SQFT"]);

    const address = row["SITEADDRESS"] || "";
    const city = row["SITECITY"] || "";
    if (!address && !city) {
      skipped++;
      continue;
    }

    const record: Record<string, unknown> = {
      county_id: county!.id,
      parcel_id: parcelId,
      owner_name: row["NAME1"] || null,
      address: address || "UNKNOWN",
      city: city || "UNKNOWN",
      state_code: "MI",
      zip: row["SITEZIP5"] || null,
      assessed_value: assessedValue,
      property_type: row["STRUCTURE_DESC"] || row["CLASSCODE"] || null,
      total_sqft: livingArea,
      total_units: 1,
    };

    batch.push(record);
    if (batch.length >= BATCH_SIZE) {
      const { error } = await db
        .from("properties")
        .upsert(batch, { onConflict: "county_id,parcel_id" });
      if (error) {
        console.error(`  Insert error at line ${lineNum}: ${error.message.slice(0, 120)}`);
      } else {
        inserted += batch.length;
      }
      batch = [];
      if (inserted % 2000 === 0) {
        process.stdout.write(`\r  Inserted: ${inserted.toLocaleString()}`);
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    const { error } = await db
      .from("properties")
      .upsert(batch, { onConflict: "county_id,parcel_id" });
    if (error) {
      console.error(`  Final batch error: ${error.message.slice(0, 120)}`);
    } else {
      inserted += batch.length;
    }
  }

  console.log(`\n\n  Done: ${inserted.toLocaleString()} properties upserted for Oakland County, MI`);
  console.log(`  Skipped: ${skipped} (no parcel ID)`);
  console.log(`  Total lines: ${(lineNum - 1).toLocaleString()}`);

  // Sample records
  const { data: samples } = await db
    .from("properties")
    .select("parcel_id, owner_name, address, city, zip, assessed_value, total_sqft, property_type")
    .eq("county_id", county!.id)
    .limit(5);

  console.log("\n  Sample records:");
  for (const s of samples ?? []) {
    console.log(`    ${s.parcel_id} | ${s.owner_name ?? "(none)"} | ${s.address}, ${s.city} ${s.zip} | val=$${s.assessed_value?.toLocaleString()} | sqft=${s.total_sqft} | type=${s.property_type}`);
  }
}

main().catch(console.error);
