#!/usr/bin/env tsx
/**
 * Download full Oakland County MI parcel data from ArcGIS feature server.
 * The CSV download was capped at 10K, so we paginate via the API.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { writeFileSync } from "node:fs";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Find the feature server URL from the ArcGIS portal
const BASE_URL = "https://gisservices.oakgov.com/arcgis/rest/services/Enterprise/EnterpriseOpenParcelDataMapService/MapServer/1";

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created } = await db.from("counties").insert({ county_name: name, state_code: state, active: true }).select("id").single();
  return created!.id;
}

async function main() {
  console.log("MXRE — Download & Ingest Oakland County MI (Full ArcGIS)\n");

  // Check total count
  try {
    const countResp = await fetch(`${BASE_URL}/query?where=1=1&returnCountOnly=true&f=json`);
    const countData = await countResp.json();
    console.log(`Total features: ${countData.count || "unknown"}`);
  } catch { console.log("Could not get total count, will paginate until empty"); }

  const countyId = await getOrCreateCounty("Oakland", "MI");
  console.log("County ID:", countyId);

  const { count: existing } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log(`Existing properties: ${existing} (keeping, will upsert)`);
  // Skip delete — use upsert to add missing records

  // Paginate through the feature server
  let offset = 0;
  const PAGE_SIZE = 2000;
  let totalInserted = 0;
  let totalErrors = 0;

  while (true) {
    const queryUrl = `${BASE_URL}/query?where=1=1&outFields=KEYPIN,NAME1,NAME2,SITEADDRESS,SITECITY,SITESTATE,SITEZIP5,ASSESSEDVALUE,TAXABLEVALUE,NUM_BEDS,NUM_BATHS,STRUCTURE_DESC,LIVING_AREA_SQFT,CLASSCODE&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

    const resp = await fetch(queryUrl);
    if (!resp.ok) {
      console.error(`Fetch error: ${resp.status}`);
      break;
    }

    const data = await resp.json();
    const features = data.features || [];
    if (features.length === 0) break;

    const batch: any[] = [];
    for (const f of features) {
      const a = f.attributes;
      const address = (a.SITEADDRESS || "").trim();
      const city = (a.SITECITY || "").trim();

      if (!address && !a.KEYPIN) continue;

      // Classify from CLASSCODE
      const classCode = (a.CLASSCODE || "").toString();
      let propertyType = "residential";
      if (classCode.startsWith("2") || classCode.startsWith("3")) propertyType = "commercial";
      else if (classCode.startsWith("4")) propertyType = "industrial";

      batch.push({
        county_id: countyId,
        parcel_id: (a.KEYPIN || "").toString().trim() || "",
        address: address || "",
        city: city || "",
        state_code: "MI",
        zip: (a.SITEZIP5 || "").toString().trim() || "",
        owner_name: (a.NAME1 || "").trim() || "",
        assessed_value: a.ASSESSEDVALUE > 0 ? a.ASSESSEDVALUE : null,
        taxable_value: a.TAXABLEVALUE > 0 ? a.TAXABLEVALUE : null,
        total_sqft: a.LIVING_AREA_SQFT > 0 ? a.LIVING_AREA_SQFT : null,
        total_units: 1,
        property_type: propertyType,
        source: "oakland-mi-arcgis",
      });
    }

    if (batch.length > 0) {
      // Insert in smaller chunks to avoid timeouts
      for (let i = 0; i < batch.length; i += 200) {
        const chunk = batch.slice(i, i + 200);
        const { error } = await db.from("properties").insert(chunk);
        if (error) {
          totalErrors++;
          if (totalErrors <= 5) console.error(`  Insert error: ${error.message.slice(0, 80)}`);
        } else {
          totalInserted += chunk.length;
        }
      }
    }

    offset += features.length;
    process.stdout.write(`\r  Downloaded: ${offset.toLocaleString()} | Inserted: ${totalInserted.toLocaleString()} | Errors: ${totalErrors}`);

    if (!data.exceededTransferLimit && features.length < PAGE_SIZE) break;
  }

  console.log(`\n\nDone: ${totalInserted.toLocaleString()} inserted | ${totalErrors} errors`);
  const { count: total } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log(`Oakland County MI now has ${total?.toLocaleString()} properties`);
}

main().catch(console.error);
