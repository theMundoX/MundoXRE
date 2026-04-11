/**
 * Black Hawk County IA ingest - resilient version with retry logic and pool-friendly batching.
 */
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";
import { resolve } from "path";

// Manual dotenv loading
const envPath = resolve(new URL(".", import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1'), "../.env");
try {
  const lines = readFileSync(envPath, "utf8").split("\n");
  for (const line of lines) {
    const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (m) process.env[m[1]] = m[2].trim();
  }
} catch {}

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const STATEWIDE_URL =
  "https://services3.arcgis.com/kd9gaiUExYqUbnoq/arcgis/rest/services/Iowa_Parcels_2017/FeatureServer/0/query";

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function queryArcGIS(offset, limit, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
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
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      return data.features || [];
    } catch (e) {
      if (attempt < retries - 1) {
        await sleep(2000 * (attempt + 1));
      } else {
        console.error(`\n  ArcGIS fetch failed at offset ${offset}: ${e.message}`);
        return [];
      }
    }
  }
}

async function insertWithRetry(rows, retries = 5) {
  for (let attempt = 0; attempt < retries; attempt++) {
    const { error } = await db.from("properties").insert(rows);
    if (!error) return { inserted: rows.length, duplicates: 0, errors: 0 };
    if (error.code === "23505" || error.message.includes("duplicate")) {
      // Split in half and recurse
      if (rows.length === 1) return { inserted: 0, duplicates: 1, errors: 0 };
      const mid = Math.floor(rows.length / 2);
      const a = await insertWithRetry(rows.slice(0, mid));
      const b = await insertWithRetry(rows.slice(mid));
      return {
        inserted: a.inserted + b.inserted,
        duplicates: a.duplicates + b.duplicates,
        errors: a.errors + b.errors,
      };
    }
    if (error.message.includes("Timed out") || error.message.includes("connection pool")) {
      const wait = 3000 * Math.pow(2, attempt);
      process.stdout.write(`\n  [pool timeout, waiting ${wait / 1000}s...]`);
      await sleep(wait);
      continue;
    }
    // Other error
    return { inserted: 0, duplicates: 0, errors: rows.length };
  }
  return { inserted: 0, duplicates: 0, errors: rows.length };
}

function classifyProperty(parcelClass) {
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
  console.log("MXRE — Ingest Black Hawk County IA (resilient)\n");

  // Get county ID
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Black Hawk")
    .eq("state_code", "IA")
    .single();
  if (!county) { console.error("County not found"); process.exit(1); }
  const countyId = county.id;
  console.log("  County ID:", countyId);

  // Get existing count and find start offset to resume
  const { count: existing } = await db
    .from("properties")
    .select("*", { count: "exact", head: true })
    .eq("county_id", countyId);
  console.log("  Existing properties:", existing || 0);

  const FETCH_SIZE = 1000;
  const SUB_BATCH = 50; // Small batches to be pool-friendly
  let offset = 0;
  let inserted = 0, skipped = 0, duplicates = 0, errors = 0;

  while (true) {
    const features = await queryArcGIS(offset, FETCH_SIZE);
    if (features.length === 0) break;

    const batch = [];
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

    for (let i = 0; i < batch.length; i += SUB_BATCH) {
      const chunk = batch.slice(i, i + SUB_BATCH);
      const result = await insertWithRetry(chunk);
      inserted += result.inserted;
      duplicates += result.duplicates;
      errors += result.errors;
      // Small delay between sub-batches to avoid pool exhaustion
      await sleep(100);
    }

    offset += features.length;
    process.stdout.write(
      `\r  Progress: ${offset.toLocaleString()} fetched | ${inserted.toLocaleString()} inserted | ${duplicates} dups | ${skipped} skipped | ${errors} errors`
    );

    if (features.length < FETCH_SIZE) break;

    // Brief pause between page fetches
    await sleep(200);
  }

  console.log(`\n\n  Done.`);
  const { count: total } = await db
    .from("properties")
    .select("*", { count: "exact", head: true })
    .eq("county_id", countyId);
  console.log(`  Black Hawk County IA final count: ${total?.toLocaleString()}`);
}

main().catch(console.error);
