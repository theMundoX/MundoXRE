#!/usr/bin/env tsx
/**
 * Ingest Illinois Parcels — Cook County CCAO Parcel Universe (Socrata API)
 * ~1.86M parcels for 2023, paginated via Socrata $offset/$limit.
 *
 * Source: https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json
 * Dataset: Cook County Assessor - Parcel Universe (2001-2023)
 *
 * Usage:
 *   npx tsx scripts/ingest-il-parcels.ts
 *   npx tsx scripts/ingest-il-parcels.ts --offset=500000   # resume
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SOCRATA_URL = "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json";
const YEAR = "2023";
const PAGE_SIZE = 5000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const IL_CLASS_TO_TYPE: Record<string, string> = {
  "0": "exempt",      // Exempt
  "1": "vacant",      // Vacant land
  "2": "residential", // Residential
  "3": "commercial",  // Commercial
  "4": "commercial",  // Industrial
  "5": "residential", // Railroad
};

function classifyByClass(classCode: string): string {
  const prefix = (classCode || "").substring(0, 1);
  const cls = parseInt(classCode || "0", 10);
  if (cls >= 200 && cls < 300) return "residential";
  if (cls >= 300 && cls < 400) return "commercial";
  if (cls >= 400 && cls < 500) return "industrial";
  if (cls >= 500 && cls < 600) return "residential"; // condos/co-ops
  if (cls >= 600 && cls < 700) return "residential"; // 6+ unit apts
  if (cls >= 700 && cls < 800) return "commercial";
  if (cls === 100) return "vacant";
  return IL_CLASS_TO_TYPE[prefix] || "other";
}

async function getOrCreateCounty(name: string): Promise<number> {
  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", name)
    .eq("state_code", "IL")
    .single();
  if (data) return data.id;

  const { data: created, error } = await db.from("counties").insert({
    county_name: name,
    state_code: "IL",
    state_fips: "17",
    county_fips: "031", // Cook County FIPS
    active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  return created.id;
}

async function fetchPage(offset: number): Promise<any[]> {
  const params = new URLSearchParams({
    $where: `year='${YEAR}'`,
    $limit: String(PAGE_SIZE),
    $offset: String(offset),
    $select: "pin,class,prop_address_full,prop_address_city_name,prop_address_state,prop_address_zipcode_1,mail_address_name,lat,lon",
  });
  const url = `${SOCRATA_URL}?${params}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(120000) });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text().then(t => t.slice(0, 200))}`);
      return await resp.json();
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(3000 * Math.pow(2, attempt - 1), 30000);
      console.error(`  Retry ${attempt}/${MAX_RETRIES} at offset ${offset}: ${err.message}`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Illinois Parcels (Cook County CCAO)");
  console.log(`Source: ${SOCRATA_URL}`);
  console.log(`Year: ${YEAR}\n`);

  // Get total count (use known value for 2023 to avoid slow count query)
  // Verified via: curl "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.csv?$select=count(*)&$where=year='2023'"
  const KNOWN_COUNT_2023 = 1865515;
  let totalCount = KNOWN_COUNT_2023;
  try {
    const countResp = await fetch(
      `https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.csv?%24select=count%28%2A%29&%24where=year%3D%27${YEAR}%27`,
      { signal: AbortSignal.timeout(60000) }
    );
    const countText = await countResp.text();
    const parsed = parseInt(countText.split("\n")[1]?.replace(/"/g, "") || "0");
    if (parsed > 0) totalCount = parsed;
  } catch {
    console.log(`  Count query timed out, using known count: ${totalCount.toLocaleString()}`);
  }
  console.log(`Total parcels for ${YEAR}: ${totalCount.toLocaleString()}\n`);

  // Create Cook County record
  const cookCountyId = await getOrCreateCounty("Cook");
  console.log(`Cook County ID: ${cookCountyId}`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;
  if (offset > 0) console.log(`Resuming from offset ${offset.toLocaleString()}`);

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) break;

    const rows: any[] = [];
    for (const r of records) {
      const pin = (r.pin || "").trim();
      if (!pin) continue;

      const address = (r.prop_address_full || "").trim();
      const city = (r.prop_address_city_name || "").trim().toUpperCase();
      const zip = (r.prop_address_zipcode_1 || "").trim().substring(0, 5);
      const ownerName = (r.mail_address_name || "").trim();
      const classCode = (r.class || "").trim();

      let lat: number | null = null;
      let lng: number | null = null;
      if (r.lat && r.lon) {
        lat = parseFloat(r.lat);
        lng = parseFloat(r.lon);
        if (isNaN(lat) || isNaN(lng)) { lat = null; lng = null; }
      }

      rows.push({
        county_id: cookCountyId,
        parcel_id: pin,
        address: address.toUpperCase(),
        city,
        state_code: "IL",
        zip: zip || "00000",
        owner_name: ownerName,
        property_type: classifyByClass(classCode),
        lat,
        lng,
        source: "ccao-parcel-universe",
      });
    }

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(batch, {
        onConflict: "county_id,parcel_id",
        ignoreDuplicates: true,
      });
      if (error) {
        console.error(`  DB error at offset ${offset}+${i}: ${error.message.slice(0, 120)}`);
        totalErrors += batch.length;
      } else {
        totalInserted += batch.length;
      }
    }

    offset += records.length;
    const pct = ((offset / totalCount) * 100).toFixed(1);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = (totalInserted / (parseFloat(elapsed) || 1)).toFixed(0);
    const eta = rate > 0 ? ((totalCount - offset) / parseFloat(rate) / 60).toFixed(0) : "?";
    process.stdout.write(
      `\r  [IL] ${offset.toLocaleString()}/${totalCount.toLocaleString()} (${pct}%) | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate}/s | ETA ${eta}min   `
    );

    // Be polite
    if (offset % 100000 === 0) await new Promise(r => setTimeout(r, 500));
  }

  const { count: finalCount } = await db.from("properties").select("*", { count: "exact", head: true });
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  console.log(`\n\n══════════════════════════════════════════════`);
  console.log(`  IL (Cook County) parcels inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  Errors: ${totalErrors}`);
  console.log(`  Total properties in DB: ${finalCount?.toLocaleString()}`);
  console.log(`  Time: ${elapsed} min`);
  console.log(`══════════════════════════════════════════════\n`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
