#!/usr/bin/env tsx
/**
 * Ingest Arkansas Statewide Parcels from AGISO Planning_Cadastre FeatureServer
 * ~2.1M parcels, including Saline County (target for lien records).
 *
 * Source: https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/0
 * Layer 0: PARCEL_CENTROID_CAMP (has all attributes, no geometry needed)
 *
 * Usage:
 *   npx tsx scripts/ingest-ar-parcels.ts
 *   npx tsx scripts/ingest-ar-parcels.ts --offset=500000   # resume
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gis.arkansas.gov/arcgis/rest/services/FEATURESERVICES/Planning_Cadastre/FeatureServer/0";
const OUT_FIELDS = "objectid,county,countyfips,parcelid,adrlabel,adrnum,predir,pstrnam,pstrtype,adrcity,adrzip5,ownername,assessvalue,totalvalue,landvalue,parceltype,taxarea";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, countyFips: string): Promise<number> {
  const key = name.toLowerCase().trim();
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", "AR").single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const fips3 = (countyFips || "000").padStart(5, "0").slice(2);
  const { data: created, error } = await db.from("counties").insert({
    county_name: name,
    state_code: "AR",
    state_fips: "05",
    county_fips: fips3,
    active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  countyCache.set(key, created.id);
  return created.id;
}

function titleCase(s: string): string {
  return (s || "").toLowerCase().replace(/\b\w/g, c => c.toUpperCase()).trim();
}

function classifyARParcel(parcelType: string): string {
  const pt = (parcelType || "").toUpperCase();
  if (pt === "AV") return "vacant";       // Vacant
  if (pt === "AI") return "residential";  // Improved agricultural
  if (pt === "RI") return "residential";  // Residential improved
  if (pt === "CI") return "commercial";   // Commercial improved
  if (pt === "II") return "industrial";   // Industrial improved
  if (pt === "EX") return "exempt";       // Exempt
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    returnGeometry: "false",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });
  const url = `${SERVICE_URL}/query?${params}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(90000) });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) ?? [];
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
  console.log("MXRE — Ingest Arkansas Statewide Parcels (AGISO)");
  console.log(`Source: ${SERVICE_URL}\n`);

  const countResp = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`, {
    signal: AbortSignal.timeout(30000),
  });
  const { count: totalCount } = await countResp.json();
  console.log(`Total AR parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;
  if (offset > 0) console.log(`Resuming from offset ${offset.toLocaleString()}`);

  let totalInserted = 0;
  let totalErrors = 0;
  let totalSkipped = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);
    if (records.length === 0) {
      console.log(`No records at offset ${offset}, stopping.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const countyName = titleCase(r.county || "");
      if (!countyName) { totalSkipped++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, r.countyfips || "05000");
      } catch {
        totalSkipped++;
        continue;
      }

      const parcelId = (r.parcelid || "").trim();
      const address = (r.adrlabel || "").trim().toUpperCase();
      const city = titleCase(r.adrcity || "").toUpperCase();
      const zip = r.adrzip5 && r.adrzip5 > 0 ? String(r.adrzip5).padStart(5, "0") : "00000";
      const ownerName = titleCase(r.ownername || "");

      const totalVal = r.totalvalue && r.totalvalue > 0 ? Math.round(r.totalvalue) : null;
      const landVal = r.landvalue && r.landvalue > 0 ? Math.round(r.landvalue) : null;
      const acreage = r.taxarea && r.taxarea > 0 ? r.taxarea : null;

      rows.push({
        county_id: countyId,
        parcel_id: parcelId || "",
        address,
        city,
        state_code: "AR",
        zip,
        owner_name: ownerName,
        assessed_value: totalVal,
        land_value: landVal,
        land_sqft: acreage ? Math.round(acreage * 43560) : null,
        property_type: classifyARParcel(r.parceltype),
        source: "agiso-planning-cadastre",
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
      `\r  [AR] ${offset.toLocaleString()}/${totalCount.toLocaleString()} (${pct}%) | inserted=${totalInserted.toLocaleString()} | skipped=${totalSkipped} | errors=${totalErrors} | ${rate}/s | ETA ${eta}min   `
    );
  }

  const { count: finalCount } = await db.from("properties").select("*", { count: "exact", head: true });
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  console.log(`\n\n══════════════════════════════════════════════`);
  console.log(`  AR parcels inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  Skipped: ${totalSkipped} | Errors: ${totalErrors}`);
  console.log(`  Total properties in DB: ${finalCount?.toLocaleString()}`);
  console.log(`  Time: ${elapsed} min`);
  console.log(`══════════════════════════════════════════════\n`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
