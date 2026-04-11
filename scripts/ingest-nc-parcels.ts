#!/usr/bin/env tsx
/**
 * Ingest North Carolina statewide parcels from NC OneMap FeatureServer.
 * ~5.9M parcels, paginated at 5000 records per request.
 * Source: https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL = "https://services.nconemap.gov/secure/rest/services/NC1Map_Parcels/FeatureServer/0/query";
const PAGE_SIZE = 5000;
const DB_BATCH = 500;
const OUT_FIELDS = "parno,altparno,ownname,ownname2,improvval,landval,parval,siteadd,scity,szip,sstate,gisacres,parusedesc,parusecode,cntyname,cntyfips,stfips,structyear,structno,presentval";

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, state: string, stateFips: string, countyFips: string): Promise<number> {
  const key = `${name}|${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }
  const { data: created, error } = await db.from("counties").insert({
    county_name: name, state_code: state, state_fips: stateFips || "00",
    county_fips: countyFips || "000", active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  countyCache.set(key, created.id);
  return created.id;
}

function classifyPropertyType(useCode: string, useDesc: string): string {
  const code = (useCode || "").toUpperCase().trim();
  const desc = (useDesc || "").toUpperCase().trim();
  if (desc.includes("RESIDENTIAL") || desc.includes("SINGLE FAM") || code === "R") return "residential";
  if (desc.includes("COMMERCIAL") || code === "C") return "commercial";
  if (desc.includes("INDUSTRIAL") || code === "I") return "industrial";
  if (desc.includes("AGRICULTUR") || desc.includes("FARM") || code === "A") return "agricultural";
  if (desc.includes("APARTMENT") || desc.includes("MULTI") || code === "M") return "apartment";
  if (desc.includes("VACANT") || code === "V") return "vacant";
  if (desc.includes("EXEMPT") || code === "E") return "exempt";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}?${params}`, { signal: AbortSignal.timeout(60000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      return json.features?.map((f: any) => f.attributes) || [];
    } catch (err: any) {
      if (attempt === 2) throw err;
      console.error(`  Retry ${attempt + 1}: ${err.message}`);
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest NC Statewide Parcels (NC OneMap)\n");

  // Get total count
  const countRes = await fetch(`${BASE_URL}?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countRes.json();
  console.log(`Total parcels available: ${totalCount?.toLocaleString()}\n`);

  // Check if we should resume
  const startOffset = parseInt(process.argv[2] || "0");
  if (startOffset > 0) console.log(`Resuming from offset ${startOffset.toLocaleString()}\n`);

  let inserted = 0, skipped = 0, errors = 0, dbErrors = 0;
  let offset = startOffset;

  while (true) {
    const features = await fetchPage(offset);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const p of features) {
      const countyName = (p.cntyname || "").trim();
      const parcelId = (p.parno || p.altparno || "").trim();
      const address = (p.siteadd || "").trim();

      if (!countyName) { skipped++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "NC", p.stfips || "37", p.cntyfips || "000");
      } catch {
        skipped++;
        continue;
      }

      const totalVal = p.parval || p.presentval || null;
      const yearBuilt = p.structyear && p.structyear > 1700 && p.structyear < 2030 ? p.structyear : null;

      batch.push({
        county_id: countyId,
        parcel_id: parcelId || "",
        address: address || "",
        city: (p.scity || "").trim(),
        state_code: "NC",
        zip: (p.szip || "").trim(),
        owner_name: (p.ownname || "").trim(),
        assessed_value: totalVal && totalVal > 0 ? totalVal : null,
        year_built: yearBuilt,
        total_sqft: null,
        total_units: p.structno && p.structno > 0 ? p.structno : 1,
        property_type: classifyPropertyType(p.parusecode, p.parusedesc),
        source: "nc-onemap-parcels",
      });

      if (batch.length >= DB_BATCH) {
        const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
        if (error) {
          dbErrors++;
          if (dbErrors <= 5) console.error(`\n  DB error: ${error.message.slice(0, 120)}`);
        } else {
          inserted += batch.length;
        }
        batch.length = 0;
      }
    }

    // Flush remaining
    if (batch.length > 0) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
      if (error) dbErrors++;
      else inserted += batch.length;
    }

    offset += features.length;
    const pct = totalCount ? ((offset / totalCount) * 100).toFixed(1) : "?";
    process.stdout.write(`\r  Offset: ${offset.toLocaleString()} / ${totalCount?.toLocaleString()} (${pct}%) | Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped} | Errors: ${dbErrors}   `);

    // Small delay to be polite to the server
    if (offset % 50000 === 0) await new Promise(r => setTimeout(r, 500));
  }

  const { count } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`\n\n══════════════════════════════════════════════`);
  console.log(`  NC parcels inserted: ${inserted.toLocaleString()}`);
  console.log(`  Skipped: ${skipped} | DB errors: ${dbErrors}`);
  console.log(`  Total properties in DB: ${count?.toLocaleString()}`);
  console.log(`══════════════════════════════════════════════\n`);
}

main().catch(console.error);
