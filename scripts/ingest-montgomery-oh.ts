#!/usr/bin/env tsx
/**
 * Ingest Montgomery County OH (Dayton) property data.
 * Endpoint: https://services.arcgis.com/OYwao4bWJR5ergop/arcgis/rest/services/MCEO_TaxParcelQuery/FeatureServer/0
 * ~287,750 records — has address/owner but no appraisal values
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://services.arcgis.com/OYwao4bWJR5ergop/arcgis/rest/services/MCEO_TaxParcelQuery/FeatureServer/0";
const PAGE_SIZE = 1000;

const FIELDS = [
  "TAXPINNO","PARLOC","OWNER_NAME1","OWNER_NAME2",
  "OWNER_ADDR1","OWNER_ADDR2","OWNER_ADDR3",
  "NAME","ACREAGE","LOTNUMBER"
].join(",");

async function fetchPage(offset: number): Promise<Record<string, unknown>[]> {
  const url = `${ARCGIS_URL}/query?where=1%3D1&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json() as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));
      return ((json.features as Array<{ attributes: Record<string, unknown> }>) ?? []).map(f => f.attributes);
    } catch (err) {
      if (attempt === 4) throw err;
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Montgomery County OH Properties\n");
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let { data: county } = await db.from("counties").select("id").eq("county_name", "Montgomery").eq("state_code", "OH").single();
  if (!county) {
    const { data: c, error } = await db.from("counties").insert({ county_name: "Montgomery", state_code: "OH", state_fips: "39", county_fips: "113", active: true }).select("id").single();
    if (error) {
      console.error("County insert error (may already exist):", error.message);
      // Re-fetch in case of duplicate key (county inserted by a prior run)
      const { data: existing } = await db.from("counties").select("id").eq("county_name", "Montgomery").eq("state_code", "OH").single();
      if (!existing) { console.error("Could not find or create Montgomery county"); return; }
      county = existing;
    } else {
      county = c;
    }
  }
  console.log(`  County ID: ${county!.id} | Starting offset: ${offset}`);

  // Parse city/zip from OWNER_ADDR3 like "DAYTON, OH 45458" or PARLOC
  function parseCityZipFromAddr3(addr3: string | null): { city: string; zip: string } {
    const s = String(addr3 ?? "").trim().toUpperCase();
    const m = s.match(/^(.+?),?\s+OH\s+(\d{5})/);
    if (m) return { city: m[1].trim(), zip: m[2] };
    return { city: "", zip: "" };
  }

  let inserted = 0, emptyPages = 0;
  while (true) {
    const rows = await fetchPage(offset);
    if (rows.length === 0) { if (++emptyPages >= 3) break; offset += PAGE_SIZE; continue; }
    emptyPages = 0;

    const batch = rows.map(r => {
      const pin = String(r.TAXPINNO ?? "").trim().replace(/\s+/g, "");
      const address = String(r.PARLOC ?? "").trim().toUpperCase();
      const { city, zip } = parseCityZipFromAddr3(r.OWNER_ADDR3 as string);

      return {
        county_id: county!.id,
        parcel_id: pin,
        address,
        city,
        state_code: "OH",
        zip,
        owner_name: String(r.OWNER_NAME1 ?? "").trim(),
        property_type: "residential" as string,
        land_sqft: (() => {
          const acres = parseFloat(String(r.ACREAGE ?? ""));
          return acres > 0 ? Math.round(acres * 43560) : null;
        })(),
        source: "montgomery-oh-mceo",
      };
    }).filter(r => r.parcel_id);

    const seen = new Map<string, Record<string, unknown>>();
    for (const rec of batch) seen.set(rec.parcel_id, rec);
    const deduped = Array.from(seen.values());

    if (deduped.length > 0) {
      const { error } = await db.from("properties").upsert(deduped, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) console.error(`\n  Upsert error at ${offset}: ${error.message.slice(0, 100)}`);
      else inserted += deduped.length;
    }
    offset += PAGE_SIZE;
    process.stdout.write(`\r  Upserted: ${inserted.toLocaleString()} | Offset: ${offset.toLocaleString()}    `);
    if (rows.length < PAGE_SIZE) break;
  }

  console.log(`\n  Done: ${inserted.toLocaleString()} properties for Montgomery County, OH`);
  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Montgomery County total in DB: ${count?.toLocaleString()}`);
}
main().catch(console.error);
