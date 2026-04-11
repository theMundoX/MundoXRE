#!/usr/bin/env tsx
/**
 * Ingest Colorado Statewide Parcels from Colorado Public Parcels FeatureServer.
 * Paginated at 2000 records per request (no geometry).
 *
 * Source: https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0
 *
 * Usage:
 *   npx tsx scripts/ingest-co-parcels.ts
 *   npx tsx scripts/ingest-co-parcels.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0";
const OUT_FIELDS = "countyName,countyFips,parcel_id,account,situsAdd,sitAddCty,sitAddZip,owner,owner2,legalDesc,landSqft,landAcres,landUseCde,landUseDsc,saleDate,salePrice,apprValTot,asedValTot";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// Colorado county FIPS codes (state FIPS = 08)
const CO_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ALAMOSA": "003", "ARAPAHOE": "005", "ARCHULETA": "007",
  "BACA": "009", "BENT": "011", "BOULDER": "013", "BROOMFIELD": "014",
  "CHAFFEE": "015", "CHEYENNE": "017", "CLEAR CREEK": "019", "CONEJOS": "021",
  "COSTILLA": "023", "CROWLEY": "025", "CUSTER": "027", "DELTA": "029",
  "DENVER": "031", "DOLORES": "033", "DOUGLAS": "035", "EAGLE": "037",
  "EL PASO": "041", "ELBERT": "039", "FREMONT": "043", "GARFIELD": "045",
  "GILPIN": "047", "GRAND": "049", "GUNNISON": "051", "HINSDALE": "053",
  "HUERFANO": "055", "JACKSON": "057", "JEFFERSON": "059", "KIOWA": "061",
  "KIT CARSON": "063", "LA PLATA": "067", "LAKE": "065", "LARIMER": "069",
  "LAS ANIMAS": "071", "LINCOLN": "073", "LOGAN": "075", "MESA": "077",
  "MINERAL": "079", "MOFFAT": "081", "MONTEZUMA": "083", "MONTROSE": "085",
  "MORGAN": "087", "OTERO": "089", "OURAY": "091", "PARK": "093",
  "PHILLIPS": "095", "PITKIN": "097", "PROWERS": "099", "PUEBLO": "101",
  "RIO BLANCO": "103", "RIO GRANDE": "105", "ROUTT": "107", "SAGUACHE": "109",
  "SAN JUAN": "111", "SAN MIGUEL": "113", "SEDGWICK": "115", "SUMMIT": "117",
  "TELLER": "119", "WASHINGTON": "121", "WELD": "123", "YUMA": "125",
};

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

async function getOrCreateCounty(name: string, state: string, countyFips?: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const fips = countyFips || CO_COUNTY_FIPS[name.toUpperCase()] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "08", county_fips: fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

function classifyLandUse(code: string, desc: string): string {
  const d = (desc || "").toUpperCase();
  const c = (code || "").toUpperCase();
  if (d.includes("RESIDENTIAL") || d.includes("SINGLE FAM") || c.startsWith("R")) return "residential";
  if (d.includes("COMMERCIAL") || c.startsWith("C")) return "commercial";
  if (d.includes("INDUSTRIAL") || c.startsWith("I")) return "industrial";
  if (d.includes("AGRICULTUR") || d.includes("FARM") || c.startsWith("A")) return "agricultural";
  if (d.includes("VACANT") || c.startsWith("V")) return "vacant";
  if (d.includes("EXEMPT") || d.includes("GOVERNMENT") || c.startsWith("E")) return "exempt";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const url = `${SERVICE_URL}/query?where=1%3D1&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(60000) });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) ?? [];
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(2000 * Math.pow(2, attempt - 1), 30000);
      console.log(`  Retry ${attempt}/${MAX_RETRIES} after ${delay}ms: ${err.message}`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Colorado Statewide Parcels");
  console.log("Source:", SERVICE_URL);
  console.log();

  const countResp = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const countyName = r.countyName;
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(titleCase(countyName.trim()), "CO", r.countyFips);
      } catch {
        totalErrors++;
        continue;
      }

      const lotSqft = r.landSqft && r.landSqft > 0 ? r.landSqft :
        (r.landAcres && r.landAcres > 0 ? Math.round(r.landAcres * 43560) : null);

      rows.push({
        county_id: countyId,
        parcel_id: r.parcel_id || r.account || "",
        address: r.situsAdd || "",
        city: r.sitAddCty || "",
        state_code: "CO",
        zip: r.sitAddZip || "",
        owner_name: [r.owner, r.owner2].filter(Boolean).join("; "),
        assessed_value: r.asedValTot && r.asedValTot > 0 ? r.asedValTot : null,
        market_value: r.apprValTot && r.apprValTot > 0 ? r.apprValTot : null,
        land_sqft: lotSqft,
        property_type: classifyLandUse(r.landUseCde, r.landUseDsc),
        last_sale_price: r.salePrice && r.salePrice > 0 ? r.salePrice : null,
        source: "co-gis-parcels",
      });
    }

    // Dedup within page
    const seen = new Map<string, any>();
    for (const row of rows) {
      seen.set(`${row.county_id}|${row.parcel_id}`, row);
    }
    const dedupedRows = Array.from(seen.values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      if (batch.length === 0) continue;
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) {
        console.error(`  DB error at offset ${offset}+${i}: ${error.message}`);
        totalErrors += batch.length;
      } else {
        totalInserted += batch.length;
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000);
    const pct = ((offset + records.length) / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsed}s] offset=${offset} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min`);

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone! Inserted ${totalInserted.toLocaleString()} records in ${totalElapsed} minutes. Errors: ${totalErrors}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
