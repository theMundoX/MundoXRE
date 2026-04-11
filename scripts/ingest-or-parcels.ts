#!/usr/bin/env tsx
/**
 * Ingest Oregon Parcels from multiple public sources:
 *   - Oregon Metro RLIS Taxlots (Public): Multnomah, Clackamas, Washington counties
 *     https://services2.arcgis.com/McQ0OlIABe29rJJy/arcgis/rest/services/Taxlots_%28Public%29/FeatureServer/3
 *   - Marion County Parcels FeatureServer
 *     https://services3.arcgis.com/SXXjryU22GsO8OEC/arcgis/rest/services/Parcels/FeatureServer/0
 *
 * Note: The original ODF TaxlotsDisplay MapServer only supports Map (tile) capability,
 * not Query operations, so this script uses accessible ArcGIS Online FeatureServers instead.
 *
 * Usage:
 *   npx tsx scripts/ingest-or-parcels.ts
 *   npx tsx scripts/ingest-or-parcels.ts --source=metro   # Metro RLIS only
 *   npx tsx scripts/ingest-or-parcels.ts --source=marion  # Marion County only
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const METRO_URL = "https://services2.arcgis.com/McQ0OlIABe29rJJy/arcgis/rest/services/Taxlots_%28Public%29/FeatureServer/3";
const MARION_URL = "https://services3.arcgis.com/SXXjryU22GsO8OEC/arcgis/rest/services/Parcels/FeatureServer/0";

const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// Oregon county FIPS codes (state FIPS = 41)
const OR_COUNTY_FIPS: Record<string, string> = {
  "Baker": "001", "Benton": "003", "Clackamas": "005", "Clatsop": "007",
  "Columbia": "009", "Coos": "011", "Crook": "013", "Curry": "015",
  "Deschutes": "017", "Douglas": "019", "Gilliam": "021", "Grant": "023",
  "Harney": "025", "Hood River": "027", "Jackson": "029", "Jefferson": "031",
  "Josephine": "033", "Klamath": "035", "Lake": "037", "Lane": "039",
  "Lincoln": "041", "Linn": "043", "Malheur": "045", "Marion": "047",
  "Morrow": "049", "Multnomah": "051", "Polk": "053", "Sherman": "055",
  "Tillamook": "057", "Umatilla": "059", "Union": "061", "Wallowa": "063",
  "Wasco": "065", "Washington": "067", "Wheeler": "069", "Yamhill": "071",
};

// Metro RLIS county code mapping
const METRO_COUNTY_MAP: Record<string, string> = {
  "C": "Clackamas",
  "M": "Multnomah",
  "W": "Washington",
};

async function getOrCreateCounty(name: string): Promise<number> {
  const key = `${name}_OR`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", "OR").single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const fips = OR_COUNTY_FIPS[name] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: "OR", state_fips: "41", county_fips: fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

function classifyPropCode(code: string, desc?: string): string {
  const c = (code || "").toUpperCase().trim();
  const d = (desc || "").toUpperCase();
  if (d.includes("RESIDENTIAL") || c === "R" || c === "101" || c.startsWith("1")) return "residential";
  if (d.includes("COMMERCIAL") || c === "C" || c.startsWith("2")) return "commercial";
  if (d.includes("INDUSTRIAL") || c === "I" || c.startsWith("3")) return "industrial";
  if (d.includes("FARM") || d.includes("AGRICULTUR") || c === "F" || c === "A") return "agricultural";
  if (d.includes("MULTI") || d.includes("APARTMENT")) return "apartment";
  if (d.includes("VACANT") || d.includes("UNDEVELOPED")) return "vacant";
  if (d.includes("EXEMPT")) return "exempt";
  return "other";
}

async function postQuery(url: string, params: Record<string, string>): Promise<any[]> {
  const body = new URLSearchParams(params);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(`${url}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
        signal: AbortSignal.timeout(60000),
      });
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

async function getCount(url: string): Promise<number> {
  const body = new URLSearchParams({ where: "1=1", returnCountOnly: "true", f: "json" });
  const resp = await fetch(`${url}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
    signal: AbortSignal.timeout(30000),
  });
  const json = await resp.json();
  if (json.error) throw new Error(json.error.message);
  return json.count as number;
}

async function upsertBatch(rows: any[]): Promise<{ inserted: number; errors: number }> {
  let totalInserted = 0;
  let totalErrors = 0;

  // Dedup within batch
  const seen = new Map<string, any>();
  for (const row of rows) {
    seen.set(`${row.county_id}|${row.parcel_id}`, row);
  }
  const dedupedRows = Array.from(seen.values());

  for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
    const batch = dedupedRows.slice(i, i + BATCH_SIZE);
    if (batch.length === 0) continue;
    let lastErr: any;
    let ok = false;
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
        if (error) { lastErr = error; break; }
        ok = true; break;
      } catch (err: any) {
        lastErr = err;
        if (attempt < 5) await new Promise(r => setTimeout(r, 5000 * attempt));
      }
    }
    if (!ok) {
      console.error(`  DB error: ${lastErr?.message}`);
      totalErrors += batch.length;
    } else {
      totalInserted += batch.length;
    }
  }
  return { inserted: totalInserted, errors: totalErrors };
}

async function ingestMetroRLIS() {
  console.log("\n--- Oregon Metro RLIS Taxlots (Multnomah, Clackamas, Washington) ---");
  console.log("Source:", METRO_URL);

  const totalCount = await getCount(METRO_URL);
  console.log(`Total records: ${totalCount.toLocaleString()}\n`);

  let offset = 0;
  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await postQuery(METRO_URL, {
      where: "1=1",
      outFields: "ORTAXLOT,SITEADDR,SITECITY,SITEZIP,COUNTY,BLDGSQFT,A_T_ACRES,YEARBUILT,PROP_CODE,LANDUSE,SALEPRICE,LANDVAL,BLDGVAL,TOTALVAL,PRIMACCNUM",
      returnGeometry: "false",
      resultOffset: String(offset),
      resultRecordCount: String(PAGE_SIZE),
      f: "json",
    });

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const countyCode = (r.COUNTY || "").trim();
      const countyName = METRO_COUNTY_MAP[countyCode];
      if (!countyName) { totalErrors++; continue; }

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName);
      } catch {
        totalErrors++;
        continue;
      }

      const acres = r.A_T_ACRES ? parseFloat(r.A_T_ACRES) : null;
      const lotSqft = acres && acres > 0 ? Math.round(acres * 43560) : null;
      const yrRaw = r.YEARBUILT ? parseInt(String(r.YEARBUILT)) : 0;
      const yearBuilt = yrRaw > 1700 && yrRaw < 2030 ? yrRaw : null;

      rows.push({
        county_id: countyId,
        parcel_id: r.ORTAXLOT || r.PRIMACCNUM || "",
        address: (r.SITEADDR || "").trim(),
        city: (r.SITECITY || "").trim(),
        state_code: "OR",
        zip: (r.SITEZIP || "").trim(),
        owner_name: "",
        land_sqft: lotSqft,
        total_sqft: r.BLDGSQFT && r.BLDGSQFT > 0 ? Math.round(Number(r.BLDGSQFT)) : null,
        year_built: yearBuilt,
        property_type: classifyPropCode(r.PROP_CODE, r.LANDUSE),
        assessed_value: r.TOTALVAL && r.TOTALVAL > 0 ? Math.round(Number(r.TOTALVAL)) : null,
        land_value: r.LANDVAL && r.LANDVAL > 0 ? Math.round(Number(r.LANDVAL)) : null,
        last_sale_price: r.SALEPRICE && r.SALEPRICE > 0 ? Math.round(Number(r.SALEPRICE)) : null,
        source: "or-metro-rlis",
      });
    }

    const { inserted, errors } = await upsertBatch(rows);
    totalInserted += inserted;
    totalErrors += errors;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000);
    const pct = ((offset + records.length) / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsed}s] offset=${offset} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min`);

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nMetro RLIS done! Inserted ${totalInserted.toLocaleString()} in ${totalElapsed} min. Errors: ${totalErrors}`);
  return { inserted: totalInserted, errors: totalErrors };
}

async function ingestMarionCounty() {
  console.log("\n--- Marion County, Oregon ---");
  console.log("Source:", MARION_URL);

  const totalCount = await getCount(MARION_URL);
  console.log(`Total records: ${totalCount.toLocaleString()}\n`);

  const countyId = await getOrCreateCounty("Marion");

  let offset = 0;
  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await postQuery(MARION_URL, {
      where: "1=1",
      outFields: "TAXLOT,TAXACCT,SITUS,SITUSCSZ,CITY,ACRES,OWNERNAME,OWNERADDR,OWNERCSZ,YEARBUILT,BLDGAREA,PROPCLASS,RMVLND,RMVIMP,RMVTOTAL,SALEPRICE,INSTDATE",
      returnGeometry: "false",
      resultOffset: String(offset),
      resultRecordCount: String(PAGE_SIZE),
      f: "json",
    });

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const acres = r.ACRES ? parseFloat(r.ACRES) : null;
      const lotSqft = acres && acres > 0 ? Math.round(acres * 43560) : null;
      const yrRaw = r.YEARBUILT ? parseInt(String(r.YEARBUILT)) : 0;
      const yearBuilt = yrRaw > 1700 && yrRaw < 2030 ? yrRaw : null;

      rows.push({
        county_id: countyId,
        parcel_id: r.TAXLOT || r.TAXACCT || "",
        address: (r.SITUS || "").trim(),
        city: (r.CITY || "").trim(),
        state_code: "OR",
        zip: "",
        owner_name: (r.OWNERNAME || "").trim(),
        land_sqft: lotSqft,
        total_sqft: r.BLDGAREA && r.BLDGAREA > 0 ? Math.round(Number(r.BLDGAREA)) : null,
        year_built: yearBuilt,
        property_type: classifyPropCode(r.PROPCLASS),
        assessed_value: r.RMVTOTAL && r.RMVTOTAL > 0 ? Math.round(Number(r.RMVTOTAL)) : null,
        land_value: r.RMVLND && r.RMVLND > 0 ? Math.round(Number(r.RMVLND)) : null,
        last_sale_price: r.SALEPRICE && r.SALEPRICE > 0 ? Math.round(Number(r.SALEPRICE)) : null,
        source: "or-marion-county",
      });
    }

    const { inserted, errors } = await upsertBatch(rows);
    totalInserted += inserted;
    totalErrors += errors;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000);
    const pct = ((offset + records.length) / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsed}s] offset=${offset} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min`);

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nMarion done! Inserted ${totalInserted.toLocaleString()} in ${totalElapsed} min. Errors: ${totalErrors}`);
  return { inserted: totalInserted, errors: totalErrors };
}

async function main() {
  console.log("MXRE — Ingest Oregon Parcels (Metro RLIS + Marion County)");
  console.log("Note: ODF MapServer does not support Query operations; using ArcGIS Online sources.");

  const sourceArg = process.argv.find(a => a.startsWith("--source="))?.split("=")[1];

  let grandTotal = 0;
  let grandErrors = 0;

  if (!sourceArg || sourceArg === "metro") {
    const { inserted, errors } = await ingestMetroRLIS();
    grandTotal += inserted;
    grandErrors += errors;
  }

  if (!sourceArg || sourceArg === "marion") {
    const { inserted, errors } = await ingestMarionCounty();
    grandTotal += inserted;
    grandErrors += errors;
  }

  console.log(`\n============================`);
  console.log(`Grand total: ${grandTotal.toLocaleString()} inserted, ${grandErrors} errors`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
