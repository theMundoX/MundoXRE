#!/usr/bin/env tsx
/**
 * Ingest Hamilton County OH (Cincinnati) property data from CAGIS Open Data.
 * Endpoint: https://services.arcgis.com/JyZag7oO4NteHGiq/arcgis/rest/services/OpenData/FeatureServer/10
 * ~420,289 records
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://services.arcgis.com/JyZag7oO4NteHGiq/arcgis/rest/services/OpenData/FeatureServer/10";
const PAGE_SIZE = 2000;
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "PARCELID","OWNNM1","ADDRNO","ADDRST","ADDRSF","OWNADCITY","OWNADSTATE","OWNADZIP",
  "MKT_TOTAL_VAL","MKTLND","MKTIMP","SALAMT","SALDAT","EXLUCODE","CLASS",
  "NUM_UNITS","ACREDEED","ANNUAL_TAXES"
].join(",");

function classifyUseCode(code: string | null | undefined): string {
  const c = String(code ?? "").trim().toUpperCase();
  if (c === "SF") return "single_family";
  if (c === "MF" || c === "AP") return "multifamily";
  if (c === "CD" || c === "CO") return "condo";
  if (c === "CM" || c === "OF" || c === "RT") return "commercial";
  if (c === "IN" || c === "WH") return "industrial";
  if (c === "VA" || c === "VL") return "land";
  if (c === "EX") return "exempt";
  if (c === "AG") return "agricultural";
  return "residential";
}

function parseNum(v: unknown): number | null {
  const n = parseFloat(String(v ?? ""));
  if (isNaN(n) || n <= 0) return null;
  const rounded = Math.round(n);
  return rounded > INT_MAX ? null : rounded;
}

async function fetchPage(offset: number): Promise<Record<string, unknown>[]> {
  const url = `${ARCGIS_URL}/query?where=1%3D1&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json() as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));
      const features = (json.features as Array<{ attributes: Record<string, unknown> }>) ?? [];
      return features.map(f => f.attributes);
    } catch (err) {
      if (attempt === 4) throw err;
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Hamilton County OH Properties\n");

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  // Get or create county
  let { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Hamilton").eq("state_code", "OH").single();

  if (!county) {
    const { data: newCounty, error } = await db.from("counties")
      .insert({ county_name: "Hamilton", state_code: "OH", state_fips: "39", county_fips: "061", active: true })
      .select("id").single();
    if (error) { console.error("County insert error:", error.message); return; }
    county = newCounty;
  }
  console.log(`  County ID: ${county!.id}`);
  console.log(`  Starting at offset: ${offset}`);

  let inserted = 0;
  let emptyPages = 0;

  while (true) {
    const rows = await fetchPage(offset);

    if (rows.length === 0) {
      emptyPages++;
      if (emptyPages >= 3) break;
      offset += PAGE_SIZE;
      continue;
    }
    emptyPages = 0;

    const batch = rows.map(r => {
      const parcelId = String(r.PARCELID ?? "").trim();
      // Build address from components
      const addrParts = [r.ADDRNO, r.ADDRST, r.ADDRSF].filter(Boolean);
      const address = addrParts.length > 0
        ? addrParts.map(p => String(p).trim()).join(" ").replace(/\s+/g, " ").trim().toUpperCase()
        : "";
      const city = String(r.OWNADCITY ?? "").trim().toUpperCase();
      const zip = String(r.OWNADZIP ?? "").trim().slice(0, 5);
      const owner = String(r.OWNNM1 ?? "").trim();

      // Sale date from epoch ms
      let saleDate: string | null = null;
      const saleDateRaw = r.SALDAT;
      if (saleDateRaw && typeof saleDateRaw === "number" && saleDateRaw > 0) {
        const dt = new Date(saleDateRaw);
        if (dt.getFullYear() > 1970 && dt.getFullYear() < 2030) {
          saleDate = dt.toISOString().slice(0, 10);
        }
      }

      return {
        county_id: county!.id,
        parcel_id: parcelId,
        address,
        city,
        state_code: "OH",
        zip,
        owner_name: owner,
        assessed_value: parseNum(r.MKT_TOTAL_VAL),
        market_value: parseNum(r.MKT_TOTAL_VAL),
        land_value: parseNum(r.MKTLND),
        property_type: classifyUseCode(r.EXLUCODE as string),
        last_sale_price: parseNum(r.SALAMT),
        last_sale_date: saleDate,
        land_sqft: (() => {
          const acres = parseFloat(String(r.ACREDEED ?? ""));
          return acres > 0 ? Math.round(acres * 43560) : null;
        })(),
        source: "hamilton-oh-cagis",
      };
    }).filter(r => r.parcel_id);

    // Deduplicate within batch
    const seen = new Map<string, Record<string, unknown>>();
    for (const rec of batch) seen.set(rec.parcel_id as string, rec);
    const dedupedBatch = Array.from(seen.values());

    if (dedupedBatch.length > 0) {
      const { error } = await db.from("properties")
        .upsert(dedupedBatch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
      if (error) {
        console.error(`\n  Upsert error at offset ${offset}: ${error.message.slice(0, 100)}`);
      } else {
        inserted += dedupedBatch.length;
      }
    }

    offset += PAGE_SIZE;
    process.stdout.write(`\r  Upserted: ${inserted.toLocaleString()} | Offset: ${offset.toLocaleString()}    `);

    if (rows.length < PAGE_SIZE) break;
  }

  console.log(`\n  Done: ${inserted.toLocaleString()} properties upserted for Hamilton County, OH`);

  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Hamilton County total in DB: ${count?.toLocaleString()}`);
}

main().catch(console.error);
