#!/usr/bin/env tsx
/**
 * Ingest Cuyahoga County OH (Cleveland) property data from county GIS.
 * Endpoint: https://gis.cuyahogacounty.us/server/rest/services/CUYAHOGA_BASE/Combined_Parcels_RP_CAMA_WGS84/MapServer/0
 * ~521,440 records
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://gis.cuyahogacounty.us/server/rest/services/CUYAHOGA_BASE/Combined_Parcels_RP_CAMA_WGS84/MapServer/0";
const PAGE_SIZE = 1000;
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "parcel_id","parcel_owner","par_addr","par_predir","par_street","par_suffix",
  "par_city","par_zip","gross_certified_total","gross_certified_land","gross_certified_building",
  "sales_amount","transfer_date","property_class","tax_luc","tax_luc_description",
  "total_res_liv_area","total_acreage","total_square_ft","total_res_rooms"
].join(",");

function classifyProperty(propClass: string | null, luc: string | null): string {
  const pc = String(propClass ?? "").trim().toUpperCase();
  const code = String(luc ?? "").trim();
  const num = parseInt(code);

  if (pc === "R") {
    if (num >= 401 && num <= 499) return "multifamily"; // apartments
    if (num >= 500 && num <= 599) return "single_family";
    return "residential";
  }
  if (pc === "C") return "commercial";
  if (pc === "I") return "industrial";
  if (pc === "A") return "agricultural";
  if (pc === "E") return "exempt";
  if (pc === "V") return "land";
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
  console.log("MXRE — Ingest Cuyahoga County OH Properties\n");

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  // Get or create county
  let { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Cuyahoga").eq("state_code", "OH").single();

  if (!county) {
    const { data: newCounty, error } = await db.from("counties")
      .insert({ county_name: "Cuyahoga", state_code: "OH", state_fips: "39", county_fips: "035", active: true })
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
      const parcelId = String(r.parcel_id ?? "").trim();
      // Build address from components
      const addrParts = [r.par_addr, r.par_predir, r.par_street, r.par_suffix].filter(Boolean);
      const address = addrParts.length > 0
        ? addrParts.map(p => String(p).trim()).join(" ").replace(/\s+/g, " ").trim().toUpperCase()
        : "";
      const city = String(r.par_city ?? "").trim().toUpperCase();
      const zip = String(r.par_zip ?? "").trim().slice(0, 5);
      const owner = String(r.parcel_owner ?? "").trim();

      // Sale/transfer date from epoch ms
      let saleDate: string | null = null;
      const saleDateRaw = r.transfer_date;
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
        assessed_value: parseNum(r.gross_certified_total),
        market_value: parseNum(r.gross_certified_total),
        land_value: parseNum(r.gross_certified_land),
        property_type: classifyProperty(r.property_class as string, r.tax_luc as string),
        total_sqft: parseNum(r.total_res_liv_area) || parseNum(r.total_square_ft),
        last_sale_price: parseNum(r.sales_amount),
        last_sale_date: saleDate,
        land_sqft: (() => {
          const acres = parseFloat(String(r.total_acreage ?? ""));
          return acres > 0 ? Math.round(acres * 43560) : null;
        })(),
        source: "cuyahoga-oh-gis",
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

  console.log(`\n  Done: ${inserted.toLocaleString()} properties upserted for Cuyahoga County, OH`);

  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Cuyahoga County total in DB: ${count?.toLocaleString()}`);
}

main().catch(console.error);
