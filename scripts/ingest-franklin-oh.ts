#!/usr/bin/env tsx
/**
 * Ingest Franklin County OH property data from CAGIS MapServer.
 * Endpoint: https://gis.franklincountyohio.gov/hosting/rest/services/ParcelFeatures/Parcel_Features/MapServer/0
 * ~493,443 records
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://gis.franklincountyohio.gov/hosting/rest/services/ParcelFeatures/Parcel_Features/MapServer/0";
const PAGE_SIZE = 1000;
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "PARCELID","SITEADDRESS","ZIPCD","PSTLCITYSTZIP","OWNERNME1",
  "TOTVALUEBASE","LNDVALUEBASE","RESYRBLT","RESFLRAREA",
  "SALEPRICE","SALEDATE","BEDRMS","BATHS","CLASSCD","ACRES"
].join(",");

// Land use classification from CLASSCD
function classifyLandUse(code: string | null | undefined): string {
  const c = String(code ?? "").trim();
  if (!c) return "residential";
  const num = parseInt(c);
  if (num >= 100 && num < 200) return "single_family";
  if (num >= 200 && num < 300) return "multifamily";
  if (num >= 300 && num < 400) return "condo";
  if (num >= 400 && num < 500) return "commercial";
  if (num >= 500 && num < 600) return "industrial";
  if (num >= 600 && num < 700) return "agricultural";
  if (num >= 700 && num < 800) return "land";
  if (num >= 800) return "exempt";
  return "residential";
}

// Parse city from PSTLCITYSTZIP like "COLUMBUS OH 43215-1234" or "GROVE CITY OH 43123"
function parseCity(raw: string | null | undefined): string {
  if (!raw) return "";
  const s = raw.trim().toUpperCase();
  // Find " OH " pattern
  const ohIdx = s.lastIndexOf(" OH ");
  if (ohIdx > 0) return s.slice(0, ohIdx).trim();
  // Fallback: everything before last two words (state + zip)
  const parts = s.split(/\s+/);
  if (parts.length >= 3) return parts.slice(0, parts.length - 2).join(" ");
  return s;
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
  console.log("MXRE — Ingest Franklin County OH Properties\n");

  // Resolve start offset from CLI args
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  // Get or create county
  let { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Franklin").eq("state_code", "OH").single();

  if (!county) {
    const { data: newCounty, error } = await db.from("counties")
      .insert({ county_name: "Franklin", state_code: "OH", state_fips: "39", county_fips: "049", active: true })
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
      const address = String(r.SITEADDRESS ?? "").trim().toUpperCase();
      const zip = String(r.ZIPCD ?? "").trim().slice(0, 5);
      const city = parseCity(r.PSTLCITYSTZIP as string);
      const owner = String(r.OWNERNME1 ?? "").trim();

      // Sale date from epoch ms
      let saleDate: string | null = null;
      const saleDateRaw = r.SALEDATE;
      if (saleDateRaw && typeof saleDateRaw === "number" && saleDateRaw > 0) {
        const dt = new Date(saleDateRaw);
        if (dt.getFullYear() > 1970 && dt.getFullYear() < 2030) {
          saleDate = dt.toISOString().slice(0, 10);
        }
      }

      const propType = classifyLandUse(r.CLASSCD as string);

      return {
        county_id: county!.id,
        parcel_id: parcelId || `NOID-${offset}-${Math.random().toString(36).slice(2)}`,
        address: address || "",
        city,
        state_code: "OH",
        zip,
        owner_name: owner,
        assessed_value: parseNum(r.TOTVALUEBASE),
        land_value: parseNum(r.LNDVALUEBASE),
        market_value: parseNum(r.TOTVALUEBASE),
        property_type: propType,
        total_sqft: parseNum(r.RESFLRAREA),
        year_built: (() => {
          const y = parseInt(String(r.RESYRBLT ?? ""));
          return y > 1700 && y < 2030 ? y : null;
        })(),
        bedrooms: (() => {
          const b = parseInt(String(r.BEDRMS ?? ""));
          return b > 0 ? b : null;
        })(),
        bathrooms_full: (() => {
          const b = parseInt(String(r.BATHS ?? ""));
          return b > 0 ? b : null;
        })(),
        last_sale_price: parseNum(r.SALEPRICE),
        last_sale_date: saleDate,
        land_sqft: (() => {
          const acres = parseFloat(String(r.ACRES ?? ""));
          return acres > 0 ? Math.round(acres * 43560) : null;
        })(),
        source: "franklin-oh-auditor",
      };
    }).filter(r => r.parcel_id && !r.parcel_id.startsWith("NOID"));

    // Deduplicate within batch by parcel_id (keep last occurrence)
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

    // Last page
    if (rows.length < PAGE_SIZE) break;
  }

  console.log(`\n  Done: ${inserted.toLocaleString()} properties upserted for Franklin County, OH`);

  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Franklin County total in DB: ${count?.toLocaleString()}`);
}

main().catch(console.error);
