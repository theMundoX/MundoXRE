#!/usr/bin/env tsx
/**
 * Ingest Stark County OH (Canton) property data.
 * Endpoint: https://scgisa.starkcountyohio.gov/arcgis/rest/services/Auditor/StarkCountyParcels/MapServer/0
 * ~201,950 records — richest OH dataset (values, tax, sale history, mortgage co)
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://scgisa.starkcountyohio.gov/arcgis/rest/services/Auditor/StarkCountyParcels/MapServer/0";
const PAGE_SIZE = 1000;
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "PIN","SITE_ADDRESS","OWNER","OWNER_ADDRESS",
  "APPRAISED_TOTAL_VALUE","APPRAISED_LAND_VALUE","APPRAISED_BUILDING_VALUE",
  "ASSESSED_TOTAL_VALUE","MOST_RECENT_SALE_DATE","MOST_RECENT_SALE_PRICE",
  "LAND_USE_CODE","LAND_USE_DESCRIPTION","CLASSIFICATION","ACRES",
  "TAX_DISTRICT_NAME"
].join(",");

function classifyLandUse(desc: string | null): string {
  const d = String(desc ?? "").trim().toUpperCase();
  if (d.includes("SINGLE") || d.includes("1 FAMILY") || d.includes("ONE FAMILY")) return "single_family";
  if (d.includes("MULTI") || d.includes("APART") || d.includes("2 FAMILY") || d.includes("3 FAMILY")) return "multifamily";
  if (d.includes("CONDO")) return "condo";
  if (d.includes("COMMERCIAL") || d.includes("OFFICE") || d.includes("RETAIL")) return "commercial";
  if (d.includes("INDUSTRIAL") || d.includes("WAREHOUSE")) return "industrial";
  if (d.includes("AGRI") || d.includes("FARM")) return "agricultural";
  if (d.includes("VACANT") || d.includes("LAND")) return "land";
  if (d.includes("EXEMPT") || d.includes("PUBLIC")) return "exempt";
  return "residential";
}

function parseNum(v: unknown): number | null {
  const n = parseFloat(String(v ?? ""));
  if (isNaN(n) || n <= 0) return null;
  const rounded = Math.round(n);
  return rounded > INT_MAX ? null : rounded;
}

// Parse city/zip from SITE_ADDRESS like "516 ENTERPRISE CIR LOUISVILLE OH 44641"
// or from OWNER_ADDRESS
function parseCityZip(addr: string): { city: string; zip: string } {
  const s = addr.trim().toUpperCase();
  // Try to find "OH XXXXX" pattern
  const ohMatch = s.match(/\s+OH\s+(\d{5})/);
  if (ohMatch) {
    const zip = ohMatch[1];
    const beforeOH = s.slice(0, s.lastIndexOf(" OH ")).trim();
    // City is the last word(s) before OH that aren't part of street address
    // Simple heuristic: if there's a known pattern, use it
    const parts = beforeOH.split(/\s+/);
    // Take last 1-2 words as city (rough heuristic)
    const city = parts.length >= 3 ? parts.slice(-2).join(" ") : parts.slice(-1).join(" ");
    return { city, zip };
  }
  return { city: "", zip: "" };
}

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
  console.log("MXRE — Ingest Stark County OH Properties\n");
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let { data: county } = await db.from("counties").select("id").eq("county_name", "Stark").eq("state_code", "OH").single();
  if (!county) {
    const { data: c, error } = await db.from("counties").insert({ county_name: "Stark", state_code: "OH", state_fips: "39", county_fips: "151", active: true }).select("id").single();
    if (error) {
      if (error.message.includes("duplicate key")) {
        // Row already exists — re-fetch
        const { data: existing } = await db.from("counties").select("id").eq("county_name", "Stark").eq("state_code", "OH").single();
        if (!existing) { console.error("County error: could not find or create county row"); return; }
        county = existing;
      } else {
        console.error("County error:", error.message); return;
      }
    } else {
      county = c;
    }
  }
  console.log(`  County ID: ${county!.id} | Starting offset: ${offset}`);

  let inserted = 0, emptyPages = 0;
  while (true) {
    const rows = await fetchPage(offset);
    if (rows.length === 0) { if (++emptyPages >= 3) break; offset += PAGE_SIZE; continue; }
    emptyPages = 0;

    const batch = rows.map(r => {
      const pin = String(r.PIN ?? "").trim();
      const siteAddr = String(r.SITE_ADDRESS ?? "").trim().toUpperCase();
      const { city, zip } = parseCityZip(siteAddr);
      // Strip city+state+zip from end of address for clean street address
      const streetAddr = siteAddr.replace(/\s+([\w\s]+)\s+OH\s+\d{5}.*$/, "").trim();

      let saleDate: string | null = null;
      const sdRaw = r.MOST_RECENT_SALE_DATE;
      if (sdRaw && typeof sdRaw === "number" && sdRaw > 0) {
        const dt = new Date(sdRaw);
        if (dt.getFullYear() > 1970 && dt.getFullYear() < 2030) saleDate = dt.toISOString().slice(0, 10);
      } else if (typeof sdRaw === "string" && sdRaw.length >= 8) {
        // Try parsing string date
        const dt = new Date(sdRaw);
        if (!isNaN(dt.getTime()) && dt.getFullYear() > 1970) saleDate = dt.toISOString().slice(0, 10);
      }

      return {
        county_id: county!.id,
        parcel_id: pin,
        address: streetAddr,
        city,
        state_code: "OH",
        zip,
        owner_name: String(r.OWNER ?? "").trim(),
        assessed_value: parseNum(r.ASSESSED_TOTAL_VALUE),
        market_value: parseNum(r.APPRAISED_TOTAL_VALUE),
        land_value: parseNum(r.APPRAISED_LAND_VALUE),
        property_type: classifyLandUse(r.LAND_USE_DESCRIPTION as string),
        last_sale_price: parseNum(r.MOST_RECENT_SALE_PRICE),
        last_sale_date: saleDate,
        land_sqft: (() => {
          const acres = parseFloat(String(r.ACRES ?? ""));
          return acres > 0 ? Math.round(acres * 43560) : null;
        })(),
        source: "stark-oh-auditor",
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

  console.log(`\n  Done: ${inserted.toLocaleString()} properties for Stark County, OH`);
  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Stark County total in DB: ${count?.toLocaleString()}`);
}
main().catch(console.error);
