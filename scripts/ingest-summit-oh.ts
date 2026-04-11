#!/usr/bin/env tsx
/**
 * Ingest Summit County OH (Akron) property data.
 * Endpoint: https://scgis.summitoh.net/hosted/rest/services/parcels_web_GEODATA_Tax_Parcels/FeatureServer/0
 * ~260,940 records
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const ARCGIS_URL = "https://scgis.summitoh.net/hosted/rest/services/parcels_web_GEODATA_Tax_Parcels/FeatureServer/0";
const PAGE_SIZE = 1000;
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "parcelid","ownernme1","siteaddress","pstlcity","pstlstate","pstlzip5",
  "cntmarval","lndvalue","bldgvalue","resflrarea","resyrblt",
  "usecd","usedscrp","classcd"
].join(",");

function classifyUse(code: string | null): string {
  const c = String(code ?? "").trim();
  const num = parseInt(c);
  if (num >= 500 && num < 520) return "single_family";
  if (num >= 520 && num < 530) return "multifamily";
  if (num >= 400 && num < 500) return "commercial";
  if (num >= 300 && num < 400) return "industrial";
  if (num >= 600 && num < 700) return "agricultural";
  if (num >= 700) return "exempt";
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
      return ((json.features as Array<{ attributes: Record<string, unknown> }>) ?? []).map(f => f.attributes);
    } catch (err) {
      if (attempt === 4) throw err;
      await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Summit County OH Properties\n");
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let { data: county } = await db.from("counties").select("id").eq("county_name", "Summit").eq("state_code", "OH").single();
  if (!county) {
    const { data: c, error } = await db.from("counties").insert({ county_name: "Summit", state_code: "OH", state_fips: "39", county_fips: "153", active: true }).select("id").single();
    if (error) {
      if (error.message.includes("duplicate key")) {
        // Row already exists — re-fetch
        const { data: existing } = await db.from("counties").select("id").eq("county_name", "Summit").eq("state_code", "OH").single();
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

    const batch = rows.map(r => ({
      county_id: county!.id,
      parcel_id: String(r.parcelid ?? "").trim(),
      address: String(r.siteaddress ?? "").trim().toUpperCase(),
      city: String(r.pstlcity ?? "").trim().toUpperCase(),
      state_code: "OH",
      zip: String(r.pstlzip5 ?? "").trim().slice(0, 5),
      owner_name: String(r.ownernme1 ?? "").trim(),
      assessed_value: parseNum(r.cntmarval),
      market_value: parseNum(r.cntmarval),
      land_value: parseNum(r.lndvalue),
      property_type: classifyUse(r.usecd as string),
      total_sqft: parseNum(r.resflrarea),
      year_built: (() => { const y = parseInt(String(r.resyrblt ?? "")); return y > 1700 && y < 2030 ? y : null; })(),
      source: "summit-oh-gis",
    })).filter(r => r.parcel_id);

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

  console.log(`\n  Done: ${inserted.toLocaleString()} properties for Summit County, OH`);
  const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", county!.id);
  console.log(`  Summit County total in DB: ${count?.toLocaleString()}`);
}
main().catch(console.error);
