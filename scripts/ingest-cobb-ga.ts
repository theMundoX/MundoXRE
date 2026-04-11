#!/usr/bin/env tsx
/**
 * MXRE — Cobb County, GA Assessor Parcel Ingest (Marietta / Smyrna)
 *
 * Source: Cobb County GIS — taxassessorsdaily MapServer (Layer 0: CobbParcels)
 *   https://gis.cobbcounty.gov/gisserver/rest/services/tax/taxassessorsdaily/MapServer/0
 *   ~279K parcels, offset-based pagination, MaxRecordCount=1000
 *
 * Fields: PIN, PARID, OWNER_NAM1, SITUS_ADDR, FMV_TOTAL, ASV_TOTAL,
 *         FMV_LAND, FMV_BLDG, CLASS, TAXDIST
 *
 * GA: assessed value = 40% of appraised (fair market) value.
 *     FMV_TOTAL is appraised value; ASV_TOTAL is assessed (40%).
 *
 * Cobb County CLASS codes:
 *   R = residential, C = commercial, I = industrial, E = exempt,
 *   B = business/commercial, H = historic, U = utility, V/W = vacant land
 *
 * Year built and sale data live in related tables (Tables 5, 9).
 * This script ingests the core parcel layer. A follow-up enrichment pass
 * can JOIN YearBuilt and ParcelSales by PIN if needed.
 *
 * Usage:
 *   npx tsx scripts/ingest-cobb-ga.ts
 *   npx tsx scripts/ingest-cobb-ga.ts --skip=50000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL =
  "https://gis.cobbcounty.gov/gisserver/rest/services/tax/taxassessorsdaily/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "GA";
const INT_MAX = 2_147_483_647;

// Cobb County CLASS codes → property_type
// R = residential (single-family), R4 = condo, R5 = multifamily
// C/B = commercial, I = industrial, E = exempt
// H = historic residential, U = utility, V/W = vacant
function classifyClass(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).toUpperCase().trim();
  if (c === "R3") return "residential";
  if (c === "R4") return "condo";
  if (c === "R5") return "multifamily";
  if (c.startsWith("C") || c.startsWith("B")) return "commercial";
  if (c.startsWith("I")) return "industrial";
  if (c.startsWith("E")) return "exempt";
  if (c.startsWith("H")) return "residential"; // historic residential
  if (c.startsWith("U")) return "utility";
  if (c.startsWith("V") || c.startsWith("W")) return "land";
  return "residential";
}

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

// Cobb parcels use SITUS_ADDR but no city/zip in the parcel layer.
// Tax district maps roughly to city; encode defaults by TAXDIST.
// TAXDIST: 1=unincorporated, 4=Marietta, 5=Smyrna, 6=Austell, 7=Powder Springs,
//          8=Kennesaw, 9=unincorp south, etc. Approximate city only.
function cityFromTaxDist(taxdist: string | null): string {
  if (!taxdist) return "MARIETTA";
  switch (String(taxdist).trim()) {
    case "4": return "MARIETTA";
    case "5": return "SMYRNA";
    case "6": return "AUSTELL";
    case "7": return "POWDER SPRINGS";
    case "8": return "KENNESAW";
    case "3": return "ACWORTH";
    case "2": return "MABLETON";
    default: return "MARIETTA"; // unincorporated Cobb default
  }
}

const FIELDS = [
  "OBJECTID",
  "PIN",
  "PARID",
  "OWNER_NAM1",
  "SITUS_ADDR",
  "FMV_TOTAL",
  "ASV_TOTAL",
  "FMV_LAND",
  "FMV_BLDG",
  "CLASS",
  "TAXDIST",
  "ACRES",
].join(",");

async function fetchPage(offset: number): Promise<{ features: Record<string, unknown>[]; count: number }> {
  const url =
    `${BASE_URL}/query?where=1%3D1` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = ((json.features as Array<{ attributes: Record<string, unknown> }>) || []).map(
        (f) => f.attributes,
      );
      return { features, count: features.length };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0 };
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOffset = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Cobb County, GA Assessor Parcel Ingest (Marietta / Smyrna)");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Cobb")
    .eq("state_code", "GA")
    .single();
  if (!county) { console.error("Cobb County, GA not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs to skip dupes
  const existing = new Set<string>();
  let exOffset = 0;
  while (true) {
    const { data } = await db
      .from("properties")
      .select("parcel_id")
      .eq("county_id", COUNTY_ID)
      .not("parcel_id", "is", null)
      .range(exOffset, exOffset + 999);
    if (!data || data.length === 0) break;
    for (const r of data) if (r.parcel_id) existing.add(r.parcel_id);
    if (data.length < 1000) break;
    exOffset += 1000;
  }
  console.log(`  ${existing.size.toLocaleString()} parcels already in DB\n`);

  let inserted = 0, dupes = 0, errors = 0, skipped = 0;
  let offset = skipOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // PIN and PARID are the same; use PARID as the canonical parcel identifier
      const pin = String(f.PARID || f.PIN || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.SITUS_ADDR || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const taxdist = String(f.TAXDIST || "").trim();
      const city = cityFromTaxDist(taxdist);

      // FMV_TOTAL = appraised (fair market) value
      // ASV_TOTAL = assessed = 40% of FMV (GA law)
      const marketValue = parseNum(f.FMV_TOTAL);
      const assessedValue = parseNum(f.ASV_TOTAL) ?? (marketValue ? Math.round(marketValue * 0.40) : null);
      const landValue = parseNum(f.FMV_LAND);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER_NAM1 || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip: null, // not in parcel layer; enrich separately if needed
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: landValue,
        property_type: classifyClass(f.CLASS as string | null),
        source: "cobb_ga_gis",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(chunk, { onConflict: "county_id,parcel_id" });
      if (error) {
        for (const record of chunk) {
          const { error: e2 } = await db
            .from("properties")
            .upsert(record, { onConflict: "county_id,parcel_id" });
          if (e2) {
            if (errors < 5) console.error(`\n  Error: ${JSON.stringify(e2).slice(0, 120)}`);
            errors++;
          } else {
            inserted++;
          }
        }
      } else {
        inserted += chunk.length;
      }
    }

    process.stdout.write(
      `\r  offset ${offset.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skip ${skipped} | errs ${errors}   `,
    );

    offset += count;
    if (count < PAGE_SIZE) break;
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
