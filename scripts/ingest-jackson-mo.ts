#!/usr/bin/env tsx
/**
 * MXRE — Jackson County, MO Assessor Parcel Ingest (Kansas City)
 *
 * Source: Jackson County GIS — ParcelsPointsAscendBackup FeatureServer
 *   Base: https://jcgis.jacksongov.org/arcgis/rest/services/ParcelViewer/ParcelsPointsAscendBackup/FeatureServer
 *   Table ID 2: Ascend_GisInfo  (~300K rows, MaxRecordCount=2000)
 *
 * NOTE: The four jacksongov.org GIS subdomains in the original brief
 *   (gis/maps/gis2/arcgis) do not resolve. The live host is jcgis.jacksongov.org.
 *   Discovered via: https://jcgis.jacksongov.org/arcgis/rest/services?f=json
 *
 * Key fields available:
 *   parcel_number, situs_address, situs_city, situs_zip,
 *   owner_info, Market_Value_Total, Assessed_Value_Total,
 *   Assessed_Val_Res, Assessed_Val_Comm, Assessed_Val_Ag,
 *   year_built, landuse_cd, landuse_cd_descr, recording_num
 *
 * MO assessment ratios (stored as-is from source):
 *   Residential: 19% of appraised value
 *   Commercial:  32% of appraised value
 *   Agricultural: 12% of appraised value
 *   Source provides both Market_Value_Total (appraised) and Assessed_Value_Total
 *
 * Sale date/price: NOT available in this GIS layer. The recording_num field
 *   is the deed instrument number (not a sale date or price).
 *
 * Parcels: ~300,626 (as of 2026)
 *
 * Usage:
 *   npx tsx scripts/ingest-jackson-mo.ts
 *   npx tsx scripts/ingest-jackson-mo.ts --offset=100000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL =
  "https://jcgis.jacksongov.org/arcgis/rest/services/ParcelViewer/ParcelsPointsAscendBackup/FeatureServer";
const TABLE_ID = 2; // Ascend_GisInfo table
const PAGE_SIZE = 2000; // server MaxRecordCount
const BATCH_SIZE = 500;
const STATE_CODE = "MO";
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "parcel_number",
  "situs_address",
  "situs_city",
  "situs_zip",
  "owner_info",
  "Market_Value_Total",
  "Assessed_Value_Total",
  "Assessed_Val_Res",
  "Assessed_Val_Comm",
  "Assessed_Val_Ag",
  "year_built",
  "landuse_cd",
  "landuse_cd_descr",
  "recording_num",
].join(",");

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function parseYear(v: unknown): number | null {
  if (v == null) return null;
  const y = parseInt(String(v), 10);
  if (isNaN(y) || y < 1700 || y > 2100) return null;
  return y;
}

/**
 * MO land-use codes: first digit encodes the broad class
 *   1xxx = residential
 *   2xxx = commercial (note: some 2xxx multifamily taxed at 19%)
 *   3xxx = industrial
 *   4xxx = agricultural
 *   5xxx = utilities / railroad
 *
 * We also use landuse_cd_descr for disambiguation.
 */
function classifyLU(code: string | null, descr: string | null): string {
  const c = String(code ?? "").trim();
  const d = String(descr ?? "").toUpperCase();

  if (!c) return "residential";

  const prefix = c.charAt(0);

  if (prefix === "1") {
    // 1110 = SF Residence, 1112 = SF Condo, 1111 = Townhouse
    // 1120 = Duplex, 1130 = Triplex, 1140 = Fourplex, 1150 = Conv. House to MF
    // 1160 = Res Co-op, 1190 = Det. Garage, 1191 = Outbuilding, 1199 = Misc Res
    // 1200 = Res & Com (res predominant)
    if (d.includes("CONDO") || c === "1112" || c === "1113") return "condo";
    if (d.includes("TOWNHOUSE")) return "multifamily";
    if (
      d.includes("DUPLEX") || d.includes("TRIPLEX") || d.includes("FOURPLEX") ||
      d.includes("MULTI") || c === "1120" || c === "1130" || c === "1140" ||
      c === "1150" || c === "1160"
    ) return "multifamily";
    if (d.includes("MOBILE HOME")) return "residential";
    return "residential";
  }

  if (prefix === "2") {
    // 2101/2102 = Vacant Comm Land  2103 = Imp Comm Land
    // 2120 = Comm Multi-Fam @19%, 2130 = Section 8, 2140 = Section 42,
    // 2150-2199 = Apartments / retirement / group homes
    // 2190-2194 = Garden/LowRise/HighRise Apts
    if (
      c === "2120" || c === "2130" || c === "2140" ||
      (parseInt(c, 10) >= 2150 && parseInt(c, 10) <= 2199) ||
      d.includes("APART") || d.includes("SECTION 8") || d.includes("SECTION 42") ||
      d.includes("RETIREMENT") || d.includes("GROUP HOME") || d.includes("MOBILE HOME PARK")
    ) return "multifamily";
    if (d.includes("INDUST") || d.includes("WAREHOUSE")) return "industrial";
    return "commercial";
  }

  if (prefix === "3") return "industrial";
  if (prefix === "4") return "agricultural";
  if (prefix === "5") return "exempt"; // utilities, railroad

  return "residential";
}

async function fetchPage(
  offset: number,
): Promise<{ features: Record<string, unknown>[]; count: number }> {
  const url =
    `${BASE_URL}/${TABLE_ID}/query?where=1%3D1` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}` +
    `&orderByFields=parcel_number&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(60000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = (
        (json.features as Array<{ attributes: Record<string, unknown> }>) || []
      ).map((f) => f.attributes);
      return { features, count: features.length };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0 };
}

async function main() {
  const offsetArg = process.argv.find((a) => a.startsWith("--offset="))?.split("=")[1];
  const startOffset = offsetArg ? parseInt(offsetArg, 10) : 0;

  console.log("MXRE — Jackson County, MO Assessor Parcel Ingest (Kansas City)");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Jackson")
    .eq("state_code", "MO")
    .single();
  if (!county) {
    console.error("Jackson County, MO not in DB — run seed first");
    process.exit(1);
  }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Pre-load existing parcel IDs to skip dupes without re-upsert
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
  let offset = startOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // parcel_number format: "33-420-10-11-00-0-00-000"  (dashes)
      // Normalize to no-dash form for parcel_id consistency
      const rawPin = String(f.parcel_number || "").trim();
      if (!rawPin) { skipped++; continue; }
      const pin = rawPin.replace(/-/g, "");
      if (!pin) { skipped++; continue; }

      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.situs_address || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.situs_city || "KANSAS CITY").trim().toUpperCase();
      const zip = String(f.situs_zip || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // MO: Market_Value_Total = appraised/market value
      //     Assessed_Value_Total = what the assessor certified (res=19%, comm=32%)
      const marketValue = parseNum(f.Market_Value_Total);
      const assessedValue = parseNum(f.Assessed_Value_Total);

      // Derive land value: use the separate land buckets
      const resLand = parseNum(f.Res_Land) ?? 0;   // not in fetch but won't break
      // We didn't fetch Res_Land/Comm_Land separately to keep field list lean;
      // land_value is not critical for this layer, set null.
      const landValue: number | null = null;

      const ownerName = String(f.owner_info || "").trim() || null;

      const yearBuilt = parseYear(f.year_built);

      const luCode = String(f.landuse_cd ?? "").trim() || null;
      const luDescr = String(f.landuse_cd_descr ?? "").trim() || null;
      const propertyType = classifyLU(luCode, luDescr);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: landValue,
        year_built: yearBuilt,
        last_sale_date: null,   // not available in this GIS layer
        last_sale_price: null,  // not available in this GIS layer
        property_type: propertyType,
        source: "jackson_mo_jcgis",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db
        .from("properties")
        .upsert(chunk, { onConflict: "county_id,parcel_id" });
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
  console.log(
    `TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`,
  );
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
