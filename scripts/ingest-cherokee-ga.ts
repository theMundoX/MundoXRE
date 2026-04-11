#!/usr/bin/env tsx
/**
 * MXRE — Cherokee County, GA Assessor Parcel Ingest (Canton)
 *
 * Source: Cherokee County GIS — MainLayersPRO MapServer (Layer 1: Parcels)
 *   https://gis.cherokeecountyga.gov/arcgis/rest/services/MainLayersPRO/MapServer/1
 *   ~115K parcels, offset-based pagination, MaxRecordCount=2000
 *
 * Fields: PIN, OWNER, Property_Address, Property_City, Property_Zip,
 *         TaxDistrict, Zoning, Acreage, DEEDBOOK, DEEDPAGE
 *
 * NOTE: Cherokee County GIS does NOT publish FMV or assessed values in the
 * parcel layer. The tax assessor data is behind the qPublic portal
 * (qpublic.schneidercorp.com, Cloudflare-protected). Assessed and market
 * values are left null and flagged for future enrichment.
 *
 * GA rule: assessed_value = 40% of appraised (fair market) value.
 *   appraised = assessed / 0.40  (when available from another source).
 *
 * Cherokee County Zoning codes (sample):
 *   R = residential, C = commercial, I = industrial, AG = agricultural,
 *   MF = multifamily, NS = neighborhood shopping, GC = general commercial
 *
 * TaxDistrict 01 = unincorporated, 02 = Canton, 03 = Ball Ground,
 *   04 = Holly Springs, 05 = Nelson, 06 = Waleska, 07 = Woodstock
 *
 * Usage:
 *   npx tsx scripts/ingest-cherokee-ga.ts
 *   npx tsx scripts/ingest-cherokee-ga.ts --skip=20000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL =
  "https://gis.cherokeecountyga.gov/arcgis/rest/services/MainLayersPRO/MapServer/1";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "GA";

// Cherokee Zoning codes → property_type
function classifyZoning(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).toUpperCase().trim();
  if (c === "AG" || c.startsWith("A-") || c === "A") return "agricultural";
  if (c.startsWith("MF") || c.includes("MULTI") || c.includes("APT")) return "multifamily";
  if (c.startsWith("C") || c.includes("COMM") || c === "NS" || c === "GC" || c === "LC" || c === "HC") return "commercial";
  if (c.startsWith("I") || c.includes("IND")) return "industrial";
  if (c.startsWith("E") || c.includes("EXE") || c.includes("PUB")) return "exempt";
  if (c.startsWith("R")) return "residential";
  return "residential";
}

// Cherokee County TaxDistrict → city name
function cityFromTaxDist(taxdist: string | null, propertyCity: string | null): string {
  // Prefer the explicit Property_City field if present
  if (propertyCity && String(propertyCity).trim()) {
    return String(propertyCity).trim().toUpperCase();
  }
  if (!taxdist) return "CANTON";
  switch (String(taxdist).trim()) {
    case "01": return "CANTON"; // unincorporated → default to Canton
    case "02": return "CANTON";
    case "03": return "BALL GROUND";
    case "04": return "HOLLY SPRINGS";
    case "05": return "NELSON";
    case "06": return "WALESKA";
    case "07": return "WOODSTOCK";
    default: return "CANTON";
  }
}

const FIELDS = [
  "OBJECTID",
  "PIN",
  "OWNER",
  "Property_Address",
  "Property_City",
  "Property_Zip",
  "TaxDistrict",
  "Zoning",
  "Acreage",
  "DEEDBOOK",
  "DEEDPAGE",
  "Subdivision",
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

  console.log("MXRE — Cherokee County, GA Assessor Parcel Ingest (Canton)");
  console.log("═".repeat(60));
  console.log("NOTE: FMV/assessed values not available in Cherokee GIS layer.");
  console.log("      market_value and assessed_value will be null.\n");

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Cherokee")
    .eq("state_code", "GA")
    .single();
  if (!county) { console.error("Cherokee County, GA not in DB"); process.exit(1); }
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
      const pin = String(f.PIN || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.Property_Address || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const taxdist = String(f.TaxDistrict || "").trim();
      const propertyCity = String(f.Property_City || "").trim() || null;
      const city = cityFromTaxDist(taxdist, propertyCity);

      const zip = f.Property_Zip ? String(f.Property_Zip).trim() : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip: zip || null,
        // FMV and assessed values not available in this GIS layer.
        // Cherokee County assessor data is behind qPublic (CF-protected).
        // Enrich via a separate scrape pass targeting:
        //   https://qpublic.schneidercorp.com/Application.aspx?AppID=1050
        market_value: null,
        assessed_value: null,
        land_value: null,
        property_type: classifyZoning(f.Zoning as string | null),
        source: "cherokee_ga_gis",
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
