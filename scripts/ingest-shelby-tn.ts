#!/usr/bin/env tsx
/**
 * MXRE — Shelby County, TN Assessor Parcel Ingest (Memphis)
 *
 * Source: Shelby County GIS — CERT_Parcel MapServer
 *   https://gis.shelbycountytn.gov/public/rest/services/Parcel/CERT_Parcel/MapServer/0
 *   ~350K+ parcels, ArcGIS Server, OBJECTID-based pagination
 *
 * Fields: OBJECTID, PARCELID, MAP, OWNER, OWNER_EXT, PAR_ADRNO, PAR_ADRSTR,
 *         CLASS, LUC, MUNI, TAXYR, Longitude, Latitude
 *
 * Note: No property value fields in this layer — market_value and assessed_value
 *       are stored as null. Owner and address data are captured.
 *
 * Usage:
 *   npx tsx scripts/ingest-shelby-tn.ts
 *   npx tsx scripts/ingest-shelby-tn.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis.shelbycountytn.gov/public/rest/services/Parcel/CERT_Parcel/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "TN";

const FIELDS = [
  "OBJECTID",
  "PARCELID",
  "MAP",
  "OWNER",
  "OWNER_EXT",
  "PAR_ADRNO",
  "PAR_ADRSTR",
  "CLASS",
  "LUC",
  "MUNI",
  "TAXYR",
  "Longitude",
  "Latitude",
].join(",");

/**
 * Classify property_type from CLASS or LUC field values.
 * Shelby County CLASS codes follow standard TN assessor conventions:
 *   R / 1xx = residential, C / 2xx = commercial, I / 3xx = industrial,
 *   A / 4xx = agricultural/farm, E / 5xx = exempt/institutional
 */
function classifyPropertyType(cls: unknown, luc: unknown): string {
  const c = String(cls || "").trim().toUpperCase();
  const l = String(luc || "").trim().toUpperCase();

  // Check CLASS field first (single-letter codes are most reliable)
  if (c.startsWith("R")) return "residential";
  if (c.startsWith("C")) return "commercial";
  if (c.startsWith("I")) return "industrial";
  if (c.startsWith("A") || c.startsWith("F")) return "agricultural";
  if (c.startsWith("E") || c.startsWith("X")) return "exempt";

  // Fall back to LUC (land use code) numeric prefix
  const lucNum = parseInt(l, 10);
  if (!isNaN(lucNum)) {
    if (lucNum >= 100 && lucNum < 200) return "residential";
    if (lucNum >= 200 && lucNum < 300) return "commercial";
    if (lucNum >= 300 && lucNum < 400) return "industrial";
    if (lucNum >= 400 && lucNum < 500) return "agricultural";
    if (lucNum >= 500 && lucNum < 600) return "exempt";
  }

  // Check LUC string codes
  if (l.startsWith("R")) return "residential";
  if (l.startsWith("C")) return "commercial";
  if (l.startsWith("I")) return "industrial";
  if (l.startsWith("A") || l.startsWith("F")) return "agricultural";

  return "unknown";
}

async function fetchPage(minOid: number): Promise<{ features: Record<string, unknown>[]; maxOid: number }> {
  const url =
    `${PARCELS_URL}/query?where=OBJECTID+>+${minOid}` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultRecordCount=${PAGE_SIZE}&orderByFields=OBJECTID+ASC&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = ((json.features as Array<{ attributes: Record<string, unknown> }>) || []).map(
        (f) => f.attributes,
      );
      const maxOid = features.reduce((m, f) => {
        const oid = f["OBJECTID"] as number;
        return oid > m ? oid : m;
      }, minOid);
      return { features, maxOid };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], maxOid: minOid };
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Shelby County, TN Assessor Parcel Ingest (Memphis)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Shelby").eq("state_code", "TN").single();
  if (!county) { console.error("Shelby County, TN not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs to avoid duplicate upsert churn
  const existing = new Set<string>();
  let exOffset = 0;
  while (true) {
    const { data } = await db.from("properties").select("parcel_id")
      .eq("county_id", COUNTY_ID).not("parcel_id", "is", null)
      .range(exOffset, exOffset + 999);
    if (!data || data.length === 0) break;
    for (const r of data) if (r.parcel_id) existing.add(r.parcel_id);
    if (data.length < 1000) break;
    exOffset += 1000;
  }
  console.log(`  ${existing.size.toLocaleString()} parcels already in DB\n`);

  let inserted = 0, dupes = 0, errors = 0, skipped = 0, minOid = skipOid, totalFetched = 0;

  while (true) {
    const { features, maxOid } = await fetchPage(minOid);
    if (features.length === 0) break;
    totalFetched += features.length;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.PARCELID || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // Build address from PAR_ADRNO + PAR_ADRSTR
      const adrNo = String(f.PAR_ADRNO || "").trim();
      const adrStr = String(f.PAR_ADRSTR || "").trim();
      const address = `${adrNo} ${adrStr}`.trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.MUNI || "MEMPHIS").trim().toUpperCase() || "MEMPHIS";

      // Build owner name: OWNER + OWNER_EXT if present
      const ownerBase = String(f.OWNER || "").trim();
      const ownerExt = String(f.OWNER_EXT || "").trim();
      const ownerName = ownerBase
        ? ownerExt
          ? `${ownerBase} ${ownerExt}`.trim()
          : ownerBase
        : null;

      const propertyType = classifyPropertyType(f.CLASS, f.LUC);

      // Coordinates — store if valid
      const lon = typeof f.Longitude === "number" && f.Longitude !== 0 ? f.Longitude : null;
      const lat = typeof f.Latitude === "number" && f.Latitude !== 0 ? f.Latitude : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName,
        address,
        city,
        state_code: STATE_CODE,
        zip: null,              // Not present in this layer
        market_value: null,     // Not available in this layer
        assessed_value: null,   // Not available in this layer
        land_value: null,       // Not available in this layer
        last_sale_price: null,  // Not available in this layer
        year_built: null,       // Not available in this layer
        property_type: propertyType,
        latitude: lat,
        longitude: lon,
        source: "shelby_tn_regis",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(chunk, { onConflict: "county_id,parcel_id" });
      if (error) {
        for (const record of chunk) {
          const { error: e2 } = await db.from("properties").upsert(record, { onConflict: "county_id,parcel_id" });
          if (e2) {
            if (errors < 5) console.error(`\n  Error: ${JSON.stringify(e2).slice(0, 120)}`);
            errors++;
          } else { inserted++; }
        }
      } else {
        inserted += chunk.length;
      }
    }

    process.stdout.write(
      `\r  OID ${minOid.toLocaleString()} → ${maxOid.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | errs ${errors}   `,
    );

    if (maxOid === minOid) break;
    minOid = maxOid;
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
