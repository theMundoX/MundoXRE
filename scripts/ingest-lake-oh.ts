#!/usr/bin/env tsx
/**
 * MXRE — Lake County, OH Assessor Parcel Ingest
 *
 * Source: Lake County, OH GIS - Auditor Appraised Values
 *   https://gis.lakecountyohio.gov/arcgis/rest/services/Auditor/Parcels_AppraisedValues_Publish/FeatureServer/0
 *   ~114,639 parcels
 *
 * Has: full address with ZIP, owner, appraised value, year built,
 *      sale data, lat/lng (G_X/Y_COORD_DD), property class
 *
 * Usage:
 *   npx tsx scripts/ingest-lake-oh.ts
 *   npx tsx scripts/ingest-lake-oh.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis.lakecountyohio.gov/arcgis/rest/services/Auditor/Parcels_AppraisedValues_Publish/FeatureServer/0";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "OH";
const INT_MAX = 2_147_483_647;

// Lake County, OH county_id (must exist in DB)
let COUNTY_ID = 1741130; // pre-known from DB query

// ─── Property type from Ohio class code ──────────────────────────
function classifyPropClass(cls: number | null): string {
  if (!cls) return "residential";
  const c = Math.floor(cls / 100);
  if (c === 1) return "agricultural";
  if (c === 2) return "single_family"; // Residential SFR
  if (c === 3) return "multifamily";   // 2+ family
  if (c === 4) return "commercial";
  if (c === 5) return "industrial";
  if (c === 6) return "exempt";
  return "residential";
}

// ─── Helpers ──────────────────────────────────────────────────────
function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function parseDate(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "number" && v > 0) {
    const dt = new Date(v);
    if (dt.getFullYear() > 1900 && dt.getFullYear() < 2100) {
      return dt.toISOString().split("T")[0];
    }
  }
  return null;
}

// ─── Fetch page ────────────────────────────────────────────────────
const FIELDS = [
  "OBJECTID",
  "PIN",
  "G_FULLADDRESS",
  "A_USPS_CITY",
  "A_ZIPCODE",
  "A_OWNER_NAME",
  "A_VAL_TOTAL",
  "A_VAL_LAND",
  "A_VAL_BLDG",
  "A_YEAR_BUILT",
  "A_SALE_DATE",
  "A_SALE_AMOUNT",
  "A_PROP_CLASS",
  "G_X_COORD_DD",
  "G_Y_COORD_DD",
  "A_ACRES",
].join(",");

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

// ─── Main ────────────────────────────────────────────────────────
async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Lake County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));

  // Confirm county ID
  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Lake").eq("state_code", "OH").single();
  if (county) COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}`);
  console.log(`Starting OBJECTID: ${skipOid}\n`);

  // Load existing parcel IDs
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

  let inserted = 0, dupes = 0, errors = 0, minOid = skipOid, totalFetched = 0;

  while (true) {
    const { features, maxOid } = await fetchPage(minOid);
    if (features.length === 0) break;
    totalFetched += features.length;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.PIN || "").trim();
      if (!pin) continue;
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.G_FULLADDRESS || "").trim().toUpperCase();
      if (!address) { dupes++; continue; }

      const city = String(f.A_USPS_CITY || "").trim().toUpperCase();
      const zip = String(f.A_ZIPCODE || "").trim();
      if (!zip) { dupes++; continue; } // zip required

      const marketValue = parseNum(f.A_VAL_TOTAL);
      const assessedValue = marketValue ? Math.round(marketValue * 0.35) : null;
      const landValue = parseNum(f.A_VAL_LAND);
      const yearBuilt = f.A_YEAR_BUILT && (f.A_YEAR_BUILT as number) > 1700
        ? (f.A_YEAR_BUILT as number)
        : null;
      const lat = f.G_Y_COORD_DD && (f.G_Y_COORD_DD as number) > 30
        ? (f.G_Y_COORD_DD as number) : null;
      const lng = f.G_X_COORD_DD && (f.G_X_COORD_DD as number) < -70
        ? (f.G_X_COORD_DD as number) : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.A_OWNER_NAME || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: landValue,
        year_built: yearBuilt,
        last_sale_price: parseNum(f.A_SALE_AMOUNT),
        last_sale_date: parseDate(f.A_SALE_DATE),
        property_type: classifyPropClass(f.A_PROP_CLASS as number | null),
        lat,
        lng,
        source: "lake_oh_auditor_gis",
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
          } else {
            inserted++;
          }
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
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
