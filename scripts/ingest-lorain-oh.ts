#!/usr/bin/env tsx
/**
 * MXRE — Lorain County, OH Assessor Parcel Ingest
 *
 * Source: Lorain County GIS ArcGIS Online — 2025 Ownership Parcels Public View
 *   https://services1.arcgis.com/vGBb7WYV10mOJRNM/arcgis/rest/services/OwnershipParcels_2025_Public_View/FeatureServer/1
 *   ~202,027 parcels, MaxRecordCount=2000
 *
 * Fields: PARCELID, OWNERNME1/2, SITEADDRESS, PSTLCITY, PSTLZIP5,
 *         MARKETTOTAL, MARKETLAND, CNTASSDVAL, LASTSALEDATE, LASTSALEPRICE,
 *         BEDROOMS, BATHS, RESYRBLT, RESFLRAREA
 *
 * Usage:
 *   npx tsx scripts/ingest-lorain-oh.ts
 *   npx tsx scripts/ingest-lorain-oh.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://services1.arcgis.com/vGBb7WYV10mOJRNM/arcgis/rest/services/OwnershipParcels_2025_Public_View/FeatureServer/1";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "OH";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

const FIELDS = [
  "OBJECTID",
  "PARCELID",
  "OWNERNME1",
  "OWNERNME2",
  "SITEADDRESS",
  "PSTLCITY",
  "PSTLZIP5",
  "MARKETTOTAL",
  "MARKETLAND",
  "CNTASSDVAL",
  "LASTSALEDATE",
  "LASTSALEPRICE",
  "BEDROOMS",
  "BATHS",
  "RESYRBLT",
  "RESFLRAREA",
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

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Lorain County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Lorain").eq("state_code", "OH").single();
  if (!county) { console.error("Lorain County not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

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
      const pin = String(f.PARCELID || "").trim();
      if (!pin) continue;
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.SITEADDRESS || "").trim().toUpperCase();
      if (!address) { dupes++; continue; }

      const city = String(f.PSTLCITY || "").trim().toUpperCase();
      const zip = String(f.PSTLZIP5 || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { dupes++; continue; }

      const marketValue = parseNum(f.MARKETTOTAL);
      const assessedValue = parseNum(f.CNTASSDVAL) ?? (marketValue ? Math.round(marketValue * 0.35) : null);
      const ownerName = [f.OWNERNME1, f.OWNERNME2].filter(Boolean).map((s) => String(s).trim()).join("; ") || null;

      const lastSaleDate = (typeof f.LASTSALEDATE === "number" && f.LASTSALEDATE > 0)
        ? new Date(f.LASTSALEDATE).toISOString().split("T")[0]
        : null;
      const yearBuilt = f.RESYRBLT && (f.RESYRBLT as number) > 1700 ? (f.RESYRBLT as number) : null;
      const livingSqft = parseNum(f.RESFLRAREA);

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
        land_value: parseNum(f.MARKETLAND),
        last_sale_price: parseNum(f.LASTSALEPRICE),
        last_sale_date: lastSaleDate,
        year_built: yearBuilt,
        living_sqft: livingSqft,
        property_type: "residential",
        source: "lorain_oh_auditor_gis",
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
