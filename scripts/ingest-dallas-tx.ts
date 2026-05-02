#!/usr/bin/env tsx
/**
 * MXRE — Dallas County, TX Assessor Parcel Ingest
 *
 * Source: Dallas City Hall ArcGIS MapServer
 *   https://egis.dallascityhall.com/arcgis/rest/services/Basemap/DallasTaxParcels/MapServer/0
 *   ~496K parcels, MaxRecordCount=1000 (OBJECTID-based pagination)
 *
 * Fields available:
 *   OBJECTID, ACCT (account/parcel id), GIS_ACCT,
 *   TAXPANAME1, TAXPANAME2 (taxpayer/owner names),
 *   ST_NUM, ST_NAME, ST_TYPE, ST_DIR, UNITID (situs address components),
 *   CITY (situs city), COUNTY,
 *   TAXPAZIP (taxpayer mailing zip — used as best available zip),
 *   TAXPACITY, TAXPASTA (taxpayer mailing city/state),
 *   SPTBCODE (TX state property type code), PROP_CL, BLDG_CL, ResCom
 *
 * NOTE: This GIS layer does not carry appraisal values, land/building values,
 *       year built, or sale date/price.  Those fields are not published in the
 *       public MapServer — only parcel geometry + ownership/address data.
 *       Financial fields will be null in the DB record.
 *
 * TX has no state-set assessment ratio — appraised value = market value.
 *
 * Usage:
 *   npx tsx scripts/ingest-dallas-tx.ts
 *   npx tsx scripts/ingest-dallas-tx.ts --min-objectid=250000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://egis.dallascityhall.com/arcgis/rest/services/Basemap/DallasTaxParcels/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "TX";
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "OBJECTID",
  "ACCT",
  "GIS_ACCT",
  "TAXPANAME1",
  "TAXPANAME2",
  "ST_NUM",
  "ST_NAME",
  "ST_TYPE",
  "ST_DIR",
  "UNITID",
  "CITY",
  "COUNTY",
  "TAXPAZIP",
  "TAXPACITY",
  "TAXPASTA",
  "SPTBCODE",
  "PROP_CL",
  "BLDG_CL",
  "ResCom",
].join(",");

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

/** Compute centroid of the first polygon ring (simple average of vertices). */
function ringCentroid(rings: number[][][]): { lat: number; lon: number } | null {
  if (!rings || rings.length === 0) return null;
  const ring = rings[0];
  if (!ring || ring.length === 0) return null;
  let sumX = 0, sumY = 0;
  for (const [x, y] of ring) {
    sumX += x;
    sumY += y;
  }
  const lon = sumX / ring.length;
  const lat = sumY / ring.length;
  // Sanity check: Dallas is roughly 32–33°N, 96–97°W
  if (lat < 30 || lat > 35 || lon < -100 || lon > -94) return null;
  return { lat: parseFloat(lat.toFixed(7)), lon: parseFloat(lon.toFixed(7)) };
}

/** Classify SPTBCODE / PROP_CL into a standard property_type bucket. */
function classifyPropType(sptbCode: string | null, propCl: string | null, resCom: string | null): string {
  // ResCom: R = residential, C = commercial
  const rc = String(resCom || "").toUpperCase();
  const cl = String(propCl || "").toUpperCase();
  const code = String(sptbCode || "").toUpperCase();

  if (cl.includes("SINGLE FAMILY") || code.startsWith("A1")) return "residential";
  if (cl.includes("MULTI") || cl.includes("APT") || code.startsWith("A4") || code.startsWith("A5")) return "multifamily";
  if (cl.includes("CONDO")) return "condo";
  if (cl.includes("MOBILE HOME")) return "residential";
  if (cl.includes("COMMERCIAL") || cl.includes("RETAIL") || cl.includes("OFFICE") || code.startsWith("F") || code.startsWith("E") || rc === "C") return "commercial";
  if (cl.includes("INDUSTRIAL") || code.startsWith("L")) return "industrial";
  if (cl.includes("EXEMPT") || cl.includes("GOVERNMENT") || cl.includes("PUBLIC") || code.startsWith("X")) return "exempt";
  if (cl.includes("FARM") || cl.includes("AG") || cl.includes("RANCH") || code.startsWith("D")) return "agricultural";
  if (cl.includes("VACANT") || cl.includes("LAND")) return "land";
  if (rc === "R") return "residential";
  return "residential";
}

interface FetchResult {
  features: Array<{ attributes: Record<string, unknown>; geometry?: { rings?: number[][][] } }>;
  count: number;
  maxObjectId: number;
}

async function fetchPage(minObjectId: number): Promise<FetchResult> {
  const where = encodeURIComponent(`OBJECTID > ${minObjectId}`);
  const url =
    `${PARCELS_URL}/query?where=${where}` +
    `&outFields=${encodeURIComponent(FIELDS)}` +
    `&returnGeometry=true&outSR=4326` +
    `&orderByFields=OBJECTID+ASC` +
    `&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(45000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const rawFeatures = (json.features as Array<{
        attributes: Record<string, unknown>;
        geometry?: { rings?: number[][][] };
      }>) || [];

      let maxOid = minObjectId;
      for (const f of rawFeatures) {
        const oid = Number(f.attributes.OBJECTID);
        if (oid > maxOid) maxOid = oid;
      }

      return { features: rawFeatures, count: rawFeatures.length, maxObjectId: maxOid };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0, maxObjectId: minObjectId };
}

async function main() {
  const minOidArg = process.argv.find((a) => a.startsWith("--min-objectid="))?.split("=")[1];
  const startMinOid = minOidArg ? parseInt(minOidArg, 10) : 0;
  const skipExistingScan = process.argv.includes("--skip-existing-scan");

  console.log("MXRE — Dallas County, TX Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Dallas")
    .eq("state_code", "TX")
    .single();
  if (!county) {
    console.error("Dallas County, TX not found in DB");
    process.exit(1);
  }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs to skip dupes. On resume runs, this can be skipped
  // because the DB upsert already handles existing parcel_ids.
  const existing = new Set<string>();
  if (!skipExistingScan) {
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
  } else {
    console.log("  Existing parcel scan skipped; relying on DB upsert for duplicates\n");
  }

  let inserted = 0, dupes = 0, errors = 0, skipped = 0;
  let minObjectId = startMinOid;
  let totalFetched = 0;

  while (true) {
    const { features, count, maxObjectId } = await fetchPage(minObjectId);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const attrs = f.attributes;
      const pin = String(attrs.ACCT || attrs.GIS_ACCT || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // Build situs address from component fields
      const addrParts = [
        String(attrs.ST_NUM || "").trim(),
        String(attrs.ST_DIR || "").trim(),
        String(attrs.ST_NAME || "").trim(),
        String(attrs.ST_TYPE || "").trim(),
      ].filter(Boolean);
      const unitId = String(attrs.UNITID || "").trim();
      if (unitId) addrParts.push(unitId);
      const address = addrParts.join(" ").toUpperCase();
      if (!address || address === "0") { skipped++; continue; }

      const city = String(attrs.CITY || "DALLAS").trim().toUpperCase();

      // TAXPAZIP is the taxpayer mailing zip — best available zip for this layer
      const rawZip = String(attrs.TAXPAZIP || "").trim().replace(/\D/g, "").slice(0, 5);
      const zip = rawZip || null;

      // Owner names
      const ownerName1 = String(attrs.TAXPANAME1 || "").trim();
      const ownerName2 = String(attrs.TAXPANAME2 || "").trim();
      const ownerName = [ownerName1, ownerName2].filter(Boolean).join(" / ") || null;

      // Geometry centroid (polygon rings in WGS84)
      const centroid = f.geometry?.rings ? ringCentroid(f.geometry.rings) : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        // Financial fields not available in this GIS layer
        market_value: null,
        assessed_value: null,
        year_built: null,
        last_sale_date: null,
        last_sale_price: null,
        latitude: centroid?.lat ?? null,
        longitude: centroid?.lon ?? null,
        property_type: classifyPropType(
          attrs.SPTBCODE as string | null,
          attrs.PROP_CL as string | null,
          attrs.ResCom as string | null,
        ),
        source: "dallas_tx_egis",
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
      `\r  oid>${minObjectId.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skip ${skipped} | errs ${errors}   `,
    );

    minObjectId = maxObjectId;
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
