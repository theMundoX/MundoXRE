#!/usr/bin/env tsx
/**
 * MXRE — Hennepin County, MN Assessor Parcel Ingest (Minneapolis)
 *
 * Source: Hennepin County GIS — LAND_PROPERTY MapServer layer 2
 *   https://gis.hennepin.us/arcgis/rest/services/HennepinData/LAND_PROPERTY/MapServer/2
 *   ~600K parcels, ArcGIS Server, OBJECTID-based pagination (maxRecordCount: 2000)
 *
 * Key fields:
 *   COUNTY_PIN   — parcel ID (22 chars)
 *   OWNER_NAME   — primary owner
 *   OWNER_MORE   — secondary owner / care-of
 *   OWN_ADD_L1   — owner mailing address line 1
 *   ANUMBER      — house number
 *   ST_NAME      — street name
 *   ST_POS_TYP   — street type (AVE, ST, BLVD, …)
 *   ZIP          — zip code
 *   EMV_TOTAL    — estimated market value (Int); MN assessed = 100% of market
 *   EMV_LAND     — land portion of EMV
 *   EMV_BLDG     — building portion of EMV
 *   SALE_DATE    — last sale date (epoch ms timestamp)
 *   SALE_VALUE   — last sale price
 *   TAX_EXEMPT   — tax-exempt flag (String)
 *
 * MN assessed value = 100% of market value (EMV_TOTAL serves as both).
 *
 * Usage:
 *   npx tsx scripts/ingest-hennepin-mn.ts
 *   npx tsx scripts/ingest-hennepin-mn.ts --skip=12000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis.hennepin.us/arcgis/rest/services/HennepinData/LAND_PROPERTY/MapServer/2";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "MN";
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "OBJECTID",
  "COUNTY_PIN",
  "OWNER_NAME",
  "OWNER_MORE",
  "OWN_ADD_L1",
  "ANUMBER",
  "ST_NAME",
  "ST_POS_TYP",
  "ZIP",
  "EMV_TOTAL",
  "EMV_LAND",
  "EMV_BLDG",
  "SALE_DATE",
  "SALE_VALUE",
  "TAX_EXEMPT",
].join(",");

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

/**
 * Convert an ArcGIS epoch-ms timestamp to a YYYY-MM-DD string.
 * Returns null if the value is absent or clearly invalid (pre-1800).
 */
function parseDate(v: unknown): string | null {
  if (v == null) return null;
  const ms = typeof v === "number" ? v : parseFloat(String(v));
  if (isNaN(ms) || ms <= 0) return null;
  const d = new Date(ms);
  if (d.getFullYear() < 1800) return null;
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

interface PageResult {
  features: Record<string, unknown>[];
  maxOid: number;
}

async function fetchPage(minOid: number): Promise<PageResult> {
  const url =
    `${PARCELS_URL}/query?where=${encodeURIComponent(`OBJECTID > ${minOid}`)}` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultRecordCount=${PAGE_SIZE}&orderByFields=OBJECTID+ASC&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = (
        (json.features as Array<{ attributes: Record<string, unknown> }>) || []
      ).map((f) => f.attributes);

      const maxOid = features.reduce((m, f) => {
        const oid = f["OBJECTID"] as number;
        return oid > m ? oid : m;
      }, minOid);

      return { features, maxOid };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      const delay = 2000 * (attempt + 1);
      console.warn(`\n  Attempt ${attempt + 1} failed, retrying in ${delay / 1000}s… ${String(err)}`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  return { features: [], maxOid: minOid };
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Hennepin County, MN Assessor Parcel Ingest (Minneapolis)");
  console.log("═".repeat(62));

  // Resolve county record
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Hennepin")
    .eq("state_code", "MN")
    .single();
  if (!county) {
    console.error("Hennepin County, MN not found in DB — run seed/counties first.");
    process.exit(1);
  }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs to detect dupes without re-upserting
  console.log("  Loading existing parcel IDs…");
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

  let inserted = 0,
    updated = 0,
    dupes = 0,
    errors = 0,
    skipped = 0,
    minOid = skipOid,
    totalFetched = 0;

  while (true) {
    const { features, maxOid } = await fetchPage(minOid);
    if (features.length === 0) break;
    totalFetched += features.length;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // COUNTY_PIN is the canonical parcel identifier (22-char string)
      const pin = String(f.COUNTY_PIN || "").trim();
      if (!pin) { skipped++; continue; }

      // Build situs address from component fields
      const houseNum = String(f.ANUMBER || "").trim();
      const stName   = String(f.ST_NAME   || "").trim();
      const stType   = String(f.ST_POS_TYP || "").trim();
      const addressParts = [houseNum, stName, stType].filter(Boolean);
      if (addressParts.length === 0) { skipped++; continue; }
      const address = addressParts.join(" ").toUpperCase();

      const zip = String(f.ZIP || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // MN: EMV_TOTAL = 100% of market value, so market_value === assessed_value
      const marketValue = parseNum(f.EMV_TOTAL);
      const assessedValue = marketValue; // MN assessed = 100% of market

      const ownerName = String(f.OWNER_NAME || "").trim() || null;
      const ownerMore = String(f.OWNER_MORE || "").trim() || null;
      // Combine owner lines when both are present
      const fullOwner =
        ownerName && ownerMore
          ? `${ownerName} ${ownerMore}`.trim()
          : ownerName ?? ownerMore ?? null;

      const taxExempt = String(f.TAX_EXEMPT || "").trim().toUpperCase();
      const isTaxExempt = taxExempt === "Y" || taxExempt === "YES" || taxExempt === "TRUE";

      const isDupe = existing.has(pin);
      if (!isDupe) existing.add(pin);

      batch.push({
        county_id:       COUNTY_ID,
        parcel_id:       pin,
        owner_name:      fullOwner,
        owner_address:   String(f.OWN_ADD_L1 || "").trim().toUpperCase() || null,
        address,
        city:            "MINNEAPOLIS",
        state_code:      STATE_CODE,
        zip,
        market_value:    marketValue,
        assessed_value:  assessedValue,
        land_value:      parseNum(f.EMV_LAND),
        building_value:  parseNum(f.EMV_BLDG),
        last_sale_price: parseNum(f.SALE_VALUE),
        last_sale_date:  parseDate(f.SALE_DATE),
        tax_exempt:      isTaxExempt,
        source:          "hennepin_mn_gis",
      });

      if (isDupe) dupes++;
    }

    // Upsert in chunks
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db
        .from("properties")
        .upsert(chunk, { onConflict: "county_id,parcel_id" });

      if (error) {
        // Fall back to row-by-row to isolate bad records
        for (const record of chunk) {
          const { error: e2 } = await db
            .from("properties")
            .upsert(record, { onConflict: "county_id,parcel_id" });
          if (e2) {
            if (errors < 5) console.error(`\n  Error (${record.parcel_id}): ${JSON.stringify(e2).slice(0, 160)}`);
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
      `\r  OID ${minOid.toLocaleString()} → ${maxOid.toLocaleString()}` +
        ` | fetched ${totalFetched.toLocaleString()}` +
        ` | upserted ${inserted.toLocaleString()}` +
        ` | dupes ${dupes.toLocaleString()}` +
        ` | skip ${skipped}` +
        ` | errs ${errors}   `,
    );

    if (maxOid === minOid) break; // No progress — end of data
    minOid = maxOid;
  }

  console.log(`\n\n${"═".repeat(62)}`);
  console.log(
    `TOTAL: ${inserted.toLocaleString()} upserted, ${dupes.toLocaleString()} existing, ` +
      `${skipped} skipped, ${errors} errors`,
  );
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
