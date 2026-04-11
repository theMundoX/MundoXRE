#!/usr/bin/env tsx
/**
 * MXRE — Ramsey County, MN Assessor Parcel Ingest (St. Paul)
 *
 * Source: Ramsey County GIS — MapRamseyBackgroundLayers MapServer layer 26
 *   https://maps.co.ramsey.mn.us/arcgis/rest/services/MapRamsey/MapRamseyBackgroundLayers/MapServer/26
 *   ~250K parcels, ArcGIS Server, OBJECTID-based pagination (maxRecordCount: 1000)
 *
 * Key fields:
 *   ParcelID      — parcel identifier (22 chars, MetroGIS standard)
 *   OwnerName     — primary owner name (up to 256 chars)
 *   OwnerName1    — secondary owner / care-of line
 *   OwnerAddress1 — owner mailing address line 1
 *   SiteAddress   — full situs address string (e.g. "2480 7TH AVE E")
 *   SiteCityName  — situs city name
 *   SiteZIP5      — 5-digit ZIP code
 *   EMVTotal      — estimated market value total (Double); MN assessed = 100%
 *   EMVLand       — land portion of EMV
 *   EMVBuilding   — building portion of EMV
 *   SalePrice     — last sale price
 *   LastSaleDate  — last sale date (epoch ms timestamp)
 *   TaxExemptYN   — tax-exempt flag ("Y" / "N")
 *
 * MN assessed value = 100% of market value (EMVTotal serves as both).
 *
 * Usage:
 *   npx tsx scripts/ingest-ramsey-mn.ts
 *   npx tsx scripts/ingest-ramsey-mn.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://maps.co.ramsey.mn.us/arcgis/rest/services/MapRamsey/MapRamseyBackgroundLayers/MapServer/26";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "MN";
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "OBJECTID",
  "ParcelID",
  "OwnerName",
  "OwnerName1",
  "OwnerAddress1",
  "SiteAddress",
  "SiteCityName",
  "SiteZIP5",
  "EMVTotal",
  "EMVLand",
  "EMVBuilding",
  "SalePrice",
  "LastSaleDate",
  "TaxExemptYN",
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

  console.log("MXRE — Ramsey County, MN Assessor Parcel Ingest (St. Paul)");
  console.log("═".repeat(62));

  // Resolve county record
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Ramsey")
    .eq("state_code", "MN")
    .single();
  if (!county) {
    console.error("Ramsey County, MN not found in DB — run seed/counties first.");
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
      // ParcelID is the canonical identifier (22-char MetroGIS standard)
      const pin = String(f.ParcelID || "").trim();
      if (!pin) { skipped++; continue; }

      // SiteAddress is the full situs address string from the source
      const address = String(f.SiteAddress || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const zip = String(f.SiteZIP5 || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      const city = String(f.SiteCityName || "").trim().toUpperCase() || null;

      // MN: EMVTotal = 100% of market value, so market_value === assessed_value
      const marketValue = parseNum(f.EMVTotal);
      const assessedValue = marketValue; // MN assessed = 100% of market

      // Combine owner name lines when both are present
      const ownerName  = String(f.OwnerName  || "").trim() || null;
      const ownerName1 = String(f.OwnerName1 || "").trim() || null;
      const fullOwner =
        ownerName && ownerName1
          ? `${ownerName} ${ownerName1}`.trim()
          : ownerName ?? ownerName1 ?? null;

      const taxExemptRaw = String(f.TaxExemptYN || "").trim().toUpperCase();
      const isTaxExempt = taxExemptRaw === "Y" || taxExemptRaw === "YES" || taxExemptRaw === "TRUE";

      const isDupe = existing.has(pin);
      if (!isDupe) existing.add(pin);

      batch.push({
        county_id:       COUNTY_ID,
        parcel_id:       pin,
        owner_name:      fullOwner,
        owner_address:   String(f.OwnerAddress1 || "").trim().toUpperCase() || null,
        address,
        city,
        state_code:      STATE_CODE,
        zip,
        market_value:    marketValue,
        assessed_value:  assessedValue,
        land_value:      parseNum(f.EMVLand),
        building_value:  parseNum(f.EMVBuilding),
        last_sale_price: parseNum(f.SalePrice),
        last_sale_date:  parseDate(f.LastSaleDate),
        tax_exempt:      isTaxExempt,
        source:          "ramsey_mn_gis",
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
