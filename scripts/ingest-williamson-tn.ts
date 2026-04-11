#!/usr/bin/env tsx
/**
 * MXRE — Williamson County, TN Assessor Parcel Ingest (Franklin / Brentwood)
 *
 * Source: Williamson County GIS — IDT/DataPull Parcels MapServer (layer 4)
 *   http://arcgis2.williamson-tn.org/arcgis/rest/services/IDT/DataPull/MapServer/4
 *   ~100K+ parcels, ArcGIS v10.22, OBJECTID-range pagination (1000 rec max)
 *
 * Fields: parcel_id, owner1/2, ADDRESS, CITY, own_zip,
 *         total_mark (appraised/market), total_asse (assessed = 25% of market),
 *         land_marke, imp_val (improvement value),
 *         considerat/consider_1 (sale price slots), pxfer_date/pxfer_da_1 (transfer dates)
 *
 * TN: assessed_value = 25% of appraised value (residential)
 *     → appraised_value = total_mark  (market value field)
 *     → assessed_value  = total_asse  (or total_mark * 0.25)
 *
 * Usage:
 *   npx tsx scripts/ingest-williamson-tn.ts
 *   npx tsx scripts/ingest-williamson-tn.ts --skip=5000
 *
 * Note: The server uses plain HTTP (not HTTPS); the SSL cert is invalid.
 *       The fetch call uses http:// directly.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Use http:// — the county ArcGIS server has an invalid SSL cert on the HTTPS endpoint
const PARCELS_URL =
  "http://arcgis2.williamson-tn.org/arcgis/rest/services/IDT/DataPull/MapServer/4";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "TN";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function parseDate(v: unknown): string | null {
  if (v == null) return null;
  // ArcGIS returns epoch ms for Date fields
  const ms = typeof v === "number" ? v : parseInt(String(v), 10);
  if (isNaN(ms) || ms <= 0) return null;
  try {
    return new Date(ms).toISOString().slice(0, 10);
  } catch {
    return null;
  }
}

const FIELDS = [
  "OBJECTID",
  "parcel_id",
  "GISLINK",
  "owner1",
  "owner2",
  "ADDRESS",
  "CITY",
  "own_zip",
  "total_mark",
  "total_asse",
  "land_marke",
  "imp_val",
  "considerat",
  "pxfer_date",
  "consider_1",
  "pxfer_da_1",
  "SQFT_ASSES",
  "SUBDIVISION",
  "property_T",
  "CALC_ACRE",
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

/**
 * Williamson County stores two sale slots:
 *   slot 1: considerat + pxfer_date
 *   slot 2: consider_1 + pxfer_da_1
 * Pick the most recent non-trivial sale.
 */
function bestSale(f: Record<string, unknown>): { price: number | null; date: string | null } {
  const slots = [
    { price: parseNum(f.considerat), date: parseDate(f.pxfer_date) },
    { price: parseNum(f.consider_1), date: parseDate(f.pxfer_da_1) },
  ].filter((s) => s.price && s.price > 1000);

  if (slots.length === 0) return { price: null, date: null };
  slots.sort((a, b) => {
    if (!a.date && !b.date) return 0;
    if (!a.date) return 1;
    if (!b.date) return -1;
    return b.date.localeCompare(a.date);
  });
  return slots[0];
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Williamson County, TN Assessor Parcel Ingest (Franklin/Brentwood)");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Williamson")
    .eq("state_code", "TN")
    .single();
  if (!county) { console.error("Williamson County, TN not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

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

  let inserted = 0, dupes = 0, errors = 0, skipped = 0, minOid = skipOid, totalFetched = 0;

  while (true) {
    const { features, maxOid } = await fetchPage(minOid);
    if (features.length === 0) break;
    totalFetched += features.length;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // parcel_id is the assessor parcel number; GISLINK is fallback
      const pin = String(f.parcel_id || f.GISLINK || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.ADDRESS || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.CITY || "FRANKLIN").trim().toUpperCase();
      const zip = String(f.own_zip || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // total_mark = market/appraised value (TN: assessed = 25% of appraised)
      const marketValue = parseNum(f.total_mark);
      const assessedValue = parseNum(f.total_asse) ??
        (marketValue ? Math.round(marketValue * 0.25) : null);

      const ownerName = [f.owner1, f.owner2]
        .filter(Boolean)
        .map((o) => String(o).trim())
        .filter((o) => o.length > 0)[0] ?? null;

      const { price: lastSalePrice, date: lastSaleDate } = bestSale(f);

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
        land_value: parseNum(f.land_marke),
        improvement_value: parseNum(f.imp_val),
        last_sale_price: lastSalePrice,
        last_sale_date: lastSaleDate,
        sqft: parseNum(f.SQFT_ASSES),
        source: "williamson_tn_gis",
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
