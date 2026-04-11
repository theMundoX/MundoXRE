#!/usr/bin/env tsx
/**
 * MXRE — Hamilton County, TN Assessor Parcel Ingest (Chattanooga)
 *
 * Source: Hamilton County GIS — Live_Parcels MapServer
 *   https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/0
 *   ~168K parcels, ArcGIS v10.61, OBJECTID-based pagination (supportsPagination: true)
 *
 * Fields: PBA_NUM (parcel ID), GISLINK, OWNERNAME1/2, ADDRESS, MACITY, MAZIP,
 *         APPVALUE (appraised), ASSVALUE (assessed = 25% of appraised for residential),
 *         LANDVALUE, BUILDVALUE, SALE1-4DATE/CONSD, PROPTYPE, LUCODE
 *
 * TN: assessed_value = 25% of appraised value (residential)
 *
 * Usage:
 *   npx tsx scripts/ingest-hamilton-tn.ts
 *   npx tsx scripts/ingest-hamilton-tn.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/0";
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
  "PBA_NUM",
  "GISLINK",
  "OWNERNAME1",
  "OWNERNAME2",
  "ADDRESS",
  "MACITY",
  "MAZIP",
  "APPVALUE",
  "ASSVALUE",
  "LANDVALUE",
  "BUILDVALUE",
  "SALE1DATE",
  "SALE1CONSD",
  "SALE2DATE",
  "SALE2CONSD",
  "SALE3DATE",
  "SALE3CONSD",
  "SALE4DATE",
  "SALE4CONSD",
  "PROPTYPE",
  "LUCODE",
  "CURRENTUSE",
  "EXEMPTCODE",
  "CALCACRES",
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

/** Pick the most recent non-zero sale from up to 4 sale slots */
function bestSale(f: Record<string, unknown>): { price: number | null; date: string | null } {
  const slots = [
    { price: parseNum(f.SALE1CONSD), date: parseDate(f.SALE1DATE) },
    { price: parseNum(f.SALE2CONSD), date: parseDate(f.SALE2DATE) },
    { price: parseNum(f.SALE3CONSD), date: parseDate(f.SALE3DATE) },
    { price: parseNum(f.SALE4CONSD), date: parseDate(f.SALE4DATE) },
  ].filter((s) => s.price && s.price > 1000);

  if (slots.length === 0) return { price: null, date: null };
  // Sort by date descending (most recent first); nulls go last
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

  console.log("MXRE — Hamilton County, TN Assessor Parcel Ingest (Chattanooga)");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Hamilton")
    .eq("state_code", "TN")
    .single();
  if (!county) { console.error("Hamilton County, TN not in DB"); process.exit(1); }
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
      // PBA_NUM is the primary parcel number; GISLINK is the GIS key
      const pin = String(f.PBA_NUM || f.GISLINK || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.ADDRESS || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.MACITY || "CHATTANOOGA").trim().toUpperCase();
      const zip = String(f.MAZIP || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // Hamilton County fields: APPVALUE = appraised, ASSVALUE = assessed (25% of appraised)
      const appraisedValue = parseNum(f.APPVALUE);
      const assessedValue = parseNum(f.ASSVALUE) ??
        (appraisedValue ? Math.round(appraisedValue * 0.25) : null);
      // Derive market value: appraised IS market value in TN
      const marketValue = appraisedValue ??
        (assessedValue ? assessedValue * 4 : null);

      const ownerName = [f.OWNERNAME1, f.OWNERNAME2]
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
        land_value: parseNum(f.LANDVALUE),
        improvement_value: parseNum(f.BUILDVALUE),
        last_sale_price: lastSalePrice,
        last_sale_date: lastSaleDate,
        property_type: String(f.PROPTYPE || "").trim() || null,
        source: "hamilton_tn_gis",
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
