#!/usr/bin/env tsx
/**
 * MXRE — Portage County, OH Assessor Parcel Ingest
 *
 * Source: Portage County GIS — TaxParcel_GE FeatureServer
 *   https://services.portageco.com/arcgis/rest/services/TaxParcel_GE/FeatureServer/0
 *   ~130K+ parcels
 *
 * Fields: PARCELID, OwnName, mlocStrNo, mlocStrName, mlocStrSuffix,
 *         mlocCity, mlocZipCode, MKT_Total_Value, MKT_Land_Value, OBJECTID
 *
 * Usage:
 *   npx tsx scripts/ingest-portage-oh.ts
 *   npx tsx scripts/ingest-portage-oh.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://services.portageco.com/arcgis/rest/services/TaxParcel_GE/FeatureServer/0";
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
  "OwnName",
  "OwnFirstName",
  "OwnLastName",
  "mlocStrNo",
  "mlocStrName",
  "mlocStrSuffix",
  "mlocCity",
  "mlocZipCode",
  "MKT_Total_Value",
  "MKT_Land_Value",
  "MKT_Impr_Value",
  "AnnualTax",
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

  console.log("MXRE — Portage County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Portage").eq("state_code", "OH").single();
  if (!county) { console.error("Portage County not in DB"); process.exit(1); }
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

      // Assemble address from parts
      const strNo = String(f.mlocStrNo || "").trim();
      const strName = String(f.mlocStrName || "").trim();
      const strSuffix = String(f.mlocStrSuffix || "").trim();
      const address = [strNo, strName, strSuffix].filter(Boolean).join(" ").toUpperCase();
      if (!address) { dupes++; continue; }

      const city = String(f.mlocCity || "").trim().toUpperCase();
      const zip = String(f.mlocZipCode || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { dupes++; continue; }

      // Owner name: try full OwnName first, then combine OwnFirstName + OwnLastName
      let ownerName = String(f.OwnName || "").trim();
      if (!ownerName) {
        const first = String(f.OwnFirstName || "").trim();
        const last = String(f.OwnLastName || "").trim();
        ownerName = [last, first].filter(Boolean).join(", ");
      }

      const marketValue = parseNum(f.MKT_Total_Value);
      const assessedValue = marketValue ? Math.round(marketValue * 0.35) : null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName || null,
        address,
        city: city || "RAVENNA",
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.MKT_Land_Value),
        property_tax: parseNum(f.AnnualTax),
        property_type: "residential",
        source: "portage_oh_auditor_gis",
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
