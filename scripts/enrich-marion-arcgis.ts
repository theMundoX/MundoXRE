#!/usr/bin/env tsx
/**
 * Marion County ArcGIS enrichment pass.
 *
 * The 345K "in-data-harvest-parcels" rows have 18-digit Indiana state parcel IDs
 * but no owner_name / market_value / property_use. The xmaps.indy.gov ArcGIS layer
 * has all of that. We convert each 18-digit ID to STATEPARCELNUMBER format and
 * batch-query ArcGIS (50 parcels per request).
 *
 * Also patches 236K "assessor" rows missing market_value (those use 7-digit PARCEL_C).
 *
 * Uses the Supabase JS client (PostgREST) for reads — this handles large tables correctly
 * via keyset pagination on the PK index. Raw /pg/query only for multi-statement UPDATEs.
 *
 * Usage:
 *   npx tsx scripts/enrich-marion-arcgis.ts
 *   npx tsx scripts/enrich-marion-arcgis.ts --source=in-data-harvest-parcels
 *   npx tsx scripts/enrich-marion-arcgis.ts --source=assessor
 *   npx tsx scripts/enrich-marion-arcgis.ts --limit=5000 --dry-run
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const args = process.argv.slice(2);
const getArg = (n: string) => args.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => args.includes(`--${n}`);

const ARCGIS_BASE = "https://xmaps.indy.gov/arcgis/rest/services/Common/CommonlyUsedLayers/MapServer/0/query";
const COUNTY_ID  = 797583;
const BATCH_DB   = 500;   // properties per PostgREST page
const BATCH_GIS  = 50;    // parcel IDs per ArcGIS IN() clause
const DRY_RUN    = hasFlag("dry-run");
const LIMIT      = getArg("limit") ? parseInt(getArg("limit")!, 10) : Infinity;
const SOURCE_FILTER = getArg("source");

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pgWrite(query: string): Promise<void> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  await res.json();
}

/**
 * Convert 18-digit Indiana state parcel number to STATEPARCELNUMBER format.
 * "491104180012000901" → "49-11-04-180-012.000-901"
 * Segment widths: 2-2-2-3-3-3-3 = 18 chars
 */
function toStateParcelNumber(id18: string): string | null {
  if (!id18 || id18.length !== 18) return null;
  return `${id18.slice(0,2)}-${id18.slice(2,4)}-${id18.slice(4,6)}-${id18.slice(6,9)}-${id18.slice(9,12)}.${id18.slice(12,15)}-${id18.slice(15,18)}`;
}

async function fetchByStateParcelNumbers(spns: string[]): Promise<Map<string, any>> {
  if (spns.length === 0) return new Map();
  const inClause = spns.map(s => `'${s}'`).join(",");
  const url = `${ARCGIS_BASE}?where=STATEPARCELNUMBER+IN+(${encodeURIComponent(inClause)})` +
    `&outFields=PARCEL_C,STATEPARCELNUMBER,FULLOWNERNAME,ASSESSORYEAR_TOTALAV,ASSESSORYEAR_LANDTOTAL,ASSESSORYEAR_IMPTOTAL,ESTSQFT,ACREAGE,CAMAPARCELID,PROPERTY_CLASS,PROPERTY_SUB_CLASS_DESCRIPTION,OWNERADDRESS,OWNERCITY,OWNERSTATE,OWNERZIP,LEGAL_DESCRIPTION_,CITY,ZIPCODE` +
    `&returnGeometry=false&f=json`;
  const res = await fetch(url, { headers: { "User-Agent": "MXRE-Ingest/1.0" } });
  if (!res.ok) throw new Error(`ArcGIS ${res.status}`);
  const data = await res.json();
  const map = new Map<string, any>();
  for (const f of data.features ?? []) {
    if (f.attributes?.STATEPARCELNUMBER) map.set(f.attributes.STATEPARCELNUMBER, f.attributes);
  }
  return map;
}

async function fetchByParcelC(parcelCs: string[]): Promise<Map<string, any>> {
  if (parcelCs.length === 0) return new Map();
  const inClause = parcelCs.map(s => `'${s}'`).join(",");
  const url = `${ARCGIS_BASE}?where=PARCEL_C+IN+(${encodeURIComponent(inClause)})` +
    `&outFields=PARCEL_C,FULLOWNERNAME,ASSESSORYEAR_TOTALAV,ESTSQFT,CAMAPARCELID,PROPERTY_CLASS,PROPERTY_SUB_CLASS_DESCRIPTION,OWNERADDRESS,OWNERCITY,OWNERSTATE,OWNERZIP` +
    `&returnGeometry=false&f=json`;
  const res = await fetch(url, { headers: { "User-Agent": "MXRE-Ingest/1.0" } });
  if (!res.ok) throw new Error(`ArcGIS ${res.status}`);
  const data = await res.json();
  const map = new Map<string, any>();
  for (const f of data.features ?? []) {
    if (f.attributes?.PARCEL_C) map.set(String(f.attributes.PARCEL_C), f.attributes);
  }
  return map;
}

function buildUpdateSQL(id: number, attrs: any): string | null {
  const sets: string[] = [];
  const esc = (s: string) => s.replace(/'/g, "''").trim();
  if (attrs.FULLOWNERNAME?.trim()) sets.push(`owner_name='${esc(attrs.FULLOWNERNAME)}'`);
  if (attrs.ASSESSORYEAR_TOTALAV != null && attrs.ASSESSORYEAR_TOTALAV > 0) {
    const v = Math.round(attrs.ASSESSORYEAR_TOTALAV);
    sets.push(`market_value=${v}`, `assessed_value=${v}`);
  }
  if (attrs.ASSESSORYEAR_LANDTOTAL != null && attrs.ASSESSORYEAR_LANDTOTAL > 0)
    sets.push(`land_value=${Math.round(attrs.ASSESSORYEAR_LANDTOTAL)}`, `appraised_land=${Math.round(attrs.ASSESSORYEAR_LANDTOTAL)}`);
  if (attrs.ASSESSORYEAR_IMPTOTAL != null && attrs.ASSESSORYEAR_IMPTOTAL > 0)
    sets.push(`appraised_building=${Math.round(attrs.ASSESSORYEAR_IMPTOTAL)}`);
  // ESTSQFT is lot square footage in ArcGIS — NOT building sqft. Map to lot_sqft.
  if (attrs.ESTSQFT != null && attrs.ESTSQFT > 0) sets.push(`lot_sqft=${attrs.ESTSQFT}`);
  if (attrs.ACREAGE != null && attrs.ACREAGE !== "" && parseFloat(attrs.ACREAGE) > 0)
    sets.push(`lot_acres=${parseFloat(attrs.ACREAGE)}`);
  if (attrs.PROPERTY_SUB_CLASS_DESCRIPTION) sets.push(`property_use='${esc(attrs.PROPERTY_SUB_CLASS_DESCRIPTION)}'`);
  if (attrs.PROPERTY_CLASS) sets.push(`property_class='${esc(attrs.PROPERTY_CLASS)}'`);
  if (attrs.OWNERADDRESS) { sets.push(`mail_address='${esc(attrs.OWNERADDRESS)}'`, `mailing_address='${esc(attrs.OWNERADDRESS)}'`); }
  if (attrs.OWNERCITY)    { sets.push(`mail_city='${esc(attrs.OWNERCITY)}'`, `mailing_city='${esc(attrs.OWNERCITY)}'`); }
  if (attrs.OWNERSTATE)   { sets.push(`mail_state='${esc(attrs.OWNERSTATE)}'`, `mailing_state='${esc(attrs.OWNERSTATE)}'`); }
  if (attrs.OWNERZIP)     { sets.push(`mail_zip='${esc(attrs.OWNERZIP)}'`, `mailing_zip='${esc(attrs.OWNERZIP)}'`); }
  if (attrs.LEGAL_DESCRIPTION_) sets.push(`legal_description='${esc(attrs.LEGAL_DESCRIPTION_)}'`);
  if (attrs.CAMAPARCELID != null) sets.push(`apn_formatted='${attrs.CAMAPARCELID}'`);
  // Property city/zip from ArcGIS — only write when zip is a valid Indiana code (46xxx)
  if (attrs.ZIPCODE && String(attrs.ZIPCODE).startsWith("46"))
    sets.push(`zip='${esc(String(attrs.ZIPCODE).slice(0,5))}'`);
  if (attrs.CITY?.trim() && attrs.ZIPCODE && String(attrs.ZIPCODE).startsWith("46"))
    sets.push(`city='${esc(attrs.CITY.trim().toUpperCase())}'`);
  // Derive owner flags from name pattern and mail state
  if (attrs.FULLOWNERNAME) {
    const n = attrs.FULLOWNERNAME.toUpperCase();
    const isCorp = /\b(LLC|INC|CORP|LP|LLP|TRUST|ASSOC|FOUNDATION|GROUP|HOLDINGS|PARTNERS|PROPERTIES|REALTY|INVESTMENT|VENTURES|ENTERPRISES)\b/.test(n);
    if (isCorp) sets.push(`corporate_owned=true`, `absentee_owner=true`, `owner_occupied=false`);
  }
  if (attrs.OWNERSTATE) {
    const mailState = (attrs.OWNERSTATE || "").trim().toUpperCase();
    if (mailState === "IN") {
      sets.push(`in_state_absentee=true`);
    } else if (mailState && mailState !== "IN") {
      sets.push(`absentee_owner=true`, `in_state_absentee=false`);
    }
  }
  if (sets.length === 0) return null;
  sets.push(`updated_at=now()`);
  return `UPDATE properties SET ${sets.join(",")} WHERE id=${id}`;
}

type Stats = { processed: number; updated: number; noMatch: number; errors: number };

async function enrichRows(rows: any[], source: string, stats: Stats) {
  const isStateFormat = source === "in-data-harvest-parcels";
  const idToKey = new Map<string, number>();

  for (const r of rows) {
    const key = isStateFormat ? toStateParcelNumber(r.parcel_id) : String(r.parcel_id);
    if (key) idToKey.set(key, r.id);
  }

  const keys = [...idToKey.keys()];
  for (let i = 0; i < keys.length; i += BATCH_GIS) {
    const chunk = keys.slice(i, i + BATCH_GIS);
    try {
      const attrMap = isStateFormat
        ? await fetchByStateParcelNumbers(chunk)
        : await fetchByParcelC(chunk);

      const sqlBatch: string[] = [];
      for (const key of chunk) {
        const attrs = attrMap.get(key);
        if (!attrs) { stats.noMatch++; continue; }
        const sql = buildUpdateSQL(idToKey.get(key)!, attrs);
        if (sql) sqlBatch.push(sql);
      }

      if (!DRY_RUN && sqlBatch.length > 0) {
        await pgWrite(`BEGIN; ${sqlBatch.join("; ")}; COMMIT;`);
      }
      stats.updated += sqlBatch.length;
    } catch (e) {
      stats.errors++;
      console.error(`  [${source}] chunk error: ${(e as Error).message}`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

async function main() {
  console.log("MXRE — Marion County ArcGIS enrichment pass");
  console.log(`  County ID: ${COUNTY_ID} | Dry run: ${DRY_RUN} | Limit: ${LIMIT === Infinity ? "all" : LIMIT}`);
  if (SOURCE_FILTER) console.log(`  Source filter: ${SOURCE_FILTER}`);
  console.log();

  const activeSources = SOURCE_FILTER
    ? [SOURCE_FILTER]
    : ["in-data-harvest-parcels", "assessor"];

  const stats: Record<string, Stats> = {};
  for (const s of activeSources) stats[s] = { processed: 0, updated: 0, noMatch: 0, errors: 0 };
  let totalScanned = 0;
  let lastId = 0;

  // Use Supabase JS client (PostgREST) with keyset pagination — handles large tables well
  while (totalScanned < LIMIT) {
    const pageSize = Math.min(BATCH_DB, LIMIT - totalScanned);

    let query = db.from("properties")
      .select("id, parcel_id, source, owner_name, market_value")
      .eq("county_id", COUNTY_ID)
      .gt("id", lastId)
      .order("id", { ascending: true })
      .limit(pageSize);

    // Optionally filter to specific source to skip irrelevant rows at DB level
    if (SOURCE_FILTER) query = query.eq("source", SOURCE_FILTER);

    const { data: rows, error } = await query;
    if (error) throw new Error(`Supabase: ${error.message}`);
    if (!rows || rows.length === 0) break;

    lastId = rows[rows.length - 1].id;
    totalScanned += rows.length;

    // Route rows to enrichment buckets
    const buckets: Record<string, any[]> = {};
    for (const s of activeSources) buckets[s] = [];

    for (const r of rows) {
      if (r.source === "in-data-harvest-parcels" && r.owner_name == null)
        buckets["in-data-harvest-parcels"]?.push(r);
      else if (r.source === "assessor" && r.market_value == null)
        buckets["assessor"]?.push(r);
    }

    for (const source of activeSources) {
      const batch = buckets[source];
      if (!batch || batch.length === 0) continue;
      stats[source].processed += batch.length;
      await enrichRows(batch, source, stats[source]);
    }

    if (totalScanned % 10000 === 0 || rows.length < pageSize) {
      const parts = activeSources.map(s =>
        `${s}: ${stats[s].processed.toLocaleString()} enriched / ${stats[s].updated.toLocaleString()} updated`
      );
      console.log(`  Scanned ${totalScanned.toLocaleString()} | ${parts.join(" | ")}`);
    }
  }

  console.log(`\nDone. Scanned ${totalScanned.toLocaleString()} Marion properties.`);
  for (const s of activeSources) {
    const st = stats[s];
    console.log(`  [${s}] processed=${st.processed.toLocaleString()} updated=${st.updated.toLocaleString()} no-match=${st.noMatch.toLocaleString()} errors=${st.errors}`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
