#!/usr/bin/env tsx
/**
 * MXRE — Harris County, TX Assessor Parcel Ingest (Houston)
 *
 * Source: HCAD ArcGIS MapServer
 *   https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0
 *   ~1.2M parcels, MaxRecordCount=1000 (offset-based)
 *
 * Fields: OBJECTID, HCAD_NUM, owner_name_1, owner_name_2, site_str_num,
 *         site_str_name, site_str_sfx, site_city, site_zip,
 *         total_appraised_val, land_value, bld_value, new_owner_date,
 *         land_use, acct_num
 *
 * TX has no state-set assessment ratio — appraised value = market value
 *
 * Usage:
 *   npx tsx scripts/ingest-harris-tx.ts
 *   npx tsx scripts/ingest-harris-tx.ts --offset=250000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://www.gis.hctx.net/arcgis/rest/services/HCAD/Parcels/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "TX";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

/** Convert an ArcGIS epoch-ms timestamp to "YYYY-MM-DD", or null. */
function parseDate(v: unknown): string | null {
  if (v == null) return null;
  const ms = typeof v === "number" ? v : parseInt(String(v), 10);
  if (isNaN(ms) || ms <= 0) return null;
  const d = new Date(ms);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function classifyLU(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).toUpperCase();
  // HCAD land-use codes: A=residential, B=multifamily, C=commercial, D=industrial, etc.
  if (c.startsWith("A")) return "residential";
  if (c.startsWith("B") || c.includes("MULTI") || c.includes("APT")) return "multifamily";
  if (c.startsWith("C") || c.includes("COMM") || c.includes("RETAIL")) return "commercial";
  if (c.startsWith("D") || c.includes("IND")) return "industrial";
  if (c.startsWith("E") || c.includes("EXE") || c.includes("EXEMPT")) return "exempt";
  if (c.startsWith("F") || c.includes("FARM") || c.includes("AG")) return "agricultural";
  if (c.includes("CONDO")) return "condo";
  return "residential";
}

const FIELDS = [
  "OBJECTID",
  "HCAD_NUM",
  "owner_name_1",
  "owner_name_2",
  "site_str_num",
  "site_str_name",
  "site_str_sfx",
  "site_city",
  "site_zip",
  "total_appraised_val",
  "land_value",
  "bld_value",
  "new_owner_date",
  "land_use",
  "acct_num",
].join(",");

async function fetchPage(offset: number): Promise<{ features: Record<string, unknown>[]; count: number }> {
  const url =
    `${PARCELS_URL}/query?where=1%3D1` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = ((json.features as Array<{ attributes: Record<string, unknown> }>) || []).map(
        (f) => f.attributes,
      );
      return { features, count: features.length };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0 };
}

async function main() {
  const offsetArg = process.argv.find((a) => a.startsWith("--offset="))?.split("=")[1];
  const startOffset = offsetArg ? parseInt(offsetArg, 10) : 0;

  console.log("MXRE — Harris County, TX Assessor Parcel Ingest (Houston)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Harris").eq("state_code", "TX").single();
  if (!county) { console.error("Harris County, TX not in DB"); process.exit(1); }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

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

  let inserted = 0, dupes = 0, errors = 0, skipped = 0;
  let offset = startOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.HCAD_NUM || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // Build site address from component fields
      const addrParts = [
        String(f.site_str_num || "").trim(),
        String(f.site_str_name || "").trim(),
        String(f.site_str_sfx || "").trim(),
      ].filter(Boolean);
      const address = addrParts.join(" ").toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.site_city || "HOUSTON").trim().toUpperCase();
      const zip = String(f.site_zip || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip) { skipped++; continue; }

      // TX: appraised value = market value (no state-set assessment ratio)
      const marketValue = parseNum(f.total_appraised_val);

      // Combine owner names
      const ownerName1 = String(f.owner_name_1 || "").trim();
      const ownerName2 = String(f.owner_name_2 || "").trim();
      const ownerName = [ownerName1, ownerName2].filter(Boolean).join(" / ") || null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: ownerName,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: marketValue, // TX: assessed = appraised
        land_value: parseNum(f.land_value),
        last_sale_date: parseDate(f.new_owner_date),
        last_sale_price: null, // not available from HCAD GIS layer
        property_type: classifyLU(f.land_use as string | null),
        source: "harris_tx_hcad",
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
      `\r  offset ${offset.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skip ${skipped} | errs ${errors}   `,
    );

    offset += count;
    if (count < PAGE_SIZE) break;
  }

  console.log(`\n\n${"═".repeat(60)}`);
  console.log(`TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`);
  console.log("Done.");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
