#!/usr/bin/env tsx
/**
 * MXRE — Dakota County, MN Assessor Parcel Ingest
 *
 * Source: Dakota County GIS — DC_OL_DCPIAdvanced MapServer layer 9 (Tax Parcels)
 *   https://gis2.co.dakota.mn.us/arcgis/rest/services/DC_OL_DCPIAdvanced/MapServer/9
 *   ~180K parcels, ArcGIS Server, OBJECTID-based pagination (maxRecordCount: 1000)
 *
 * Key fields:
 *   TAXPIN           — parcel tax PIN identifier
 *   PIN              — alternate parcel PIN (fallback)
 *   FULLNAME         — owner name (full combined name)
 *   JOINT_OWNER      — joint owner name (second owner)
 *   OWN_ADD_L1       — owner mailing address line 1
 *   OWN_ADD_L2       — owner mailing address line 2
 *   OWN_ADD_L3       — owner mailing address line 3 (city/state/zip)
 *   SITEADDRESS      — full situs address string (e.g. "123 MAIN ST APPLE VALLEY MN")
 *   PHOUSE           — situs house number
 *   PSTREET          — situs street name
 *   MUNICIPALITY     — situs municipality/city name
 *   P_CITY_ST_ZIP    — situs city+state+ZIP combined string (parse ZIP from end)
 *   TOTALVAL         — total EMV (Estimated Market Value); MN assessed = 100%
 *   LANDVAL          — land portion of EMV
 *   BLDGVAL          — building portion of EMV
 *   SALE_DATE        — last sale date (epoch ms timestamp)
 *   SALE_VALUE       — last sale price
 *   HOMESTEAD        — homestead flag ("Y" / "N" or similar)
 *   PROPTYPE         — property type code
 *
 * MN assessed value = 100% of market value (TOTALVAL serves as both).
 *
 * Usage:
 *   npx tsx scripts/ingest-dakota-mn.ts
 *   npx tsx scripts/ingest-dakota-mn.ts --skip=5000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis2.co.dakota.mn.us/arcgis/rest/services/DC_OL_DCPIAdvanced/MapServer/9";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "MN";
const INT_MAX = 2_147_483_647;

const FIELDS = [
  "OBJECTID",
  "TAXPIN",
  "PIN",
  "FULLNAME",
  "JOINT_OWNER",
  "OWN_ADD_L1",
  "OWN_ADD_L2",
  "OWN_ADD_L3",
  "SITEADDRESS",
  "PHOUSE",
  "PSTREET",
  "MUNICIPALITY",
  "P_CITY_ST_ZIP",
  "TOTALVAL",
  "LANDVAL",
  "BLDGVAL",
  "SALE_DATE",
  "SALE_VALUE",
  "HOMESTEAD",
  "PROPTYPE",
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

/**
 * Extract a 5-digit ZIP from a combined "CITY MN 55XXX" or "CITY MN 55XXX-1234" string.
 * Returns null if no valid ZIP found.
 */
function extractZip(cityStateZip: unknown): string | null {
  if (cityStateZip == null) return null;
  const s = String(cityStateZip).trim();
  // Match last 5-digit sequence (optionally followed by -NNNN)
  const m = s.match(/(\d{5})(?:-\d{4})?$/);
  if (!m) return null;
  const zip = m[1];
  return zip.length === 5 ? zip : null;
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

  console.log("MXRE — Dakota County, MN Assessor Parcel Ingest");
  console.log("═".repeat(62));

  // Resolve county record
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Dakota")
    .eq("state_code", "MN")
    .single();
  if (!county) {
    console.error("Dakota County, MN not found in DB — run seed/counties first.");
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
      // TAXPIN is the primary parcel identifier; fall back to PIN
      const pin = String(f.TAXPIN || f.PIN || "").trim();
      if (!pin) { skipped++; continue; }

      // Build situs address — prefer the full SITEADDRESS string, else compose from parts
      let address = String(f.SITEADDRESS || "").trim().toUpperCase();
      if (!address) {
        const houseNum = String(f.PHOUSE   || "").trim();
        const stName   = String(f.PSTREET  || "").trim();
        const parts    = [houseNum, stName].filter(Boolean);
        address        = parts.join(" ").toUpperCase();
      }
      if (!address) { skipped++; continue; }

      // ZIP from P_CITY_ST_ZIP (e.g. "HAMPTON MN 55031")
      const zip = extractZip(f.P_CITY_ST_ZIP);
      if (!zip) { skipped++; continue; }

      const city = String(f.MUNICIPALITY || "").trim().toUpperCase() || null;

      // MN: TOTALVAL = 100% of market value, so market_value === assessed_value
      const marketValue   = parseNum(f.TOTALVAL);
      const assessedValue = marketValue; // MN assessed = 100% of market

      // FULLNAME is the primary owner; JOINT_OWNER is secondary
      const fullName   = String(f.FULLNAME    || "").trim() || null;
      const jointOwner = String(f.JOINT_OWNER || "").trim() || null;
      const ownerName =
        fullName && jointOwner
          ? `${fullName} ${jointOwner}`.trim()
          : fullName ?? jointOwner ?? null;

      // Owner mailing address: combine available lines
      const ownLine1 = String(f.OWN_ADD_L1 || "").trim();
      const ownLine2 = String(f.OWN_ADD_L2 || "").trim();
      const ownerAddr = [ownLine1, ownLine2].filter(Boolean).join(", ").toUpperCase() || null;

      // Homestead flag — Dakota uses "Y"/"N" or similar
      const homesteadRaw = String(f.HOMESTEAD || "").trim().toUpperCase();
      // Dakota doesn't have a separate tax-exempt field; treat non-homestead commercial
      // as non-exempt — store homestead as a proxy boolean only (no tax_exempt column impact)
      // We map HOMESTEAD "Y" → tax_exempt false (homestead = owner-occupied = NOT exempt)
      // Actual tax-exempt parcels are government/non-profit; not distinguishable from this field
      // alone. We leave tax_exempt null rather than guessing.
      const isTaxExempt: boolean | null = null; // Dakota layer lacks a dedicated exempt flag

      const isDupe = existing.has(pin);
      if (!isDupe) existing.add(pin);

      batch.push({
        county_id:       COUNTY_ID,
        parcel_id:       pin,
        owner_name:      ownerName,
        owner_address:   ownerAddr,
        address,
        city,
        state_code:      STATE_CODE,
        zip,
        market_value:    marketValue,
        assessed_value:  assessedValue,
        land_value:      parseNum(f.LANDVAL),
        building_value:  parseNum(f.BLDGVAL),
        last_sale_price: parseNum(f.SALE_VALUE),
        last_sale_date:  parseDate(f.SALE_DATE),
        tax_exempt:      isTaxExempt,
        source:          "dakota_mn_gis",
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
