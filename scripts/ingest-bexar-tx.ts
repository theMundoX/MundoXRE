#!/usr/bin/env tsx
/**
 * MXRE — Bexar County, TX Assessor Parcel Ingest (San Antonio)
 *
 * Source: Bexar County GIS — Parcels MapServer
 *   https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0
 *   ~600K parcels, MaxRecordCount=1000 (offset-based)
 *
 * Fields: OBJECTID, PropID, Situs, Owner, AddrLn1, AddrCity, Zip,
 *         LandVal, ImprVal, TotVal, YrBlt, PropUse, Acres, GBA, AcctNumb
 *
 * TX appraised value (TotVal) = market value for BCAD purposes.
 * No sale price available from this source.
 *
 * Usage:
 *   npx tsx scripts/ingest-bexar-tx.ts
 *   npx tsx scripts/ingest-bexar-tx.ts --skip=200000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0";
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

/**
 * Classify TX PropUse codes into MXRE property_type values.
 * BCAD uses numeric codes; common ranges:
 *   A = single-family residential (A1, A2 etc. encoded as strings)
 *   B = multifamily
 *   C = vacant commercial
 *   D = acreage/rural land
 *   E = farm/ranch improvements
 *   F = commercial/retail
 *   G = oil/gas/minerals (skip or misc)
 *   L = commercial personal property
 *   M = mobile homes
 *   O = residential inventory
 *   S = special inventory
 *   X = exempt
 */
function classifyPropUse(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).trim().toUpperCase();

  if (c.startsWith("A")) return "residential";
  if (c.startsWith("B")) return "multifamily";
  if (c.startsWith("C") || c.startsWith("L") || c.startsWith("S")) return "commercial";
  if (c.startsWith("D") || c.startsWith("E")) return "agricultural";
  if (c.startsWith("F")) return "commercial";
  if (c.startsWith("M") || c.startsWith("O")) return "residential";
  if (c.startsWith("I")) return "industrial";
  if (c.startsWith("X")) return "exempt";

  // Numeric codes (some BCAD exports use numeric strings)
  const n = parseInt(c, 10);
  if (!isNaN(n)) {
    if (n >= 100 && n < 200) return "residential";
    if (n >= 200 && n < 300) return "multifamily";
    if (n >= 300 && n < 500) return "commercial";
    if (n >= 500 && n < 600) return "industrial";
    if (n >= 600 && n < 700) return "agricultural";
    if (n >= 900) return "exempt";
  }

  return "residential";
}

/**
 * Parse a Bexar County Situs (site address) string into components.
 *
 * The Situs field contains the full address in a single string, e.g.:
 *   "1234 MAIN ST SAN ANTONIO TX 78205"
 *   "456 OAK AVE UNIT 3 CONVERSE TX 78109"
 *
 * Strategy:
 *   1. Strip trailing ZIP (5 digits) and STATE abbreviation (2 letters)
 *   2. Match a known city suffix by checking the end of the remaining string
 *      against a list of common Bexar County cities/communities.
 *   3. Everything before the city match is the street address.
 *   4. Fall back to "SAN ANTONIO" if no city match found.
 */
const BEXAR_CITIES = [
  "SAN ANTONIO",
  "CONVERSE",
  "UNIVERSAL CITY",
  "LIVE OAK",
  "SCHERTZ",
  "CIBOLO",
  "LEON VALLEY",
  "WINDCREST",
  "BALCONES HEIGHTS",
  "CASTLE HILLS",
  "CHINA GROVE",
  "HILL COUNTRY VILLAGE",
  "HOLLYWOOD PARK",
  "KIRBY",
  "OLMOS PARK",
  "SHAVANO PARK",
  "ST HEDWIG",
  "ELMENDORF",
  "GREY FOREST",
  "HELOTES",
  "LYTLE",
  "MACDONA",
  "SOMERSET",
  "VON ORMY",
];

interface SitusComponents {
  address: string;
  city: string;
  zip: string;
}

function parseSitus(raw: unknown): SitusComponents | null {
  if (!raw) return null;
  let s = String(raw).trim().toUpperCase();
  if (!s) return null;

  // Extract trailing ZIP (5-digit)
  let zip = "";
  const zipMatch = s.match(/\b(\d{5})(?:-\d{4})?\s*$/);
  if (zipMatch) {
    zip = zipMatch[1];
    s = s.slice(0, zipMatch.index!).trim();
  }

  // Strip trailing state abbreviation (2 letters)
  s = s.replace(/\s+TX\s*$/, "").trim();

  // Try to find a known city at the end of the remaining string
  let city = "SAN ANTONIO";
  let address = s;

  // Sort cities longest-first to prefer more-specific matches
  const sortedCities = [...BEXAR_CITIES].sort((a, b) => b.length - a.length);
  for (const c of sortedCities) {
    if (s.endsWith(" " + c) || s === c) {
      city = c;
      address = s.slice(0, s.length - c.length).trim();
      break;
    }
  }

  address = address.replace(/\s+/g, " ").trim();
  if (!address) return null;

  return { address, city, zip };
}

const FIELDS = [
  "OBJECTID",
  "PropID",
  "Situs",
  "Owner",
  "AddrLn1",
  "AddrCity",
  "Zip",
  "LandVal",
  "ImprVal",
  "TotVal",
  "YrBlt",
  "PropUse",
  "Acres",
  "GBA",
  "AcctNumb",
].join(",");

async function fetchPage(
  offset: number,
): Promise<{ features: Record<string, unknown>[]; count: number }> {
  const url =
    `${PARCELS_URL}/query?where=1%3D1` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(45000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as Record<string, unknown>;
      if (json.error) throw new Error(JSON.stringify(json.error));

      const features = (
        (json.features as Array<{ attributes: Record<string, unknown> }>) || []
      ).map((f) => f.attributes);
      return { features, count: features.length };
    } catch (err: unknown) {
      if (attempt === 4) throw err;
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return { features: [], count: 0 };
}

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOffset = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Bexar County, TX Assessor Parcel Ingest (San Antonio)");
  console.log("═".repeat(60));

  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Bexar")
    .eq("state_code", "TX")
    .single();
  if (!county) {
    console.error("Bexar County, TX not in DB — run county seed first");
    process.exit(1);
  }
  const COUNTY_ID = county.id;
  console.log(`County ID: ${COUNTY_ID}\n`);

  // Load existing parcel IDs to skip dupes without a full re-upsert scan
  console.log("  Loading existing parcel IDs...");
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

  let inserted = 0;
  let dupes = 0;
  let errors = 0;
  let skipped = 0;
  let offset = skipOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // PropID is the BCAD parcel identifier (string)
      const pin = String(f.PropID || "").trim();
      if (!pin) {
        skipped++;
        continue;
      }
      if (existing.has(pin)) {
        dupes++;
        continue;
      }
      existing.add(pin);

      // Parse Situs for site address components
      const situs = parseSitus(f.Situs);
      if (!situs || !situs.address) {
        skipped++;
        continue;
      }

      // ZIP: prefer parsed-from-Situs; fall back to dedicated Zip field
      const zip =
        situs.zip ||
        String(f.Zip || "")
          .trim()
          .replace(/\D/g, "")
          .slice(0, 5);
      if (!zip) {
        skipped++;
        continue;
      }

      // TotVal = full appraised / market value in TX
      const marketValue = parseNum(f.TotVal);

      // TX has no mandated assessed ratio statewide; BCAD assessed = appraised
      const assessedValue = marketValue;

      // Year built stored as string in BCAD ("2001", "0", etc.)
      const yearBuilt = parseNum(f.YrBlt);

      // Gross Building Area
      const totalSqft = parseNum(f.GBA);

      // Owner mailing address (AddrLn1 / AddrCity / Zip from owner fields)
      const ownerAddress = String(f.AddrLn1 || "").trim() || null;
      const ownerCity = String(f.AddrCity || "").trim().toUpperCase() || null;

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.Owner || "").trim() || null,
        address: situs.address,
        city: situs.city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: parseNum(f.LandVal),
        impr_value: parseNum(f.ImprVal),
        last_sale_price: null, // not available from BCAD GIS
        property_type: classifyPropUse(f.PropUse as string | null),
        year_built: yearBuilt,
        total_sqft: totalSqft,
        acres: (() => {
          const a = parseFloat(String(f.Acres || ""));
          return isNaN(a) || a <= 0 ? null : Math.round(a * 10000) / 10000;
        })(),
        acct_numb: String(f.AcctNumb || "").trim() || null,
        owner_address: ownerAddress,
        owner_city: ownerCity,
        source: "bexar_tx_bcad",
      });
    }

    // Upsert in BATCH_SIZE chunks; fall back to row-by-row on error
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db
        .from("properties")
        .upsert(chunk, { onConflict: "county_id,parcel_id" });
      if (error) {
        // Row-by-row fallback to isolate bad records
        for (const record of chunk) {
          const { error: e2 } = await db
            .from("properties")
            .upsert(record, { onConflict: "county_id,parcel_id" });
          if (e2) {
            if (errors < 5)
              console.error(`\n  Error: ${JSON.stringify(e2).slice(0, 160)}`);
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
      `\r  offset ${offset.toLocaleString()} | fetched ${totalFetched.toLocaleString()} | ins ${inserted.toLocaleString()} | dupes ${dupes.toLocaleString()} | skip ${skipped} | errs ${errors}   `,
    );

    offset += count;
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
