#!/usr/bin/env tsx
/**
 * MXRE — Butler County, OH Assessor Parcel Ingest
 *
 * Source: Butler County Auditor GIS (ArcGIS Online)
 *   Parcels: https://services7.arcgis.com/K1dCXq6MUpNgd6A3/arcgis/rest/services/Final_Parcels/FeatureServer/31
 *   ~166,387 parcels
 *
 * Fields: PIN, OWNER, LOCATION (address), CURRENTVALUE (market value),
 *         MUN_NAME (city for incorporated), TAXDIST (city for unincorporated),
 *         TOT_RETAX (annual taxes), VALUE23/VALUE24 etc.
 *
 * Usage:
 *   npx tsx scripts/ingest-butler-oh.ts
 *   npx tsx scripts/ingest-butler-oh.ts --skip=5000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://services7.arcgis.com/K1dCXq6MUpNgd6A3/arcgis/rest/services/Final_Parcels/FeatureServer/31";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const COUNTY_ID = 1741128; // Butler County, OH
const STATE_CODE = "OH";
const INT_MAX = 2_147_483_647;

// ─── City → Primary ZIP lookup (Butler County, OH) ────────────────
const CITY_ZIP: Record<string, string> = {
  FAIRFIELD: "45014",
  "FAIRFIELD CITY": "45014",
  "FAIRFIELD TWP": "45014",
  HAMILTON: "45011",
  "HAMILTON CITY": "45011",
  MIDDLETOWN: "45044",
  "MIDDLETOWN CITY": "45044",
  OXFORD: "45056",
  "OXFORD CITY": "45056",
  "OXFORD TWP": "45056",
  MONROE: "45050",
  "MONROE CORP": "45050",
  TRENTON: "45067",
  "TRENTON CITY": "45067",
  "WEST CHESTER": "45069",
  LIBERTY: "45044",
  "NEW MIAMI": "45011",
  "NEW MIAMI CORP": "45011",
  MILLVILLE: "45013",
  "MILLVILLE CORP": "45013",
  "COLLEGE CORNER": "45003",
  "COLLEGE CORNER CORP": "45003",
  JACKSONBURG: "45030",
  "JACKSONBURG CORP": "45030",
  "SEVEN MILE": "45062",
  "SEVEN MILE CORP": "45062",
  LEMON: "45036",
  HANOVER: "45013",
  ROSS: "45013",
  REILY: "45056",
  MORGAN: "45002",
  MADISON: "45044",
  MILFORD: "45150",
  "ST CLAIR": "45011",
  WAYNE: "45042",
  SHARONVILLE: "45241",
};

// ─── Helpers ──────────────────────────────────────────────────────

function extractCity(munName: string | null, taxDist: string | null): string {
  const raw = (munName || taxDist || "").trim();
  // Remove suffixes: " CITY", " CORP", " TWP", " TOWNSHIP"
  return raw
    .replace(/\s+(CITY|CORP|TWP|TOWNSHIP|CORPORATION)$/i, "")
    .toUpperCase()
    .trim();
}

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function classifyLandUse(taxDist: string | null, munName: string | null): string {
  // Butler County doesn't expose LUC directly in this layer
  // Use context clues — default residential
  return "residential";
}

// ─── Fetch page with OBJECTID-based pagination ───────────────────

const FIELDS = [
  "OBJECTID_1",
  "PIN",
  "OWNER",
  "LOCATION",
  "CURRENTVALUE",
  "VALUE",
  "TOT_RETAX",
  "MUN_NAME",
  "TAXDIST",
  "SALEPRICE",
  "SALEDT",
  "CNT_NAME",
].join(",");

async function fetchPage(minOid: number): Promise<{ features: Record<string, unknown>[]; maxOid: number }> {
  const url =
    `${PARCELS_URL}/query?where=OBJECTID_1+>+${minOid}` +
    `&outFields=${encodeURIComponent(FIELDS)}&returnGeometry=false` +
    `&resultRecordCount=${PAGE_SIZE}&orderByFields=OBJECTID_1+ASC&f=json`;

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
        const oid = f["OBJECTID_1"] as number;
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

// ─── Get existing parcel IDs to skip ─────────────────────────────

async function getExistingParcelIds(): Promise<Set<string>> {
  const existing = new Set<string>();
  let offset = 0;
  const PAGE = 1000;
  while (true) {
    const { data } = await db
      .from("properties")
      .select("parcel_id")
      .eq("county_id", COUNTY_ID)
      .not("parcel_id", "is", null)
      .range(offset, offset + PAGE - 1);
    if (!data || data.length === 0) break;
    for (const row of data) {
      if (row.parcel_id) existing.add(row.parcel_id);
    }
    if (data.length < PAGE) break;
    offset += PAGE;
  }
  return existing;
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOid = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Butler County, OH Assessor Parcel Ingest");
  console.log("═".repeat(60));
  console.log(`County ID: ${COUNTY_ID}`);
  console.log(`Starting OBJECTID: ${skipOid}\n`);

  console.log("Loading existing parcel IDs from DB...");
  const existing = await getExistingParcelIds();
  console.log(`  ${existing.size.toLocaleString()} parcels already in DB\n`);

  let inserted = 0;
  let dupes = 0;
  let errors = 0;
  let minOid = skipOid;
  let totalFetched = 0;

  while (true) {
    const { features, maxOid } = await fetchPage(minOid);

    if (features.length === 0) {
      console.log("\nNo more records — done.");
      break;
    }

    totalFetched += features.length;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.PIN || "").trim();
      if (!pin) continue;

      if (existing.has(pin)) {
        dupes++;
        continue;
      }
      existing.add(pin);

      const locationRaw = String(f.LOCATION || "").trim();
      if (!locationRaw) {
        dupes++; // skip parcels without a street address
        continue;
      }
      const city = extractCity(f.MUN_NAME as string | null, f.TAXDIST as string | null);
      // Try both normalized city and raw MUN_NAME/TAXDIST as lookup keys
      const rawMun = String(f.MUN_NAME || f.TAXDIST || "").trim().toUpperCase();
      const zip = CITY_ZIP[city] || CITY_ZIP[rawMun] || "45011"; // fallback: Hamilton (county seat)
      const marketValue = parseNum(f.CURRENTVALUE ?? f.VALUE);
      const assessedValue = marketValue ? Math.round(marketValue * 0.35) : null; // OH assessed = 35% of appraised
      const salePrice = parseNum(f.SALEPRICE);

      // Parse sale date (format varies)
      let lastSaleDate: string | null = null;
      const saleDtRaw = f.SALEDT;
      if (saleDtRaw && typeof saleDtRaw === "number" && saleDtRaw > 0) {
        // ArcGIS epoch ms
        const dt = new Date(saleDtRaw);
        if (dt.getFullYear() > 1900) lastSaleDate = dt.toISOString().split("T")[0];
      } else if (saleDtRaw && typeof saleDtRaw === "string" && saleDtRaw.length >= 8) {
        if (/^\d{8}$/.test(saleDtRaw)) {
          lastSaleDate = `${saleDtRaw.slice(0, 4)}-${saleDtRaw.slice(4, 6)}-${saleDtRaw.slice(6, 8)}`;
        }
      }

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER || "").trim() || null,
        address: locationRaw.toUpperCase(),
        city: city || "HAMILTON", // fallback to county seat
        state_code: STATE_CODE,
        zip: zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        property_tax: parseNum(f.TOT_RETAX),
        last_sale_price: salePrice,
        last_sale_date: lastSaleDate,
        property_type: "residential",
        source: "butler_oh_auditor_gis",
      });
    }

    // Upsert in sub-batches
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      const { error } = await db.from("properties").upsert(chunk, { onConflict: "county_id,parcel_id" });
      if (error) {
        // Retry individually
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

    if (maxOid === minOid) break; // safety
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
