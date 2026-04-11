#!/usr/bin/env tsx
/**
 * MXRE — Marion County, IN Assessor Parcel Ingest (Indianapolis)
 *
 * Source: IndyGIS / Marion County Assessor — Tax Parcels FeatureServer
 *   https://services.arcgis.com/f4rR7WnIfGBdVYFd/arcgis/rest/services/Tax_Parcels/FeatureServer/0
 *   Updated nightly; ~350K+ parcels, offset-based pagination
 *
 * Fields: PAN, Owner1, Owner2, Owner3, Mailing_Address, CityStateZip,
 *         Total_Value, Land_Value, Improvements, Assessing_Primary_Use
 *
 * CityStateZip format: "INDIANAPOLIS IN 46220" — parsed for city/state/zip
 *
 * IN assessed value = 100% of appraised (Indiana is a full-value state)
 *
 * Usage:
 *   npx tsx scripts/ingest-marion-in.ts
 *   npx tsx scripts/ingest-marion-in.ts --skip=50000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://services.arcgis.com/f4rR7WnIfGBdVYFd/arcgis/rest/services/Tax_Parcels/FeatureServer/0";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const STATE_CODE = "IN";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

/**
 * Parse "INDIANAPOLIS IN 46220" → { city: "INDIANAPOLIS", state: "IN", zip: "46220" }
 * Also handles "INDIANAPOLIS  IN  46220-1234"
 */
function parseCityStateZip(csz: string): { city: string; zip: string } | null {
  if (!csz) return null;
  // Match: city words, 2-letter state, 5-digit zip (optional +4)
  const m = csz.trim().match(/^(.+?)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?$/);
  if (!m) return null;
  return { city: m[1].trim().toUpperCase(), zip: m[3] };
}

function classifyUse(code: string | null): string {
  if (!code) return "residential";
  const c = String(code).toUpperCase();
  if (c.includes("COMMERCIAL") || c.includes("RETAIL") || c.includes("OFFICE")) return "commercial";
  if (c.includes("INDUSTRIAL") || c.includes("WAREHOUSE") || c.includes("MANUF")) return "industrial";
  if (c.includes("EXEMPT") || c.includes("GOV") || c.includes("PUBLIC")) return "exempt";
  if (c.includes("MULTI") || c.includes("APART") || c.includes("CONDO")) return "multifamily";
  if (c.includes("AG") || c.includes("FARM")) return "agricultural";
  return "residential";
}

const FIELDS = [
  "OBJECTID",
  "PAN",
  "Owner1",
  "Owner2",
  "Owner3",
  "Mailing_Address",
  "CityStateZip",
  "Total_Value",
  "Land_Value",
  "Improvements",
  "Assessing_Primary_Use",
  "Tax_Status",
  "MillRate",
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
  const skipArg = process.argv.find((a) => a.startsWith("--skip="))?.split("=")[1];
  const skipOffset = skipArg ? parseInt(skipArg, 10) : 0;

  console.log("MXRE — Marion County, IN Assessor Parcel Ingest (Indianapolis)");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Marion").eq("state_code", "IN").single();
  if (!county) { console.error("Marion County, IN not in DB"); process.exit(1); }
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
  let offset = skipOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      const pin = String(f.PAN || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.Mailing_Address || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const cszRaw = String(f.CityStateZip || "").trim();
      const csz = parseCityStateZip(cszRaw);
      if (!csz) { skipped++; continue; }

      const { city, zip } = csz;
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.Total_Value);
      // Indiana: assessed value = 100% of true tax value (AV = market value)
      const assessedValue = marketValue;

      // Combine owner names
      const owners = [f.Owner1, f.Owner2, f.Owner3]
        .filter(Boolean)
        .map((o) => String(o).trim())
        .filter((o) => o.length > 0);
      const ownerName = owners.join("; ") || null;

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
        land_value: parseNum(f.Land_Value),
        property_type: classifyUse(f.Assessing_Primary_Use as string | null),
        source: "marion_in_assessor_gis",
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
