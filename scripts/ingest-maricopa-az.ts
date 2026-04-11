#!/usr/bin/env tsx
/**
 * MXRE — Maricopa County, AZ Assessor Parcel Ingest (Phoenix)
 *
 * Source: Maricopa County Assessor GIS Dynamic Query Service
 *   https://gis.mcassessor.maricopa.gov/arcgis/rest/services/MaricopaDynamicQueryService/MapServer/3
 *   ~1,754,677 parcels, offset-based pagination, MaxRecordCount=1000
 *
 * Fields: APN (parcel ID), OWNER_NAME, PHYSICAL_ADDRESS, FCV_CUR (Full Cash Value),
 *         LPV_CUR (Limited Property Value), year built, living sqft, zoning
 *
 * Note: AZ residential assessed value ≈ 10% of FCV (Class 3 owner-occupied)
 *
 * Usage:
 *   npx tsx scripts/ingest-maricopa-az.ts
 *   npx tsx scripts/ingest-maricopa-az.ts --skip=500000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://gis.mcassessor.maricopa.gov/arcgis/rest/services/MaricopaDynamicQueryService/MapServer/3";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "AZ";
const INT_MAX = 2_147_483_647;

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const s = String(v).replace(/,/g, "").trim();
  const n = parseFloat(s);
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function parseDate(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "number" && v > 0) {
    const dt = new Date(v);
    if (dt.getFullYear() > 1900 && dt.getFullYear() < 2100) {
      return dt.toISOString().split("T")[0];
    }
  }
  if (typeof v === "string" && v.trim()) {
    // Handle MM/DD/YYYY format from Maricopa
    const mmddyyyy = v.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (mmddyyyy) {
      const [, m, d, y] = mmddyyyy;
      return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
    }
    // Try generic parse
    const dt = new Date(v);
    if (!isNaN(dt.getTime()) && dt.getFullYear() > 1900 && dt.getFullYear() < 2100) {
      return dt.toISOString().split("T")[0];
    }
  }
  return null;
}

function classifyZoning(code: string | null): string {
  if (!code) return "residential";
  const c = code.toUpperCase();
  if (c.startsWith("A") || c.includes("AG")) return "agricultural";
  if (c.startsWith("C") || c.includes("COMM")) return "commercial";
  if (c.startsWith("I") || c.includes("IND")) return "industrial";
  if (c.includes("MF") || c.includes("APT") || c.startsWith("R-") && parseInt(c.slice(2)) >= 4) return "multifamily";
  return "residential";
}

const FIELDS = [
  "OBJECTID",
  "APN",
  "APN_DASH",
  "OWNER_NAME",
  "PHYSICAL_ADDRESS",
  "PHYSICAL_CITY",
  "PHYSICAL_ZIP",
  "FCV_CUR",
  "LPV_CUR",
  "CONST_YEAR",
  "LIVING_SPACE",
  "CITY_ZONING",
  "SALE_DATE",
  "SALE_PRICE",
  "LC_CUR",
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

  console.log("MXRE — Maricopa County, AZ Assessor Parcel Ingest");
  console.log("═".repeat(60));

  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Maricopa").eq("state_code", "AZ").single();
  if (!county) { console.error("Maricopa County, AZ not in DB"); process.exit(1); }
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
      // Use APN_DASH as canonical parcel ID (formatted), fallback to APN
      const pin = String(f.APN_DASH || f.APN || "").trim().replace(/\s+/g, "");
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      const address = String(f.PHYSICAL_ADDRESS || "").trim().toUpperCase();
      if (!address) { skipped++; continue; }

      const city = String(f.PHYSICAL_CITY || "").trim().toUpperCase();
      let zip = String(f.PHYSICAL_ZIP || "").trim();
      // Sometimes ZIP is embedded in address string — extract last 5 digits
      if (!zip || zip.length < 5) {
        const zipMatch = address.match(/\b(\d{5})(?:-\d{4})?\s*$/);
        if (zipMatch) zip = zipMatch[1];
      }
      if (!zip) { skipped++; continue; }

      const marketValue = parseNum(f.FCV_CUR);
      // AZ: assessed value varies by legal class; use 10% as conservative residential estimate
      const legalClass = String(f.LC_CUR || "").trim();
      const assessRate = legalClass === "04" ? 0.18 : 0.10; // class 4 rental = 18%, else 10%
      const assessedValue = marketValue ? Math.round(marketValue * assessRate) : null;

      const constYearRaw = f.CONST_YEAR != null ? parseInt(String(f.CONST_YEAR).trim(), 10) : NaN;
      const yearBuilt = !isNaN(constYearRaw) && constYearRaw > 1700 && constYearRaw < 2100 ? constYearRaw : null;
      const livingSqft = parseNum(f.LIVING_SPACE);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER_NAME || "").trim() || null,
        address,
        city: city || "PHOENIX",
        state_code: STATE_CODE,
        zip: zip.slice(0, 5),
        market_value: marketValue,
        assessed_value: assessedValue,
        year_built: yearBuilt,
        living_sqft: livingSqft,
        last_sale_price: parseNum(f.SALE_PRICE),
        last_sale_date: parseDate(f.SALE_DATE),
        property_type: classifyZoning(f.CITY_ZONING as string | null),
        source: "maricopa_az_assessor_gis",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      let chunkErr: unknown = null;
      for (let attempt = 0; attempt < 5; attempt++) {
        const { error } = await db.from("properties").upsert(chunk, { onConflict: "county_id,parcel_id" });
        if (!error) { chunkErr = null; break; }
        chunkErr = error;
        // Retry on connection pool timeout
        if ((error as { code?: string }).code === "PGRST003" || String(error).includes("connection")) {
          await new Promise((r) => setTimeout(r, 5000 * (attempt + 1)));
        } else {
          break;
        }
      }
      if (chunkErr) {
        for (const record of chunk) {
          let recErr: unknown = null;
          for (let attempt = 0; attempt < 3; attempt++) {
            const { error: e2 } = await db.from("properties").upsert(record, { onConflict: "county_id,parcel_id" });
            if (!e2) { recErr = null; break; }
            recErr = e2;
            if ((e2 as { code?: string }).code === "PGRST003" || String(e2).includes("connection")) {
              await new Promise((r) => setTimeout(r, 5000 * (attempt + 1)));
            } else { break; }
          }
          if (recErr) {
            if (errors < 5) console.error(`\n  Error: ${JSON.stringify(recErr).slice(0, 120)}`);
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
