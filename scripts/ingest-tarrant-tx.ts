#!/usr/bin/env tsx
/**
 * MXRE — Tarrant County, TX Assessor Parcel Ingest (Fort Worth)
 *
 * Source: Tarrant County Appraisal District (TAD) — MapIT ArcGIS MapServer
 *   https://mapit.tarrantcounty.com/arcgis/rest/services/Tax/TCProperty/MapServer/0
 *   ~600K parcels, MaxRecordCount=1000, offset-based pagination
 *
 * Key fields:
 *   TAXPIN      — parcel/account ID
 *   OWNER_NAME  — owner
 *   SITUS_ADDR  — full site address (includes city, ST, ZIP)
 *   ZIPCODE     — zip from separate field (fallback)
 *   STATE       — state code field
 *   APPRAISEDV  — TX full appraised value (= market_value)
 *   LAND_VALUE  — land value component
 *   IMPR_VALUE  — improvement value component
 *   YEAR_BUILT  — year built
 *   BEDROOMS    — bedrooms
 *   BATHROOMS   — bathrooms
 *   LIVING_ARE  — living area sq ft (total_sqft)
 *   DEED_DATE   — date of deed recording (epoch ms) → last_sale_date (approximate)
 *
 * Note: No sale price available in this source. DEED_DATE = recording date, not
 * contract date. Labeled "approximate" downstream per MXRE actual-vs-estimated policy.
 *
 * Usage:
 *   npx tsx scripts/ingest-tarrant-tx.ts
 *   npx tsx scripts/ingest-tarrant-tx.ts --offset=200000
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCELS_URL =
  "https://mapit.tarrantcounty.com/arcgis/rest/services/Tax/TCProperty/MapServer/0";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const STATE_CODE = "TX";
const INT_MAX = 2_147_483_647;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseNum(v: unknown): number | null {
  if (v == null) return null;
  const s = String(v).replace(/,/g, "").trim();
  const n = parseFloat(s);
  if (isNaN(n) || n <= 0) return null;
  const r = Math.round(n);
  return r > INT_MAX ? null : r;
}

function parseIntVal(v: unknown): number | null {
  if (v == null) return null;
  const n = parseInt(String(v).trim(), 10);
  return isNaN(n) ? null : n;
}

/**
 * Parse ArcGIS epoch milliseconds (or string date) → "YYYY-MM-DD".
 * DEED_DATE comes as a numeric epoch from ArcGIS Date fields.
 */
function parseDate(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "number" && v > 0) {
    const dt = new Date(v);
    const y = dt.getUTCFullYear();
    if (y > 1900 && y < 2100) {
      const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
      const d = String(dt.getUTCDate()).padStart(2, "0");
      return `${y}-${m}-${d}`;
    }
  }
  if (typeof v === "string" && v.trim()) {
    const mmddyyyy = v.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (mmddyyyy) {
      const [, mo, day, yr] = mmddyyyy;
      return `${yr}-${mo.padStart(2, "0")}-${day.padStart(2, "0")}`;
    }
    const dt = new Date(v);
    if (!isNaN(dt.getTime())) {
      const y = dt.getUTCFullYear();
      if (y > 1900 && y < 2100) return dt.toISOString().split("T")[0];
    }
  }
  return null;
}

/**
 * Parse city out of SITUS_ADDR.
 *
 * TAD SITUS_ADDR format: "123 MAIN ST  FORT WORTH TX 76102"
 * Strategy: find " TX " or " TX\d{5}" and take the word(s) before it
 * as the city. Falls back to ZIPCODE-based default if unparseable.
 */
function parseCityFromSitus(addr: string): string {
  const up = addr.trim().toUpperCase();
  // Match "...CITY TX 76XXX" or "...CITY TX76XXX"
  const m = up.match(/\s+([A-Z][A-Z\s]{0,29?})\s+TX[\s,]?\d{5}/);
  if (m) return m[1].trim();
  // Fallback: try splitting on "  " (double-space separator TAD sometimes uses)
  const parts = up.split(/\s{2,}/);
  if (parts.length >= 2) {
    // Last part likely "TX 76XXX" — second-to-last is city
    const cityPart = parts[parts.length - 2];
    if (cityPart && /^[A-Z\s]{2,}$/.test(cityPart)) return cityPart.trim();
  }
  return "FORT WORTH";
}

/**
 * Classify property type from TAD fields.
 * TAD doesn't expose a land use code in this layer so we infer from
 * bedroom/bathroom presence and improvement value.
 */
function classifyProperty(f: Record<string, unknown>): string {
  const beds = parseIntVal(f.BEDROOMS);
  const baths = parseIntVal(f.BATHROOMS);
  const imprVal = parseNum(f.IMPR_VALUE);
  // If it has bedrooms/baths it's residential
  if (beds != null && beds > 0) return "residential";
  if (baths != null && baths > 0) return "residential";
  // No living data and no improvement = likely land only
  if (!imprVal) return "land";
  return "residential";
}

// ─── ArcGIS Fetch ─────────────────────────────────────────────────────────────

const FIELDS = [
  "OBJECTID",
  "TAXPIN",
  "OWNER_NAME",
  "SITUS_ADDR",
  "ZIPCODE",
  "STATE",
  "APPRAISEDV",
  "LAND_VALUE",
  "IMPR_VALUE",
  "YEAR_BUILT",
  "BEDROOMS",
  "BATHROOMS",
  "LIVING_ARE",
  "DEED_DATE",
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
      const wait = 2000 * (attempt + 1);
      process.stderr.write(`\n  fetch err (attempt ${attempt + 1}): ${String(err).slice(0, 80)} — retry in ${wait}ms\n`);
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  return { features: [], count: 0 };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const offsetArg = process.argv.find((a) => a.startsWith("--offset="))?.split("=")[1];
  const startOffset = offsetArg ? parseInt(offsetArg, 10) : 0;

  console.log("MXRE — Tarrant County, TX Assessor Parcel Ingest (Fort Worth)");
  console.log("═".repeat(64));
  if (startOffset > 0) console.log(`  Resuming from offset ${startOffset.toLocaleString()}`);

  // ── Resolve county ID ──────────────────────────────────────────────
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("county_name", "Tarrant")
    .eq("state_code", STATE_CODE)
    .single();
  if (!county) {
    console.error("Tarrant County, TX not found in counties table");
    process.exit(1);
  }
  const COUNTY_ID = county.id;
  console.log(`  County ID: ${COUNTY_ID}`);

  // ── Load existing parcel IDs to skip dupes ─────────────────────────
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

  // ── Ingest loop ────────────────────────────────────────────────────
  let inserted = 0,
    dupes = 0,
    errors = 0,
    skipped = 0;
  let offset = startOffset;
  let totalFetched = 0;

  while (true) {
    const { features, count } = await fetchPage(offset);
    if (count === 0) break;
    totalFetched += count;

    const batch: Array<Record<string, unknown>> = [];

    for (const f of features) {
      // ── Parcel ID ──────────────────────────────────────────────────
      const pin = String(f.TAXPIN || "").trim();
      if (!pin) { skipped++; continue; }
      if (existing.has(pin)) { dupes++; continue; }
      existing.add(pin);

      // ── Address ────────────────────────────────────────────────────
      const rawAddr = String(f.SITUS_ADDR || "").trim();
      if (!rawAddr) { skipped++; continue; }
      const address = rawAddr.toUpperCase();

      // ── City ───────────────────────────────────────────────────────
      const city = parseCityFromSitus(address);

      // ── ZIP — prefer dedicated field, fallback to parse from addr ──
      let zip = String(f.ZIPCODE || "").trim().replace(/\D/g, "").slice(0, 5);
      if (!zip || zip.length < 5) {
        const zipMatch = address.match(/\b(\d{5})(?:-\d{4})?\s*$/);
        if (zipMatch) zip = zipMatch[1];
      }
      if (!zip || zip.length < 5) { skipped++; continue; }

      // ── Values ─────────────────────────────────────────────────────
      const marketValue = parseNum(f.APPRAISEDV);
      // TX appraisal: assessed = appraised for most purposes (no statutory ratio like TN)
      const assessedValue = marketValue;
      const landValue = parseNum(f.LAND_VALUE);
      const imprValue = parseNum(f.IMPR_VALUE);

      // ── Physical attributes ────────────────────────────────────────
      const yearBuiltRaw = parseIntVal(f.YEAR_BUILT);
      const yearBuilt =
        yearBuiltRaw != null && yearBuiltRaw > 1700 && yearBuiltRaw < 2100
          ? yearBuiltRaw
          : null;
      const totalSqft = parseNum(f.LIVING_ARE);
      const bedrooms = parseIntVal(f.BEDROOMS);
      const bathrooms = parseIntVal(f.BATHROOMS);

      // ── Deed date (approximate sale date — recording date, not contract) ─
      const lastSaleDate = parseDate(f.DEED_DATE);

      batch.push({
        county_id: COUNTY_ID,
        parcel_id: pin,
        owner_name: String(f.OWNER_NAME || "").trim() || null,
        address,
        city,
        state_code: STATE_CODE,
        zip,
        market_value: marketValue,
        assessed_value: assessedValue,
        land_value: landValue,
        improvement_value: imprValue,
        year_built: yearBuilt,
        total_sqft: totalSqft,
        bedrooms: bedrooms != null && bedrooms > 0 ? bedrooms : null,
        bathrooms: bathrooms != null && bathrooms > 0 ? bathrooms : null,
        // No sale price in TAD parcel layer
        last_sale_price: null,
        last_sale_date: lastSaleDate,
        property_type: classifyProperty(f),
        source: "tarrant_tx_tad",
      });
    }

    // ── Upsert in BATCH_SIZE chunks with connection-retry ──────────────
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      let chunkErr: unknown = null;

      for (let attempt = 0; attempt < 5; attempt++) {
        const { error } = await db
          .from("properties")
          .upsert(chunk, { onConflict: "county_id,parcel_id" });
        if (!error) { chunkErr = null; break; }
        chunkErr = error;
        const errCode = (error as { code?: string }).code;
        if (errCode === "PGRST003" || String(error).includes("connection")) {
          await new Promise((r) => setTimeout(r, 5000 * (attempt + 1)));
        } else {
          break;
        }
      }

      if (chunkErr) {
        // Fall back to row-by-row
        for (const record of chunk) {
          let recErr: unknown = null;
          for (let attempt = 0; attempt < 3; attempt++) {
            const { error: e2 } = await db
              .from("properties")
              .upsert(record, { onConflict: "county_id,parcel_id" });
            if (!e2) { recErr = null; break; }
            recErr = e2;
            const errCode2 = (e2 as { code?: string }).code;
            if (errCode2 === "PGRST003" || String(e2).includes("connection")) {
              await new Promise((r) => setTimeout(r, 5000 * (attempt + 1)));
            } else { break; }
          }
          if (recErr) {
            if (errors < 5) console.error(`\n  Error: ${JSON.stringify(recErr).slice(0, 120)}`);
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

  console.log(`\n\n${"═".repeat(64)}`);
  console.log(
    `TOTAL: ${inserted.toLocaleString()} inserted, ${dupes.toLocaleString()} dupes, ${skipped} skipped, ${errors} errors`,
  );
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
