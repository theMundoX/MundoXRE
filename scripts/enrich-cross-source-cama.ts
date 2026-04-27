#!/usr/bin/env tsx
/**
 * Cross-source CAMA enrichment for Marion County.
 *
 * Problem: assessor rows (236K) have sqft but are missing year_built, bedrooms.
 *          in-data-harvest-parcels rows have year_built (50%) and bedrooms (52%) for the same parcels.
 *
 * Strategy: For each assessor property missing year_built or bedrooms, find the
 *           matching in-data-harvest-parcels row (same county_id + parcel_id overlap)
 *           and copy the CAMA fields across.
 *
 * Also: for all Marion County properties, if market_value is missing and assessor row
 *       has it, fill from ArcGIS ASSESSORYEAR_TOTALAV (already done by enrich-marion-arcgis.ts).
 *       This script focuses on year_built / bedrooms cross-fill.
 *
 * Usage:
 *   npx tsx scripts/enrich-cross-source-cama.ts
 *   npx tsx scripts/enrich-cross-source-cama.ts --dry-run
 *   npx tsx scripts/enrich-cross-source-cama.ts --county-id=797583
 */

import "dotenv/config";

const args = process.argv.slice(2);
const hasFlag = (n: string) => args.includes(`--${n}`);
const getArg  = (n: string) => args.find(a => a.startsWith(`--${n}=`))?.split("=")[1];

const DRY_RUN   = hasFlag("dry-run");
const COUNTY_ID = getArg("county-id") ? parseInt(getArg("county-id")!, 10) : 797583;

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(q: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
    signal: AbortSignal.timeout(55_000),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return res.json();
}

async function main() {
  console.log("MXRE — Cross-source CAMA enrichment");
  console.log(`  County: ${COUNTY_ID} | Dry run: ${DRY_RUN}`);
  console.log();

  // Step 1: Update assessor rows that are missing year_built/bedrooms by joining to
  //         in-data-harvest-parcels rows with the same parcel_id in same county.
  // Parcel IDs: assessor uses 7-digit PARCEL_C, harvest uses 18-digit state parcel.
  // We join by address or by looking at apn_formatted vs parcel_id patterns.
  // Actually: join by CAMAPARCELID or by 7-digit parcel prefix.

  // The assessor rows have parcel_id that's a 7-digit code.
  // The in-data-harvest-parcels rows have an 18-digit parcel_id.
  // CAMAPARCELID in ArcGIS is the 7-digit code, which maps to the assessor parcel_id.
  // We can join: assessor.parcel_id = harvest.apn_formatted (set by ArcGIS enrichment)
  //          OR: assessor.parcel_id = LEFT(harvest.parcel_id, 7) — last 7 digits of 18-char ID

  // Strategy: Join on county_id + address (most reliable cross-source join)
  // Actually the simplest is: for each assessor row, find a harvest row at the same address

  // Let's use: match by county_id AND (apn_formatted = assessor.parcel_id OR address similarity)
  // apn_formatted was populated by ArcGIS enrichment with CAMAPARCELID (7-digit) for harvest rows

  const stats = { scanned: 0, updated: 0, skipped: 0, errors: 0 };

  // Use a single UPDATE ... FROM ... subquery for efficiency
  const updateSQL = `
    UPDATE properties a
    SET
      year_built = COALESCE(a.year_built, h.year_built),
      bedrooms   = COALESCE(a.bedrooms,   h.bedrooms),
      updated_at = now()
    FROM (
      SELECT parcel_id, apn_formatted, year_built, bedrooms
      FROM properties
      WHERE county_id = ${COUNTY_ID}
        AND source = 'in-data-harvest-parcels'
        AND (year_built IS NOT NULL OR bedrooms IS NOT NULL)
    ) h
    WHERE a.county_id = ${COUNTY_ID}
      AND a.source = 'assessor'
      AND (a.year_built IS NULL OR a.bedrooms IS NULL)
      AND (
        -- Match by the 7-digit assessor parcel ID that ArcGIS wrote into apn_formatted
        h.apn_formatted = a.parcel_id
        -- Fallback: last 7 chars of 18-digit harvest parcel_id match assessor's parcel_id
        OR (LENGTH(h.parcel_id) = 18 AND RIGHT(h.parcel_id, 7) = a.parcel_id)
      )
  `;

  if (DRY_RUN) {
    console.log("  [dry-run] Would execute cross-source join UPDATE");
    // Count how many assessor rows would be updated
    const countSQL = `
      SELECT COUNT(*)::int AS cnt
      FROM properties a
      JOIN (
        SELECT parcel_id, apn_formatted, year_built, bedrooms
        FROM properties
        WHERE county_id = ${COUNTY_ID}
          AND source = 'in-data-harvest-parcels'
          AND (year_built IS NOT NULL OR bedrooms IS NOT NULL)
      ) h ON (h.apn_formatted = a.parcel_id OR (LENGTH(h.parcel_id) = 18 AND RIGHT(h.parcel_id, 7) = a.parcel_id))
      WHERE a.county_id = ${COUNTY_ID}
        AND a.source = 'assessor'
        AND (a.year_built IS NULL OR a.bedrooms IS NULL)
    `;
    const rows = await pg(countSQL);
    console.log(`  Would update ${rows[0]?.cnt?.toLocaleString() ?? 0} assessor rows`);
    return;
  }

  console.log("  Running cross-source CAMA join update...");
  try {
    await pg(updateSQL);
    console.log("  Done.");
  } catch (e: any) {
    console.error(`  Error: ${(e as Error).message}`);
    stats.errors++;
    return;
  }

  // Step 2: Also fill market_value for assessor rows from harvest rows when missing
  // (only fill NULL → non-null, never overwrite)
  console.log("  Filling assessor market_value from harvest rows...");
  const mvSQL = `
    UPDATE properties a
    SET
      market_value   = COALESCE(a.market_value, h.market_value),
      assessed_value = COALESCE(a.assessed_value, h.assessed_value),
      land_value     = COALESCE(a.land_value, h.land_value),
      updated_at     = now()
    FROM (
      SELECT parcel_id, apn_formatted, market_value, assessed_value, land_value
      FROM properties
      WHERE county_id = ${COUNTY_ID}
        AND source = 'in-data-harvest-parcels'
        AND market_value IS NOT NULL
    ) h
    WHERE a.county_id = ${COUNTY_ID}
      AND a.source = 'assessor'
      AND a.market_value IS NULL
      AND (
        h.apn_formatted = a.parcel_id
        OR (LENGTH(h.parcel_id) = 18 AND RIGHT(h.parcel_id, 7) = a.parcel_id)
      )
  `;
  try {
    await pg(mvSQL);
    console.log("  Done.");
  } catch (e: any) {
    console.error(`  market_value fill error: ${(e as Error).message}`);
  }

  // Step 3: Also enrich harvest rows missing year_built from their ACS neighbourhood
  // — skip this for now, needs census_tract

  // Verify results
  const check = await pg(`
    SELECT
      source,
      COUNT(*)::int AS total,
      COUNT(year_built)::int AS has_year_built,
      COUNT(bedrooms)::int AS has_bedrooms,
      COUNT(market_value)::int AS has_market_value
    FROM properties
    WHERE county_id = ${COUNTY_ID}
    GROUP BY source
    ORDER BY total DESC
  `).catch(() => []);

  console.log("\nPost-enrichment coverage:");
  for (const r of check) {
    const pct = (n: number, d: number) => d > 0 ? `${Math.round(n/d*100)}%` : "0%";
    console.log(`  ${r.source} (${r.total?.toLocaleString()}): year_built=${pct(r.has_year_built, r.total)} bedrooms=${pct(r.has_bedrooms, r.total)} market_value=${pct(r.has_market_value, r.total)}`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
