#!/usr/bin/env tsx
/**
 * Backfill canonical asset classification and inferred unit counts for Indianapolis.
 *
 * Marion/DLGF property_use codes give reliable small-multifamily signals:
 * - RES TWO FAMILY ... 520   => duplex, 2 units
 * - RES THREE FAMILY ... 530 => triplex, 3 units
 * - COMM - APT 4 - 19 ... 401 => commercial multifamily, inferred 4+ units
 * - COM - APT 20-39 ... 402   => commercial multifamily, inferred 20+ units
 * - COM - APT 40+ ... 403     => commercial multifamily, inferred 40+ units
 */

import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_SIZE = 5000;

async function pg(query: string): Promise<any[]> {
  if (DRY_RUN && /^\s*(update|insert|delete|alter|create)/i.test(query)) {
    console.log(`[dry-run] ${query.replace(/\s+/g, " ").slice(0, 180)}...`);
    return [];
  }

  const res = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!res.ok) throw new Error(`pg/query ${res.status}: ${await res.text()}`);
  return res.json();
}

async function updateBatched(label: string, setSql: string, whereSql: string) {
  console.log(`\n${label}`);
  let total = 0;

  while (true) {
    const rows = await pg(`
      WITH target AS (
        SELECT id
          FROM properties
         WHERE ${whereSql}
         LIMIT ${BATCH_SIZE}
      ),
      changed AS (
        UPDATE properties p
           SET ${setSql}
          FROM target
         WHERE p.id = target.id
         RETURNING p.id
      )
      SELECT count(*)::int AS updated FROM changed;
    `);
    const count = Number(rows?.[0]?.updated ?? 0);
    total += count;
    process.stdout.write(`\r  updated ${total.toLocaleString()}   `);
    if (count === 0 || DRY_RUN) break;
  }

  console.log();
}

async function main() {
  console.log("MXRE - Indianapolis asset classification");
  console.log("=".repeat(48));
  console.log(`Dry run: ${DRY_RUN}`);

  const indyWhere = `
    county_id = 797583
    AND upper(coalesce(city,'')) LIKE '%INDIANAPOLIS%'
  `;

  await updateBatched(
    "Commercial multifamily 40+ unit code",
    `asset_type = 'commercial_multifamily',
             asset_subtype = 'apartment_40_plus',
             total_units = CASE WHEN coalesce(total_units,0) >= 40 THEN total_units ELSE 40 END,
             unit_count_source = CASE WHEN coalesce(total_units,0) >= 40 THEN coalesce(unit_count_source, 'assessor_explicit') ELSE 'assessor_use_minimum' END,
             asset_confidence = CASE WHEN coalesce(total_units,0) >= 40 THEN 'high' ELSE 'medium' END,
             is_apartment = true,
             is_sfr = false,
             updated_at = now()`,
    `${indyWhere}
     AND upper(coalesce(property_use,'')) LIKE 'COM%APT 40 OR MORE UNITS%403%'
     AND (asset_type IS DISTINCT FROM 'commercial_multifamily'
       OR asset_subtype IS DISTINCT FROM 'apartment_40_plus'
       OR coalesce(total_units,0) < 40)`,
  );

  await updateBatched(
    "Commercial multifamily 20-39 unit code",
    `asset_type = 'commercial_multifamily',
             asset_subtype = 'apartment_20_39',
             total_units = CASE WHEN coalesce(total_units,0) BETWEEN 20 AND 39 THEN total_units ELSE 20 END,
             unit_count_source = CASE WHEN coalesce(total_units,0) BETWEEN 20 AND 39 THEN coalesce(unit_count_source, 'assessor_explicit') ELSE 'assessor_use_minimum' END,
             asset_confidence = CASE WHEN coalesce(total_units,0) BETWEEN 20 AND 39 THEN 'high' ELSE 'medium' END,
             is_apartment = true,
             is_sfr = false,
             updated_at = now()`,
    `${indyWhere}
     AND upper(coalesce(property_use,'')) LIKE 'COM%APT 20-39 UNITS%402%'
     AND (asset_type IS DISTINCT FROM 'commercial_multifamily'
       OR asset_subtype IS DISTINCT FROM 'apartment_20_39'
       OR coalesce(total_units,0) < 20)`,
  );

  await updateBatched(
    "Commercial multifamily 4-19 unit code",
    `asset_type = 'commercial_multifamily',
             asset_subtype = 'apartment_4_19',
             total_units = CASE WHEN coalesce(total_units,0) BETWEEN 4 AND 19 THEN total_units ELSE 4 END,
             unit_count_source = CASE WHEN coalesce(total_units,0) BETWEEN 4 AND 19 THEN coalesce(unit_count_source, 'assessor_explicit') ELSE 'assessor_use_minimum' END,
             asset_confidence = CASE WHEN coalesce(total_units,0) BETWEEN 4 AND 19 THEN 'high' ELSE 'medium' END,
             is_apartment = true,
             is_sfr = false,
             updated_at = now()`,
    `${indyWhere}
     AND upper(coalesce(property_use,'')) LIKE 'COMM%APT 4 - 19 UNITS%401%'
     AND (asset_type IS DISTINCT FROM 'commercial_multifamily'
       OR asset_subtype IS DISTINCT FROM 'apartment_4_19'
       OR coalesce(total_units,0) < 4)`,
  );

  await updateBatched(
    "Duplex from DLGF two-family code",
    `asset_type = 'small_multifamily',
             asset_subtype = 'duplex',
             total_units = 2,
             unit_count_source = 'assessor_property_use',
             asset_confidence = 'high',
             is_sfr = false,
             is_apartment = false,
             updated_at = now()`,
    `${indyWhere}
     AND upper(coalesce(property_use,'')) LIKE 'RES TWO FAMILY%'
     AND (asset_type IS DISTINCT FROM 'small_multifamily'
       OR asset_subtype IS DISTINCT FROM 'duplex'
       OR coalesce(total_units,0) IS DISTINCT FROM 2)`,
  );

  await updateBatched(
    "Triplex from DLGF three-family code",
    `asset_type = 'small_multifamily',
             asset_subtype = 'triplex',
             total_units = 3,
             unit_count_source = 'assessor_property_use',
             asset_confidence = 'high',
             is_sfr = false,
             is_apartment = false,
             updated_at = now()`,
    `${indyWhere}
     AND upper(coalesce(property_use,'')) LIKE 'RES THREE FAMILY%'
     AND (asset_type IS DISTINCT FROM 'small_multifamily'
       OR asset_subtype IS DISTINCT FROM 'triplex'
       OR coalesce(total_units,0) IS DISTINCT FROM 3)`,
  );

  await updateBatched(
    "Apartment / 5+ multifamily flags",
    `asset_type = 'apartment',
             asset_subtype = coalesce(asset_subtype, 'apartment_community'),
             unit_count_source = coalesce(unit_count_source, CASE WHEN total_units > 0 THEN 'assessor_explicit' ELSE 'unknown' END),
             asset_confidence = CASE WHEN total_units > 0 THEN 'medium' ELSE 'low' END,
             is_apartment = true,
             is_sfr = false,
             updated_at = now()`,
    `${indyWhere}
     AND (
           is_apartment IS TRUE
           OR lower(coalesce(property_type,'')) LIKE '%apartment%'
           OR lower(coalesce(property_use,'')) LIKE '%apartment%'
           OR coalesce(total_units,0) >= 5
         )
     AND asset_type IS DISTINCT FROM 'apartment'`,
  );

  await updateBatched(
    "Unspecified multifamily flags",
    `asset_type = 'small_multifamily',
             asset_subtype = 'multifamily_unknown',
             total_units = NULL,
             unit_count_source = 'unknown',
             asset_confidence = 'low',
             is_sfr = false,
             is_apartment = false,
             updated_at = now()`,
    `${indyWhere}
     AND lower(coalesce(property_type,'')) LIKE '%multi%'
     AND asset_subtype NOT IN ('duplex','triplex')`,
  );

  await updateBatched(
    "SFR / condo / residential defaults",
    `asset_type = CASE
               WHEN lower(coalesce(property_type,'')) LIKE '%condo%' OR is_condo IS TRUE THEN 'residential'
               WHEN lower(coalesce(property_type,'')) IN ('residential','single_family') OR upper(coalesce(property_use,'')) LIKE 'RES ONE FAMILY%' THEN 'residential'
               ELSE coalesce(asset_type, 'other')
             END,
             asset_subtype = CASE
               WHEN lower(coalesce(property_type,'')) LIKE '%condo%' OR is_condo IS TRUE THEN 'condo'
               WHEN lower(coalesce(property_type,'')) IN ('residential','single_family') OR upper(coalesce(property_use,'')) LIKE 'RES ONE FAMILY%' THEN 'sfr'
               ELSE coalesce(asset_subtype, lower(nullif(property_type,'')), 'unknown')
             END,
             total_units = CASE
               WHEN total_units IS NULL OR total_units <= 0 THEN 1
               ELSE total_units
             END,
             unit_count_source = CASE
               WHEN total_units IS NULL OR total_units <= 0 THEN 'inferred_single_unit'
               ELSE coalesce(unit_count_source, 'assessor_explicit')
             END,
             asset_confidence = coalesce(asset_confidence, 'medium'),
             is_sfr = CASE
               WHEN lower(coalesce(property_type,'')) LIKE '%condo%' OR is_condo IS TRUE THEN false
               WHEN lower(coalesce(property_type,'')) IN ('residential','single_family') OR upper(coalesce(property_use,'')) LIKE 'RES ONE FAMILY%' THEN true
               ELSE is_sfr
             END,
             updated_at = now()`,
    `${indyWhere}
     AND asset_type IS NULL`,
  );

  const summary = await pg(`
    SELECT asset_type, asset_subtype, unit_count_source, asset_confidence,
           count(*)::int AS properties,
           sum(coalesce(total_units,0))::int AS units
      FROM properties
     WHERE county_id = 797583
       AND upper(coalesce(city,'')) LIKE '%INDIANAPOLIS%'
     GROUP BY 1,2,3,4
     ORDER BY properties DESC
     LIMIT 25;
  `);

  console.log("\nSummary:");
  for (const row of summary) {
    console.log(
      `  ${row.asset_type ?? "null"} / ${row.asset_subtype ?? "null"} ` +
      `(${row.unit_count_source ?? "unknown"}, ${row.asset_confidence ?? "unknown"}): ` +
      `${Number(row.properties).toLocaleString()} properties, ${Number(row.units).toLocaleString()} units`,
    );
  }
}

main().catch((err) => {
  console.error("Fatal:", err instanceof Error ? err.message : err);
  process.exit(1);
});
