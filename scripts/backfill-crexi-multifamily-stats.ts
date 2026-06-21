#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseCrexiMultifamilyStats } from "./lib/crexi-multifamily-stats.ts";
import { makeDbClient } from "./lib/db.ts";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const DRY_RUN = process.argv.includes("--dry-run");
const WRITE_LOG = !process.argv.includes("--no-write");
const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const LIMIT = Math.max(1, Number.parseInt(arg("limit", "5000") ?? "5000", 10));

type ExternalListingRow = {
  id: number;
  title: string | null;
  units: number | null;
  list_price: number | null;
  price_per_unit: number | null;
  raw: Record<string, unknown> | null;
};

function unitMixWithApartmentUnits(raw: Record<string, unknown>, apartmentUnits: number | null) {
  const existing = raw.unit_mix && typeof raw.unit_mix === "object"
    ? raw.unit_mix as Record<string, unknown>
    : {};
  return {
    ...existing,
    apartmentUnits,
  };
}

async function main() {
  const db = await makeDbClient();
  const { rows } = await db.query<ExternalListingRow>(`
    select id, title, units, list_price, price_per_unit, raw
      from external_market_listings
     where source = 'crexi_rapidapi'
       and asset_class = 'multifamily'
     order by id
     limit $1;
  `, [LIMIT]);

  let updated = 0;
  let withUnits = 0;
  let withSquareFootage = 0;
  let withOccupancy = 0;
  let withYearBuilt = 0;
  let withBuildings = 0;
  const examples: Array<Record<string, unknown>> = [];

  for (const row of rows) {
    const raw = row.raw ?? {};
    const detail = raw.detail && typeof raw.detail === "object" ? raw.detail as Record<string, unknown> : undefined;
    const stats = parseCrexiMultifamilyStats(detail, row.list_price);
    if (stats.units !== null) withUnits++;
    if (stats.squareFootage !== null) withSquareFootage++;
    if (stats.occupancyPct !== null || stats.occupancyStatus !== null) withOccupancy++;
    if (stats.yearBuilt !== null) withYearBuilt++;
    if (stats.buildings !== null) withBuildings++;

    const patchRaw = {
      multifamily: stats,
      apartment_units: stats.apartmentUnits,
      building_count: stats.buildings,
      story_count: stats.stories,
      year_built: stats.yearBuilt,
      years_built: stats.yearsBuilt,
      square_footage: stats.squareFootage ?? raw.square_footage ?? null,
      total_units: stats.units ?? row.units,
      unit_count: stats.units ?? row.units,
      unit_count_status: stats.units !== null || row.units !== null ? "explicit_value" : "no_data",
      unit_count_source: stats.units !== null ? "crexi_details.units" : row.units !== null ? "external_market_listings.units" : "no_data",
      unit_mix: unitMixWithApartmentUnits(raw, stats.apartmentUnits),
      occupancy_pct: stats.occupancyPct,
      occupancy_status: stats.occupancyStatus,
      tenancy: stats.tenancy,
      zoning: stats.zoning,
      keys: stats.keys,
      price_per_sqft: stats.pricePerSqft,
      avg_unit_sqft: stats.avgUnitSqft,
    };
    const nextUnits = stats.units ?? row.units;
    const nextPricePerUnit = stats.pricePerUnit ?? row.price_per_unit;
    const changed = JSON.stringify({
      multifamily: raw.multifamily,
      apartment_units: raw.apartment_units,
      building_count: raw.building_count,
      story_count: raw.story_count,
      year_built: raw.year_built,
      years_built: raw.years_built,
      occupancy_pct: raw.occupancy_pct,
      occupancy_status: raw.occupancy_status,
      tenancy: raw.tenancy,
      zoning: raw.zoning,
      keys: raw.keys,
      price_per_sqft: raw.price_per_sqft,
      avg_unit_sqft: raw.avg_unit_sqft,
      total_units: raw.total_units,
      unit_count: raw.unit_count,
      unit_count_status: raw.unit_count_status,
      unit_count_source: raw.unit_count_source,
      unit_mix: raw.unit_mix,
      units: row.units,
      price_per_unit: row.price_per_unit,
    }) !== JSON.stringify({
      multifamily: patchRaw.multifamily,
      apartment_units: patchRaw.apartment_units,
      building_count: patchRaw.building_count,
      story_count: patchRaw.story_count,
      year_built: patchRaw.year_built,
      years_built: patchRaw.years_built,
      occupancy_pct: patchRaw.occupancy_pct,
      occupancy_status: patchRaw.occupancy_status,
      tenancy: patchRaw.tenancy,
      zoning: patchRaw.zoning,
      keys: patchRaw.keys,
      price_per_sqft: patchRaw.price_per_sqft,
      avg_unit_sqft: patchRaw.avg_unit_sqft,
      total_units: patchRaw.total_units,
      unit_count: patchRaw.unit_count,
      unit_count_status: patchRaw.unit_count_status,
      unit_count_source: patchRaw.unit_count_source,
      unit_mix: patchRaw.unit_mix,
      units: nextUnits,
      price_per_unit: nextPricePerUnit,
    });

    if (changed) {
      updated++;
      if (!DRY_RUN) {
        await db.query(`
          update external_market_listings
             set units = $1,
                 price_per_unit = $2,
                 raw = coalesce(raw, '{}'::jsonb) || $3::jsonb,
                 updated_at = now()
           where id = $4;
        `, [nextUnits, nextPricePerUnit, JSON.stringify(patchRaw), row.id]);
      }
    }

    if (examples.length < 20) {
      examples.push({
        id: row.id,
        title: row.title,
        units: nextUnits,
        squareFootage: stats.squareFootage,
        buildings: stats.buildings,
        stories: stats.stories,
        yearBuilt: stats.yearBuilt,
        occupancyPct: stats.occupancyPct,
        pricePerSqft: stats.pricePerSqft,
      });
    }
  }

  await db.end();
  const report = {
    schemaVersion: "mxre.crexiMultifamilyStatsBackfill.v1",
    generatedAt: new Date().toISOString(),
    dryRun: DRY_RUN,
    scanned: rows.length,
    updated,
    withUnits,
    withSquareFootage,
    withOccupancy,
    withYearBuilt,
    withBuildings,
    examples,
  };

  if (WRITE_LOG) {
    const dir = join(process.cwd(), "logs", "crexi-multifamily-stats");
    await mkdir(dir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    await writeFile(join(dir, `crexi-multifamily-stats-${stamp}.json`), `${JSON.stringify(report, null, 2)}\n`);
  }
  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
