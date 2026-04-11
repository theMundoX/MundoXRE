#!/usr/bin/env tsx
/**
 * MXRE Command Center — Data coverage dashboard.
 *
 * Shows completion percentage for each county across all data dimensions:
 *   - Properties ingested
 *   - Properties with addresses
 *   - Properties with assessed values (detail page data)
 *   - Rent estimates generated
 *   - Mortgage estimates generated
 *
 * Usage:
 *   npx tsx scripts/command-center.ts
 *   npx tsx scripts/command-center.ts --state=OK
 *   npx tsx scripts/command-center.ts --watch   (refresh every 30s)
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}
function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const stateFilter = getArg("state");
const watchMode = hasFlag("watch");
const watchInterval = parseInt(getArg("interval") || "30", 10) * 1000;

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

// Known estimated totals for counties
const KNOWN_TOTALS: Record<string, number> = {
  "Comanche_OK": 57_081,
  "Oklahoma_OK": 350_000,
  "Tulsa_OK": 280_000,
  "Dallas_TX": 1_200_000,
  "Tarrant_TX": 700_000,
  // Florida counties
  "Alachua_FL": 108_000,
  "Baker_FL": 14_000,
  "Bay_FL": 115_000,
  "Bradford_FL": 15_000,
  "Brevard_FL": 330_000,
  "Broward_FL": 750_000,
  "Calhoun_FL": 8_000,
  "Charlotte_FL": 120_000,
  "Citrus_FL": 95_000,
  "Clay_FL": 110_000,
  "Collier_FL": 220_000,
  "Columbia_FL": 40_000,
  "Dade_FL": 950_000,
  "Desoto_FL": 22_000,
  "Dixie_FL": 10_000,
  "Duval_FL": 450_000,
  "Escambia_FL": 165_000,
  "Flagler_FL": 65_000,
  "Franklin_FL": 18_000,
  "Gadsden_FL": 28_000,
  "Gilchrist_FL": 10_000,
  "Glades_FL": 10_000,
  "Gulf_FL": 14_000,
  "Hamilton_FL": 9_000,
  "Hardee_FL": 16_000,
  "Hendry_FL": 22_000,
  "Hernando_FL": 115_000,
  "Highlands_FL": 75_000,
  "Hillsborough_FL": 550_000,
  "Holmes_FL": 12_000,
  "Indian River_FL": 95_000,
  "Jackson_FL": 30_000,
  "Jefferson_FL": 10_000,
  "Lafayette_FL": 5_000,
  "Lake_FL": 210_000,
  "Lee_FL": 400_000,
  "Leon_FL": 135_000,
  "Levy_FL": 30_000,
  "Liberty_FL": 5_000,
  "Madison_FL": 12_000,
  "Manatee_FL": 210_000,
  "Marion_FL": 210_000,
  "Martin_FL": 80_000,
  "Monroe_FL": 55_000,
  "Nassau_FL": 55_000,
  "Okaloosa_FL": 115_000,
  "Okeechobee_FL": 25_000,
  "Orange_FL": 500_000,
  "Osceola_FL": 180_000,
  "Palm Beach_FL": 650_000,
  "Pasco_FL": 280_000,
  "Pinellas_FL": 400_000,
  "Polk_FL": 370_000,
  "Putnam_FL": 50_000,
  "Saint Johns_FL": 135_000,
  "Saint Lucie_FL": 165_000,
  "Santa Rosa_FL": 100_000,
  "Sarasota_FL": 230_000,
  "Seminole_FL": 195_000,
  "Sumter_FL": 75_000,
  "Suwannee_FL": 25_000,
  "Taylor_FL": 14_000,
  "Union_FL": 6_000,
  "Volusia_FL": 310_000,
  "Wakulla_FL": 20_000,
  "Walton_FL": 55_000,
  "Washington_FL": 15_000,
};

interface CountyStats {
  id: number;
  county_name: string;
  state_code: string;
  total_properties: number;
  with_address: number;
  with_assessed_value: number;
  with_year_built: number;
  with_sqft: number;
  with_rent: number;
  with_mortgage: number;
  estimated_total: number;
}

async function getStats(): Promise<CountyStats[]> {
  // Get all counties
  let query = db.from("counties").select("id, county_name, state_code").eq("active", true);
  if (stateFilter) query = query.eq("state_code", stateFilter);
  const { data: counties, error } = await query.order("state_code").order("county_name");
  if (error || !counties) return [];

  const stats: CountyStats[] = [];

  for (const county of counties) {
    const key = `${county.county_name}_${county.state_code}`;
    const estimatedTotal = KNOWN_TOTALS[key] || 0;

    // Total properties
    const { count: total } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", county.id);

    // With real address (not empty)
    const { count: withAddr } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", county.id)
      .neq("address", "");

    // With assessed value (detail page data)
    const { count: withAV } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", county.id)
      .not("assessed_value", "is", null);

    // With year built
    const { count: withYB } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", county.id)
      .not("year_built", "is", null);

    // With sqft
    const { count: withSqft } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", county.id)
      .not("total_sqft", "is", null);

    // Get property IDs range for this county to check related tables
    const { data: propRange } = await db
      .from("properties")
      .select("id")
      .eq("county_id", county.id)
      .order("id")
      .limit(1);

    const { data: propRangeEnd } = await db
      .from("properties")
      .select("id")
      .eq("county_id", county.id)
      .order("id", { ascending: false })
      .limit(1);

    let withRent = 0;
    let withMortgage = 0;

    if (propRange?.length && propRangeEnd?.length) {
      const minId = propRange[0].id;
      const maxId = propRangeEnd[0].id;

      // Count distinct properties with rent snapshots
      const { data: rentProps } = await db
        .from("rent_snapshots")
        .select("property_id")
        .gte("property_id", minId)
        .lte("property_id", maxId);
      withRent = new Set((rentProps ?? []).map((r) => r.property_id)).size;

      // Count distinct properties with mortgage records
      const { data: mortProps } = await db
        .from("mortgage_records")
        .select("property_id")
        .gte("property_id", minId)
        .lte("property_id", maxId);
      withMortgage = new Set((mortProps ?? []).map((m) => m.property_id)).size;
    }

    stats.push({
      id: county.id,
      county_name: county.county_name,
      state_code: county.state_code,
      total_properties: total ?? 0,
      with_address: withAddr ?? 0,
      with_assessed_value: withAV ?? 0,
      with_year_built: withYB ?? 0,
      with_sqft: withSqft ?? 0,
      with_rent: withRent,
      with_mortgage: withMortgage,
      estimated_total: estimatedTotal,
    });
  }

  return stats;
}

function pct(n: number, total: number): string {
  if (total === 0) return "  -  ";
  const p = Math.round((n / total) * 100);
  if (p >= 100) return " 100%";
  if (p === 0 && n > 0) return "  <1%";
  return `${String(p).padStart(3)}% `;
}

function bar(n: number, total: number, width: number = 10): string {
  if (total === 0) return "░".repeat(width);
  const filled = Math.round((n / total) * width);
  return "█".repeat(filled) + "░".repeat(width - filled);
}

function formatNum(n: number): string {
  return n.toLocaleString().padStart(8);
}

function render(stats: CountyStats[]) {
  const now = new Date().toLocaleTimeString();
  console.clear();
  console.log(`╔══════════════════════════════════════════════════════════════════════════════════════════════════╗`);
  console.log(`║  MXRE COMMAND CENTER                                                          ${now.padStart(12)}  ║`);
  console.log(`║  Data Coverage Dashboard                                               DB: ${SUPABASE_URL.includes("207.244") ? "self-hosted" : "managed    "}  ║`);
  console.log(`╠══════════════════════════════════════════════════════════════════════════════════════════════════╣`);
  console.log(`║                                                                                                ║`);

  if (stats.length === 0) {
    console.log(`║  No counties found.                                                                              ║`);
    console.log(`╚══════════════════════════════════════════════════════════════════════════════════════════════════╝`);
    return;
  }

  for (const s of stats) {
    const total = s.estimated_total || s.total_properties;
    const overallPct = total > 0 ? Math.round((s.total_properties / total) * 100) : 0;

    // Calculate overall "readiness" — all dimensions filled
    const dimensions = s.total_properties > 0 ? [
      s.with_address / s.total_properties,
      s.with_rent / s.total_properties,
    ] : [0, 0];
    const readiness = Math.round(dimensions.reduce((a, b) => a + b, 0) / dimensions.length * 100);

    console.log(`║  ┌─ ${(s.county_name + " County, " + s.state_code).padEnd(25)} ${"─".repeat(60)}──┐  ║`);
    console.log(`║  │  Overall Readiness: ${bar(readiness, 100, 20)} ${String(readiness).padStart(3)}%                                     │  ║`);
    console.log(`║  │                                                                                          │  ║`);
    console.log(`║  │  Properties:  ${formatNum(s.total_properties)} / ${formatNum(total)} ${pct(s.total_properties, total)} ${bar(s.total_properties, total, 15)}                     │  ║`);
    console.log(`║  │  Addresses:   ${formatNum(s.with_address)} / ${formatNum(s.total_properties)} ${pct(s.with_address, s.total_properties)} ${bar(s.with_address, s.total_properties, 15)}                     │  ║`);
    console.log(`║  │  Assessed $:  ${formatNum(s.with_assessed_value)} / ${formatNum(s.total_properties)} ${pct(s.with_assessed_value, s.total_properties)} ${bar(s.with_assessed_value, s.total_properties, 15)}  (needs detail backfill) │  ║`);
    console.log(`║  │  Year Built:  ${formatNum(s.with_year_built)} / ${formatNum(s.total_properties)} ${pct(s.with_year_built, s.total_properties)} ${bar(s.with_year_built, s.total_properties, 15)}  (needs detail backfill) │  ║`);
    console.log(`║  │  Sqft:        ${formatNum(s.with_sqft)} / ${formatNum(s.total_properties)} ${pct(s.with_sqft, s.total_properties)} ${bar(s.with_sqft, s.total_properties, 15)}  (needs detail backfill) │  ║`);
    console.log(`║  │  Rent Est:    ${formatNum(s.with_rent)} / ${formatNum(s.total_properties)} ${pct(s.with_rent, s.total_properties)} ${bar(s.with_rent, s.total_properties, 15)}                     │  ║`);
    console.log(`║  │  Mortgage:    ${formatNum(s.with_mortgage)} / ${formatNum(s.total_properties)} ${pct(s.with_mortgage, s.total_properties)} ${bar(s.with_mortgage, s.total_properties, 15)}                     │  ║`);
    console.log(`║  └${"─".repeat(90)}┘  ║`);
    console.log(`║                                                                                                ║`);
  }

  // Migration status
  console.log(`╠══════════════════════════════════════════════════════════════════════════════════════════════════╣`);
  console.log(`║  Legend:                                                                                        ║`);
  console.log(`║  █ = complete    ░ = remaining    "detail backfill" = requires per-parcel scrape (slow)         ║`);
  console.log(`║  Rent estimates use FMR model (fast). Mortgage estimates need sale price (from detail pages).   ║`);
  console.log(`╚══════════════════════════════════════════════════════════════════════════════════════════════════╝`);
}

async function run() {
  if (watchMode) {
    while (true) {
      const stats = await getStats();
      render(stats);
      await new Promise((r) => setTimeout(r, watchInterval));
    }
  } else {
    const stats = await getStats();
    render(stats);
  }
}

run().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
