#!/usr/bin/env node
/**
 * TIGER-Aware Gap Analysis
 *
 * Compares current Supabase coverage against:
 * 1. Configured counties in registry
 * 2. TIGER/Line gap analysis priority phases
 *
 * Output: Prioritized backfill roadmap with deployment phases
 */

import "dotenv/config";
import { getCountyConfigs } from "./src/discovery/registry.js";
import { getDb } from "./src/db/client.js";

// TIGER priority phases from TIGER-GAP-ANALYSIS.md
const TIGER_PHASES = {
  phase1: [
    { state: "CA", county: "Los Angeles" },
    { state: "IL", county: "Cook" },
    { state: "TX", county: "Harris" },
    { state: "AZ", county: "Maricopa" },
    { state: "CA", county: "San Diego" },
    { state: "CA", county: "Orange" },
    { state: "NY", county: "Kings" },
    { state: "FL", county: "Miami-Dade" },
    { state: "NY", county: "Queens" },
    { state: "NV", county: "Clark" },
  ],
  phase2: [
    { state: "CA", county: "Riverside" },
    { state: "TX", county: "Bexar" },
    { state: "FL", county: "Broward" },
    { state: "CA", county: "Alameda" },
    { state: "NY", county: "New York" },
    { state: "FL", county: "Hillsborough" },
    { state: "FL", county: "Palm Beach" },
    { state: "FL", county: "Orange" },
    { state: "TX", county: "Travis" },
    { state: "GA", county: "Fulton" },
    { state: "TX", county: "Collin" },
    { state: "CA", county: "Kern" },
  ],
};

async function main() {
  console.log("========================================");
  console.log("TIGER-AWARE GAP ANALYSIS");
  console.log("Current coverage vs. TIGER priority phases");
  console.log("========================================\n");

  // Load configured counties
  const configured = getCountyConfigs();
  console.log(`Configured in registry: ${configured.length}\n`);

  // Query Supabase
  const db = getDb();

  const { data: counties } = await db
    .from("counties")
    .select("id, state_code, county_name, state_fips, county_fips");

  const { data: counts } = await db.rpc("get_property_counts_by_county");

  const countyMap = new Map<string, { id: number; propCount: number }>();
  for (const row of counts || []) {
    countyMap.set(row.county_name, { id: row.county_id, propCount: row.count });
  }

  // Map of configured counties by state + name
  const configuredMap = new Map<string, typeof configured[0]>();
  for (const c of configured) {
    const key = `${c.state}:${c.name}`;
    configuredMap.set(key, c);
  }

  // Analyze TIGER Phase 1
  console.log("── PHASE 1: TOP 10 PRIORITY COUNTIES (Week 1) ──\n");
  let phase1Complete = 0;
  for (const target of TIGER_PHASES.phase1) {
    const key = `${target.state}:${target.county}`;
    const configured = configuredMap.has(key);
    const data = countyMap.get(target.county);
    const status = !data ? "MISSING (not in Supabase)" : data.propCount === 0 ? "EMPTY (0 properties)" : "✓ LOADED";

    if (data && data.propCount > 0) phase1Complete++;
    console.log(`  ${target.county}, ${target.state} — ${status}${data ? ` (${data.propCount.toLocaleString()})` : ""}`);
  }
  console.log(`\n  Phase 1 Coverage: ${phase1Complete}/10 (${(phase1Complete / 10 * 100).toFixed(0)}%)\n`);

  // Analyze TIGER Phase 2
  console.log("── PHASE 2: MAJOR METROS (Weeks 2-3) ──\n");
  let phase2Complete = 0;
  for (const target of TIGER_PHASES.phase2) {
    const key = `${target.state}:${target.county}`;
    const data = countyMap.get(target.county);
    const status = !data ? "MISSING" : data.propCount === 0 ? "EMPTY (0 props)" : "✓ LOADED";

    if (data && data.propCount > 0) phase2Complete++;
    console.log(`  ${target.county}, ${target.state} — ${status}${data ? ` (${data.propCount.toLocaleString()})` : ""}`);
  }
  console.log(`\n  Phase 2 Coverage: ${phase2Complete}/${TIGER_PHASES.phase2.length} (${(phase2Complete / TIGER_PHASES.phase2.length * 100).toFixed(0)}%)\n`);

  // Summary
  console.log("── DEPLOYMENT ROADMAP ──\n");
  console.log("Current Status:");
  const totalPhaseCounties = TIGER_PHASES.phase1.length + TIGER_PHASES.phase2.length;
  const totalComplete = phase1Complete + phase2Complete;
  console.log(`  Phase 1+2: ${totalComplete}/${totalPhaseCounties} deployed (${(totalComplete / totalPhaseCounties * 100).toFixed(0)}%)`);
  console.log(`  Phase 1 readiness: ${phase1Complete === 10 ? "✓ READY FOR DEPLOYMENT" : `${10 - phase1Complete} counties remain"}`);

  console.log("\nNext Steps:");
  if (phase1Complete < 10) {
    console.log(`  1. Deploy Phase 1 gap counties: ${10 - phase1Complete} missing`);
    console.log(`     Run: npm run ingest -- --tiger-phase 1`);
  }

  if (phase2Complete < TIGER_PHASES.phase2.length) {
    console.log(`  2. Deploy Phase 2 expansion: ${TIGER_PHASES.phase2.length - phase2Complete} missing`);
    console.log(`     Run: npm run ingest -- --tiger-phase 2`);
  }

  console.log("\nNote: Phase 3+ deployment (60 total counties) requires adapters");
  console.log("for additional states (PA, MI, MA, WA, etc.)");
}

main().catch(console.error);
