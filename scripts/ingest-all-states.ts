#!/usr/bin/env tsx
/**
 * Master Statewide Ingest — runs all available statewide parcel scripts in sequence.
 *
 * Available statewide datasets (estimated parcels):
 *   NC:  5.9M parcels (NC OneMap ArcGIS)
 *   PA:  4.7M parcels (PA DEP ArcGIS)
 *   CA:  4.0M parcels (multi-county ArcGIS/Socrata)
 *   NY:  3.7M parcels (NYS GIS ArcGIS)
 *   WI:  3.6M parcels (WI SCO ArcGIS)
 *   IL:  1.9M parcels (Cook County Socrata)
 *   MN:  statewide GeoPackage
 *   FL:  67 counties via NAL bulk download (separate process)
 *   TX:  4 counties via bulk CSV (separate process)
 *   OK:  11 counties via ActDataScout/OKTaxRolls (separate process)
 *   CO:  2.0M+ parcels (CO GIS ArcGIS FeatureServer, partial county coverage)
 *   IN:  3.5M parcels (IN Data Harvest ArcGIS FeatureServer)
 *   MD:  2.4M parcels (MD iMAP ArcGIS MapServer)
 *   VA:  4.0M+ parcels (VDEM ArcGIS FeatureServer, parcel IDs only)
 *   WA:  3.0M+ parcels (WA Geospatial ArcGIS FeatureServer)
 *   OR:  1.8M parcels (ODF Taxlots ArcGIS MapServer)
 *
 * Usage:
 *   npx tsx scripts/ingest-all-states.ts              # Run all statewide ingests
 *   npx tsx scripts/ingest-all-states.ts NC PA         # Run specific states
 *   npx tsx scripts/ingest-all-states.ts --dry-run     # Just show what would run
 *   npx tsx scripts/ingest-all-states.ts --skip=CA,MN  # Skip specific states
 */
import "dotenv/config";
import { execSync, type ExecSyncOptions } from "node:child_process";

interface StateIngest {
  code: string;
  name: string;
  script: string;
  estimatedParcels: string;
  notes: string;
}

const STATEWIDE_INGESTS: StateIngest[] = [
  { code: "NC", name: "North Carolina", script: "scripts/ingest-nc-parcels.ts", estimatedParcels: "5.9M", notes: "NC OneMap ArcGIS FeatureServer" },
  { code: "PA", name: "Pennsylvania", script: "scripts/ingest-pa-statewide.ts", estimatedParcels: "4.7M", notes: "PA DEP ArcGIS MapServer" },
  { code: "NY", name: "New York", script: "scripts/ingest-ny-statewide.ts", estimatedParcels: "3.7M", notes: "NYS GIS FeatureServer" },
  { code: "WI", name: "Wisconsin", script: "scripts/ingest-wi-parcels.ts", estimatedParcels: "3.6M", notes: "WI SCO ArcGIS FeatureServer" },
  { code: "CA", name: "California", script: "scripts/ingest-ca-parcels.ts", estimatedParcels: "4.0M+", notes: "Multi-county ArcGIS/Socrata" },
  { code: "IL", name: "Illinois", script: "scripts/ingest-il-parcels.ts", estimatedParcels: "1.9M", notes: "Cook County Socrata API" },
  { code: "MN", name: "Minnesota", script: "scripts/ingest-minnesota.ts", estimatedParcels: "2.7M", notes: "MnGeo GeoPackage + ArcGIS fallback" },
  { code: "OH", name: "Ohio", script: "scripts/ingest-geauga-oh.ts", estimatedParcels: "varies", notes: "Individual county scripts" },
  { code: "IA", name: "Iowa", script: "scripts/ingest-blackhawk-county-ia.ts", estimatedParcels: "varies", notes: "Individual county scripts" },
  { code: "NH", name: "New Hampshire", script: "scripts/ingest-nh-parcels.ts", estimatedParcels: "600K", notes: "NH GRANIT ArcGIS" },
  { code: "MI", name: "Michigan", script: "scripts/ingest-oakland-mi.ts", estimatedParcels: "500K", notes: "Oakland County" },
  { code: "AR", name: "Arkansas", script: "scripts/ingest-arcgis-bulk.ts", estimatedParcels: "varies", notes: "ArcGIS bulk" },
  { code: "CO", name: "Colorado", script: "scripts/ingest-co-parcels.ts", estimatedParcels: "2M+", notes: "CO GIS FeatureServer (partial county coverage)" },
  { code: "IN", name: "Indiana", script: "scripts/ingest-in-parcels.ts", estimatedParcels: "3.5M", notes: "IN Data Harvest ArcGIS FeatureServer" },
  { code: "MD", name: "Maryland", script: "scripts/ingest-md-parcels.ts", estimatedParcels: "2.4M", notes: "MD iMAP ArcGIS MapServer" },
  { code: "VA", name: "Virginia", script: "scripts/ingest-va-parcels.ts", estimatedParcels: "4M+", notes: "VDEM ArcGIS FeatureServer (parcel IDs only)" },
  { code: "WA", name: "Washington", script: "scripts/ingest-wa-parcels.ts", estimatedParcels: "3M+", notes: "WA Geospatial FeatureServer" },
  { code: "OR", name: "Oregon", script: "scripts/ingest-or-parcels.ts", estimatedParcels: "1.8M", notes: "ODF Taxlots ArcGIS MapServer" },
];

function parseArgs() {
  const args = process.argv.slice(2);
  const dryRun = args.includes("--dry-run");
  const skipArg = args.find(a => a.startsWith("--skip="));
  const skip = new Set(skipArg?.split("=")[1]?.split(",").map(s => s.toUpperCase()) ?? []);
  const specific = args.filter(a => !a.startsWith("--")).map(a => a.toUpperCase());
  return { dryRun, skip, specific };
}

async function main() {
  const { dryRun, skip, specific } = parseArgs();

  console.log("╔══════════════════════════════════════════════════════╗");
  console.log("║         MXRE Statewide Parcel Ingestion             ║");
  console.log("╚══════════════════════════════════════════════════════╝\n");

  let ingests = STATEWIDE_INGESTS;
  if (specific.length > 0) {
    ingests = ingests.filter(i => specific.includes(i.code));
  }
  if (skip.size > 0) {
    ingests = ingests.filter(i => !skip.has(i.code));
  }

  console.log(`States to process: ${ingests.length}`);
  console.log("─".repeat(60));
  for (const state of ingests) {
    console.log(`  ${state.code}  ${state.name.padEnd(20)} ${state.estimatedParcels.padEnd(8)} ${state.notes}`);
  }
  console.log("─".repeat(60));

  if (dryRun) {
    console.log("\n[DRY RUN] Would run the above scripts. Remove --dry-run to execute.");
    return;
  }

  const results: Array<{ state: string; status: string; duration: number }> = [];

  for (const state of ingests) {
    console.log(`\n${"═".repeat(60)}`);
    console.log(`  ${state.code} — ${state.name} (est. ${state.estimatedParcels} parcels)`);
    console.log(`${"═".repeat(60)}\n`);

    const start = Date.now();
    try {
      const opts: ExecSyncOptions = {
        stdio: "inherit",
        timeout: 4 * 60 * 60 * 1000, // 4 hour timeout per state
        env: process.env,
        cwd: process.cwd(),
      };
      execSync(`npx tsx ${state.script}`, opts);
      const duration = (Date.now() - start) / 1000;
      results.push({ state: state.code, status: "OK", duration });
      console.log(`\n  ✓ ${state.code} complete in ${duration.toFixed(0)}s`);
    } catch (err) {
      const duration = (Date.now() - start) / 1000;
      const msg = err instanceof Error ? err.message : "Unknown error";
      results.push({ state: state.code, status: `FAIL: ${msg.slice(0, 50)}`, duration });
      console.error(`\n  ✗ ${state.code} failed after ${duration.toFixed(0)}s: ${msg}`);
      // Continue to next state — don't stop on failure
    }
  }

  // Summary
  console.log("\n" + "═".repeat(60));
  console.log("  INGESTION SUMMARY");
  console.log("═".repeat(60));

  for (const r of results) {
    const icon = r.status === "OK" ? "✓" : "✗";
    console.log(`  ${icon}  ${r.state.padEnd(4)} ${r.status.padEnd(40)} ${r.duration.toFixed(0)}s`);
  }

  const succeeded = results.filter(r => r.status === "OK").length;
  const failed = results.filter(r => r.status !== "OK").length;
  const totalTime = results.reduce((s, r) => s + r.duration, 0);
  console.log("─".repeat(60));
  console.log(`  ${succeeded} succeeded, ${failed} failed, ${(totalTime / 3600).toFixed(1)} hours total`);
}

main().catch(console.error);
