#!/usr/bin/env tsx
/**
 * County assessor data ingestion script.
 *
 * Usage:
 *   npm run ingest -- --county=Comanche --state=OK
 *   npm run ingest -- --state=OK
 *   npm run ingest -- --state=OK --platform=oktaxrolls
 *   npm run ingest -- --county=Comanche --state=OK --dry-run
 *   npm run ingest -- --county=Comanche --state=OK --max=100
 */

import "dotenv/config";
import { ingest } from "../src/discovery/ingest.js";
import { registerAdapter } from "../src/discovery/registry.js";
import { initProxies, getProxyStats } from "../src/utils/proxy.js";
import { OKTaxRollsAdapter } from "../src/discovery/adapters/oktaxrolls.js";
import { ActDataScoutAdapter } from "../src/discovery/adapters/actdatascout.js";
import { DCADAdapter } from "../src/discovery/adapters/dcad.js";
import { HCADAdapter } from "../src/discovery/adapters/hcad.js";
import { FloridaNALAdapter } from "../src/discovery/adapters/florida-nal.js";
import { TADAdapter } from "../src/discovery/adapters/tad.js";
import { CookCountyAdapter } from "../src/discovery/adapters/cook-county.js";
import { DentonAdapter } from "../src/discovery/adapters/denton.js";
import { ArcGISAdapter } from "../src/discovery/adapters/arcgis.js";

// Initialize proxies from environment
initProxies();

// Register available adapters
registerAdapter(new OKTaxRollsAdapter());
registerAdapter(new ActDataScoutAdapter());
registerAdapter(new DCADAdapter());
registerAdapter(new HCADAdapter());
registerAdapter(new FloridaNALAdapter());
registerAdapter(new TADAdapter());
registerAdapter(new CookCountyAdapter());
registerAdapter(new DentonAdapter());
registerAdapter(new ArcGISAdapter());

// Parse CLI args
const args = process.argv.slice(2);

function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

function hasFlag(name: string): boolean {
  return args.includes(`--${name}`);
}

const state = getArg("state");
const county = getArg("county");
const platform = getArg("platform");
const dryRun = hasFlag("dry-run");
const maxRecords = getArg("max") ? parseInt(getArg("max")!, 10) : undefined;

if (!state && !county) {
  console.log("Usage: npm run ingest -- --state=OK [--county=Comanche] [--platform=oktaxrolls] [--dry-run] [--max=100]");
  process.exit(1);
}

console.log("MXRE County Assessor Ingestion");
console.log("─".repeat(40));
if (state) console.log(`State: ${state}`);
if (county) console.log(`County: ${county}`);
if (platform) console.log(`Platform: ${platform}`);
if (dryRun) console.log("Mode: DRY RUN (no database writes)");
if (maxRecords) console.log(`Max records: ${maxRecords}`);
const proxyStats = getProxyStats();
if (proxyStats.residential.total > 0) {
  console.log(`Proxies: ${proxyStats.residential.alive} residential alive`);
} else {
  console.log("Proxies: none configured (using direct connection)");
}
console.log();

ingest({ state, county, platform, dryRun, maxRecords })
  .then((results) => {
    if (results.length === 0) {
      console.log("No counties processed.");
    }
  })
  .catch((err) => {
    console.error("Ingestion failed:", err instanceof Error ? err.stack : err);
    process.exit(1);
  });
