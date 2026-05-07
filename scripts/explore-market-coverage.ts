#!/usr/bin/env tsx
/**
 * Lightweight source-discovery placeholder for new MXRE coverage markets.
 *
 * This does not ingest paid data. It records the county/source hypotheses that
 * the next market-specific runner should turn into reusable ingestion scripts.
 */
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const args = process.argv.slice(2);
const valueArg = (name: string) => args.find((arg) => arg.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? "";

const city = valueArg("city");
const state = valueArg("state").toUpperCase();
if (!city || !state) throw new Error("Usage: npx tsx scripts/explore-market-coverage.ts --city=Dayton --state=OH");

const slug = `${city.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}-${state.toLowerCase()}`;

const sourceHints: Record<string, {
  counties: string[];
  parcel: string[];
  recorder: string[];
  notes: string[];
}> = {
  "dayton-oh": {
    counties: ["Montgomery County"],
    parcel: ["Montgomery County Auditor property search / GIS parcel downloads"],
    recorder: ["Montgomery County Recorder public search"],
    notes: ["Ohio county pattern should reuse Franklin/Dallas-style assessor and recorder adapters where possible."],
  },
  "toledo-oh": {
    counties: ["Lucas County"],
    parcel: ["Lucas County Auditor AREIS / parcel data"],
    recorder: ["Lucas County Recorder public document search"],
    notes: ["Prioritize city situs filter after full county parcel load."],
  },
  "cleveland-oh": {
    counties: ["Cuyahoga County"],
    parcel: ["Cuyahoga County Fiscal Officer property records / GIS"],
    recorder: ["Cuyahoga County Fiscal Office recorder document search"],
    notes: ["Large old-housing-stock market; taxes and municipality-level condition risk should be tracked."],
  },
  "akron-oh": {
    counties: ["Summit County"],
    parcel: ["Summit County Fiscal Office property records / GIS"],
    recorder: ["Summit County Fiscal Office recorder search"],
    notes: ["Likely reusable with Ohio public-record adapters after county-specific endpoint discovery."],
  },
  "fort-wayne-in": {
    counties: ["Allen County"],
    parcel: ["Allen County IN GIS / assessor property search"],
    recorder: ["Allen County Recorder public search"],
    notes: ["Indiana market; reuse Indianapolis public-signal and recorder patterns where source terms allow."],
  },
  "south-bend-in": {
    counties: ["St. Joseph County"],
    parcel: ["St. Joseph County assessor / GIS parcel search"],
    recorder: ["St. Joseph County Recorder public search"],
    notes: ["Indiana market; prioritize Notre Dame/near-campus rental submarket tagging later."],
  },
  "peoria-il": {
    counties: ["Peoria County"],
    parcel: ["Peoria County property tax / GIS parcel search"],
    recorder: ["Peoria County Recorder of Deeds public search"],
    notes: ["Illinois taxes can materially alter DSCR; tax fields should be first-class coverage metrics."],
  },
  "birmingham-al": {
    counties: ["Jefferson County", "Shelby County"],
    parcel: ["Jefferson County Tax Assessor / GIS", "Shelby County property tax / GIS"],
    recorder: ["Jefferson County Probate Court records", "Shelby County Probate records"],
    notes: ["Metro spans multiple counties; start Jefferson city coverage, then add Shelby as expansion."],
  },
  "memphis-tn": {
    counties: ["Shelby County"],
    parcel: ["Shelby County Assessor property search / parcel data"],
    recorder: ["Shelby County Register of Deeds"],
    notes: ["High-yield/high-risk market; crime/location signals are required before publishable scoring."],
  },
  "detroit-mi": {
    counties: ["Wayne County"],
    parcel: ["City of Detroit parcel/property data", "Wayne County tax parcel resources"],
    recorder: ["Wayne County Register of Deeds"],
    notes: ["Use Detroit city parcel sources plus Wayne recorder; block-level risk and tax delinquency matter."],
  },
};

const hints = sourceHints[slug] ?? { counties: [], parcel: [], recorder: [], notes: ["Source discovery required."] };
const out = {
  market: { city, state, slug },
  status: "source_discovery",
  generated_at: new Date().toISOString(),
  counties: hints.counties,
  source_hints: {
    parcel: hints.parcel,
    recorder: hints.recorder,
    listings: ["Reuse Redfin/listing ingestion where legally/publicly available; preserve source URLs and stale history."],
    rents: ["Reuse public apartment/floorplan discovery and rent snapshot framework."],
    paid_fallback: ["Use RealEstateAPI/Zillow RapidAPI only after public/local sources are exhausted and only property-scoped."],
  },
  next_steps: [
    "Verify county boundaries and source terms.",
    "Build or adapt parcel/recorder ingestion script.",
    "Run smoke ingestion and classification.",
    "Run listing, agent-contact, rent, creative-finance, and readiness audits.",
  ],
  notes: hints.notes,
};

await mkdir(join(process.cwd(), "logs", "market-refresh"), { recursive: true });
const path = join(process.cwd(), "logs", "market-refresh", `${slug}-source-discovery.json`);
await writeFile(path, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ wrote: path, ...out }, null, 2));
