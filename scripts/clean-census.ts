#!/usr/bin/env tsx
/**
 * Clean the census data by removing known false-positive platforms
 * and generating accurate coverage numbers.
 *
 * false-positive platforms (return 200 for any county):
 * - true_automation: generic "Property Search" page for any cid
 * - cott_recordhub: generic login page for any path
 * - actdatascout: returns page for any state/county combo
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

interface CensusEntry {
  fips: string;
  state_fips: string;
  county_fips: string;
  name: string;
  state: string;
  state_name: string;
  assessor?: { platform: string; url: string; confidence: string };
  recorder?: { platform: string; url: string; confidence: string };
  status: string;
  checked_at: string;
}

// Known false-positive platforms
const FALSE_POSITIVE_ASSESSOR = new Set(["true_automation", "actdatascout"]);
const FALSE_POSITIVE_RECORDER = new Set(["cott_recordhub"]);

// actdatascout REAL states (verified to actually have data)
const ACTDS_REAL_STATES = new Set(["OK", "AR", "LA", "PA", "VA", "CT", "ME", "MA"]);

const censusPath = join(__dirname, "..", "data", "census.json");
const census: CensusEntry[] = JSON.parse(readFileSync(censusPath, "utf-8"));

let cleaned = 0;
for (const entry of census) {
  // Remove false positive assessors
  if (entry.assessor && FALSE_POSITIVE_ASSESSOR.has(entry.assessor.platform)) {
    // actdatascout is real for its known states
    if (entry.assessor.platform === "actdatascout" && ACTDS_REAL_STATES.has(entry.state)) {
      // Keep it — this is a real state for actdatascout
    } else {
      delete entry.assessor;
      cleaned++;
    }
  }

  // Remove false positive recorders
  if (entry.recorder && FALSE_POSITIVE_RECORDER.has(entry.recorder.platform)) {
    delete entry.recorder;
    cleaned++;
  }

  // Update status
  if (entry.assessor && entry.recorder) entry.status = "identified";
  else if (entry.assessor || entry.recorder) entry.status = "partial";
  else entry.status = "unknown";
}

// Save cleaned census
const cleanPath = join(__dirname, "..", "data", "census-clean.json");
writeFileSync(cleanPath, JSON.stringify(census, null, 2));

// Generate report
const total = census.length;
const full = census.filter(e => e.status === "identified").length;
const partial = census.filter(e => e.status === "partial").length;
const unknown = census.filter(e => e.status === "unknown").length;

console.log("═══════════════════════════════════════════════════════");
console.log("  MXRE COUNTY CENSUS — CLEANED (False Positives Removed)");
console.log("═══════════════════════════════════════════════════════\n");
console.log(`Total counties: ${total}`);
console.log(`Cleaned entries: ${cleaned}`);
console.log(`Fully identified: ${full} (${Math.round(full/total*100)}%)`);
console.log(`Partial: ${partial} (${Math.round(partial/total*100)}%)`);
console.log(`Unknown: ${unknown} (${Math.round(unknown/total*100)}%)\n`);

// Platform breakdown
const assessorPlatforms = new Map<string, number>();
const recorderPlatforms = new Map<string, number>();
let noAssessor = 0, noRecorder = 0;

for (const e of census) {
  if (e.assessor) assessorPlatforms.set(e.assessor.platform, (assessorPlatforms.get(e.assessor.platform) || 0) + 1);
  else noAssessor++;
  if (e.recorder) recorderPlatforms.set(e.recorder.platform, (recorderPlatforms.get(e.recorder.platform) || 0) + 1);
  else noRecorder++;
}

console.log("─── ASSESSOR PLATFORMS (Validated) ────────────────────\n");
for (const [p, c] of [...assessorPlatforms.entries()].sort((a, b) => b[1] - a[1])) {
  console.log(`  ${p.padEnd(25)} ${String(c).padStart(5)} counties`);
}
console.log(`  ${"(not found)".padEnd(25)} ${String(noAssessor).padStart(5)} counties`);

console.log("\n─── RECORDER PLATFORMS (Validated) ────────────────────\n");
for (const [p, c] of [...recorderPlatforms.entries()].sort((a, b) => b[1] - a[1])) {
  console.log(`  ${p.padEnd(25)} ${String(c).padStart(5)} counties`);
}
console.log(`  ${"(not found)".padEnd(25)} ${String(noRecorder).padStart(5)} counties`);

// State coverage
console.log("\n─── STATE COVERAGE ──────────────────────────────────\n");
const byState = new Map<string, { total: number; assessor: number; recorder: number }>();
for (const e of census) {
  const s = byState.get(e.state) || { total: 0, assessor: 0, recorder: 0 };
  s.total++;
  if (e.assessor) s.assessor++;
  if (e.recorder) s.recorder++;
  byState.set(e.state, s);
}

console.log(`  ${"State".padEnd(6)} ${"Total".padStart(6)} ${"Assess".padStart(8)} ${"Record".padStart(8)} ${"A%".padStart(6)} ${"R%".padStart(6)}`);
console.log(`  ${"─".repeat(6)} ${"─".repeat(6)} ${"─".repeat(8)} ${"─".repeat(8)} ${"─".repeat(6)} ${"─".repeat(6)}`);
for (const [st, stats] of [...byState.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
  const aPct = Math.round(stats.assessor / stats.total * 100);
  const rPct = Math.round(stats.recorder / stats.total * 100);
  console.log(`  ${st.padEnd(6)} ${String(stats.total).padStart(6)} ${String(stats.assessor).padStart(8)} ${String(stats.recorder).padStart(8)} ${(aPct + "%").padStart(6)} ${(rPct + "%").padStart(6)}`);
}
