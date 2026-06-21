#!/usr/bin/env tsx
import "dotenv/config";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parse } from "csv-parse/sync";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

type Target = {
  market: string;
  city: string;
  state: string;
  lat: string;
  lng: string;
};

const CURATED_TARGETS: Target[] = [
  { market: "branson-mo", city: "Branson", state: "MO", lat: "36.6437", lng: "-93.2185" },
  { market: "apache-junction-az", city: "Apache Junction", state: "AZ", lat: "33.4150", lng: "-111.5496" },
  { market: "bullhead-city-az", city: "Bullhead City", state: "AZ", lat: "35.1359", lng: "-114.5286" },
  { market: "quartzsite-az", city: "Quartzsite", state: "AZ", lat: "33.6639", lng: "-114.2299" },
  { market: "yuma-az", city: "Yuma", state: "AZ", lat: "32.6927", lng: "-114.6277" },
  { market: "lake-havasu-city-az", city: "Lake Havasu City", state: "AZ", lat: "34.4839", lng: "-114.3225" },
  { market: "mesa-az", city: "Mesa", state: "AZ", lat: "33.4152", lng: "-111.8315" },
  { market: "zephyrhills-fl", city: "Zephyrhills", state: "FL", lat: "28.2336", lng: "-82.1812" },
  { market: "lakeland-fl", city: "Lakeland", state: "FL", lat: "28.0395", lng: "-81.9498" },
  { market: "ocala-fl", city: "Ocala", state: "FL", lat: "29.1872", lng: "-82.1401" },
  { market: "kissimmee-fl", city: "Kissimmee", state: "FL", lat: "28.2920", lng: "-81.4076" },
  { market: "pigeon-forge-tn", city: "Pigeon Forge", state: "TN", lat: "35.7884", lng: "-83.5543" },
  { market: "gatlinburg-tn", city: "Gatlinburg", state: "TN", lat: "35.7143", lng: "-83.5102" },
  { market: "sevierville-tn", city: "Sevierville", state: "TN", lat: "35.8681", lng: "-83.5618" },
  { market: "myrtle-beach-sc", city: "Myrtle Beach", state: "SC", lat: "33.6891", lng: "-78.8867" },
  { market: "gulf-shores-al", city: "Gulf Shores", state: "AL", lat: "30.2460", lng: "-87.7008" },
  { market: "foley-al", city: "Foley", state: "AL", lat: "30.4066", lng: "-87.6836" },
  { market: "moab-ut", city: "Moab", state: "UT", lat: "38.5733", lng: "-109.5498" },
  { market: "custer-sd", city: "Custer", state: "SD", lat: "43.7667", lng: "-103.5988" },
  { market: "deadwood-sd", city: "Deadwood", state: "SD", lat: "44.3767", lng: "-103.7296" },
  { market: "rapid-city-sd", city: "Rapid City", state: "SD", lat: "44.0805", lng: "-103.2310" },
  { market: "rockport-tx", city: "Rockport", state: "TX", lat: "28.0206", lng: "-97.0544" },
  { market: "port-aransas-tx", city: "Port Aransas", state: "TX", lat: "27.8339", lng: "-97.0611" },
  { market: "fredericksburg-tx", city: "Fredericksburg", state: "TX", lat: "30.2752", lng: "-98.8719" },
  { market: "broken-bow-ok", city: "Broken Bow", state: "OK", lat: "34.0293", lng: "-94.7391" },
  { market: "eureka-springs-ar", city: "Eureka Springs", state: "AR", lat: "36.4012", lng: "-93.7379" },
  { market: "pagosa-springs-co", city: "Pagosa Springs", state: "CO", lat: "37.2694", lng: "-107.0098" },
  { market: "glenwood-springs-co", city: "Glenwood Springs", state: "CO", lat: "39.5505", lng: "-107.3248" },
  { market: "tampa-fl", city: "Tampa", state: "FL", lat: "27.9506", lng: "-82.4572" },
  { market: "clearwater-fl", city: "Clearwater", state: "FL", lat: "27.9659", lng: "-82.8001" },
  { market: "fort-myers-fl", city: "Fort Myers", state: "FL", lat: "26.6406", lng: "-81.8723" },
  { market: "cape-coral-fl", city: "Cape Coral", state: "FL", lat: "26.5629", lng: "-81.9495" },
  { market: "sarasota-fl", city: "Sarasota", state: "FL", lat: "27.3364", lng: "-82.5307" },
  { market: "orlando-fl", city: "Orlando", state: "FL", lat: "28.5383", lng: "-81.3792" },
  { market: "panama-city-beach-fl", city: "Panama City Beach", state: "FL", lat: "30.1766", lng: "-85.8055" },
  { market: "destin-fl", city: "Destin", state: "FL", lat: "30.3935", lng: "-86.4958" },
  { market: "san-antonio-tx", city: "San Antonio", state: "TX", lat: "29.4252", lng: "-98.4946" },
  { market: "dallas-tx", city: "Dallas", state: "TX", lat: "32.7767", lng: "-96.7970" },
  { market: "fort-worth-tx", city: "Fort Worth", state: "TX", lat: "32.7555", lng: "-97.3308" },
  { market: "houston-tx", city: "Houston", state: "TX", lat: "29.7604", lng: "-95.3698" },
  { market: "austin-tx", city: "Austin", state: "TX", lat: "30.2672", lng: "-97.7431" },
  { market: "corpus-christi-tx", city: "Corpus Christi", state: "TX", lat: "27.8006", lng: "-97.3964" },
  { market: "phoenix-az", city: "Phoenix", state: "AZ", lat: "33.4484", lng: "-112.0740" },
  { market: "tucson-az", city: "Tucson", state: "AZ", lat: "32.2226", lng: "-110.9747" },
  { market: "scottsdale-az", city: "Scottsdale", state: "AZ", lat: "33.4942", lng: "-111.9261" },
  { market: "las-vegas-nv", city: "Las Vegas", state: "NV", lat: "36.1716", lng: "-115.1391" },
  { market: "colorado-springs-co", city: "Colorado Springs", state: "CO", lat: "38.8339", lng: "-104.8214" },
  { market: "knoxville-tn", city: "Knoxville", state: "TN", lat: "35.9606", lng: "-83.9207" },
  { market: "nashville-tn", city: "Nashville", state: "TN", lat: "36.1627", lng: "-86.7816" },
  { market: "little-rock-ar", city: "Little Rock", state: "AR", lat: "34.7465", lng: "-92.2896" },
  { market: "sacramento-ca", city: "Sacramento", state: "CA", lat: "38.5816", lng: "-121.4944" },
  { market: "bakersfield-ca", city: "Bakersfield", state: "CA", lat: "35.3733", lng: "-119.0187" },
  { market: "fresno-ca", city: "Fresno", state: "CA", lat: "36.7378", lng: "-119.7871" },
  { market: "redding-ca", city: "Redding", state: "CA", lat: "40.5865", lng: "-122.3917" },
  { market: "palm-springs-ca", city: "Palm Springs", state: "CA", lat: "33.8303", lng: "-116.5453" },
];

const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;

const dryRun = process.argv.includes("--dry-run");
const maxTotal = Math.max(1, Number.parseInt(arg("max-total", "25") ?? "25", 10));
const limit = Math.max(1, Number.parseInt(arg("limit", "60") ?? "60", 10));
const detailLimit = Math.max(0, Number.parseInt(arg("detail-limit", "20") ?? "20", 10));
const maxPages = Math.max(1, Number.parseInt(arg("max-pages", "6") ?? "6", 10));
const targetSource = (arg("target-source", "curated") ?? "curated").toLowerCase();
const targetLimit = Math.max(1, Number.parseInt(arg("target-limit", "250") ?? "250", 10));
const targetOffset = Math.max(0, Number.parseInt(arg("target-offset", "0") ?? "0", 10));
const minPopulation = Math.max(0, Number.parseInt(arg("min-population", "0") ?? "0", 10));
const stateFilter = new Set((arg("states", "") ?? "").split(",").map((state) => state.trim().toUpperCase()).filter(Boolean));

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function loadUsCityTargets(): Target[] {
  const csvPath = join(process.cwd(), "data", "external", "uscities.csv");
  const rows = parse(readFileSync(csvPath, "utf8"), { columns: true, skip_empty_lines: true }) as Array<Record<string, string>>;
  return rows
    .map((row) => ({
      city: row.city_ascii || row.city,
      state: row.state_id,
      lat: row.lat,
      lng: row.lng,
      population: Number.parseInt(row.population || "0", 10) || 0,
    }))
    .filter((row) => row.city && row.state && row.lat && row.lng)
    .filter((row) => row.population >= minPopulation)
    .filter((row) => stateFilter.size === 0 || stateFilter.has(row.state))
    .sort((a, b) => b.population - a.population)
    .slice(targetOffset, targetOffset + targetLimit)
    .map((row) => ({
      market: `${slug(row.city)}-${row.state.toLowerCase()}`,
      city: row.city,
      state: row.state,
      lat: row.lat,
      lng: row.lng,
    }));
}

const TARGETS = targetSource === "uscities" ? loadUsCityTargets() : CURATED_TARGETS;

const results: Array<{ market: string; page: number; upserted: number; matched: number; searchRows: number; status: string }> = [];
let totalUpserted = 0;
const tsxCli = join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");

for (const target of TARGETS) {
  if (!dryRun && totalUpserted >= maxTotal) break;
  for (let page = 1; page <= maxPages; page++) {
    if (!dryRun && totalUpserted >= maxTotal) break;
    const remaining = dryRun ? maxTotal : Math.max(1, maxTotal - totalUpserted);
    console.log(`=== CREXI RV seed ${target.market} page=${page}/${maxPages} remaining=${remaining} ===`);

    const child = spawnSync(
      process.execPath,
      [
        tsxCli,
        "scripts/ingest-crexi-rapidapi.ts",
        `--market=${target.market}`,
        `--city=${target.city}`,
        `--state=${target.state}`,
        `--lat=${target.lat}`,
        `--lng=${target.lng}`,
        "--asset-class=mobile_home_rv",
        `--limit=${limit}`,
        `--detail-limit=${detailLimit}`,
        `--max-upsert=${remaining}`,
        `--page=${page}`,
        ...(dryRun ? ["--dry-run"] : []),
      ],
      {
        cwd: process.cwd(),
        encoding: "utf8",
        timeout: 180_000,
        windowsHide: true,
      },
    );

    const output = `${child.stdout ?? ""}${child.stderr ?? ""}`;
    process.stdout.write(output);

    const upserted = Number(output.match(/"upserted"\s*:\s*(\d+)/)?.[1] ?? 0);
    const matched = Number(output.match(/"matched"\s*:\s*(\d+)/)?.[1] ?? 0);
    const searchRows = Number(output.match(/Search rows:\s*(\d+)/)?.[1] ?? 0);
    totalUpserted += upserted;
    results.push({
      market: target.market,
      page,
      upserted,
      matched,
      searchRows,
      status: child.status === 0 ? "ok" : child.error ? child.error.message : `exit_${child.status}`,
    });
    if (child.status !== 0) break;
    if (searchRows === 0 || searchRows < limit) break;
  }
}

console.log(JSON.stringify({
  dryRun,
  maxTotal,
  maxPages,
  targetSource,
  targetLimit,
  targetOffset,
  minPopulation,
  totalUpserted,
  scannedMarkets: results.length,
  results,
}, null, 2));
