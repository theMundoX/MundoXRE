#!/usr/bin/env tsx
/**
 * Discover Fidlar DirectSearch recorder portals across the Midwest.
 *
 * DirectSearch URL pattern: https://[state][county].fidlar.com/[State][County]/DirectSearch/
 * Example: https://inmarion.fidlar.com/INMarion/DirectSearch/
 *
 * Tests anonymous token access at: [webApiBase]/token
 * (grant_type=password&username=anonymous&password=anonymous)
 *
 * To find webApiBase, reads appConfig.json from the SPA root.
 *
 * Usage:
 *   npx tsx scripts/discover-fidlar-direct-counties.ts
 *   npx tsx scripts/discover-fidlar-direct-counties.ts --states=IL,WI,IA,MI
 *   npx tsx scripts/discover-fidlar-direct-counties.ts --output=results.json
 */

import "dotenv/config";
import { writeFileSync } from "node:fs";

const args = process.argv.slice(2);
const getArg  = (n: string) => args.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const STATES_ARG = getArg("states") ?? "IL,WI,IA,MI,OH,MN,MO,KS,NE,SD,ND,KY,TN";
const STATES  = STATES_ARG.split(",").map(s => s.trim().toUpperCase());
const OUTPUT  = getArg("output") ?? "fidlar-direct-counties.json";
const CONCURRENCY = 10;
const TIMEOUT_MS  = 12000;

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(q: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return res.json();
}

interface CountyRecord {
  county_id: number;
  county_name: string;
  state_code: string;
  state_fips: string;
  county_fips: string;
}

/** Build candidate URL slugs for a county name */
function buildSlugs(state: string, countyName: string): string[] {
  // Normalize: remove common suffixes, spaces, apostrophes
  const base = countyName
    .replace(/\s+county$/i, "")
    .replace(/[\s'\-\.]/g, "")
    .toLowerCase();

  const stateL = state.toLowerCase();
  const stateU = state.toUpperCase();

  return [
    // Pattern 1: state+county (most common for IN)
    `${stateL}${base}`,
    // Pattern 2: county+state
    `${base}${stateL}`,
    // Pattern 3: "county" suffix in slug
    `${stateL}${base}county`,
  ].map(s => s.replace(/[^a-z0-9]/g, ""));
}

interface ProbeResult {
  county_id: number;
  county_name: string;
  state_code: string;
  slug: string;
  base_url: string;
  web_api_base: string;
  anonymous_access: boolean;
}

async function probeSlug(slug: string, state: string, countyName: string): Promise<{ webApiBase: string; base_url: string } | null> {
  // The DirectSearch portal is at: https://[slug].fidlar.com/[Slug]/DirectSearch/
  // We need to read appConfig.json to get the webApiBase
  const slugCap = slug.charAt(0).toUpperCase() + slug.slice(1);
  const baseUrl = `https://${slug}.fidlar.com/${slugCap}/DirectSearch/`;
  const cfgUrl  = baseUrl + "appConfig.json";

  try {
    const cfgResp = await fetch(cfgUrl, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
      headers: { "User-Agent": "Mozilla/5.0 (compatible; MXRE/1.0)" },
    });
    if (!cfgResp.ok) return null;
    const cfg = await cfgResp.json() as { webApiBase?: string };
    if (!cfg.webApiBase) return null;
    return { webApiBase: cfg.webApiBase, base_url: baseUrl };
  } catch {
    return null;
  }
}

async function testAnonymousToken(webApiBase: string): Promise<boolean> {
  try {
    const resp = await fetch(webApiBase + "token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: "grant_type=password&username=anonymous&password=anonymous",
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!resp.ok) return false;
    const data = await resp.json() as { access_token?: string };
    return !!data.access_token;
  } catch {
    return false;
  }
}

async function probeCounty(county: CountyRecord): Promise<ProbeResult | null> {
  const slugs = buildSlugs(county.state_code, county.county_name);

  for (const slug of slugs) {
    const found = await probeSlug(slug, county.state_code, county.county_name);
    if (!found) continue;

    // Found the portal — check anonymous access
    const anon = await testAnonymousToken(found.webApiBase);
    return {
      county_id: county.county_id,
      county_name: county.county_name,
      state_code: county.state_code,
      slug,
      base_url: found.base_url,
      web_api_base: found.webApiBase,
      anonymous_access: anon,
    };
  }
  return null;
}

async function main() {
  console.log("Fidlar DirectSearch county discovery");
  console.log(`  States: ${STATES.join(", ")}`);
  console.log();

  // Load counties from DB
  const stateList = STATES.map(s => `'${s}'`).join(",");
  const counties = await pg(`
    SELECT id AS county_id, county_name, state_code, state_fips, county_fips
    FROM counties
    WHERE state_code IN (${stateList})
    ORDER BY state_code, county_name
  `) as CountyRecord[];

  console.log(`  Testing ${counties.length} counties...`);

  const results: ProbeResult[] = [];
  const found: ProbeResult[] = [];

  // Process with concurrency limit
  for (let i = 0; i < counties.length; i += CONCURRENCY) {
    const batch = counties.slice(i, i + CONCURRENCY);
    const probes = await Promise.all(batch.map(c => probeCounty(c)));
    for (const r of probes) {
      if (r) {
        results.push(r);
        if (r.anonymous_access) found.push(r);
        console.log(`  ✓ ${r.state_code} ${r.county_name} — ${r.base_url} [anon=${r.anonymous_access}]`);
      }
    }
    // Progress
    const pct = Math.round((i + batch.length) / counties.length * 100);
    process.stdout.write(`\r  Progress: ${i + batch.length}/${counties.length} (${pct}%) — ${results.length} portals found, ${found.length} free`);
  }

  console.log(`\n\nFound ${results.length} total DirectSearch portals`);
  console.log(`Free (anonymous): ${found.length}`);
  console.log();

  if (found.length > 0) {
    console.log("FREE counties:");
    for (const r of found) {
      console.log(`  ${r.state_code} | ${r.county_name} | ${r.base_url}`);
    }
  }

  writeFileSync(OUTPUT, JSON.stringify({ all: results, free: found }, null, 2));
  console.log(`\nResults written to ${OUTPUT}`);
}

main().catch(e => { console.error(e); process.exit(1); });
