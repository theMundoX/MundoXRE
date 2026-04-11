#!/usr/bin/env node
import "dotenv/config";
import { getDb } from "./src/db/client.js";

async function main() {
  const db = getDb();

  // Get property count
  const { data: props } = await db
    .from("properties")
    .select("count", { count: "exact" });

  // Get county breakdown
  const { data: counties } = await db
    .from("properties")
    .select("county_id, state_code")
    .limit(1000000);

  const byCounty = new Map<string, number>();
  for (const p of counties || []) {
    const key = p.state_code || "?";
    byCounty.set(key, (byCounty.get(key) || 0) + 1);
  }

  // Get county names
  const { data: countyList } = await db.from("counties").select("id, county_name, state_code");

  const countByState = new Map<string, number>();
  for (const p of counties || []) {
    const state = p.state_code;
    if (state) countByState.set(state, (countByState.get(state) || 0) + 1);
  }

  console.log("========================================");
  console.log("CURRENT MXRE STATUS");
  console.log("========================================\n");

  console.log(`Total properties: ${props?.[0]?.count || 0}`);
  console.log(`\nBy state:`);

  const sorted = Array.from(countByState.entries()).sort((a, b) => b[1] - a[1]);
  for (const [state, count] of sorted) {
    console.log(`  ${state}: ${count.toLocaleString()} properties`);
  }
}

main().catch(console.error);
