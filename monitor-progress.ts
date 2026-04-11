#!/usr/bin/env tsx
/**
 * MXRE Progress Monitor
 * Shows property counts every minute - no cloud costs, pure local monitoring
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
  { auth: { persistSession: false } }
);

async function getStats() {
  try {
    const { count: totalProperties } = await db
      .from("properties")
      .select("*", { count: "exact", head: true });

    const { count: withRent } = await db
      .from("rent_snapshots")
      .select("*", { count: "exact", head: true });

    const { count: withMortgage } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true });

    const { data: countyData } = await db
      .from("county_stats_mv")
      .select("*")
      .order("total", { ascending: false })
      .limit(10);

    return {
      totalProperties: totalProperties || 0,
      withRent: withRent || 0,
      withMortgage: withMortgage || 0,
      topCounties: countyData || [],
    };
  } catch (err: any) {
    console.error("Query error:", err.message);
    return null;
  }
}

let lastCount = 0;
let lastTime = Date.now();

async function monitor() {
  const stats = await getStats();
  if (!stats) {
    console.log("[ERROR] Cannot reach database");
    return;
  }

  const now = Date.now();
  const elapsed = ((now - lastTime) / 1000).toFixed(1);
  const newRecords = stats.totalProperties - lastCount;
  const rate = ((newRecords / (now - lastTime)) * 1000).toFixed(0);

  console.log(`\n📊 MXRE PROGRESS — ${new Date().toLocaleTimeString()}`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`✓ Total Properties: ${stats.totalProperties.toLocaleString()}`);
  console.log(`  └─ +${newRecords.toLocaleString()} in ${elapsed}s (${rate} rec/s)`);
  console.log(`✓ With Rent Data: ${stats.withRent.toLocaleString()}`);
  console.log(`✓ With Mortgage: ${stats.withMortgage.toLocaleString()}`);
  console.log(`\n📍 Top 5 Counties:`);
  stats.topCounties.slice(0, 5).forEach((county: any) => {
    console.log(`   ${county.county_name}, ${county.state_code}: ${county.total?.toLocaleString() || 0}`);
  });
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);

  lastCount = stats.totalProperties;
  lastTime = now;
}

console.log("🚀 Starting progress monitor (updates every 60 seconds)...\n");
monitor();
setInterval(monitor, 60000);
