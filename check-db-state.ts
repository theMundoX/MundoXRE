#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!,
  { auth: { persistSession: false } }
);

async function checkState() {
  try {
    console.log("Checking database state...\n");

    // Total properties
    const { count: totalProps } = await db
      .from("properties")
      .select("*", { count: "exact", head: true });

    // Properties by county (sample)
    const { data: topCounties } = await db
      .from("properties")
      .select("county_fips")
      .limit(1000);

    const countiesCounts = new Map();
    topCounties?.forEach((row: any) => {
      countiesCounts.set(row.county_fips, (countiesCounts.get(row.county_fips) || 0) + 1);
    });

    console.log(`Total Properties: ${totalProps?.toLocaleString() || 'unknown'}`);
    console.log(`\nTop counties (from sample of 1000):`);
    Array.from(countiesCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .forEach(([fips, count]) => {
        console.log(`  ${fips}: ${count}`);
      });

    // Mortgage records
    const { count: mortgages } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true });

    console.log(`\nTotal Mortgages: ${mortgages?.toLocaleString() || 'unknown'}`);

    // Rent data
    const { count: rents } = await db
      .from("rent_snapshots")
      .select("*", { count: "exact", head: true });

    console.log(`Total Rent Records: ${rents?.toLocaleString() || 'unknown'}`);

    // Check recent writes
    const { data: recentProps } = await db
      .from("properties")
      .select("created_at")
      .order("created_at", { ascending: false })
      .limit(1);

    if (recentProps?.length) {
      console.log(`\nMost recent property write: ${recentProps[0].created_at}`);
    }
  } catch (err: any) {
    console.error("Error:", err.message);
  }
}

checkState();
