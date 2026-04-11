#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const db = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_KEY!,
  );

  console.log("Running migration: add property_tax, fix data...");

  // Step 1: Add property_tax column via raw SQL (using Supabase's rpc if available)
  // Since we can't run raw DDL via the JS client, we'll update data only
  // The column needs to be added via SQL Editor

  // Step 2: Move assessed_value to property_tax for existing records
  // and clean up fake addresses
  const { data: properties, error } = await db
    .from("properties")
    .select("id, assessed_value, address")
    .eq("source", "assessor");

  if (error) {
    console.error("Failed to read properties:", error.message);
    return;
  }

  console.log(`Found ${properties?.length ?? 0} records to fix`);

  // Note: We can't add columns via the JS client
  // The property_tax column must be added via SQL Editor first
  // For now, just clean up the addresses
  let fixed = 0;
  for (const p of properties ?? []) {
    if (p.address && p.address.includes(" - ")) {
      const { error: updateError } = await db
        .from("properties")
        .update({ address: "PENDING ENRICHMENT" })
        .eq("id", p.id);

      if (!updateError) fixed++;
    }
  }

  console.log(`Fixed ${fixed} addresses`);
  console.log("\nIMPORTANT: Run this SQL in the Supabase SQL Editor:");
  console.log("ALTER TABLE properties ADD COLUMN IF NOT EXISTS property_tax INTEGER;");
  console.log("UPDATE properties SET property_tax = assessed_value, assessed_value = NULL WHERE source = 'assessor';");
}

main().catch(() => process.exit(1));
