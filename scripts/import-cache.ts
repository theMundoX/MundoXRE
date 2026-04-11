#!/usr/bin/env tsx
/**
 * Import all cached ActDataScout data into the database.
 * Reads search results and detail pages from .cache directory.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { normalizeProperty } from "../src/discovery/normalizer.js";
import type { RawPropertyRecord } from "../src/discovery/adapters/base.js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const CACHE_DIR = join(process.cwd(), ".cache");

async function main() {
  console.log("MXRE Cache Import");
  console.log("─".repeat(50));
  console.log(`DB: ${SUPABASE_URL}`);

  // Get county ID for Comanche
  const { data: county } = await db
    .from("counties")
    .select("id")
    .eq("state_fips", "40")
    .eq("county_fips", "031")
    .single();

  if (!county) {
    console.error("Comanche county not found in DB");
    process.exit(1);
  }
  const countyId = county.id;
  console.log(`County ID: ${countyId}`);

  // Read all cache files
  const subdirs = readdirSync(CACHE_DIR, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);

  const detailRecords: RawPropertyRecord[] = [];
  const searchRecords: RawPropertyRecord[] = [];
  const seenParcels = new Set<string>();

  for (const subdir of subdirs) {
    const dir = join(CACHE_DIR, subdir);
    const files = readdirSync(dir);

    for (const file of files) {
      try {
        const content = readFileSync(join(dir, file), "utf-8");
        const data = JSON.parse(content);

        if (Array.isArray(data)) {
          // Search results
          for (const item of data) {
            const parcel = item.parcel || item.crpid || "";
            if (!parcel || seenParcels.has(parcel)) continue;
            seenParcels.add(parcel);

            // Handle two formats: old format with 'owner' and new with 'ownerName'
            const ownerName = item.ownerName || item.owner || "";
            const address = item.address || "";
            const subdivision = item.subdivision || "";

            searchRecords.push({
              parcel_id: parcel,
              address: address,
              city: subdivision || "Comanche",
              state: "OK",
              zip: "",
              owner_name: ownerName || undefined,
              legal_description: item.legal || undefined,
              assessor_url: "https://www.actdatascout.com",
              raw: { ...item, source: "cached_search" },
            });
          }
        } else if (data && typeof data === "object" && (data.address || data.owner_name)) {
          // Detail record
          const parcel = data.parcel_id || "";
          // Detail records may not have parcel_id — use address as dedup key
          const key = parcel || data.address || "";
          if (!key || seenParcels.has(key)) continue;
          seenParcels.add(key);

          detailRecords.push({
            parcel_id: parcel,
            address: data.address || "",
            city: data.city || "Comanche",
            state: data.state || "OK",
            zip: data.zip || "",
            owner_name: data.owner_name || undefined,
            property_type: data.property_type || undefined,
            assessed_value: data.assessed_value || undefined,
            market_value: data.market_value || undefined,
            property_tax: data.property_tax || undefined,
            year_built: data.year_built || undefined,
            total_sqft: data.total_sqft || undefined,
            stories: data.stories || undefined,
            last_sale_price: data.last_sale_price || undefined,
            last_sale_date: data.last_sale_date || undefined,
            legal_description: data.legal_description || undefined,
            assessor_url: data.assessor_url || "https://www.actdatascout.com",
            raw: { ...data.raw, source: "cached_detail" },
          });
        }
      } catch {
        // Skip unparseable files
      }
    }
  }

  console.log(`\nFound in cache:`);
  console.log(`  ${detailRecords.length} detail records (rich data: assessed value, sqft, etc.)`);
  console.log(`  ${searchRecords.length} search records (basic data: parcel, owner, address)`);
  console.log(`  ${seenParcels.size} unique parcels total`);

  // Combine — detail records take priority
  const allRecords = [...detailRecords, ...searchRecords];

  // Normalize and upsert
  console.log(`\nUpserting ${allRecords.length} properties...`);
  const BATCH_SIZE = 100;
  let upserted = 0;
  let errors = 0;

  for (let i = 0; i < allRecords.length; i += BATCH_SIZE) {
    const batch = allRecords.slice(i, i + BATCH_SIZE);
    const normalized = batch
      .map((r) => {
        // Generate synthetic parcel_id from address if missing
        if (!r.parcel_id && r.address) {
          r.parcel_id = `ADDR-${createHash("md5").update(r.address.toUpperCase().trim()).digest("hex").slice(0, 12)}`;
        }
        return normalizeProperty(r, countyId);
      })
      .filter((p) => p.address && p.city && p.parcel_id);

    if (normalized.length === 0) continue;

    const rows = normalized.map((p) => ({
      ...p,
      updated_at: new Date().toISOString(),
    }));

    const { data, error } = await db
      .from("properties")
      .upsert(rows, { onConflict: "county_id,parcel_id" })
      .select("id");

    if (error) {
      console.error(`  Batch error: ${error.message}`);
      errors++;
    } else {
      upserted += data?.length ?? 0;
    }
  }

  console.log(`\nDone: ${upserted} properties upserted, ${errors} errors`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
