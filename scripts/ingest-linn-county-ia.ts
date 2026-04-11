#!/usr/bin/env tsx
/**
 * Ingest Linn County IA property data from ArcGIS Open Data.
 * Source: RealEstateParcel FeatureServer + AssessorCAMABuildingResidential for year_built/sqft.
 * ~105K parcels with owner, address, values, tax data, sale history.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCEL_URL = "https://services.arcgis.com/i14SLLmXo7Hn9vNc/arcgis/rest/services/RealEstateParcel/FeatureServer/0/query";
const CAMA_URL = "https://services.arcgis.com/i14SLLmXo7Hn9vNc/arcgis/rest/services/AssessorCAMABuildingResidential/FeatureServer/0/query";

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "19", county_fips: "113", active: true })
    .select("id").single();
  return created!.id;
}

async function queryArcGIS(baseUrl: string, offset: number, limit: number, fields?: string): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: fields || "*",
    resultRecordCount: String(limit),
    resultOffset: String(offset),
    f: "json",
  });
  const resp = await fetch(`${baseUrl}?${params}`, {
    headers: { "User-Agent": "Mozilla/5.0" },
  });
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.features || [];
}

async function loadCAMA(): Promise<Map<string, { yearBuilt: number | null; sqft: number | null; occupancy: string }>> {
  console.log("  Loading CAMA building data for year_built/sqft...");
  const map = new Map<string, { yearBuilt: number | null; sqft: number | null; occupancy: string }>();
  let offset = 0;
  const BATCH = 2000;

  while (true) {
    const features = await queryArcGIS(CAMA_URL, offset, BATCH, "GPN,YearBuilt,LivingArea,Occupancy");
    if (features.length === 0) break;

    for (const f of features) {
      const a = f.attributes;
      const gpn = a.GPN;
      if (!gpn) continue;
      // Only store first building per parcel (main building)
      if (!map.has(gpn)) {
        map.set(gpn, {
          yearBuilt: a.YearBuilt && a.YearBuilt > 1700 && a.YearBuilt < 2030 ? a.YearBuilt : null,
          sqft: a.LivingArea && a.LivingArea > 0 ? a.LivingArea : null,
          occupancy: (a.Occupancy || "").trim(),
        });
      }
    }

    offset += features.length;
    if (features.length < BATCH) break;
    process.stdout.write(`\r  CAMA loaded: ${map.size.toLocaleString()}`);
  }
  console.log(`\r  CAMA loaded: ${map.size.toLocaleString()} buildings`);
  return map;
}

function classifyProperty(classStr: string, occupancy: string): string {
  const cls = (classStr || "").toUpperCase();
  const occ = (occupancy || "").toUpperCase();
  if (cls.includes("COMMERCIAL")) return "commercial";
  if (cls.includes("INDUSTRIAL")) return "industrial";
  if (cls.includes("AGRIC")) return "agricultural";
  if (occ.includes("MULTI") || occ.includes("APARTMENT")) return "multifamily";
  if (occ.includes("CONDO")) return "condo";
  if (occ.includes("DUPLEX") || occ.includes("TRIPLEX") || occ.includes("FOURPLEX")) return "multifamily";
  if (cls.includes("RESIDENTIAL")) return "residential";
  return "other";
}

function formatEpochDate(epoch: number | null): string | null {
  if (!epoch) return null;
  try {
    const d = new Date(epoch);
    return d.toISOString().split("T")[0];
  } catch {
    return null;
  }
}

async function main() {
  console.log("MXRE — Ingest Linn County IA Property Data\n");

  const countyId = await getOrCreateCounty("Linn", "IA");
  console.log("  County ID:", countyId);

  const { count: existing } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log("  Existing properties:", existing);

  // Load CAMA building data for year_built/sqft enrichment
  const camaMap = await loadCAMA();

  // Query parcels in batches
  const BATCH_SIZE = 2000;
  let offset = 0;
  let inserted = 0, skipped = 0, duplicates = 0, errors = 0;

  while (true) {
    const features = await queryArcGIS(PARCEL_URL, offset, BATCH_SIZE);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const f of features) {
      const a = f.attributes;
      const gpn = (a.GPN || "").trim();
      const address = (a.SitusAddress || "").trim();

      if (!gpn && !address) { skipped++; continue; }

      const cama = camaMap.get(gpn);
      const propertyType = classifyProperty(a.Class || "", cama?.occupancy || "");

      batch.push({
        county_id: countyId,
        parcel_id: gpn,
        address: address,
        city: (a.SitusCity || "").trim(),
        state_code: "IA",
        zip: (a.SitusZip || "").trim(),
        owner_name: (a.OwnerDeed || "").trim(),
        assessed_value: a.ValueAssessedTotal && a.ValueAssessedTotal > 0 ? a.ValueAssessedTotal : null,
        taxable_value: a.ValueTaxableTotal && a.ValueTaxableTotal > 0 ? a.ValueTaxableTotal : null,
        market_value: a.ValueTotal && a.ValueTotal > 0 ? a.ValueTotal : null,
        land_value: a.ValueLand && a.ValueLand > 0 ? a.ValueLand : null,
        year_built: cama?.yearBuilt || null,
        total_sqft: cama?.sqft || null,
        property_type: propertyType,
        last_sale_date: formatEpochDate(a.RecorderDate),
        last_sale_price: null, // No sale price in parcel layer
        property_tax: a.TaxNet && a.TaxNet > 0 ? a.TaxNet : null,
        land_sqft: a.Acres && a.Acres > 0 ? Math.round(a.Acres * 43560) : null,
        assessor_url: a.AssessorUrl || "",
        source: "linn-county-ia-arcgis",
      });
    }

    if (batch.length > 0) {
      // Insert in sub-batches to handle duplicate key errors gracefully
      const SUB_BATCH = 200;
      for (let i = 0; i < batch.length; i += SUB_BATCH) {
        const chunk = batch.slice(i, i + SUB_BATCH);
        const { error } = await db.from("properties").insert(chunk);
        if (error) {
          if (error.message.includes("duplicate") || error.code === "23505") {
            // Try inserting one by one to skip duplicates
            for (const row of chunk) {
              const { error: e2 } = await db.from("properties").insert(row);
              if (e2) {
                if (e2.message.includes("duplicate") || e2.code === "23505") {
                  duplicates++;
                } else {
                  errors++;
                  if (errors <= 3) console.error(`\n  Insert error: ${e2.message.slice(0, 100)}`);
                }
              } else {
                inserted++;
              }
            }
          } else {
            errors++;
            if (errors <= 3) console.error(`\n  Insert error: ${error.message.slice(0, 100)}`);
          }
        } else {
          inserted += chunk.length;
        }
      }
    }

    offset += features.length;
    process.stdout.write(`\r  Progress: ${offset.toLocaleString()} fetched | ${inserted.toLocaleString()} inserted | ${duplicates} dups | ${skipped} skipped | ${errors} errors`);

    if (features.length < BATCH_SIZE) break;
  }

  console.log(`\n\n  Done: ${inserted.toLocaleString()} inserted | ${duplicates} duplicates | ${skipped} skipped | ${errors} errors`);

  const { count: total } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log(`  Linn County IA now has ${total?.toLocaleString()} properties`);
}

main().catch(console.error);
