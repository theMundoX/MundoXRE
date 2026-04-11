#!/usr/bin/env tsx
/**
 * Ingest Scott County IA (Davenport) property data from ArcGIS Cadastral FeatureServer.
 * Source: Cadastral/3 (Parcel layer) — ~75K parcels with owner, address, values, sale data.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PARCEL_URL = "https://services.arcgis.com/ovln19YRWV44nBqV/arcgis/rest/services/Cadastral/FeatureServer/3/query";

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) return data.id;
  const { data: created } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "19", county_fips: "163", active: true })
    .select("id").single();
  return created!.id;
}

async function queryArcGIS(offset: number, limit: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: "*",
    resultRecordCount: String(limit),
    resultOffset: String(offset),
    f: "json",
  });
  const resp = await fetch(`${PARCEL_URL}?${params}`, {
    headers: { "User-Agent": "Mozilla/5.0" },
  });
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.features || [];
}

function classifyProperty(propClass: string): string {
  const cls = (propClass || "").toUpperCase();
  if (cls === "C" || cls.includes("COM")) return "commercial";
  if (cls === "I" || cls.includes("IND")) return "industrial";
  if (cls === "A" || cls.includes("AG")) return "agricultural";
  if (cls === "R" || cls.includes("RES")) return "residential";
  if (cls === "E" || cls.includes("EXEMPT")) return "exempt";
  return "other";
}

function parseSaleDate(raw: string | null): string | null {
  if (!raw) return null;
  // Scott County sale dates come as epoch timestamps or strings
  const epoch = Number(raw);
  if (!isNaN(epoch) && epoch > 0) {
    try {
      return new Date(epoch).toISOString().split("T")[0];
    } catch {
      return null;
    }
  }
  // Try parsing as date string
  try {
    const d = new Date(raw);
    if (!isNaN(d.getTime())) return d.toISOString().split("T")[0];
  } catch {}
  return null;
}

function extractCity(address: string | null): string {
  // Address format: "1112 RIPLEY ST, DAVENPORT IA 52803"
  if (!address) return "";
  const parts = address.split(",");
  if (parts.length >= 2) {
    const cityState = parts[1].trim();
    // Remove state and zip: "DAVENPORT IA 52803" -> "DAVENPORT"
    const match = cityState.match(/^([A-Z\s]+?)(?:\s+IA\b|\s+\d{5})/i);
    return match ? match[1].trim() : cityState.split(/\s+IA\b/i)[0].trim();
  }
  return "";
}

function extractZip(address: string | null): string {
  if (!address) return "";
  const match = address.match(/\b(\d{5}(?:-\d{4})?)\s*$/);
  return match ? match[1] : "";
}

async function main() {
  console.log("MXRE — Ingest Scott County IA Property Data\n");

  const countyId = await getOrCreateCounty("Scott", "IA");
  console.log("  County ID:", countyId);

  const { count: existing } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", countyId);
  console.log("  Existing properties:", existing);

  const BATCH_SIZE = 2000;
  let offset = 0;
  let inserted = 0, skipped = 0, duplicates = 0, errors = 0;

  while (true) {
    const features = await queryArcGIS(offset, BATCH_SIZE);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const f of features) {
      const a = f.attributes;
      const pin = (a.PIN || "").trim();
      const propertyAddr = (a.PropertyAddress || "").trim();
      const fullAddr = (a.Address || "").trim(); // "1112 RIPLEY ST, DAVENPORT IA 52803"

      if (!pin && !propertyAddr) { skipped++; continue; }

      const city = extractCity(fullAddr);
      const zip = extractZip(fullAddr) || (a.DeedZip || "").trim();
      const ownerParts = [a.DeedHold, a.DeedHold2].filter(Boolean).map((s: string) => s.trim());
      const owner = ownerParts.join(" & ");

      const totalVal = a.TotVal && a.TotVal > 0 ? a.TotVal : null;
      const landVal = a.LandVal && a.LandVal > 0 ? a.LandVal : null;
      const dwellVal = a.DwellVal && a.DwellVal > 0 ? a.DwellVal : null;
      const impVal = a.ImpVal && a.ImpVal > 0 ? a.ImpVal : null;

      const salePrice = a.SalePrice && a.SalePrice > 0 ? a.SalePrice : null;
      const saleDate = parseSaleDate(a.SaleDate);

      const sqft = a.Square_Feet && a.Square_Feet > 0 ? a.Square_Feet : null;
      const acres = a.Graphic_Ac || a.Gross_AC || a.Net_AC;

      batch.push({
        county_id: countyId,
        parcel_id: pin,
        address: propertyAddr,
        city: city,
        state_code: "IA",
        zip: zip,
        owner_name: owner,
        assessed_value: totalVal,
        taxable_value: null, // Not in this dataset
        market_value: totalVal,
        land_value: landVal,
        year_built: null, // Not in Cadastral layer
        total_sqft: sqft,
        property_type: classifyProperty(a.PropClass),
        last_sale_date: saleDate,
        last_sale_price: salePrice,
        land_sqft: acres && acres > 0 ? Math.round(acres * 43560) : null,
        assessor_url: a.PQLink || "",
        source: "scott-county-ia-arcgis",
      });
    }

    if (batch.length > 0) {
      const SUB_BATCH = 200;
      for (let i = 0; i < batch.length; i += SUB_BATCH) {
        const chunk = batch.slice(i, i + SUB_BATCH);
        const { error } = await db.from("properties").insert(chunk);
        if (error) {
          if (error.message.includes("duplicate") || error.code === "23505") {
            for (const row of chunk) {
              const { error: e2 } = await db.from("properties").insert(row);
              if (e2) {
                if (e2.message.includes("duplicate") || e2.code === "23505") duplicates++;
                else {
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
  console.log(`  Scott County IA now has ${total?.toLocaleString()} properties`);
}

main().catch(console.error);
