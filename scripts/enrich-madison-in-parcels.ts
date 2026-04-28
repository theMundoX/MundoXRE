#!/usr/bin/env tsx
/**
 * Enrich Madison County, IN parcels from Heartland MPO's public ArcGIS service.
 *
 * Source:
 * https://heartlandmpo.com/arcgis/rest/services/Tax_Parcel/FeatureServer/1
 *
 * This official parcel layer exposes legal/acreage fields but not owner or
 * valuation. To avoid inflating market parcel counts, this script only updates
 * parcel IDs that already exist in MXRE for Madison County.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const COUNTY_ID = 797473;
const SERVICE_URL = "https://heartlandmpo.com/arcgis/rest/services/Tax_Parcel/FeatureServer/1";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const OUT_FIELDS = [
  "Name",
  "State_PIN",
  "Frmtd_PIN",
  "KEYNO",
  "Legal_Desc",
  "Subdivision",
  "Legal_Lot_Number",
  "Legal_Acreage",
  "StatedArea",
  "InstrumentNo",
  "SurveyorText",
  "last_edited_date",
].join(",");

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const LIMIT = Number.parseInt(args.find((arg) => arg.startsWith("--limit="))?.split("=")[1] ?? "0", 10);

function digits(value: unknown): string {
  return String(value ?? "").replace(/\D/g, "");
}

function text(value: unknown): string | null {
  const cleaned = String(value ?? "").replace(/\s+/g, " ").trim();
  return cleaned || null;
}

function num(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed !== 0 ? parsed : null;
}

interface ExistingParcel {
  address: string;
  city: string;
  state_code: string;
  zip: string;
  property_type: string | null;
}

async function fetchExistingParcels(): Promise<Map<string, ExistingParcel>> {
  const parcels = new Map<string, ExistingParcel>();
  const pageSize = 1000;

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from("properties")
      .select("parcel_id,address,city,state_code,zip,property_type")
      .eq("county_id", COUNTY_ID)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`existing parcel lookup: ${error.message}`);
    for (const row of data ?? []) {
      parcels.set(String(row.parcel_id), {
        address: row.address ?? "UNKNOWN",
        city: row.city ?? "UNKNOWN",
        state_code: row.state_code ?? "IN",
        zip: row.zip ?? "00000",
        property_type: row.property_type ?? null,
      });
    }
    if (!data || data.length < pageSize) break;
  }

  return parcels;
}

async function fetchCount(): Promise<number> {
  const url = new URL(`${SERVICE_URL}/query`);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("returnCountOnly", "true");
  url.searchParams.set("f", "json");
  const response = await fetch(url);
  if (!response.ok) throw new Error(`count HTTP ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(JSON.stringify(json.error));
  return Number(json.count ?? 0);
}

async function fetchPage(offset: number, pageSize = PAGE_SIZE): Promise<Record<string, unknown>[]> {
  const url = new URL(`${SERVICE_URL}/query`);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("outFields", OUT_FIELDS);
  url.searchParams.set("returnGeometry", "false");
  url.searchParams.set("resultOffset", String(offset));
  url.searchParams.set("resultRecordCount", String(pageSize));
  url.searchParams.set("f", "json");

  const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`page ${offset} HTTP ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(`page ${offset}: ${JSON.stringify(json.error)}`);
  return (json.features ?? []).map((feature: any) => feature.attributes ?? {});
}

function rowFromFeature(feature: Record<string, unknown>, existingParcels: Map<string, ExistingParcel>) {
  const parcelId = digits(feature.State_PIN) || digits(feature.Name);
  const existing = parcelId ? existingParcels.get(parcelId) : null;
  if (!parcelId || !existing) return null;

  const acres = num(feature.Legal_Acreage) ?? num(feature.StatedArea);
  const legalBits = [text(feature.Legal_Desc), text(feature.InstrumentNo), text(feature.SurveyorText)].filter(Boolean);

  return {
    county_id: COUNTY_ID,
    parcel_id: parcelId,
    address: existing.address,
    city: existing.city,
    state_code: existing.state_code,
    zip: existing.zip,
    property_type: existing.property_type,
    apn_formatted: text(feature.Frmtd_PIN),
    lot_acres: acres,
    lot_sqft: acres ? Math.round(acres * 43560) : null,
    legal_description: legalBits.length ? legalBits.join(" | ") : null,
    subdivision: text(feature.Subdivision),
    lot_number: text(feature.Legal_Lot_Number),
    neighborhood_code: text(feature.KEYNO),
    assessor_url: "https://beacon.schneidercorp.com/Application.aspx?AppID=166&LayerID=2145&PageTypeID=2&PageID=1104",
    source: "madison-in-tax-parcel-arcgis",
    updated_at: new Date().toISOString(),
  };
}

async function main() {
  console.log("MXRE - Madison County IN parcel enrichment");
  console.log("=".repeat(52));
  console.log(`Dry run: ${DRY_RUN}`);

  const existingParcels = await fetchExistingParcels();
  console.log(`Existing MXRE Madison parcels: ${existingParcels.size.toLocaleString()}`);

  const total = LIMIT > 0 ? Math.min(LIMIT, await fetchCount()) : await fetchCount();
  console.log(`Source records: ${total.toLocaleString()}`);

  let processed = 0;
  let written = 0;
  let skipped = 0;
  const start = Date.now();

  for (let offset = 0; offset < total; offset += PAGE_SIZE) {
    const pageSize = Math.min(PAGE_SIZE, total - offset);
    const features = await fetchPage(offset, pageSize);
    const mappedRows = features.map((feature) => rowFromFeature(feature, existingParcels)).filter(Boolean) as NonNullable<ReturnType<typeof rowFromFeature>>[];
    const rows = [...new Map(mappedRows.map((row) => [row.parcel_id, row])).values()];
    processed += features.length;
    skipped += features.length - rows.length;

    if (!DRY_RUN && rows.length > 0) {
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const { error } = await db.from("properties").upsert(batch, {
          onConflict: "county_id,parcel_id",
          ignoreDuplicates: false,
        });
        if (error) throw new Error(`upsert offset ${offset}+${i}: ${error.message}`);
        written += batch.length;
      }
    } else {
      written += rows.length;
    }

    const elapsed = Math.max(1, (Date.now() - start) / 1000);
    const pct = Math.min(100, ((offset + features.length) / total) * 100).toFixed(1);
    console.log(`[${Math.round(elapsed)}s] ${pct}% processed=${processed.toLocaleString()} written=${written.toLocaleString()} skipped=${skipped.toLocaleString()}`);
    if (LIMIT > 0 && offset + PAGE_SIZE >= LIMIT) break;
  }

  console.log("Done.");
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
