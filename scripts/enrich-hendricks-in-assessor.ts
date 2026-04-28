#!/usr/bin/env tsx
/**
 * Enrich Hendricks County, IN parcels from the county's public ArcGIS service.
 *
 * Source:
 * https://services2.arcgis.com/Y0fDSibEfxdu2Ya6/ArcGIS/rest/services/Hendricks_County_GIS_Map/FeatureServer/0
 *
 * This layer is not as rich as Hamilton's assessor layer. It currently exposes
 * parcel identity, owner, situs address, acreage, and coordinates. That still
 * closes a major zero-coverage gap for metro ownership and physical readiness.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const COUNTY_ID = 797531;
const SERVICE_URL = "https://services2.arcgis.com/Y0fDSibEfxdu2Ya6/ArcGIS/rest/services/Hendricks_County_GIS_Map/FeatureServer/0";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const OUT_FIELDS = [
  "PERMANENT_IDENTIFIER",
  "PARCEL_ID",
  "LATITUDE",
  "LONGITUDE",
  "PROP_ADD",
  "PROP_CITY",
  "PROP_ZIP",
  "Acreage",
  "Owner",
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

function splitOwner(owner: string | null): { company_name: string | null; corporate_owned: boolean } {
  if (!owner) return { company_name: null, corporate_owned: false };
  const corporate = /\b(LLC|INC|CORP|LP|L\.P\.|LTD|TRUST|HOLDINGS|PARTNERS|ASSOC|COMPANY|CO\b|BANK|CHURCH|AUTHORITY)\b/i.test(owner);
  return { company_name: corporate ? owner : null, corporate_owned: corporate };
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

function rowFromFeature(feature: Record<string, unknown>) {
  const parcelId = digits(feature.PARCEL_ID) || digits(feature.PERMANENT_IDENTIFIER);
  if (!parcelId) return null;

  const owner = text(feature.Owner);
  const { company_name, corporate_owned } = splitOwner(owner);
  const lat = num(feature.LATITUDE);
  const lng = num(feature.LONGITUDE);
  const acres = num(feature.Acreage);

  return {
    county_id: COUNTY_ID,
    parcel_id: parcelId,
    apn_formatted: text(feature.PERMANENT_IDENTIFIER),
    address: text(feature.PROP_ADD) ?? "UNKNOWN",
    city: text(feature.PROP_CITY) ?? "UNKNOWN",
    state_code: "IN",
    zip: text(feature.PROP_ZIP) ?? "00000",
    owner_name: owner,
    company_name,
    corporate_owned,
    lot_acres: acres,
    lot_sqft: acres ? Math.round(acres * 43560) : null,
    latitude: lat,
    longitude: lng,
    lat,
    lng,
    assessor_url: "https://beacon.schneidercorp.com/Application.aspx?AppID=327&LayerID=3383&PageTypeID=2&PageID=2196",
    source: "hendricks-in-parcels-arcgis",
    updated_at: new Date().toISOString(),
  };
}

async function main() {
  console.log("MXRE - Hendricks County IN assessor enrichment");
  console.log("=".repeat(52));
  console.log(`Dry run: ${DRY_RUN}`);

  const total = LIMIT > 0 ? Math.min(LIMIT, await fetchCount()) : await fetchCount();
  console.log(`Source records: ${total.toLocaleString()}`);

  let processed = 0;
  let written = 0;
  let skipped = 0;
  const start = Date.now();

  for (let offset = 0; offset < total; offset += PAGE_SIZE) {
    const pageSize = Math.min(PAGE_SIZE, total - offset);
    const features = await fetchPage(offset, pageSize);
    const mappedRows = features.map(rowFromFeature).filter(Boolean) as NonNullable<ReturnType<typeof rowFromFeature>>[];
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
