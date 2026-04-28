#!/usr/bin/env tsx
/**
 * Enrich Hamilton County, IN parcels from the county's public ArcGIS service.
 *
 * Source:
 * https://gis1.hamiltoncounty.in.gov/arcgis/rest/services/HamCoParcelsPublic/FeatureServer/0
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const COUNTY_ID = 797457;
const SERVICE_URL = "https://gis1.hamiltoncounty.in.gov/arcgis/rest/services/HamCoParcelsPublic/FeatureServer/0";
const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const OUT_FIELDS = [
  "FMTPRCLNO",
  "PARCELNO",
  "STPRCLNO",
  "STPRCLNO_UNFORMATTED",
  "DEEDEDOWNR",
  "OWNNAME",
  "OWNADDRESS",
  "OWNCITY",
  "OWNSTATE",
  "OWNZIP",
  "LOCADDRESS",
  "LOCCITY",
  "LOCZIP",
  "LEGALDESC",
  "SUBDIVNAME",
  "LOTNUMBER",
  "DEEDACRES",
  "PROPCLASS",
  "PROPUSE",
  "AVLAND",
  "AVIMPROVE",
  "AVTOTGROSS",
  "AVTAXYR",
  "sq_ft_comm",
  "sq_ft_res",
  "num_floors",
  "year_built",
  "LSTXFRDATE",
  "PROPERTYREPORT",
  "neighborhood",
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

function dateFromEsri(value: unknown): string | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return new Date(parsed).toISOString().slice(0, 10);
}

function wholeNumber(value: unknown): number | null {
  const parsed = num(value);
  return parsed === null ? null : Math.max(0, Math.round(parsed));
}

function classifyPropClass(code: unknown): string {
  const value = String(code ?? "").trim();
  if (value.startsWith("1") || value.startsWith("5")) return "residential";
  if (value.startsWith("2") || value.startsWith("4")) return "commercial";
  if (value.startsWith("3")) return "industrial";
  if (value.startsWith("6") || value.startsWith("7")) return "exempt";
  return "other";
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

async function fetchPage(offset: number): Promise<Record<string, unknown>[]> {
  const url = new URL(`${SERVICE_URL}/query`);
  url.searchParams.set("where", "1=1");
  url.searchParams.set("outFields", OUT_FIELDS);
  url.searchParams.set("returnGeometry", "false");
  url.searchParams.set("resultOffset", String(offset));
  url.searchParams.set("resultRecordCount", String(PAGE_SIZE));
  url.searchParams.set("f", "json");

  const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`page ${offset} HTTP ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(`page ${offset}: ${JSON.stringify(json.error)}`);
  return (json.features ?? []).map((feature: any) => feature.attributes ?? {});
}

function rowFromFeature(feature: Record<string, unknown>) {
  const parcelId = digits(feature.STPRCLNO_UNFORMATTED) || digits(feature.STPRCLNO) || digits(feature.PARCELNO);
  if (!parcelId) return null;

  const owner = text(feature.OWNNAME) ?? text(feature.DEEDEDOWNR);
  const { company_name, corporate_owned } = splitOwner(owner);
  const landValue = num(feature.AVLAND);
  const buildingValue = num(feature.AVIMPROVE);
  const assessedValue = num(feature.AVTOTGROSS) ?? ((landValue ?? 0) + (buildingValue ?? 0) || null);
  const livingSqft = num(feature.sq_ft_res);
  const commercialSqft = num(feature.sq_ft_comm);
  const totalSqft = livingSqft ?? commercialSqft;
  const acres = num(feature.DEEDACRES);
  const lastSaleDate = dateFromEsri(feature.LSTXFRDATE);

  return {
    county_id: COUNTY_ID,
    parcel_id: parcelId,
    apn_formatted: text(feature.STPRCLNO) ?? text(feature.FMTPRCLNO),
    address: text(feature.LOCADDRESS),
    city: text(feature.LOCCITY),
    state_code: "IN",
    zip: text(feature.LOCZIP),
    property_type: classifyPropClass(feature.PROPCLASS),
    property_class: text(feature.PROPCLASS),
    property_use: text(feature.PROPUSE),
    land_use: text(feature.PROPUSE),
    owner_name: owner,
    company_name,
    corporate_owned,
    mail_address: text(feature.OWNADDRESS),
    mail_city: text(feature.OWNCITY),
    mail_state: text(feature.OWNSTATE),
    mail_zip: text(feature.OWNZIP),
    mailing_address: text(feature.OWNADDRESS),
    mailing_city: text(feature.OWNCITY),
    mailing_state: text(feature.OWNSTATE),
    mailing_zip: text(feature.OWNZIP),
    assessed_value: assessedValue,
    market_value: assessedValue,
    appraised_land: landValue,
    appraised_building: buildingValue,
    land_value: landValue,
    living_sqft: livingSqft,
    total_sqft: totalSqft,
    lot_acres: acres,
    lot_sqft: acres ? Math.round(acres * 43560) : null,
    year_built: num(feature.year_built),
    stories: wholeNumber(feature.num_floors),
    last_sale_date: lastSaleDate,
    sale_year: lastSaleDate ? Number(lastSaleDate.slice(0, 4)) : null,
    ownership_start_date: lastSaleDate,
    legal_description: text(feature.LEGALDESC),
    subdivision: text(feature.SUBDIVNAME),
    lot_number: text(feature.LOTNUMBER),
    neighborhood_code: text(feature.neighborhood),
    assessor_url: text(feature.PROPERTYREPORT),
    tax_year: num(feature.AVTAXYR),
    source: "hamilton-in-assessor-arcgis",
    updated_at: new Date().toISOString(),
  };
}

async function main() {
  console.log("MXRE - Hamilton County IN assessor enrichment");
  console.log("=".repeat(52));
  console.log(`Dry run: ${DRY_RUN}`);

  const total = LIMIT > 0 ? Math.min(LIMIT, await fetchCount()) : await fetchCount();
  console.log(`Source records: ${total.toLocaleString()}`);

  let processed = 0;
  let written = 0;
  let skipped = 0;
  const start = Date.now();

  for (let offset = 0; offset < total; offset += PAGE_SIZE) {
    const features = await fetchPage(offset);
    const mappedRows = features.map(rowFromFeature).filter(Boolean).slice(0, Math.max(0, total - offset)) as NonNullable<ReturnType<typeof rowFromFeature>>[];
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
