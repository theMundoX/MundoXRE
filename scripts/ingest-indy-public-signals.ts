#!/usr/bin/env tsx
/**
 * MXRE - Indianapolis public property signals.
 *
 * Source: official City of Indianapolis / Marion County ArcGIS REST services.
 * Ingests factual public signals that complement listings, rents, liens, and assessor data:
 * - registered landlord properties
 * - abandoned/vacant properties
 * - tax sale parcels
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const MARION_COUNTY_ID = 797583;
const PAGE_SIZE = 1000;
const PROPERTY_LOOKUP_CHUNK = 1000;

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);
const getArg = (name: string) => args.find((arg) => arg.startsWith(`--${name}=`))?.split("=")[1];

const DRY_RUN = hasFlag("dry-run");
const ONLY = (getArg("only") ?? "landlord,vacant,taxsale")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

interface SignalRow {
  property_id: number;
  parcel_id: string | null;
  signal_type: string;
  source_system: string;
  source_id: string;
  status: string | null;
  observed_date: string | null;
  amount: number | null;
  address: string | null;
  raw: Record<string, unknown>;
}

function normParcel(value: unknown): string {
  return String(value ?? "").replace(/\D/g, "");
}

function isoDate(value: unknown): string | null {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return new Date(n).toISOString().slice(0, 10);
  const text = String(value ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}/.test(text) ? text.slice(0, 10) : null;
}

function num(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function pg(query: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`pg/query ${res.status}: ${await res.text()}`);
  return res.json();
}

async function ensureTable() {
  await pg(`
    CREATE TABLE IF NOT EXISTS property_public_signals (
      id BIGSERIAL PRIMARY KEY,
      property_id INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
      parcel_id TEXT,
      signal_type TEXT NOT NULL,
      source_system TEXT NOT NULL,
      source_id TEXT NOT NULL,
      status TEXT,
      observed_date DATE,
      amount NUMERIC,
      address TEXT,
      raw JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(source_system, source_id, signal_type)
    );
    CREATE INDEX IF NOT EXISTS idx_property_public_signals_property
      ON property_public_signals(property_id, signal_type);
    CREATE INDEX IF NOT EXISTS idx_property_public_signals_type
      ON property_public_signals(signal_type, status);
  `);
}

async function loadPropertyIndexForParcels(parcels: string[]): Promise<Map<string, { id: number; parcel_id: string | null }>> {
  const index = new Map<string, { id: number; parcel_id: string | null }>();
  const unique = [...new Set(parcels.map(normParcel).filter(Boolean))];

  for (let i = 0; i < unique.length; i += PROPERTY_LOOKUP_CHUNK) {
    const chunk = unique.slice(i, i + PROPERTY_LOOKUP_CHUNK);
    const quoted = chunk.map((value) => `'${value.replace(/'/g, "''")}'`).join(",");
    const data = await pg(`
      SELECT id, parcel_id, apn_formatted
      FROM properties
      WHERE county_id = ${MARION_COUNTY_ID}
        AND (parcel_id IN (${quoted}) OR apn_formatted IN (${quoted}));
    `);
    for (const row of data as Array<{ id: number; parcel_id: string | null; apn_formatted: string | null }>) {
      for (const value of [row.parcel_id, row.apn_formatted]) {
        const key = normParcel(value);
        if (key) index.set(key, { id: row.id, parcel_id: row.parcel_id });
      }
    }
  }
  return index;
}

async function arcgisRows(service: string, layer: number): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const url = new URL(`https://gis.indy.gov/server/rest/services/${service}/${layer}/query`);
    url.searchParams.set("where", "1=1");
    url.searchParams.set("outFields", "*");
    url.searchParams.set("returnGeometry", "false");
    url.searchParams.set("resultOffset", String(offset));
    url.searchParams.set("resultRecordCount", String(PAGE_SIZE));
    url.searchParams.set("f", "json");

    const json = await fetch(url).then((r) => {
      if (!r.ok) throw new Error(`${service}/${layer} HTTP ${r.status}`);
      return r.json();
    });
    if (json.error) throw new Error(`${service}/${layer}: ${JSON.stringify(json.error)}`);
    const batch = (json.features ?? []).map((f: any) => f.attributes ?? {});
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
  }
  return rows;
}

function landlordSignal(row: Record<string, unknown>, index: Map<string, { id: number; parcel_id: string | null }>): SignalRow | null {
  const parcel = normParcel(row.PARCEL_C);
  const property = index.get(parcel);
  if (!property) return null;
  const street = [row.STNUM, row.DIR, row.STREET_NAME].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  return {
    property_id: property.id,
    parcel_id: property.parcel_id ?? parcel,
    signal_type: "registered_landlord",
    source_system: "indy_gis_registered_landlord",
    source_id: `${parcel}-${row.REGISTRATION_NUMBER ?? row.OBJECTID ?? "registered"}`,
    status: "registered",
    observed_date: null,
    amount: null,
    address: street || null,
    raw: row,
  };
}

function vacantSignal(row: Record<string, unknown>, index: Map<string, { id: number; parcel_id: string | null }>): SignalRow | null {
  const parcel = normParcel(row.PARCEL_I);
  const property = index.get(parcel);
  if (!property) return null;
  return {
    property_id: property.id,
    parcel_id: property.parcel_id ?? parcel,
    signal_type: "vacant_abandoned",
    source_system: "indy_gis_vacant_abandoned",
    source_id: `${parcel}-${row.OBJECTID ?? row.STATUS ?? "vacant"}`,
    status: String(row.STATUS ?? "listed"),
    observed_date: null,
    amount: null,
    address: String(row.ADDRESS ?? "").trim() || null,
    raw: row,
  };
}

function taxSaleSignal(row: Record<string, unknown>, index: Map<string, { id: number; parcel_id: string | null }>): SignalRow | null {
  const parcel = normParcel(row.PARCEL_I ?? row.PARCELNUMBER);
  const property = index.get(parcel);
  if (!property) return null;
  return {
    property_id: property.id,
    parcel_id: property.parcel_id ?? parcel,
    signal_type: "tax_sale",
    source_system: "indy_gis_tax_sale",
    source_id: `${parcel}-${row.SALEID ?? row.OBJECTID ?? "taxsale"}-${row.TAXYEAR ?? ""}`,
    status: String(row.STATUSNAME ?? row.RECORDTYPE ?? "unknown"),
    observed_date: isoDate(row.STATUSDATE),
    amount: num(row.MINIMUMBID),
    address: [row.STNUMBER, row.PRE_DIR, row.FULL_STNAME].filter(Boolean).join(" ").replace(/\s+/g, " ").trim() || null,
    raw: row,
  };
}

async function replaceSignals(signalType: string, rows: SignalRow[]) {
  if (DRY_RUN) return;
  await db.from("property_public_signals").delete().eq("signal_type", signalType);
  for (let i = 0; i < rows.length; i += 500) {
    const batch = rows.slice(i, i + 500).map((row) => ({
      ...row,
      updated_at: new Date().toISOString(),
    }));
    const { error } = await db.from("property_public_signals").insert(batch);
    if (error) throw new Error(`Insert ${signalType}: ${error.message}`);
  }
}

async function runLayer(
  label: string,
  service: string,
  layer: number,
  parcelOf: (row: Record<string, unknown>) => string,
  mapper: (row: Record<string, unknown>, index: Map<string, { id: number; parcel_id: string | null }>) => SignalRow | null,
) {
  console.log(`\n${label}`);
  const rawRows = await arcgisRows(service, layer);
  const index = await loadPropertyIndexForParcels(rawRows.map(parcelOf));
  console.log(`  parcel index: ${index.size.toLocaleString()}`);
  const signalMap = new Map<string, SignalRow>();
  for (const row of rawRows) {
    const signal = mapper(row, index);
    if (!signal) continue;
    signalMap.set(`${signal.source_system}|${signal.source_id}|${signal.signal_type}`, signal);
  }
  const signals = [...signalMap.values()];
  console.log(`  raw: ${rawRows.length.toLocaleString()} | linked: ${signals.length.toLocaleString()}`);
  await replaceSignals(signals[0]?.signal_type ?? label, signals);
}

async function main() {
  console.log("MXRE - Indianapolis public signals");
  console.log("=".repeat(45));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Layers: ${ONLY.join(", ")}`);

  await ensureTable();

  if (ONLY.includes("landlord")) {
    await runLayer("registered landlord", "MapIndy/MapIndyProperty/MapServer", 27, (row) => String(row.PARCEL_C ?? ""), landlordSignal);
  }
  if (ONLY.includes("vacant")) {
    await runLayer("vacant/abandoned", "MapIndy/MapIndyProperty/MapServer", 16, (row) => String(row.PARCEL_I ?? ""), vacantSignal);
  }
  if (ONLY.includes("taxsale")) {
    await runLayer("tax sale", "TaxSaleViewer/TaxSaleParcels_BuildingBlocks/MapServer", 0, (row) => String(row.PARCEL_I ?? row.PARCELNUMBER ?? ""), taxSaleSignal);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal:", err instanceof Error ? err.message : err);
  process.exit(1);
});
