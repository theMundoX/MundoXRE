#!/usr/bin/env tsx
/**
 * Backfill Indianapolis/Marion links for SDF deed transfers and listing signals.
 *
 * This is intentionally conservative:
 * - parcel id match wins
 * - normalized address match must be unique, or disambiguated by ZIP / coordinates
 * - coordinate-only matching is not used, because that is too easy to poison
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { execSync, spawnSync } from "node:child_process";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);
const getArg = (name: string) => args.find((arg) => arg.startsWith(`--${name}=`))?.split("=")[1];

const DRY_RUN = hasFlag("dry-run");
const RUN_SDF = !hasFlag("listings-only");
const RUN_LISTINGS = !hasFlag("sdf-only");
const FROM_YEAR = parseInt(getArg("from-year") ?? "2021", 10);
const TO_YEAR = parseInt(getArg("to-year") ?? "2025", 10);
const MARION_COUNTY_ID = 797583;
const SDF_SOURCE = "https://www.stats.indiana.edu/sdfdata/";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

interface PropertyRow {
  id: number;
  parcel_id: string | null;
  apn_formatted: string | null;
  address: string | null;
  city: string | null;
  zip: string | null;
  latitude?: number | null;
  longitude?: number | null;
  lat?: number | null;
  lng?: number | null;
}

interface PropertyIndex {
  parcelToProperty: Map<string, PropertyRow>;
  addressToProperties: Map<string, PropertyRow[]>;
}

interface SdfParcelRow {
  doc: string;
  parcel: string;
  address: string;
  city: string;
  zip: string;
}

function normParcel(value: string | null | undefined): string {
  return String(value ?? "").replace(/[-.\s]/g, "").toUpperCase();
}

function normZip(value: string | null | undefined): string {
  return String(value ?? "").slice(0, 5);
}

function normAddress(value: string | null | undefined): string {
  let out = String(value ?? "").toUpperCase();
  out = out.replace(/[.,#]/g, " ");
  out = out.replace(/\b(APT|APARTMENT|UNIT|STE|SUITE|BLDG|BUILDING|LOT|SPACE)\s+\S+/g, " ");
  out = out.replace(/\b0+(\d+(?:ST|ND|RD|TH))\b/g, "$1");
  out = out.replace(/\bAVENUE\b/g, "AVE");
  out = out.replace(/\bSTREET\b/g, "ST");
  out = out.replace(/\bROAD\b/g, "RD");
  out = out.replace(/\bDRIVE\b/g, "DR");
  out = out.replace(/\bBOULEVARD\b/g, "BLVD");
  out = out.replace(/\bCOURT\b/g, "CT");
  out = out.replace(/\bLANE\b/g, "LN");
  out = out.replace(/\bPLACE\b/g, "PL");
  out = out.replace(/\bTERRACE\b/g, "TER");
  out = out.replace(/\bTRAIL\b/g, "TRL");
  return out.replace(/\s+/g, " ").trim();
}

function addressVariants(value: string | null | undefined): string[] {
  const base = normAddress(value);
  if (!base) return [];
  const variants = new Set<string>([base]);
  variants.add(base.replace(/^(\d+)\s+(N|S|E|W)\s+/, "$1 "));
  return [...variants].filter(Boolean);
}

function coord(row: PropertyRow): { lat: number; lng: number } | null {
  const lat = Number(row.latitude ?? row.lat);
  const lng = Number(row.longitude ?? row.lng);
  return Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
}

function distanceMeters(a: { lat: number; lng: number }, b: { lat: number; lng: number }): number {
  const r = 6371000;
  const dLat = (b.lat - a.lat) * Math.PI / 180;
  const dLng = (b.lng - a.lng) * Math.PI / 180;
  const lat1 = a.lat * Math.PI / 180;
  const lat2 = b.lat * Math.PI / 180;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * r * Math.asin(Math.sqrt(h));
}

async function loadProperties(): Promise<PropertyIndex> {
  const parcelToProperty = new Map<string, PropertyRow>();
  const addressToProperties = new Map<string, PropertyRow[]>();
  let total = 0;

  for (let offset = 0; ; offset += 1000) {
    const { data, error } = await db.from("properties")
      .select("id,parcel_id,apn_formatted,address,city,zip,latitude,longitude,lat,lng")
      .eq("county_id", MARION_COUNTY_ID)
      .range(offset, offset + 999);
    if (error) throw new Error(`Failed to load Marion properties: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const property of data as PropertyRow[]) {
      total++;
      for (const value of [property.parcel_id, property.apn_formatted]) {
        const key = normParcel(value);
        if (key) parcelToProperty.set(key, property);
      }
      for (const key of addressVariants(property.address)) {
        const rows = addressToProperties.get(key) ?? [];
        rows.push(property);
        addressToProperties.set(key, rows);
      }
    }

    if (data.length < 1000) break;
  }

  console.log(`Loaded ${total.toLocaleString()} Marion properties`);
  console.log(`  parcel identifiers: ${parcelToProperty.size.toLocaleString()}`);
  console.log(`  address keys:       ${addressToProperties.size.toLocaleString()}`);
  return { parcelToProperty, addressToProperties };
}

function chooseByAddress(
  index: PropertyIndex,
  address: string,
  zip?: string | null,
  point?: { lat: number; lng: number } | null,
): PropertyRow | null {
  for (const variant of addressVariants(address)) {
    const candidates = index.addressToProperties.get(variant) ?? [];
    if (candidates.length === 0) continue;
    if (candidates.length === 1) return candidates[0];

    const zipMatches = candidates.filter((candidate) => normZip(candidate.zip) === normZip(zip));
    if (zipMatches.length === 1) return zipMatches[0];

    if (point) {
      const ranked = candidates
        .map((candidate) => ({ candidate, c: coord(candidate) }))
        .filter((entry): entry is { candidate: PropertyRow; c: { lat: number; lng: number } } => Boolean(entry.c))
        .map((entry) => ({ candidate: entry.candidate, meters: distanceMeters(point, entry.c) }))
        .sort((a, b) => a.meters - b.meters);
      if (ranked[0] && ranked[0].meters <= 75 && (!ranked[1] || ranked[1].meters - ranked[0].meters > 25)) {
        return ranked[0].candidate;
      }
    }
  }
  return null;
}

function parseTsvUtf16(filePath: string): Record<string, string>[] {
  const raw = readFileSync(filePath);
  const text = raw.slice(raw[0] === 0xFF && raw[1] === 0xFE ? 2 : 0).toString("utf16le");
  const lines = text.split(/\r?\n/);
  const unquote = (s: string) => s.replace(/^"|"$/g, "").replace(/""/g, '"').trim();
  const headers = lines[0].split("\t").map(unquote);
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const cols = line.split("\t").map(unquote);
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) row[headers[j]] = cols[j] ?? "";
    rows.push(row);
  }
  return rows;
}

function ensureSdfExtracted(year: number): string {
  const workDir = join(tmpdir(), "mxre-sdf");
  const zipPath = join(workDir, `SDF_${year}.zip`);
  const extractDir = join(workDir, `SDF_${year}`);
  if (!existsSync(zipPath)) throw new Error(`Missing cached ${zipPath}`);
  if (!existsSync(join(extractDir, "SALEPARCEL.txt"))) {
    rmSync(extractDir, { recursive: true, force: true });
    const unzipResult = spawnSync("unzip", ["-o", "-q", zipPath, "-d", extractDir], { encoding: "utf8" });
    if (unzipResult.error || (unzipResult.status !== 0 && unzipResult.status !== null)) {
      execSync(`powershell -Command "Expand-Archive -Force -Path '${zipPath}' -DestinationPath '${extractDir}'"`, { stdio: "inherit" });
    }
  }
  return extractDir;
}

async function updateMortgageRows(pairs: Array<[string, number]>): Promise<number> {
  if (pairs.length === 0 || DRY_RUN) return 0;
  let updated = 0;
  for (let i = 0; i < pairs.length; i += 500) {
    const chunk = pairs.slice(i, i + 500);
    const values = chunk.map(([doc, propertyId]) => `('${doc.replace(/'/g, "''")}',${propertyId})`).join(",");
    const query = `WITH v(document_number, property_id) AS (VALUES ${values}), updated AS (
      UPDATE mortgage_records m
         SET property_id = v.property_id
        FROM v
       WHERE m.source_url = '${SDF_SOURCE}'
         AND m.document_type = 'deed'
         AND m.document_number = v.document_number
         AND m.property_id IS NULL
       RETURNING m.id
    ) SELECT count(*)::int AS updated FROM updated;`;
    const res = await fetch(`${process.env.SUPABASE_URL}/pg/query`, {
      method: "POST",
      headers: { apikey: process.env.SUPABASE_SERVICE_KEY!, Authorization: `Bearer ${process.env.SUPABASE_SERVICE_KEY!}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    if (!res.ok) throw new Error(`pg/query failed ${res.status}: ${await res.text()}`);
    const body = await res.json();
    updated += Number(body?.[0]?.updated ?? body?.data?.[0]?.updated ?? 0);
  }
  return updated;
}

async function backfillSdf(index: PropertyIndex): Promise<void> {
  console.log("\nBackfilling Marion SDF rows...");
  let matchedDocs = 0;
  let updatedRows = 0;

  for (let year = FROM_YEAR; year <= TO_YEAR; year++) {
    const extractDir = ensureSdfExtracted(year);
    const rows = parseTsvUtf16(join(extractDir, "SALEPARCEL.txt"));
    const pairs = new Map<string, number>();
    let marionRows = 0;

    for (const row of rows) {
      const doc = row.SDF_ID;
      if (!doc?.startsWith(`C49-${year}-`)) continue;
      marionRows++;

      const parcel = row.P2_1_Parcel_Num_Verified || row.A1_Parcel_Number;
      let property = index.parcelToProperty.get(normParcel(parcel)) ?? null;
      if (!property) {
        property = chooseByAddress(index, row.A5_Street1, row.A5_ZipCode);
      }
      if (property && !pairs.has(doc)) pairs.set(doc, property.id);
    }

    const docs = [...pairs.entries()];
    const updated = await updateMortgageRows(docs);
    matchedDocs += docs.length;
    updatedRows += updated;
    console.log(`  ${year}: ${marionRows.toLocaleString()} Marion parcel rows, ${docs.length.toLocaleString()} matched docs, ${DRY_RUN ? "dry-run" : updated.toLocaleString()} rows updated`);
  }

  console.log(`SDF matched docs: ${matchedDocs.toLocaleString()}`);
  console.log(`SDF updated rows:  ${updatedRows.toLocaleString()}`);
}

async function backfillListings(index: PropertyIndex): Promise<void> {
  console.log("\nBackfilling Indianapolis listing signals...");
  let updated = 0;
  let noMatch = 0;
  let loaded = 0;

  while (true) {
    const { data, error } = await db.from("listing_signals")
      .select("id,address,city,state_code,zip,raw")
      .eq("state_code", "IN")
      .ilike("city", "INDIANAPOLIS")
      .is("property_id", null)
      .not("address", "is", null)
      .order("id")
      .limit(1000);
    if (error) throw new Error(`Failed to load listing signals: ${error.message}`);
    if (!data || data.length === 0) break;

    loaded += data.length;
    let batchUpdated = 0;
    let batchNoMatch = 0;
    for (const listing of data as Array<Record<string, any>>) {
      const raw = listing.raw ?? {};
      const lat = Number(raw.latitude);
      const lng = Number(raw.longitude);
      const point = Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
      const property = chooseByAddress(index, listing.address, listing.zip, point);
      if (!property) {
        noMatch++;
        batchNoMatch++;
        continue;
      }
      if (!DRY_RUN) {
        const { error: updateError } = await db.from("listing_signals")
          .update({ property_id: property.id, updated_at: new Date().toISOString() })
          .eq("id", listing.id)
          .is("property_id", null);
        if (updateError) throw new Error(`Failed to update listing ${listing.id}: ${updateError.message}`);
      }
      updated++;
      batchUpdated++;
    }

    console.log(`  Batch loaded ${data.length.toLocaleString()}: updated ${DRY_RUN ? "dry-run " : ""}${batchUpdated.toLocaleString()}, no match ${batchNoMatch.toLocaleString()}`);

    if (batchUpdated === 0 || data.length < 1000) break;
  }

  console.log(`  Loaded:   ${loaded.toLocaleString()}`);
  console.log(`  Updated:  ${DRY_RUN ? "dry-run " : ""}${updated.toLocaleString()}`);
  console.log(`  No match: ${noMatch.toLocaleString()}`);
}

async function main() {
  console.log("MXRE Indianapolis fallback linker");
  console.log(`Dry run: ${DRY_RUN}`);
  const index = await loadProperties();
  if (RUN_SDF) await backfillSdf(index);
  if (RUN_LISTINGS) await backfillListings(index);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
