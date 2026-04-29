#!/usr/bin/env tsx
import "dotenv/config";

const SERVICE_URL = "https://gis.indy.gov/server/rest/services/IMPD/IMPD_Public_Data/MapServer/1/query";
const YEAR = parseInt(process.argv.find(a => a.startsWith("--year="))?.split("=")[1] ?? String(new Date().getFullYear()), 10);
const LIMIT = Math.min(2000, Math.max(1, parseInt(process.argv.find(a => a.startsWith("--page-size="))?.split("=")[1] ?? "2000", 10)));
const MAX_RECORDS = parseInt(process.argv.find(a => a.startsWith("--max-records="))?.split("=")[1] ?? "0", 10) || Infinity;
const DRY_RUN = process.argv.includes("--dry-run");
const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

type Feature = { attributes: Record<string, any>; geometry?: { x?: number; y?: number } };

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<Record<string, unknown>[]>;
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function isoDate(value: unknown): string | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  return new Date(value).toISOString();
}

async function fetchPage(offset: number): Promise<Feature[]> {
  const params = new URLSearchParams({
    where: `iOccYr >= ${YEAR} AND PublicData = 'Yes'`,
    outFields: "*",
    returnGeometry: "true",
    f: "json",
    resultOffset: String(offset),
    resultRecordCount: String(LIMIT),
    outSR: "4326",
    orderByFields: "OBJECTID ASC",
  });
  const response = await fetch(`${SERVICE_URL}?${params}`, { signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`ArcGIS HTTP ${response.status}: ${await response.text()}`);
  const body = await response.json() as { features?: Feature[]; error?: { message?: string } };
  if (body.error) throw new Error(body.error.message ?? "ArcGIS query failed");
  return body.features ?? [];
}

function statement(feature: Feature): string | null {
  const a = feature.attributes;
  const lon = Number(a.Longitude || feature.geometry?.x);
  const lat = Number(a.Latitude || feature.geometry?.y);
  if (!a.OBJECTID || !Number.isFinite(lat) || !Number.isFinite(lon) || lat === 0 || lon === 0) return null;
  const occurredAt = isoDate(a.OccurredFrom);
  return `
    insert into crime_incidents (
      source, source_object_id, case_number, occurred_at, incident_year, incident_type,
      nibrs_class, nibrs_class_desc, class_type, disposition, block_address, city, zip,
      district, beat, lat, lon, raw, observed_at, updated_at
    ) values (
      'impd_public_incidents', ${sql(a.OBJECTID)}, ${sql(a.CaseNum)}, ${sql(occurredAt)}, ${sql(a.iOccYr)}, ${sql(a.CR_Desc)},
      ${sql(a.NIBRSClassDesc)}, ${sql(a.NIBRSClassCodeDesc)}, ${sql(a.CAIU_ClassType)}, ${sql(a.Disposition)}, ${sql(a.sAddress)}, ${sql(a.sCity)}, ${sql(a.Geo_Zip || a.sZip)},
      ${sql(a.Geo_Districts)}, ${sql(a.Geo_Beats)}, ${lat}, ${lon}, ${sql(JSON.stringify(a))}::jsonb, now(), now()
    )
    on conflict (source, source_object_id) do update set
      occurred_at = excluded.occurred_at,
      incident_type = excluded.incident_type,
      nibrs_class = excluded.nibrs_class,
      nibrs_class_desc = excluded.nibrs_class_desc,
      class_type = excluded.class_type,
      disposition = excluded.disposition,
      block_address = excluded.block_address,
      city = excluded.city,
      zip = excluded.zip,
      district = excluded.district,
      beat = excluded.beat,
      lat = excluded.lat,
      lon = excluded.lon,
      raw = excluded.raw,
      observed_at = excluded.observed_at,
      updated_at = now();
  `;
}

async function main() {
  console.log("MXRE - IMPD public crime incident ingest");
  console.log(`Year >= ${YEAR}; page size ${LIMIT}; dry run ${DRY_RUN}`);
  let offset = 0;
  let fetched = 0;
  let upsertable = 0;

  while (fetched < MAX_RECORDS) {
    const features = await fetchPage(offset);
    if (features.length === 0) break;
    fetched += features.length;
    const statements = features.map(statement).filter((s): s is string => Boolean(s));
    upsertable += statements.length;
    if (!DRY_RUN) {
      for (let i = 0; i < statements.length; i += 100) await pg(statements.slice(i, i + 100).join("\n"));
    }
    console.log(`  offset ${offset.toLocaleString()}: fetched ${features.length}, upsertable ${statements.length}`);
    if (features.length < LIMIT) break;
    offset += LIMIT;
  }

  console.log(JSON.stringify({ year_min: YEAR, fetched, upserted: DRY_RUN ? 0 : upsertable }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
