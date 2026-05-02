#!/usr/bin/env tsx
import "dotenv/config";

const ARCGIS_URL = "https://gis.franklincountyohio.gov/hosting/rest/services/ParcelFeatures/Parcel_Features/MapServer/0";
const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const COUNTY_ID = Number(process.argv.find(a => a.startsWith("--county_id="))?.split("=")[1] ?? "1698985");
const CITY = (process.argv.find(a => a.startsWith("--city="))?.split("=")[1] ?? "COLUMBUS").toUpperCase();
const PAGE_SIZE = Number(process.argv.find(a => a.startsWith("--page-size="))?.split("=")[1] ?? "1000");
const MAX_PAGES = Number(process.argv.find(a => a.startsWith("--max-pages="))?.split("=")[1] ?? "9999");
const DRY_RUN = process.argv.includes("--dry-run");

type ArcgisRow = {
  PARCELID?: string;
  CLASSCD?: string;
  CLASSDSCRP?: string;
  PCLASS?: string;
  RESFLRAREA?: number | null;
};

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function classify(row: ArcgisRow) {
  const code = String(row.CLASSCD ?? "").trim();
  const desc = String(row.CLASSDSCRP ?? "").trim().toUpperCase();
  const pclass = String(row.PCLASS ?? "").trim().toUpperCase();

  if (/APARTMENTS 40/.test(desc) || code === "403") {
    return { property_type: "multifamily", asset_type: "commercial_multifamily", asset_subtype: "apartment_40_plus", total_units: 40, unit_count_source: "assessor_use_minimum", asset_confidence: "medium", is_apartment: true, is_sfr: false, is_condo: false };
  }
  if (/APARTMENTS 20-39|CONDO 20-39/.test(desc) || code === "402" || code === "552") {
    return { property_type: "multifamily", asset_type: "commercial_multifamily", asset_subtype: "apartment_20_39", total_units: 20, unit_count_source: "assessor_use_minimum", asset_confidence: "medium", is_apartment: true, is_sfr: false, is_condo: code === "552" };
  }
  if (/APARTMENTS 4-19|CONDO 4-19/.test(desc) || code === "401" || code === "551") {
    return { property_type: "multifamily", asset_type: "commercial_multifamily", asset_subtype: "apartment_4_19", total_units: 4, unit_count_source: "assessor_use_minimum", asset_confidence: "medium", is_apartment: true, is_sfr: false, is_condo: code === "551" };
  }
  if (/TWO FAMILY/.test(desc) || code === "520") {
    return { property_type: "multifamily", asset_type: "small_multifamily", asset_subtype: "duplex", total_units: 2, unit_count_source: "assessor_class_code", asset_confidence: "high", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (/THREE FAMILY/.test(desc) || code === "530") {
    return { property_type: "multifamily", asset_type: "small_multifamily", asset_subtype: "triplex", total_units: 3, unit_count_source: "assessor_class_code", asset_confidence: "high", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (/CONDOMINIUM/.test(desc) || code.startsWith("55")) {
    return { property_type: "condo", asset_type: "residential", asset_subtype: "condo", total_units: 1, unit_count_source: "inferred_single_unit", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: true };
  }
  if (/SINGLE FAMILY/.test(desc) || /^51\d$/.test(code)) {
    return { property_type: "single_family", asset_type: "residential", asset_subtype: "sfr", total_units: 1, unit_count_source: "inferred_single_unit", asset_confidence: "medium", is_apartment: false, is_sfr: true, is_condo: false };
  }
  if (/RESIDENTIAL/.test(desc) || pclass === "R") {
    return { property_type: "residential", asset_type: "residential", asset_subtype: /VACANT|LAND/.test(desc) ? "residential_land" : "residential_other", total_units: /VACANT|LAND/.test(desc) ? null : 1, unit_count_source: /VACANT|LAND/.test(desc) ? "unknown" : "inferred_single_unit", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (pclass === "C" || /^4/.test(code)) {
    return { property_type: "commercial", asset_type: "commercial", asset_subtype: "commercial", total_units: null, unit_count_source: "unknown", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (pclass === "I" || /^3/.test(code)) {
    return { property_type: "industrial", asset_type: "industrial", asset_subtype: "industrial", total_units: null, unit_count_source: "unknown", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (pclass === "A" || /^1/.test(code)) {
    return { property_type: "agricultural", asset_type: "agricultural", asset_subtype: "agricultural", total_units: null, unit_count_source: "unknown", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: false };
  }
  if (pclass === "E" || /^6/.test(code)) {
    return { property_type: "exempt", asset_type: "exempt", asset_subtype: "exempt", total_units: null, unit_count_source: "unknown", asset_confidence: "medium", is_apartment: false, is_sfr: false, is_condo: false };
  }
  return { property_type: "other", asset_type: "other", asset_subtype: "unknown", total_units: null, unit_count_source: "unknown", asset_confidence: "low", is_apartment: false, is_sfr: false, is_condo: false };
}

async function fetchPage(offset: number): Promise<ArcgisRow[]> {
  const where = encodeURIComponent(`upper(PSTLCITYSTZIP) like '${CITY} OH%'`);
  const outFields = encodeURIComponent("PARCELID,CLASSCD,CLASSDSCRP,PCLASS,RESFLRAREA");
  const url = `${ARCGIS_URL}/query?where=${where}&outFields=${outFields}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;
  const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`ArcGIS ${response.status}: ${await response.text()}`);
  const json = await response.json() as { features?: Array<{ attributes: ArcgisRow }> };
  return (json.features ?? []).map(f => f.attributes);
}

async function main() {
  console.log("MXRE - Franklin OH class description backfill");
  console.log(JSON.stringify({ city: CITY, county_id: COUNTY_ID, page_size: PAGE_SIZE, dry_run: DRY_RUN }, null, 2));

  let offset = 0;
  let pages = 0;
  let updated = 0;
  let fetched = 0;

  while (pages < MAX_PAGES) {
    const rows = await fetchPage(offset);
    if (rows.length === 0) break;
    fetched += rows.length;
    pages++;

    const payload = rows
      .filter(row => row.PARCELID)
      .map(row => ({ parcel_id: String(row.PARCELID), property_use: [row.CLASSCD, row.CLASSDSCRP].filter(Boolean).join(" - "), ...classify(row) }));

    if (!DRY_RUN && payload.length > 0) {
      for (let i = 0; i < payload.length; i += 500) {
        const batch = payload.slice(i, i + 500);
        const values = batch.map(item => `(
          ${sql(item.parcel_id)},
          ${sql(item.property_use)},
          ${sql(item.property_type)},
          ${sql(item.asset_type)},
          ${sql(item.asset_subtype)},
          ${sql(item.total_units)},
          ${sql(item.unit_count_source)},
          ${sql(item.asset_confidence)},
          ${sql(item.is_apartment)},
          ${sql(item.is_sfr)},
          ${sql(item.is_condo)}
        )`).join(",");
        await pg(`
          update properties p
             set property_use = v.property_use,
                 property_type = v.property_type,
                 asset_type = v.asset_type,
                 asset_subtype = v.asset_subtype,
                 total_units = v.total_units,
                 unit_count_source = v.unit_count_source,
                 asset_confidence = v.asset_confidence,
                 is_apartment = v.is_apartment,
                 is_sfr = v.is_sfr,
                 is_condo = v.is_condo,
                 updated_at = now()
            from (values ${values}) as v(
              parcel_id, property_use, property_type, asset_type, asset_subtype,
              total_units, unit_count_source, asset_confidence, is_apartment, is_sfr, is_condo
            )
           where p.county_id = ${COUNTY_ID}
             and p.parcel_id = v.parcel_id;
        `);
        updated += batch.length;
      }
    }

    offset += PAGE_SIZE;
    console.log(`  pages=${pages} fetched=${fetched.toLocaleString()} updated=${updated.toLocaleString()} offset=${offset.toLocaleString()}`);
    if (rows.length < PAGE_SIZE) break;
  }

  console.log(JSON.stringify({ pages, fetched, updated, dry_run: DRY_RUN }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
