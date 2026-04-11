#!/usr/bin/env tsx
/**
 * California Statewide Parcel Ingestion
 *
 * Ingests parcels from multiple CA county ArcGIS FeatureServers and Socrata endpoints.
 * No single statewide CA parcel API exists — this script handles multiple county sources.
 *
 * Data sources confirmed:
 *  - LA County Assessor (2.4M parcels, 2025 roll year) — best assessor data
 *  - Alameda County (490K parcels, assessor values)
 *  - Sacramento County (501K parcels, address+land use)
 *  - San Francisco County via Socrata (212K parcels, assessor values)
 *  - Napa County (51K parcels, full assessor data)
 *  - Ventura County (268K parcels, APN+lat/lon)
 *  - Kern County (422K parcels, APN+geometry)
 *
 * Usage:
 *   npx tsx scripts/ingest-ca-parcels.ts                  # all counties
 *   npx tsx scripts/ingest-ca-parcels.ts LA               # LA County only
 *   npx tsx scripts/ingest-ca-parcels.ts --offset=100000  # resume LA County
 *   npx tsx scripts/ingest-ca-parcels.ts Alameda
 *   npx tsx scripts/ingest-ca-parcels.ts Sacramento
 *   npx tsx scripts/ingest-ca-parcels.ts SF
 *   npx tsx scripts/ingest-ca-parcels.ts Napa
 *   npx tsx scripts/ingest-ca-parcels.ts Ventura
 *   npx tsx scripts/ingest-ca-parcels.ts Kern
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PAGE_SIZE = 2000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

// ─── County Management ───────────────────────────────────────────────

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string): Promise<number> {
  const key = `${name}_CA`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties")
    .select("id")
    .eq("county_name", name)
    .eq("state_code", "CA")
    .single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  // CA FIPS codes
  const CA_FIPS: Record<string, string> = {
    "Alameda": "001", "Alpine": "003", "Amador": "005", "Butte": "007",
    "Calaveras": "009", "Colusa": "011", "Contra Costa": "013", "Del Norte": "015",
    "El Dorado": "017", "Fresno": "019", "Glenn": "021", "Humboldt": "023",
    "Imperial": "025", "Inyo": "027", "Kern": "029", "Kings": "031",
    "Lake": "033", "Lassen": "035", "Los Angeles": "037", "Madera": "039",
    "Marin": "041", "Mariposa": "043", "Mendocino": "045", "Merced": "047",
    "Modoc": "049", "Mono": "051", "Monterey": "053", "Napa": "055",
    "Nevada": "057", "Orange": "059", "Placer": "061", "Plumas": "063",
    "Riverside": "065", "Sacramento": "067", "San Benito": "069",
    "San Bernardino": "071", "San Diego": "073", "San Francisco": "075",
    "San Joaquin": "077", "San Luis Obispo": "079", "San Mateo": "081",
    "Santa Barbara": "083", "Santa Clara": "085", "Santa Cruz": "087",
    "Shasta": "089", "Sierra": "091", "Siskiyou": "093", "Solano": "095",
    "Sonoma": "097", "Stanislaus": "099", "Sutter": "101", "Tehama": "103",
    "Trinity": "105", "Tulare": "107", "Tuolumne": "109", "Ventura": "111",
    "Yolo": "113", "Yuba": "115",
  };

  const { data: created, error } = await db.from("counties")
    .insert({
      county_name: name,
      state_code: "CA",
      state_fips: "06",
      county_fips: CA_FIPS[name] || "000",
      active: true,
    })
    .select("id")
    .single();
  if (error) throw new Error(`Failed to create county ${name}: ${error.message}`);
  countyCache.set(key, created!.id);
  return created!.id;
}

// ─── ArcGIS Helpers ──────────────────────────────────────────────────

async function fetchWithRetry(url: string, attempt = 1): Promise<any> {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "MXRE-Ingester/1.0" },
      signal: AbortSignal.timeout(120_000),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const json = await resp.json();
    if (json.error) throw new Error(`Service error: ${json.error.message || JSON.stringify(json.error)}`);
    return json;
  } catch (err: any) {
    if (attempt >= MAX_RETRIES) throw err;
    const delay = Math.min(3000 * Math.pow(2, attempt - 1), 60_000) + Math.random() * 2000;
    console.log(`    Retry ${attempt}/${MAX_RETRIES} in ${(delay / 1000).toFixed(1)}s: ${err.message}`);
    await new Promise(r => setTimeout(r, delay));
    return fetchWithRetry(url, attempt + 1);
  }
}

async function arcgisFetchPage(
  serviceUrl: string,
  layerId: number,
  offset: number,
  outFields: string,
  extraWhere = "1=1"
): Promise<any[]> {
  const where = encodeURIComponent(extraWhere);
  const fields = encodeURIComponent(outFields);
  const url = `${serviceUrl}/${layerId}/query?where=${where}&outFields=${fields}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;
  const data = await fetchWithRetry(url);
  return data.features?.map((f: any) => f.attributes) ?? [];
}

async function arcgisGetCount(serviceUrl: string, layerId: number, where = "1=1"): Promise<number> {
  const url = `${serviceUrl}/${layerId}/query?where=${encodeURIComponent(where)}&returnCountOnly=true&f=json`;
  const data = await fetchWithRetry(url);
  return data.count || 0;
}

// ─── DB Batch Upsert ─────────────────────────────────────────────────

async function batchUpsert(rows: any[]): Promise<number> {
  let inserted = 0;
  // Dedup within batch by (county_id, parcel_id) to prevent upsert conflict errors
  const seen = new Map<string, any>();
  for (const row of rows) {
    if (row.county_id && (row.parcel_id || row.address)) {
      const key = `${row.county_id}|${row.parcel_id}`;
      seen.set(key, row);
    }
  }
  const dedupedRows = Array.from(seen.values());
  for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
    const batch = dedupedRows.slice(i, i + BATCH_SIZE);
    if (batch.length === 0) continue;
    const { error } = await db.from("properties").upsert(batch, {
      onConflict: "county_id,parcel_id",
      ignoreDuplicates: false,
    });
    if (error) {
      console.error(`    DB error: ${error.message.substring(0, 120)}`);
    } else {
      inserted += batch.length;
    }
  }
  return inserted;
}

// ─── Property Type Classification ───────────────────────────────────

function classifyLandUse(code: string, desc: string): string {
  const c = (code || "").toLowerCase();
  const d = (desc || "").toLowerCase();
  const combined = `${c} ${d}`;
  if (combined.match(/single.?fam|sfr|resid.*01|^0100|single family/)) return "single_family";
  if (combined.match(/multi|apart|duplex|triplex|fourplex|2.?fam|3.?fam|4.?fam|^02|^03|^04/)) return "multifamily";
  if (combined.match(/condo|townho|^010[2-9]|^011/)) return "condo";
  if (combined.match(/commerc|office|retail|store|shop|^04|^05/)) return "commercial";
  if (combined.match(/industr|warehouse|manufact|^06/)) return "industrial";
  if (combined.match(/vacan|land|lot|agri|farm|ranch|timber|forest|pasture|^07|^08|^09/)) return "land";
  if (combined.match(/exempt|govern|school|church|relig|hospital|park|^00/)) return "exempt";
  return "residential";
}

// ─── LA County ──────────────────────────────────────────────────────

const LA_SERVICE = "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Parcel_Data_2021_Table/FeatureServer";
const LA_LAYER = 0;
const LA_FIELDS = "AIN,PropertyLocation,SitusHouseNo,SitusFraction,SitusDirection,SitusStreet,SitusUnit,SitusCity,SitusZIP5,UseType,UseCode,YearBuilt,SQFTmain,Units,Roll_TotalValue,Roll_LandValue,Roll_ImpValue,netTaxableValue,RecordingDate,totBuildingDataLines,CENTER_LAT,CENTER_LON";
const LA_WHERE = "RollYear='2025'";

async function ingestLA(startOffset = 0): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Los Angeles County — Assessor Parcel Data (2025 Roll)");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Source: services.arcgis.com/.../Parcel_Data_2021_Table");

  const countyId = await getOrCreateCounty("Los Angeles");
  console.log(`  County ID: ${countyId}`);

  const totalCount = await arcgisGetCount(LA_SERVICE, LA_LAYER, LA_WHERE);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = startOffset;
  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(LA_SERVICE, LA_LAYER, offset, LA_FIELDS, LA_WHERE);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      totalErrors++;
      offset += PAGE_SIZE;
      continue;
    }

    if (records.length === 0) break;

    const rows = records.map(r => {
      // Build address from component fields
      const houseNo = r.SitusHouseNo ? String(r.SitusHouseNo) : "";
      const fraction = r.SitusFraction ? ` ${r.SitusFraction}` : "";
      const direction = r.SitusDirection ? ` ${r.SitusDirection}` : "";
      const street = r.SitusStreet ? ` ${r.SitusStreet}` : "";
      const unit = r.SitusUnit ? ` ${r.SitusUnit}` : "";
      const address = houseNo
        ? `${houseNo}${fraction}${direction}${street}${unit}`.trim()
        : (r.PropertyLocation || "").split("  ")[0].trim();

      // City cleanup: "LOS ANGELES CA" → "LOS ANGELES"
      const city = (r.SitusCity || "").replace(/\s+CA\s*$/, "").trim();
      const zip = (r.SitusZIP5 || "").substring(0, 5);

      // Property type from UseType and UseCode
      const propertyType = classifyLandUse(r.UseCode || "", r.UseType || "");

      // Year built
      let yearBuilt: number | null = null;
      if (r.YearBuilt) {
        const y = parseInt(String(r.YearBuilt));
        if (y > 1700 && y <= 2030) yearBuilt = y;
      }

      // Sale date from RecordingDate (epoch ms)
      let saleDate: string | null = null;
      if (r.RecordingDate) {
        try {
          const d = new Date(r.RecordingDate);
          if (!isNaN(d.getTime()) && d.getFullYear() > 1900) {
            saleDate = d.toISOString().substring(0, 10);
          }
        } catch {}
      }

      return {
        county_id: countyId,
        parcel_id: r.AIN || "",
        address: address || r.PropertyLocation || "",
        city: city || "LOS ANGELES",
        state_code: "CA",
        zip: zip || "",
        property_type: propertyType,
        year_built: yearBuilt,
        total_sqft: r.SQFTmain && r.SQFTmain > 0 ? r.SQFTmain : null,
        total_units: r.Units && r.Units > 0 ? r.Units : null,
        assessed_value: r.Roll_TotalValue && r.Roll_TotalValue > 0 ? r.Roll_TotalValue : null,
        land_value: r.Roll_LandValue && r.Roll_LandValue > 0 ? r.Roll_LandValue : null,
        taxable_value: r.netTaxableValue && r.netTaxableValue > 0 ? r.netTaxableValue : null,
        total_buildings: r.totBuildingDataLines && r.totBuildingDataLines > 0 ? r.totBuildingDataLines : null,
        last_sale_date: saleDate,
        lat: r.CENTER_LAT && Math.abs(r.CENTER_LAT) > 0.001 ? r.CENTER_LAT : null,
        lng: r.CENTER_LON && Math.abs(r.CENTER_LON) > 0.001 ? r.CENTER_LON : null,
        source: "la-assessor-2025",
      };
    }).filter(r => r.address);

    const n = await batchUpsert(rows);
    totalInserted += n;
    totalErrors += records.length - rows.length;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000 || 1);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";

    if (offset % 20000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()} | ${rate.toFixed(0)}/s | ETA ${eta}min`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  LA County done: ${totalInserted.toLocaleString()} inserted, ${totalErrors} errors, ${elapsed} min`);
  return totalInserted;
}

// ─── Alameda County ──────────────────────────────────────────────────

const ALA_SERVICE = "https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/Parcels/FeatureServer";
const ALA_LAYER = 0;
const ALA_FIELDS = "APN,SitusAddress,SitusCity,SitusZip,Land,Imps,TotalNetValue,UseCode,EconomicUnit";

async function ingestAlameda(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Alameda County — Assessor Parcels");
  console.log("═══════════════════════════════════════════════════════════");

  const countyId = await getOrCreateCounty("Alameda");
  const totalCount = await arcgisGetCount(ALA_SERVICE, ALA_LAYER);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(ALA_SERVICE, ALA_LAYER, offset, ALA_FIELDS);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map(r => {
      const situsAddr = (r.SitusAddress || "").trim();
      // SitusAddress includes city and zip sometimes: "123 MAIN ST OAKLAND 94601"
      // Try to split
      const addrParts = situsAddr.split(" ");
      const address = situsAddr;
      const city = (r.SitusCity || "").trim();
      const zip = (r.SitusZip || "").substring(0, 5);
      const propertyType = classifyLandUse(r.UseCode || "", "");

      return {
        county_id: countyId,
        parcel_id: r.APN || "",
        address,
        city: city || "ALAMEDA COUNTY",
        state_code: "CA",
        zip,
        property_type: propertyType,
        assessed_value: r.TotalNetValue && r.TotalNetValue > 0 ? r.TotalNetValue : null,
        land_value: r.Land && r.Land > 0 ? r.Land : null,
        market_value: (r.Land || 0) + (r.Imps || 0) > 0 ? (r.Land || 0) + (r.Imps || 0) : null,
        source: "alameda-assessor",
      };
    }).filter(r => r.address);

    const n = await batchUpsert(rows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 20000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  Alameda done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── Sacramento County ───────────────────────────────────────────────

const SAC_SERVICE = "https://services1.arcgis.com/5NARefyPVtAeuJPU/arcgis/rest/services/Parcels/FeatureServer";
const SAC_LAYER = 0;
const SAC_FIELDS = "APN,STREET_NBR,STREET_NAM,CITY,ZIP,LU_GENERAL,LU_SPECIF,LANDUSE";

async function ingestSacramento(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Sacramento County — Parcels (land use + address)");
  console.log("═══════════════════════════════════════════════════════════");

  const countyId = await getOrCreateCounty("Sacramento");
  const totalCount = await arcgisGetCount(SAC_SERVICE, SAC_LAYER);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(SAC_SERVICE, SAC_LAYER, offset, SAC_FIELDS);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map(r => {
      const streetNbr = r.STREET_NBR ? String(r.STREET_NBR).trim() : "";
      const streetNam = (r.STREET_NAM || "").trim();
      const address = streetNbr && streetNam ? `${streetNbr} ${streetNam}` : (streetNbr || streetNam);
      const city = (r.CITY || "SACRAMENTO").trim();
      const zip = r.ZIP ? String(r.ZIP).substring(0, 5).padStart(5, "0") : "";
      const propertyType = classifyLandUse(r.LANDUSE || "", r.LU_GENERAL || "");

      return {
        county_id: countyId,
        parcel_id: r.APN || "",
        address,
        city,
        state_code: "CA",
        zip,
        property_type: propertyType,
        source: "sacramento-gis",
      };
    }).filter(r => r.address);

    const n = await batchUpsert(rows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 20000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  Sacramento done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── San Francisco County (Socrata) ─────────────────────────────────

const SF_API = "https://data.sfgov.org/resource/wv5m-vpq2.json";
const SF_FIELDS = "parcel_number,property_location,use_code,use_definition,year_property_built,number_of_units,property_area,lot_area,assessed_improvement_value,assessed_land_value,the_geom,closed_roll_year";
const SF_ROLL_YEAR = "2024";  // Latest complete year

async function ingestSanFrancisco(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  San Francisco County — Assessor Roll 2024 (Socrata)");
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Source: data.sfgov.org");

  const countyId = await getOrCreateCounty("San Francisco");

  // Get count
  const countResp = await fetchWithRetry(
    `${SF_API}?$select=count(*)&$where=closed_roll_year='${SF_ROLL_YEAR}'`
  );
  const totalCount = parseInt(countResp[0]?.count || "0");
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const url = `${SF_API}?$select=${encodeURIComponent(SF_FIELDS)}&$where=${encodeURIComponent(`closed_roll_year='${SF_ROLL_YEAR}'`)}&$limit=${PAGE_SIZE}&$offset=${offset}&$order=parcel_number`;
    let records: any[];
    try {
      const resp = await fetch(url, {
        headers: { "User-Agent": "MXRE-Ingester/1.0" },
        signal: AbortSignal.timeout(120_000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      records = await resp.json();
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map((r: any) => {
      const address = (r.property_location || "").trim();
      // SF property_location format: "123 MAIN ST"
      const zip = "";  // SF Socrata data doesn't include ZIP in this field
      const propertyType = classifyLandUse(r.use_code || "", r.use_definition || "");

      let yearBuilt: number | null = null;
      if (r.year_property_built) {
        const y = parseInt(r.year_property_built);
        if (y > 1700 && y <= 2030) yearBuilt = y;
      }

      let lat: number | null = null;
      let lng: number | null = null;
      if (r.the_geom?.coordinates) {
        lng = r.the_geom.coordinates[0];
        lat = r.the_geom.coordinates[1];
      }

      const landVal = r.assessed_land_value ? parseFloat(r.assessed_land_value) : null;
      const impVal = r.assessed_improvement_value ? parseFloat(r.assessed_improvement_value) : null;
      const totalVal = (landVal || 0) + (impVal || 0);

      return {
        county_id: countyId,
        parcel_id: r.parcel_number || "",
        address,
        city: "SAN FRANCISCO",
        state_code: "CA",
        zip: "941",  // SF zip prefix - won't have exact zip without geocode
        property_type: propertyType,
        year_built: yearBuilt,
        total_sqft: r.property_area ? Math.round(parseFloat(r.property_area)) || null : null,
        land_sqft: r.lot_area ? Math.round(parseFloat(r.lot_area)) || null : null,
        total_units: r.number_of_units ? Math.round(parseFloat(r.number_of_units)) || null : null,
        assessed_value: totalVal > 0 ? Math.round(totalVal) : null,
        land_value: landVal && landVal > 0 ? Math.round(landVal) : null,
        lat: lat && Math.abs(lat) > 0.001 ? lat : null,
        lng: lng && Math.abs(lng) > 0.001 ? lng : null,
        source: `sf-assessor-${SF_ROLL_YEAR}`,
      };
    }).filter(r => r.address);

    const n = await batchUpsert(rows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 10000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  San Francisco done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── Napa County ────────────────────────────────────────────────────

const NAPA_SERVICE = "https://services1.arcgis.com/Ko5rxt00spOfjMqj/arcgis/rest/services/Napa_County_Public_Parcels/FeatureServer";
const NAPA_LAYER = 1;
const NAPA_FIELDS = "ASMT,FullSitusAddress1,Community,Zip,LandUseDescription,Landuse1,YearBuilt,Building_Size_SqFt,Prcl_Size_SqFt,UnitCount,CurrentMarketLandValue,TotalLandImprValue,NetAssessedValues,BuildingCount";

async function ingestNapa(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Napa County — Public Parcels (full assessor data)");
  console.log("═══════════════════════════════════════════════════════════");

  const countyId = await getOrCreateCounty("Napa");
  const totalCount = await arcgisGetCount(NAPA_SERVICE, NAPA_LAYER);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(NAPA_SERVICE, NAPA_LAYER, offset, NAPA_FIELDS);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map(r => {
      // FullSitusAddress1 format: "123 MAIN ST, NAPA CA  "
      const rawAddr = (r.FullSitusAddress1 || "").trim();
      const addrParts = rawAddr.split(",");
      const address = addrParts[0]?.trim() || rawAddr;
      // City from community code or extract from address
      const communityMap: Record<string, string> = {
        "NAP": "NAPA", "ANA": "ANGWIN", "CAL": "CALISTOGA", "SHE": "SAINT HELENA",
        "YOU": "YOUNTVILLE", "AME": "AMERICAN CANYON",
      };
      const city = communityMap[(r.Community || "").toUpperCase()] || r.Community || "NAPA";
      const zip = (r.Zip || "").substring(0, 5);
      const propertyType = classifyLandUse(r.Landuse1 || "", r.LandUseDescription || "");

      let yearBuilt: number | null = null;
      if (r.YearBuilt) {
        const y = parseInt(r.YearBuilt);
        if (y > 1700 && y <= 2030) yearBuilt = y;
      }

      return {
        county_id: countyId,
        parcel_id: r.ASMT || "",
        address,
        city,
        state_code: "CA",
        zip,
        property_type: propertyType,
        year_built: yearBuilt,
        total_sqft: r.Building_Size_SqFt && r.Building_Size_SqFt > 0 ? Math.round(r.Building_Size_SqFt) : null,
        land_sqft: r.Prcl_Size_SqFt && r.Prcl_Size_SqFt > 0 ? Math.round(r.Prcl_Size_SqFt) : null,
        total_units: r.UnitCount && r.UnitCount > 0 ? Math.round(r.UnitCount) : null,
        total_buildings: r.BuildingCount && r.BuildingCount > 0 ? Math.round(r.BuildingCount) : null,
        assessed_value: r.NetAssessedValues && r.NetAssessedValues > 0 ? Math.round(r.NetAssessedValues) : null,
        land_value: r.CurrentMarketLandValue && r.CurrentMarketLandValue > 0 ? Math.round(r.CurrentMarketLandValue) : null,
        market_value: r.TotalLandImprValue && r.TotalLandImprValue > 0 ? Math.round(r.TotalLandImprValue) : null,
        source: "napa-assessor",
      };
    }).filter(r => r.address);

    const n = await batchUpsert(rows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 10000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  Napa done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── Ventura County (APN + coordinates only) ─────────────────────────

const VEN_SERVICE = "https://services2.arcgis.com/XJ5Tb7dTYtAMoyYT/arcgis/rest/services/Parcels_Quarterly/FeatureServer";
const VEN_LAYER = 0;
const VEN_FIELDS = "APN,APN10,LAT,LON";

async function ingestVentura(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Ventura County — Parcels (APN + coordinates)");
  console.log("  Note: No address/value data available from this source");
  console.log("═══════════════════════════════════════════════════════════");

  const countyId = await getOrCreateCounty("Ventura");
  const totalCount = await arcgisGetCount(VEN_SERVICE, VEN_LAYER);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(VEN_SERVICE, VEN_LAYER, offset, VEN_FIELDS);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map(r => ({
      county_id: countyId,
      parcel_id: r.APN10 || r.APN || "",
      address: "",  // No address data available
      city: "VENTURA COUNTY",
      state_code: "CA",
      zip: "",
      lat: r.LAT && Math.abs(r.LAT) > 0.001 ? r.LAT : null,
      lng: r.LON && Math.abs(r.LON) > 0.001 ? r.LON : null,
      source: "ventura-gis",
    })).filter(r => r.parcel_id);

    // For Ventura, we need parcel_id to upsert (no address)
    // But schema requires address NOT NULL — skip address-less records
    const validRows = rows.filter(r => r.parcel_id);
    // We'll use parcel_id as a placeholder address
    const fixedRows = validRows.map(r => ({ ...r, address: `APN ${r.parcel_id}` }));

    const n = await batchUpsert(fixedRows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 20000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  Ventura done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── Kern County (APN + geometry only) ──────────────────────────────

const KERN_SERVICE = "https://services5.arcgis.com/Y8jwjGUWbRjuqpG5/arcgis/rest/services/Assessor_Parcels_Land_2025/FeatureServer";
const KERN_LAYER = 0;
const KERN_FIELDS = "APN,APN9,SHAPE_SQFT";

async function ingestKern(): Promise<number> {
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  Kern County — Assessor Parcels 2025 (APN + area only)");
  console.log("  Note: No address/value data available from this source");
  console.log("═══════════════════════════════════════════════════════════");

  const countyId = await getOrCreateCounty("Kern");
  const totalCount = await arcgisGetCount(KERN_SERVICE, KERN_LAYER);
  console.log(`  Total parcels: ${totalCount.toLocaleString()}`);

  let offset = 0;
  let totalInserted = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    let records: any[];
    try {
      records = await arcgisFetchPage(KERN_SERVICE, KERN_LAYER, offset, KERN_FIELDS);
    } catch (err: any) {
      console.error(`  Page error at offset ${offset}: ${err.message}`);
      offset += PAGE_SIZE;
      continue;
    }
    if (records.length === 0) break;

    const rows = records.map(r => ({
      county_id: countyId,
      parcel_id: r.APN9 || r.APN || "",
      address: `APN ${r.APN9 || r.APN || ""}`,
      city: "KERN COUNTY",
      state_code: "CA",
      zip: "",
      land_sqft: r.SHAPE_SQFT && r.SHAPE_SQFT > 0 ? Math.round(r.SHAPE_SQFT) : null,
      source: "kern-assessor-2025",
    })).filter(r => r.parcel_id);

    const n = await batchUpsert(rows);
    totalInserted += n;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const pct = (((offset + records.length) / totalCount) * 100).toFixed(1);
    if (offset % 20000 === 0 || offset + records.length >= totalCount) {
      console.log(`  [${elapsed}s] offset=${offset.toLocaleString()} | ${pct}% | inserted=${totalInserted.toLocaleString()}`);
    }

    offset += records.length;
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n  Kern done: ${totalInserted.toLocaleString()} inserted, ${elapsed} min`);
  return totalInserted;
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  const target = (process.argv[2] || "ALL").toUpperCase();
  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  const startOffset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  const AVAILABLE = ["LA", "ALAMEDA", "SACRAMENTO", "SF", "NAPA", "VENTURA", "KERN", "ALL"];
  if (!AVAILABLE.includes(target) && !AVAILABLE.includes(target.replace(/ /g, ""))) {
    console.error(`Unknown target: ${target}. Available: ${AVAILABLE.join(", ")}`);
    process.exit(1);
  }

  console.log("\nMXRE — California Statewide Parcel Ingestion");
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`Target: ${target === "ALL" ? "All counties" : target}`);
  console.log(`DB: ${process.env.SUPABASE_URL}`);

  const { count: initCount } = await db.from("properties")
    .select("*", { count: "exact", head: true })
    .eq("state_code", "CA");
  console.log(`CA properties in DB before ingestion: ${(initCount || 0).toLocaleString()}`);

  let grandTotal = 0;

  const runCounty = async (name: string, fn: () => Promise<number>) => {
    try {
      const n = await fn();
      grandTotal += n;
    } catch (err: any) {
      console.error(`\n  FATAL ERROR for ${name}: ${err.message}`);
    }
  };

  const RUN_ALL = target === "ALL";

  if (RUN_ALL || target === "LA") {
    await runCounty("LA", () => ingestLA(startOffset));
  }
  if (RUN_ALL || target === "ALAMEDA") {
    await runCounty("Alameda", ingestAlameda);
  }
  if (RUN_ALL || target === "SACRAMENTO") {
    await runCounty("Sacramento", ingestSacramento);
  }
  if (RUN_ALL || target === "SF" || target === "SAN FRANCISCO") {
    await runCounty("SF", ingestSanFrancisco);
  }
  if (RUN_ALL || target === "NAPA") {
    await runCounty("Napa", ingestNapa);
  }
  if (RUN_ALL || target === "VENTURA") {
    await runCounty("Ventura", ingestVentura);
  }
  if (RUN_ALL || target === "KERN") {
    await runCounty("Kern", ingestKern);
  }

  const { count: finalCount } = await db.from("properties")
    .select("*", { count: "exact", head: true })
    .eq("state_code", "CA");

  console.log("\n═══════════════════════════════════════════════════════════");
  console.log(`GRAND TOTAL INSERTED THIS RUN: ${grandTotal.toLocaleString()}`);
  console.log(`CA properties now in DB: ${(finalCount || 0).toLocaleString()}`);
  console.log("═══════════════════════════════════════════════════════════\n");
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
