#!/usr/bin/env tsx
/**
 * Ingest Maryland Statewide Parcels from MD iMAP ParcelBoundaries MapServer.
 * ~2.4M parcels, paginated at 1000 records per request (no geometry).
 *
 * Source: https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0
 *
 * Usage:
 *   npx tsx scripts/ingest-md-parcels.ts
 *   npx tsx scripts/ingest-md-parcels.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0";
const OUT_FIELDS = "JURSCODE,ACCTID,ADDRESS,STRTNUM,STRTDIR,STRTNAM,STRTTYP,STRTSFX,CITY,ZIPCODE,PREMCITY,PREMZIP,DESCLU,LU,ACRES,LANDAREA,YEARBLT,SQFTSTRC,DESCSTYL,STRUCNST,DESCCNST,STRUGRAD,DESCGRAD,BLDG_STORY,BLDG_UNITS,CONSIDR1,TRADATE,NFMLNDVL,NFMIMPVL,NFMTTLVL";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;

const countyCache = new Map<string, number>();

// Maryland jurisdiction codes to county names and FIPS (state FIPS = 24)
const MD_JURIS: Record<string, { name: string; fips: string }> = {
  "ALLE": { name: "Allegany", fips: "001" },
  "ANNE": { name: "Anne Arundel", fips: "003" },
  "BALT": { name: "Baltimore", fips: "005" },
  "BCIT": { name: "Baltimore City", fips: "510" },
  "CALV": { name: "Calvert", fips: "009" },
  "CARO": { name: "Caroline", fips: "011" },
  "CARR": { name: "Carroll", fips: "013" },
  "CECI": { name: "Cecil", fips: "015" },
  "CHAR": { name: "Charles", fips: "017" },
  "DORC": { name: "Dorchester", fips: "019" },
  "FRED": { name: "Frederick", fips: "021" },
  "GARR": { name: "Garrett", fips: "023" },
  "HARF": { name: "Harford", fips: "025" },
  "HOWA": { name: "Howard", fips: "027" },
  "KENT": { name: "Kent", fips: "029" },
  "MONT": { name: "Montgomery", fips: "031" },
  "PRIN": { name: "Prince George's", fips: "033" },
  "QUEE": { name: "Queen Anne's", fips: "035" },
  "SOME": { name: "Somerset", fips: "039" },
  "STMA": { name: "St. Mary's", fips: "037" },
  "TALB": { name: "Talbot", fips: "041" },
  "WASH": { name: "Washington", fips: "043" },
  "WICO": { name: "Wicomico", fips: "045" },
  "WORC": { name: "Worcester", fips: "047" },
};

async function getOrCreateCounty(jurisCode: string): Promise<{ id: number; name: string }> {
  const juris = MD_JURIS[jurisCode];
  if (!juris) throw new Error(`Unknown jurisdiction: ${jurisCode}`);

  const key = `${juris.name}_MD`;
  if (countyCache.has(key)) return { id: countyCache.get(key)!, name: juris.name };

  const { data } = await db.from("counties").select("id").eq("county_name", juris.name).eq("state_code", "MD").single();
  if (data) { countyCache.set(key, data.id); return { id: data.id, name: juris.name }; }

  const { data: created, error } = await db.from("counties")
    .insert({ county_name: juris.name, state_code: "MD", state_fips: "24", county_fips: juris.fips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return { id: created!.id, name: juris.name };
}

function classifyLandUse(code: string, desc: string): string {
  const d = (desc || "").toUpperCase();
  const c = (code || "").toUpperCase().trim();
  if (d.includes("RESIDENTIAL") || c.startsWith("R") || c === "11" || c === "01" || c === "02") return "residential";
  if (d.includes("COMMERCIAL") || c.startsWith("C") || c === "20") return "commercial";
  if (d.includes("INDUSTRIAL") || c.startsWith("I") || c === "30") return "industrial";
  if (d.includes("AGRICULTUR") || c.startsWith("A") || c === "01") return "agricultural";
  if (d.includes("APARTMENT") || d.includes("MULTI")) return "apartment";
  if (d.includes("VACANT") || c.startsWith("V")) return "vacant";
  if (d.includes("EXEMPT") || c.startsWith("E")) return "exempt";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const body = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    returnGeometry: "false",
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(`${SERVICE_URL}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
        signal: AbortSignal.timeout(60000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) ?? [];
    } catch (err: any) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(2000 * Math.pow(2, attempt - 1), 30000);
      console.log(`  Retry ${attempt}/${MAX_RETRIES} after ${delay}ms: ${err.message}`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Maryland Statewide Parcels (MD iMAP)");
  console.log("Source:", SERVICE_URL);
  console.log();

  const countResp = await fetch(`${SERVICE_URL}/query?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();

  while (offset < totalCount) {
    const records = await fetchPage(offset);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      const jurisCode = (r.JURSCODE || "").trim();
      if (!jurisCode || !MD_JURIS[jurisCode]) { totalErrors++; continue; }

      let county: { id: number; name: string };
      try {
        county = await getOrCreateCounty(jurisCode);
      } catch {
        totalErrors++;
        continue;
      }

      const address = r.ADDRESS || [r.STRTNUM, r.STRTDIR, r.STRTNAM, r.STRTTYP, r.STRTSFX].filter(Boolean).join(" ").trim();
      const yrRaw = r.YEARBLT ? parseInt(String(r.YEARBLT)) : 0;
      const yearBuilt = yrRaw > 1700 && yrRaw < 2030 ? yrRaw : null;
      const sqft = r.SQFTSTRC && r.SQFTSTRC > 0 ? Math.round(Number(r.SQFTSTRC)) || null : null;
      const acres = r.ACRES ? parseFloat(r.ACRES) : null;
      const lotSqft = r.LANDAREA && r.LANDAREA > 0 ? Math.round(Number(r.LANDAREA)) || null :
        (acres && acres > 0 ? Math.round(acres * 43560) : null);

      rows.push({
        county_id: county.id,
        parcel_id: r.ACCTID || "",
        address: address || "",
        city: r.CITY || r.PREMCITY || "",
        state_code: "MD",
        zip: r.ZIPCODE || r.PREMZIP || "",
        owner_name: "",
        assessed_value: r.NFMTTLVL && r.NFMTTLVL > 0 ? Math.round(Number(r.NFMTTLVL)) || null : null,
        land_value: r.NFMLNDVL && r.NFMLNDVL > 0 ? Math.round(Number(r.NFMLNDVL)) || null : null,
        year_built: yearBuilt,
        total_sqft: sqft,
        land_sqft: lotSqft,
        total_units: r.BLDG_UNITS && r.BLDG_UNITS > 0 ? Math.round(Number(r.BLDG_UNITS)) || null : null,
        property_type: classifyLandUse(r.LU, r.DESCLU),
        construction_class: r.DESCCNST || r.DESCSTYL || null,
        last_sale_price: r.CONSIDR1 && r.CONSIDR1 > 0 ? Math.round(Number(r.CONSIDR1)) || null : null,
        source: "md-imap-parcels",
      });
    }

    // Dedup within page
    const seen = new Map<string, any>();
    for (const row of rows) {
      seen.set(`${row.county_id}|${row.parcel_id}`, row);
    }
    const dedupedRows = Array.from(seen.values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      if (batch.length === 0) continue;
      let lastErr: any;
      let ok = false;
      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
          const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: false });
          if (error) { lastErr = error; break; }
          ok = true; break;
        } catch (err: any) {
          lastErr = err;
          if (attempt < 5) await new Promise(r => setTimeout(r, 5000 * attempt));
        }
      }
      if (!ok) {
        console.error(`  DB error at offset ${offset}+${i}: ${lastErr?.message}`);
        totalErrors += batch.length;
      } else {
        totalInserted += batch.length;
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const rate = totalInserted / ((Date.now() - startTime) / 1000);
    const pct = ((offset + records.length) / totalCount * 100).toFixed(1);
    const eta = rate > 0 ? ((totalCount - offset - records.length) / rate / 60).toFixed(0) : "?";
    console.log(`[${elapsed}s] offset=${offset} | ${pct}% | inserted=${totalInserted.toLocaleString()} | errors=${totalErrors} | ${rate.toFixed(0)}/s | ETA ${eta}min`);

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone! Inserted ${totalInserted.toLocaleString()} records in ${totalElapsed} minutes. Errors: ${totalErrors}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
