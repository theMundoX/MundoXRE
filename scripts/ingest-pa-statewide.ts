#!/usr/bin/env tsx
/**
 * Ingest Pennsylvania Statewide Parcels from PA DEP MapServer.
 * ~4.7M parcels, paginated at 1000 records per request (no geometry).
 *
 * Source: https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0
 *
 * Usage:
 *   npx tsx scripts/ingest-pa-statewide.ts
 *   npx tsx scripts/ingest-pa-statewide.ts --county=Chester
 *   npx tsx scripts/ingest-pa-statewide.ts --offset=100000   # resume from offset
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const SERVICE_URL = "https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0";
const OUT_FIELDS = "PARCEL_ID,OWNER_NAME,OWNER_LAST_NAME,OWNER_FIRST_NAME,PROPERTY_ADDRESS_1,PROPERTY_ADDRESS_2,CITY,STATE,ZIP,COUNTY_NAME,COUNTY_CODE,DISTRICT,ACREAGE,ACCOUNT";
const PAGE_SIZE = 1000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 5;
const COUNTY_FILTER = process.argv.find(a => a.startsWith("--county="))?.split("=")[1]?.trim();

// Pennsylvania county FIPS codes (state FIPS = 42)
const PA_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ALLEGHENY": "003", "ARMSTRONG": "005", "BEAVER": "007",
  "BEDFORD": "009", "BERKS": "011", "BLAIR": "013", "BRADFORD": "015",
  "BUCKS": "017", "BUTLER": "019", "CAMBRIA": "021", "CAMERON": "023",
  "CARBON": "025", "CENTRE": "027", "CHESTER": "029", "CLARION": "031",
  "CLEARFIELD": "033", "CLINTON": "035", "COLUMBIA": "037", "CRAWFORD": "039",
  "CUMBERLAND": "041", "DAUPHIN": "043", "DELAWARE": "045", "ELK": "047",
  "ERIE": "049", "FAYETTE": "051", "FOREST": "053", "FRANKLIN": "055",
  "FULTON": "057", "GREENE": "059", "HUNTINGDON": "061", "INDIANA": "063",
  "JEFFERSON": "065", "JUNIATA": "067", "LACKAWANNA": "069", "LANCASTER": "071",
  "LAWRENCE": "073", "LEBANON": "075", "LEHIGH": "077", "LUZERNE": "079",
  "LYCOMING": "081", "MCKEAN": "083", "MERCER": "085", "MIFFLIN": "087",
  "MONROE": "089", "MONTGOMERY": "091", "MONTOUR": "093", "NORTHAMPTON": "095",
  "NORTHUMBERLAND": "097", "PERRY": "099", "PHILADELPHIA": "101", "PIKE": "103",
  "POTTER": "105", "SCHUYLKILL": "107", "SNYDER": "109", "SOMERSET": "111",
  "SULLIVAN": "113", "SUSQUEHANNA": "115", "TIOGA": "117", "UNION": "119",
  "VENANGO": "121", "WARREN": "123", "WASHINGTON": "125", "WAYNE": "127",
  "WESTMORELAND": "129", "WYOMING": "131", "YORK": "133",
};

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, state: string): Promise<number> {
  const key = `${name}_${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;

  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }

  const countyFips = PA_COUNTY_FIPS[name.toUpperCase()] || "000";
  const { data: created, error } = await db.from("counties")
    .insert({ county_name: name, state_code: state, state_fips: "42", county_fips: countyFips, active: true })
    .select("id").single();
  if (error) throw error;
  countyCache.set(key, created!.id);
  return created!.id;
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

function whereClause(): string {
  if (!COUNTY_FILTER) return "1=1";
  return `UPPER(COUNTY_NAME)='${COUNTY_FILTER.replace(/'/g, "''").toUpperCase()}'`;
}

async function fetchPage(offset: number): Promise<any[]> {
  const url = `${SERVICE_URL}/query?where=${encodeURIComponent(whereClause())}&outFields=${OUT_FIELDS}&returnGeometry=false&resultOffset=${offset}&resultRecordCount=${PAGE_SIZE}&f=json`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url);
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
  console.log("MXRE — Ingest Pennsylvania Statewide Parcels (PA DEP)");
  console.log("Source:", SERVICE_URL);
  if (COUNTY_FILTER) console.log("County filter:", COUNTY_FILTER);
  console.log();

  const countResp = await fetch(`${SERVICE_URL}/query?where=${encodeURIComponent(whereClause())}&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countResp.json();
  console.log(`Total parcels: ${totalCount.toLocaleString()}\n`);

  const offsetArg = process.argv.find(a => a.startsWith("--offset="));
  let offset = offsetArg ? parseInt(offsetArg.split("=")[1]) : 0;

  let totalInserted = 0;
  let totalErrors = 0;
  const startTime = Date.now();
  let consecutiveZeroInsertPages = 0;

  while (offset < totalCount) {
    const records = await fetchPage(offset);

    if (records.length === 0) {
      console.log(`No records at offset ${offset}, done.`);
      break;
    }

    const rows: any[] = [];
    for (const r of records) {
      let countyName = r.COUNTY_NAME || "";
      if (!countyName) { totalErrors++; continue; }

      // Normalize county name to title case
      countyName = titleCase(countyName.trim());

      let countyId: number;
      try {
        countyId = await getOrCreateCounty(countyName, "PA");
      } catch {
        totalErrors++;
        continue;
      }

      const ownerName = r.OWNER_NAME ||
        [r.OWNER_LAST_NAME, r.OWNER_FIRST_NAME].filter(Boolean).join(", ") || "";

      const address = [r.PROPERTY_ADDRESS_1, r.PROPERTY_ADDRESS_2].filter(Boolean).join(" ").trim();
      const acreage = r.ACREAGE ? parseFloat(r.ACREAGE) : null;

      rows.push({
        county_id: countyId,
        parcel_id: r.PARCEL_ID || r.ACCOUNT || "",
        address: address || "",
        city: r.CITY || "",
        state_code: "PA",
        zip: r.ZIP || "",
        owner_name: ownerName,
        land_sqft: acreage && acreage > 0 ? Math.round(acreage * 43560) : null,
        source: "pa-dep-parcels",
      });
    }

    // Dedup within page by (county_id, parcel_id)
    const seen = new Map<string, any>();
    for (const row of rows) {
      const key = `${row.county_id}|${row.parcel_id}`;
      seen.set(key, row);
    }
    const dedupedRows = Array.from(seen.values());

    for (let i = 0; i < dedupedRows.length; i += BATCH_SIZE) {
      const batch = dedupedRows.slice(i, i + BATCH_SIZE);
      if (batch.length === 0) continue;
      let lastErr: any;
      let ok = false;
      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
          const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id" });
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

    if (dedupedRows.length > 0 && totalInserted === 0 && totalErrors >= offset + records.length) {
      consecutiveZeroInsertPages++;
    } else if (totalInserted > 0) {
      consecutiveZeroInsertPages = 0;
    }
    if (consecutiveZeroInsertPages >= 3) {
      throw new Error(
        `PA parcel ingest is failing every row with zero inserts after ${offset + records.length} records; aborting early to avoid a noisy full-county retry.`
      );
    }

    offset += records.length;
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone! Inserted ${totalInserted.toLocaleString()} records in ${totalElapsed} minutes. Errors: ${totalErrors}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
