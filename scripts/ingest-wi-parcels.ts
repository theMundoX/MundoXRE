#!/usr/bin/env tsx
/**
 * Ingest Wisconsin statewide parcels from WI SCO ArcGIS FeatureServer.
 * ~3.6M parcels, paginated at 2000 records per request.
 * Source: https://services3.arcgis.com/n6uYoouQZW75n5WI/arcgis/rest/services/Wisconsin_Statewide_Parcels/FeatureServer/0
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BASE_URL = "https://services3.arcgis.com/n6uYoouQZW75n5WI/arcgis/rest/services/Wisconsin_Statewide_Parcels/FeatureServer/0/query";
const PAGE_SIZE = 2000;
const DB_BATCH = 500;
const OUT_FIELDS = "PARCELID,TAXPARCELID,OWNERNME1,OWNERNME2,SITEADRESS,PLACENAME,ZIPCODE,STATE,CNTASSDVALUE,LNDVALUE,IMPVALUE,ESTFMKVALUE,PROPCLASS,AUXCLASS,CONAME,ASSDACRES,DEEDACRES";

// Wisconsin county FIPS codes (state FIPS = 55)
const WI_COUNTY_FIPS: Record<string, string> = {
  "ADAMS": "001", "ASHLAND": "003", "BARRON": "005", "BAYFIELD": "007",
  "BROWN": "009", "BUFFALO": "011", "BURNETT": "013", "CALUMET": "015",
  "CHIPPEWA": "017", "CLARK": "019", "COLUMBIA": "021", "CRAWFORD": "023",
  "DANE": "025", "DODGE": "027", "DOOR": "029", "DOUGLAS": "031",
  "DUNN": "033", "EAU CLAIRE": "035", "FLORENCE": "037", "FOND DU LAC": "039",
  "FOREST": "041", "GRANT": "043", "GREEN": "045", "GREEN LAKE": "047",
  "IOWA": "049", "IRON": "051", "JACKSON": "053", "JEFFERSON": "055",
  "JUNEAU": "057", "KENOSHA": "059", "KEWAUNEE": "061", "LA CROSSE": "063",
  "LAFAYETTE": "065", "LANGLADE": "067", "LINCOLN": "069", "MANITOWOC": "071",
  "MARATHON": "073", "MARINETTE": "075", "MARQUETTE": "077", "MENOMINEE": "078",
  "MILWAUKEE": "079", "MONROE": "081", "OCONTO": "083", "ONEIDA": "085",
  "OUTAGAMIE": "087", "OZAUKEE": "089", "PEPIN": "091", "PIERCE": "093",
  "POLK": "095", "PORTAGE": "097", "PRICE": "099", "RACINE": "101",
  "RICHLAND": "103", "ROCK": "105", "RUSK": "107", "ST CROIX": "109",
  "SAINT CROIX": "109", "ST. CROIX": "109", "SAUK": "111", "SAWYER": "113",
  "SHAWANO": "115", "SHEBOYGAN": "117", "TAYLOR": "119", "TREMPEALEAU": "121",
  "VERNON": "123", "VILAS": "125", "WALWORTH": "127", "WASHBURN": "129",
  "WASHINGTON": "131", "WAUKESHA": "133", "WAUPACA": "135", "WAUSHARA": "137",
  "WINNEBAGO": "139", "WOOD": "141",
};

const countyCache = new Map<string, number>();

async function getOrCreateCounty(name: string, state: string, stateFips = "55", countyFips = "000"): Promise<number> {
  const key = `${name}|${state}`;
  if (countyCache.has(key)) return countyCache.get(key)!;
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", state).single();
  if (data) { countyCache.set(key, data.id); return data.id; }
  const { data: created, error } = await db.from("counties").insert({
    county_name: name, state_code: state, state_fips: stateFips,
    county_fips: countyFips, active: true,
  }).select("id").single();
  if (error || !created) throw new Error(`County create failed: ${error?.message}`);
  countyCache.set(key, created.id);
  return created.id;
}

function classifyPropertyType(propClass: string, auxClass: string): string {
  const cls = (propClass || "").trim().toUpperCase();
  const aux = (auxClass || "").trim().toUpperCase();
  // WI property classes: 1=Residential, 2=Commercial, 3=Manufacturing, 4=Agricultural, 5=Undeveloped, 6=Ag Forest, 7=Forest
  if (cls === "1" || cls.startsWith("1")) return "residential";
  if (cls === "2" || cls.startsWith("2")) return "commercial";
  if (cls === "3" || cls.startsWith("3")) return "industrial";
  if (cls === "4" || cls === "6" || cls.startsWith("4")) return "agricultural";
  if (cls === "5" || cls === "5M") return "vacant";
  if (cls === "7") return "agricultural";
  return "other";
}

async function fetchPage(offset: number): Promise<any[]> {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: OUT_FIELDS,
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    f: "json",
  });

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`${BASE_URL}?${params}`, { signal: AbortSignal.timeout(60000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
      return json.features?.map((f: any) => f.attributes) || [];
    } catch (err: any) {
      if (attempt === 2) throw err;
      console.error(`  Retry ${attempt + 1}: ${err.message}`);
      await new Promise(r => setTimeout(r, 3000 * (attempt + 1)));
    }
  }
  return [];
}

async function main() {
  console.log("MXRE — Ingest Wisconsin Statewide Parcels\n");

  const countRes = await fetch(`${BASE_URL}?where=1%3D1&returnCountOnly=true&f=json`);
  const { count: totalCount } = await countRes.json();
  console.log(`Total parcels available: ${totalCount?.toLocaleString()}\n`);

  const startOffset = parseInt(process.argv[2] || "0");
  if (startOffset > 0) console.log(`Resuming from offset ${startOffset.toLocaleString()}\n`);

  let inserted = 0, skipped = 0, dbErrors = 0;
  let offset = startOffset;

  while (true) {
    const features = await fetchPage(offset);
    if (features.length === 0) break;

    const batch: any[] = [];

    for (const p of features) {
      const countyName = (p.CONAME || "").trim();
      if (!countyName) { skipped++; continue; }

      let countyId: number;
      try {
        const countyFips = WI_COUNTY_FIPS[countyName.toUpperCase()] || "000";
        countyId = await getOrCreateCounty(countyName, "WI", "55", countyFips);
      } catch { skipped++; continue; }

      const totalVal = p.ESTFMKVALUE || p.CNTASSDVALUE || null;
      const owner = [p.OWNERNME1, p.OWNERNME2].filter(Boolean).join("; ").trim();

      batch.push({
        county_id: countyId,
        parcel_id: (p.TAXPARCELID || p.PARCELID || "").trim(),
        address: (p.SITEADRESS || "").trim(),
        city: (p.PLACENAME || "").trim(),
        state_code: "WI",
        zip: (p.ZIPCODE || "").trim(),
        owner_name: owner || "",
        assessed_value: totalVal && totalVal > 0 ? totalVal : null,
        year_built: null,
        total_sqft: null,
        total_units: 1,
        property_type: classifyPropertyType(p.PROPCLASS, p.AUXCLASS),
        source: "wi-sco-parcels",
      });

      if (batch.length >= DB_BATCH) {
        const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
        if (error) {
          dbErrors++;
          if (dbErrors <= 5) console.error(`\n  DB error: ${error.message.slice(0, 120)}`);
        } else {
          inserted += batch.length;
        }
        batch.length = 0;
      }
    }

    if (batch.length > 0) {
      const { error } = await db.from("properties").upsert(batch, { onConflict: "county_id,parcel_id", ignoreDuplicates: true });
      if (error) dbErrors++;
      else inserted += batch.length;
    }

    offset += features.length;
    const pct = totalCount ? ((offset / totalCount) * 100).toFixed(1) : "?";
    process.stdout.write(`\r  Offset: ${offset.toLocaleString()} / ${totalCount?.toLocaleString()} (${pct}%) | Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped} | Errors: ${dbErrors}   `);

    if (offset % 40000 === 0) await new Promise(r => setTimeout(r, 500));
  }

  const { count } = await db.from("properties").select("*", { count: "exact", head: true });
  console.log(`\n\n══════════════════════════════════════════════`);
  console.log(`  WI parcels inserted: ${inserted.toLocaleString()}`);
  console.log(`  Skipped: ${skipped} | DB errors: ${dbErrors}`);
  console.log(`  Total properties in DB: ${count?.toLocaleString()}`);
  console.log(`══════════════════════════════════════════════\n`);
}

main().catch(console.error);
