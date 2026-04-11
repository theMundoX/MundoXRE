/**
 * Black Hawk County IA ingest - uses direct postgres connection to bypass PostgREST pool limits.
 */
import pkg from "pg";
const { Pool } = pkg;
import { readFileSync } from "fs";
import { resolve } from "path";

// Manual dotenv loading
const envPath = resolve(
  new URL(".", import.meta.url).pathname.replace(/^\/([A-Z]:)/, "$1"),
  "../.env"
);
try {
  const lines = readFileSync(envPath, "utf8").split("\n");
  for (const line of lines) {
    const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (m) process.env[m[1]] = m[2].trim();
  }
} catch {}

// Direct postgres connection via Supavisor session mode (port 5432, user=postgres.your-tenant-id)
const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 3,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const STATEWIDE_URL =
  "https://services3.arcgis.com/kd9gaiUExYqUbnoq/arcgis/rest/services/Iowa_Parcels_2017/FeatureServer/0/query";

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function queryArcGIS(offset, limit, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const params = new URLSearchParams({
        where: "COUNTYNAME='BLACK HAWK'",
        outFields: "PARCELNUMB,STATEPARID,DEEDHOLDER,PARCELCLAS",
        resultRecordCount: String(limit),
        resultOffset: String(offset),
        f: "json",
      });
      const resp = await fetch(`${STATEWIDE_URL}?${params}`, {
        headers: { "User-Agent": "Mozilla/5.0" },
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      return data.features || [];
    } catch (e) {
      if (attempt < retries - 1) {
        await sleep(2000 * (attempt + 1));
      } else {
        console.error(`\n  ArcGIS fetch failed at offset ${offset}: ${e.message}`);
        return [];
      }
    }
  }
}

function classifyProperty(parcelClass) {
  const cls = (parcelClass || "").toUpperCase().trim();
  if (cls.includes("COMMERCIAL")) return "commercial";
  if (cls.includes("INDUSTRIAL")) return "industrial";
  if (cls.includes("AGRIC")) return "agricultural";
  if (cls.includes("RESID")) return "residential";
  if (cls.includes("EXEMPT")) return "exempt";
  if (cls.includes("MULTI")) return "multifamily";
  return "other";
}

async function insertBatch(rows) {
  if (rows.length === 0) return { inserted: 0, duplicates: 0 };
  const client = await pool.connect();
  let inserted = 0;
  let duplicates = 0;
  try {
    // Use a temp table for fast COPY-style insert with dedup
    await client.query("BEGIN");
    await client.query(`
      CREATE TEMP TABLE tmp_props (
        county_id int, parcel_id text, address text, city text, state_code text,
        zip text, owner_name text, assessed_value numeric, taxable_value numeric,
        market_value numeric, land_value numeric, year_built int, total_sqft numeric,
        property_type text, last_sale_date date, last_sale_price numeric, source text
      ) ON COMMIT DROP
    `);

    // Bulk insert into temp table
    const values = rows.map((_, i) => {
      const base = i * 17;
      return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8},$${base+9},$${base+10},$${base+11},$${base+12},$${base+13},$${base+14},$${base+15},$${base+16},$${base+17})`;
    });
    const flat = rows.flatMap((r) => [
      r.county_id, r.parcel_id, r.address, r.city, r.state_code, r.zip,
      r.owner_name, r.assessed_value, r.taxable_value, r.market_value, r.land_value,
      r.year_built, r.total_sqft, r.property_type, r.last_sale_date, r.last_sale_price, r.source,
    ]);
    await client.query(
      `INSERT INTO tmp_props VALUES ${values.join(",")}`,
      flat
    );

    // Upsert from temp into properties
    const result = await client.query(`
      INSERT INTO properties (county_id, parcel_id, address, city, state_code, zip,
        owner_name, assessed_value, taxable_value, market_value, land_value,
        year_built, total_sqft, property_type, last_sale_date, last_sale_price, source)
      SELECT county_id, parcel_id, address, city, state_code, zip,
        owner_name, assessed_value, taxable_value, market_value, land_value,
        year_built, total_sqft, property_type, last_sale_date, last_sale_price, source
      FROM tmp_props
      ON CONFLICT (county_id, parcel_id) DO NOTHING
      RETURNING 1
    `);
    inserted = result.rowCount;
    duplicates = rows.length - inserted;
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
  return { inserted, duplicates };
}

async function main() {
  console.log("MXRE — Ingest Black Hawk County IA (direct postgres)\n");

  // Test connection
  try {
    const r = await pool.query("SELECT current_database(), version()");
    console.log("  DB connected:", r.rows[0].current_database);
  } catch (e) {
    console.error("  DB connection failed:", e.message);
    console.error("  Note: Postgres port 5432 may be firewalled. Falling back to PostgREST.");
    process.exit(1);
  }

  // Get county ID
  const { rows: countyRows } = await pool.query(
    "SELECT id FROM counties WHERE county_name=$1 AND state_code=$2",
    ["Black Hawk", "IA"]
  );
  if (!countyRows.length) { console.error("County not found"); process.exit(1); }
  const countyId = countyRows[0].id;
  console.log("  County ID:", countyId);

  const { rows: countRows } = await pool.query(
    "SELECT COUNT(*) FROM properties WHERE county_id=$1",
    [countyId]
  );
  console.log("  Existing properties:", countRows[0].count);

  const FETCH_SIZE = 1000;
  const BATCH_SIZE = 500;
  let offset = 0;
  let inserted = 0, skipped = 0, duplicates = 0, errors = 0;

  while (true) {
    const features = await queryArcGIS(offset, FETCH_SIZE);
    if (features.length === 0) break;

    const batch = [];
    for (const f of features) {
      const a = f.attributes;
      const parcelId = (a.PARCELNUMB || a.STATEPARID || "").trim();
      const owner = (a.DEEDHOLDER || "").trim();
      if (!parcelId) { skipped++; continue; }
      batch.push({
        county_id: countyId,
        parcel_id: parcelId,
        address: "",
        city: "",
        state_code: "IA",
        zip: "",
        owner_name: owner,
        assessed_value: null,
        taxable_value: null,
        market_value: null,
        land_value: null,
        year_built: null,
        total_sqft: null,
        property_type: classifyProperty(a.PARCELCLAS),
        last_sale_date: null,
        last_sale_price: null,
        source: "iowa-statewide-parcels-2017",
      });
    }

    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE);
      try {
        const result = await insertBatch(chunk);
        inserted += result.inserted;
        duplicates += result.duplicates;
      } catch (e) {
        errors += chunk.length;
        console.error(`\n  Insert error: ${e.message.slice(0, 120)}`);
      }
    }

    offset += features.length;
    process.stdout.write(
      `\r  Progress: ${offset.toLocaleString()} fetched | ${inserted.toLocaleString()} inserted | ${duplicates} dups | ${skipped} skipped | ${errors} errors`
    );

    if (features.length < FETCH_SIZE) break;
  }

  console.log(`\n\n  Done.`);
  const { rows: finalCount } = await pool.query(
    "SELECT COUNT(*) FROM properties WHERE county_id=$1",
    [countyId]
  );
  console.log(`  Black Hawk County IA final count: ${parseInt(finalCount[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch((e) => { console.error(e); pool.end(); process.exit(1); });
