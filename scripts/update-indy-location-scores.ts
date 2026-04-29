#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "5000", 10));
const ONLY_ACTIVE = !process.argv.includes("--all-parcels");
const DRY_RUN = process.argv.includes("--dry-run");

type Stop = { stop_id: string; stop_name: string | null; lat: string | number; lon: string | number; routes: string[] | null };
type Crime = { id: number; lat: string | number; lon: string | number; class_type: string | null; occurred_at: string | null };
type Prop = { id: number; lat: string | number | null; lng: string | number | null };

async function pg<T extends Record<string, unknown>>(query: string): Promise<T[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  if (Array.isArray(value)) return `array[${value.map(sql).join(",")}]::text[]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function miles(aLat: number, aLon: number, bLat: number, bLon: number): number {
  const r = 3958.7613;
  const toRad = (n: number) => n * Math.PI / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const s1 = Math.sin(dLat / 2) ** 2;
  const s2 = Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * r * Math.asin(Math.sqrt(s1 + s2));
}

function crimeWeight(classType: string | null): number {
  const c = (classType ?? "").toLowerCase();
  if (c.includes("violent")) return 4;
  if (c.includes("property")) return 2;
  if (c.includes("drug")) return 1.5;
  return 1;
}

async function main() {
  console.log("MXRE - Indianapolis location intelligence scoring");
  console.log(`Target: ${ONLY_ACTIVE ? "active listing properties" : "all Indianapolis parcels"}; limit ${LIMIT}; dry run ${DRY_RUN}`);

  const stops = await pg<Stop>(`select stop_id, stop_name, lat, lon, routes from transit_stops where source = 'indygo_gtfs';`);
  const crimes = await pg<Crime>(`
    select id, lat, lon, class_type, occurred_at
    from crime_incidents
    where source = 'impd_public_incidents'
      and occurred_at >= now() - interval '365 days'
      and lat is not null and lon is not null
      and lat between 39 and 40 and lon between -87 and -85
    order by occurred_at desc;
  `);
  const properties = await pg<Prop>(`
    ${ONLY_ACTIVE ? `
      select distinct
        p.id,
        coalesce(p.lat, p.latitude) as lat,
        coalesce(p.lng, p.longitude) as lng
      from properties p
      join listing_signals l on l.property_id = p.id
      where l.is_on_market = true
        and p.state_code = 'IN'
        and (upper(p.city) = 'INDIANAPOLIS' or upper(l.city) = 'INDIANAPOLIS')
        and coalesce(p.lat, p.latitude) is not null
        and coalesce(p.lng, p.longitude) is not null
      limit ${LIMIT};
    ` : `
      select id, coalesce(lat, latitude) as lat, coalesce(lng, longitude) as lng
      from properties
      where state_code = 'IN'
        and upper(city) = 'INDIANAPOLIS'
        and coalesce(lat, latitude) is not null
        and coalesce(lng, longitude) is not null
      order by id
      limit ${LIMIT};
    `}
  `);

  const stopRows = stops.map(s => ({ ...s, latN: Number(s.lat), lonN: Number(s.lon) })).filter(s => Number.isFinite(s.latN) && Number.isFinite(s.lonN));
  const crimeRows = crimes.map(c => ({ ...c, latN: Number(c.lat), lonN: Number(c.lon) })).filter(c => Number.isFinite(c.latN) && Number.isFinite(c.lonN));
  const statements: string[] = [];

  for (const p of properties) {
    const lat = Number(p.lat);
    const lon = Number(p.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    let nearest = { stop_id: null as string | null, stop_name: null as string | null, distance: Infinity, routes: [] as string[] };
    for (const stop of stopRows) {
      if (Math.abs(stop.latN - lat) > 0.05 || Math.abs(stop.lonN - lon) > 0.05) continue;
      const d = miles(lat, lon, stop.latN, stop.lonN);
      if (d < nearest.distance) nearest = { stop_id: stop.stop_id, stop_name: stop.stop_name, distance: d, routes: stop.routes ?? [] };
    }

    let c025 = 0, c05 = 0, c1 = 0, violent05 = 0, property05 = 0, drug05 = 0, weighted = 0;
    for (const crime of crimeRows) {
      if (Math.abs(crime.latN - lat) > 0.02 || Math.abs(crime.lonN - lon) > 0.02) continue;
      const d = miles(lat, lon, crime.latN, crime.lonN);
      if (d <= 1) c1++;
      if (d <= 0.5) {
        c05++;
        const classType = (crime.class_type ?? "").toLowerCase();
        if (classType.includes("violent")) violent05++;
        if (classType.includes("property")) property05++;
        if (classType.includes("drug")) drug05++;
        weighted += crimeWeight(crime.class_type);
      }
      if (d <= 0.25) c025++;
    }

    const crimeScore = Math.min(100, Math.round((weighted * 1.8 + c025 * 1.5 + c1 * 0.15) * 10) / 10);
    statements.push(`
      insert into property_location_scores (
        property_id, nearest_bus_stop_id, nearest_bus_stop_name, nearest_bus_distance_miles, bus_routes,
        crime_incidents_025mi_365d, crime_incidents_05mi_365d, crime_incidents_1mi_365d,
        violent_crime_05mi_365d, property_crime_05mi_365d, drug_crime_05mi_365d,
        crime_score, crime_score_basis, scored_at, updated_at
      ) values (
        ${p.id}, ${sql(nearest.stop_id)}, ${sql(nearest.stop_name)}, ${Number.isFinite(nearest.distance) ? nearest.distance.toFixed(4) : "null"}, ${sql(nearest.routes)},
        ${c025}, ${c05}, ${c1}, ${violent05}, ${property05}, ${drug05},
        ${crimeScore}, 'IMPD public incidents within 0.25/0.5/1 mile, trailing 365 days; weighted by class type', now(), now()
      )
      on conflict (property_id) do update set
        nearest_bus_stop_id = excluded.nearest_bus_stop_id,
        nearest_bus_stop_name = excluded.nearest_bus_stop_name,
        nearest_bus_distance_miles = excluded.nearest_bus_distance_miles,
        bus_routes = excluded.bus_routes,
        crime_incidents_025mi_365d = excluded.crime_incidents_025mi_365d,
        crime_incidents_05mi_365d = excluded.crime_incidents_05mi_365d,
        crime_incidents_1mi_365d = excluded.crime_incidents_1mi_365d,
        violent_crime_05mi_365d = excluded.violent_crime_05mi_365d,
        property_crime_05mi_365d = excluded.property_crime_05mi_365d,
        drug_crime_05mi_365d = excluded.drug_crime_05mi_365d,
        crime_score = excluded.crime_score,
        crime_score_basis = excluded.crime_score_basis,
        scored_at = excluded.scored_at,
        updated_at = now();
    `);
  }

  if (!DRY_RUN) {
    for (let i = 0; i < statements.length; i += 100) await pg(statements.slice(i, i + 100).join("\n"));
  }

  console.log(JSON.stringify({
    transit_stops: stopRows.length,
    crime_incidents_365d: crimeRows.length,
    properties_scored: DRY_RUN ? 0 : statements.length,
    properties_examined: properties.length,
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
