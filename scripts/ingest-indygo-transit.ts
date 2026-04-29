#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { parse } from "csv-parse/sync";

const FEED_URL = process.argv.find(a => a.startsWith("--url="))?.split("=")[1]
  ?? "https://realtime.indygo.net/InfoPoint/gtfs-zip.ashx";
const DRY_RUN = process.argv.includes("--dry-run");
const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

type CsvRow = Record<string, string>;

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
  if (Array.isArray(value)) return `array[${value.map(sql).join(",")}]::text[]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function readCsv(dir: string, file: string): Promise<CsvRow[]> {
  const text = await readFile(join(dir, file), "utf8");
  return parse(text, { columns: true, skip_empty_lines: true, bom: true }) as CsvRow[];
}

async function main() {
  const workDir = join(process.cwd(), "data", "indygo-gtfs");
  const zipPath = join(workDir, "indygo_gtfs.zip");
  const extractDir = join(workDir, "feed");
  await mkdir(workDir, { recursive: true });
  if (existsSync(extractDir)) await rm(extractDir, { recursive: true, force: true });
  await mkdir(extractDir, { recursive: true });

  console.log("MXRE - IndyGo GTFS transit ingest");
  console.log(`Feed: ${FEED_URL}`);
  console.log(`Dry run: ${DRY_RUN}`);

  const response = await fetch(FEED_URL, { signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`GTFS download HTTP ${response.status}`);
  await writeFile(zipPath, Buffer.from(await response.arrayBuffer()));

  const tar = spawnSync("tar", ["-xf", zipPath, "-C", extractDir], { encoding: "utf8" });
  if (tar.status !== 0) throw new Error(`Failed to extract GTFS zip: ${tar.stderr || tar.stdout}`);

  const stops = await readCsv(extractDir, "stops.txt");
  const routes = await readCsv(extractDir, "routes.txt");
  const trips = await readCsv(extractDir, "trips.txt");
  const stopTimes = await readCsv(extractDir, "stop_times.txt");
  const routeNames = new Map(routes.map(r => [r.route_id, r.route_short_name || r.route_long_name || r.route_id]));
  const tripRoute = new Map(trips.map(t => [t.trip_id, t.route_id]));
  const stopRoutes = new Map<string, Set<string>>();

  for (const row of stopTimes) {
    const routeId = tripRoute.get(row.trip_id);
    if (!routeId || !row.stop_id) continue;
    const label = routeNames.get(routeId) ?? routeId;
    const set = stopRoutes.get(row.stop_id) ?? new Set<string>();
    set.add(label);
    stopRoutes.set(row.stop_id, set);
  }

  const statements = stops
    .filter(s => Number.isFinite(Number(s.stop_lat)) && Number.isFinite(Number(s.stop_lon)))
    .map(s => `
      insert into transit_stops (source, stop_id, stop_code, stop_name, lat, lon, routes, raw, observed_at, updated_at)
      values ('indygo_gtfs', ${sql(s.stop_id)}, ${sql(s.stop_code)}, ${sql(s.stop_name)}, ${Number(s.stop_lat)}, ${Number(s.stop_lon)}, ${sql([...(stopRoutes.get(s.stop_id) ?? new Set<string>())].sort())}, ${sql(JSON.stringify(s))}::jsonb, now(), now())
      on conflict (source, stop_id) do update set
        stop_code = excluded.stop_code,
        stop_name = excluded.stop_name,
        lat = excluded.lat,
        lon = excluded.lon,
        routes = excluded.routes,
        raw = excluded.raw,
        observed_at = excluded.observed_at,
        updated_at = now();
    `);

  if (!DRY_RUN) {
    for (let i = 0; i < statements.length; i += 100) await pg(statements.slice(i, i + 100).join("\n"));
  }

  console.log(JSON.stringify({
    stops: stops.length,
    routes: routes.length,
    trips: trips.length,
    stop_times: stopTimes.length,
    upserted: DRY_RUN ? 0 : statements.length,
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
