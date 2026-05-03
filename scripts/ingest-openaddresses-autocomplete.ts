#!/usr/bin/env tsx

import "dotenv/config";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readdir, rm, stat } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";
import { parse } from "csv-parse";
import { Client } from "pg";
import { from as copyFrom } from "pg-copy-streams";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};
const hasFlag = (name: string) => args.includes(`--${name}`);

const pgUrl = [process.env.MXRE_DIRECT_PG_URL, process.env.DATABASE_URL, process.env.POSTGRES_URL, process.env.MXRE_PG_URL]
  .find((value) => value?.startsWith("postgres://") || value?.startsWith("postgresql://"));
if (!pgUrl) {
  throw new Error("Missing direct Postgres URL. Set MXRE_DIRECT_PG_URL, DATABASE_URL, POSTGRES_URL, or a postgres:// MXRE_PG_URL for bulk OpenAddresses COPY import.");
}

const zipUrl = valueArg("url");
const zipPath = valueArg("zip");
const stateFilter = valueArg("state")?.toUpperCase() ?? null;
const sourceName = valueArg("source") ?? "openaddresses";
const limit = Number(valueArg("limit") ?? "0");
const batchSize = Number(valueArg("batch-size") ?? "10000");
const workDir = resolve(valueArg("work-dir") ?? join("data", "openaddresses", "work"));
const keep = hasFlag("keep");

type OaRow = {
  LON?: string;
  LAT?: string;
  NUMBER?: string;
  STREET?: string;
  UNIT?: string;
  CITY?: string;
  DISTRICT?: string;
  REGION?: string;
  POSTCODE?: string;
  ID?: string;
  HASH?: string;
};

if (!zipUrl && !zipPath) {
  throw new Error("Provide --zip=path/to/openaddresses.zip or --url=https://...");
}

await mkdir(workDir, { recursive: true });
const archivePath = zipPath ? resolve(zipPath) : join(workDir, basename(new URL(zipUrl!).pathname) || "openaddresses.zip");
const extractDir = join(workDir, `${basename(archivePath).replace(/\.zip$/i, "")}-extracted`);

if (zipUrl && !zipPath) {
  console.log(`Downloading ${zipUrl}`);
  const response = await fetch(zipUrl);
  if (!response.ok || !response.body) throw new Error(`Download failed: ${response.status} ${response.statusText}`);
  await pipeline(response.body, createWriteStream(archivePath));
}

await rm(extractDir, { recursive: true, force: true });
await mkdir(extractDir, { recursive: true });
await unzip(archivePath, extractDir);

const csvFiles = (await walk(extractDir)).filter((file) => file.toLowerCase().endsWith(".csv"));
if (csvFiles.length === 0) throw new Error(`No CSV files found in ${archivePath}`);

console.log(`Found ${csvFiles.length} CSV files.`);

const client = new Client({ connectionString: pgUrl });
await client.connect();

try {
  await client.query(`
    create temp table oa_autocomplete_stage (
      source text,
      external_id text,
      type text,
      label text,
      street text,
      city text,
      state_code text,
      zip text,
      county text,
      lat numeric,
      lng numeric,
      confidence text,
      market_key text
    );
  `);

  let imported = 0;
  for (const file of csvFiles) {
    const inferredState = inferState(file);
    if (stateFilter && inferredState !== stateFilter) continue;

    console.log(`Importing ${file} (${inferredState ?? "unknown state"})`);
    const rows = await readOpenAddressRows(file, inferredState);
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      await copyBatch(client, batch);
      imported += batch.length;
      process.stdout.write(`\r  staged ${imported.toLocaleString("en-US")}`);
      if (limit > 0 && imported >= limit) break;
    }
    process.stdout.write("\n");
    if (limit > 0 && imported >= limit) break;
  }

  console.log("Merging staged rows into address_autocomplete_entries...");
  const merge = await client.query(`
    insert into address_autocomplete_entries (
      source,
      external_id,
      type,
      label,
      street,
      city,
      state_code,
      zip,
      county,
      lat,
      lng,
      confidence,
      market_key,
      updated_at
    )
    select
      source,
      external_id,
      type,
      label,
      street,
      city,
      state_code,
      zip,
      county,
      lat,
      lng,
      confidence,
      market_key,
      now()
    from oa_autocomplete_stage
    where label is not null and label <> '' and state_code is not null and state_code <> ''
    on conflict (source, external_id) do update set
      label = excluded.label,
      street = excluded.street,
      city = excluded.city,
      state_code = excluded.state_code,
      zip = excluded.zip,
      county = excluded.county,
      lat = excluded.lat,
      lng = excluded.lng,
      confidence = excluded.confidence,
      market_key = excluded.market_key,
      updated_at = now();
  `);

  console.log(`OpenAddresses import complete. Staged ${imported.toLocaleString("en-US")} rows, merged ${merge.rowCount?.toLocaleString("en-US") ?? "unknown"} rows.`);
} finally {
  await client.end();
  if (!keep && !zipPath) await rm(workDir, { recursive: true, force: true });
}

async function readOpenAddressRows(file: string, inferredState: string | null) {
  const out: string[][] = [];
  const parser = createReadStream(file).pipe(parse({ columns: true, bom: true, skip_empty_lines: true, relax_column_count: true }));

  for await (const raw of parser as AsyncIterable<OaRow>) {
    const state = normalizeState(raw.REGION) ?? inferredState;
    if (!state) continue;
    if (stateFilter && state !== stateFilter) continue;

    const number = clean(raw.NUMBER);
    const streetName = clean(raw.STREET);
    if (!number || !streetName) continue;

    const unit = clean(raw.UNIT);
    const street = [number, streetName, unit ? `#${unit}` : ""].filter(Boolean).join(" ");
    const city = clean(raw.CITY);
    const zip = normalizeZip(raw.POSTCODE);
    const lat = numeric(raw.LAT);
    const lng = numeric(raw.LON);
    const label = [toTitleCase(street), city ? toTitleCase(city) : "", [state, zip].filter(Boolean).join(" ")].filter(Boolean).join(", ");
    const externalId = clean(raw.HASH) || clean(raw.ID) || `${state}|${zip ?? ""}|${city ?? ""}|${street}`.toUpperCase();
    const marketKey = city ? `${slug(city)}-${state.toLowerCase()}` : null;

    out.push([
      sourceName,
      externalId,
      "address",
      label,
      toTitleCase(street),
      city ? toTitleCase(city) : "",
      state,
      zip ?? "",
      "",
      lat ?? "",
      lng ?? "",
      "medium",
      marketKey ?? "",
    ]);
  }

  return out;
}

async function copyBatch(client: Client, rows: string[][]) {
  if (rows.length === 0) return;
  const stream = client.query(copyFrom(`
    copy oa_autocomplete_stage (
      source,
      external_id,
      type,
      label,
      street,
      city,
      state_code,
      zip,
      county,
      lat,
      lng,
      confidence,
      market_key
    ) from stdin with (format csv)
  `));

  for (const row of rows) {
    stream.write(row.map(csvCell).join(",") + "\n");
  }
  stream.end();
  await new Promise<void>((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });
}

async function unzip(zip: string, dest: string) {
  if (process.platform === "win32") {
    await exec("powershell", ["-NoProfile", "-Command", `Expand-Archive -LiteralPath '${zip.replace(/'/g, "''")}' -DestinationPath '${dest.replace(/'/g, "''")}' -Force`]);
    return;
  }
  await exec("unzip", ["-q", zip, "-d", dest]);
}

function exec(command: string, commandArgs: string[]) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(command, commandArgs, { stdio: "inherit" });
    child.on("close", (code) => code === 0 ? resolve() : reject(new Error(`${command} exited ${code}`)));
    child.on("error", reject);
  });
}

async function walk(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const out: string[] = [];
  for (const entry of entries) {
    const full = join(root, entry.name);
    if (entry.isDirectory()) out.push(...await walk(full));
    else if (entry.isFile()) out.push(full);
  }
  return out;
}

function inferState(file: string): string | null {
  const parts = file.split(/[\\/]/).map((part) => part.toUpperCase());
  for (const part of parts) {
    if (/^[A-Z]{2}\.CSV$/.test(part)) return part.slice(0, 2);
    if (/^[A-Z]{2}$/.test(part)) return part;
  }
  return null;
}

function clean(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeState(value: unknown): string | null {
  const cleanValue = clean(value).toUpperCase();
  return /^[A-Z]{2}$/.test(cleanValue) ? cleanValue : null;
}

function normalizeZip(value: unknown): string | null {
  const match = clean(value).match(/\d{5}/);
  return match?.[0] ?? null;
}

function numeric(value: unknown): string | null {
  const cleanValue = clean(value);
  if (!/^-?\d+(\.\d+)?$/.test(cleanValue)) return null;
  return cleanValue;
}

function toTitleCase(value: unknown): string {
  return clean(value).toLowerCase().replace(/\b[a-z]/g, (letter) => letter.toUpperCase());
}

function slug(value: string): string {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function csvCell(value: unknown): string {
  const raw = String(value ?? "");
  return `"${raw.replace(/"/g, '""')}"`;
}
