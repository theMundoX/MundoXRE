#!/usr/bin/env tsx
import "dotenv/config";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { Client } from "pg";

const migrationPath = join(process.cwd(), "migrations", "009_fallback_enrichment_queue.sql");
const sql = await readFile(migrationPath, "utf8");
const pgUrl = [
  process.env.MXRE_DIRECT_PG_URL,
  process.env.MXRE_PG_URL,
  process.env.DATABASE_URL,
  process.env.POSTGRES_URL,
].find((value) => value?.startsWith("postgres://") || value?.startsWith("postgresql://"));

if (pgUrl) {
  const client = new Client({ connectionString: pgUrl });
  await client.connect();
  try {
    await client.query(sql);
    console.log("Fallback enrichment migration applied via direct Postgres.");
  } finally {
    await client.end();
  }
} else {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY ?? process.env.MXRE_SUPABASE_SERVICE_KEY;
  if (!url || !key) {
    throw new Error("Missing direct Postgres URL or SUPABASE_URL + service key.");
  }

  const response = await fetch(`${url.replace(/\/$/, "")}/pg/query`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${key}`,
      apikey: key,
    },
    body: JSON.stringify({ query: sql }),
  });
  const body = await response.text();
  if (!response.ok) throw new Error(`Migration failed: ${response.status} ${body}`);
  console.log("Fallback enrichment migration applied via Supabase SQL endpoint.");
}
