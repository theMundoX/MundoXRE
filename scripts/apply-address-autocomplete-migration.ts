#!/usr/bin/env tsx

import "dotenv/config";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { Client } from "pg";

const pgUrl = [process.env.MXRE_PG_URL, process.env.DATABASE_URL, process.env.POSTGRES_URL]
  .find((value) => value?.startsWith("postgres://") || value?.startsWith("postgresql://"));

const migrationPath = join(process.cwd(), "migrations", "008_address_autocomplete_index.sql");
const sql = await readFile(migrationPath, "utf8");

if (pgUrl) {
  const client = new Client({ connectionString: pgUrl });
  await client.connect();
  try {
    await client.query(sql);
    console.log("Address autocomplete migration applied via direct Postgres.");
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
  console.log("Address autocomplete migration applied via Supabase SQL endpoint.");
}
