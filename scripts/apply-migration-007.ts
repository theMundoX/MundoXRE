import { readFileSync } from "fs";
import { resolve } from "path";
import pkg from "pg";

const { Client } = pkg;

async function main() {
  const sql = readFileSync(
    resolve("src/db/migrations/007_agency_lld.sql"),
    "utf8"
  );
  console.log(`Applying migration 007 (${sql.length} bytes) via direct postgres...`);

  const client = new Client({
    host: (process.env.MXRE_PG_HOST ?? ""),
    port: 5432,
    database: "postgres",
    user: "postgres.your-tenant-id",
    password: "${process.env.MXRE_PG_PASSWORD}",
    connectionTimeoutMillis: 10000,
  });
  await client.connect();
  try {
    await client.query(sql);
    console.log("OK");
    console.log("\nMigration 007 applied.");
  } finally {
    await client.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
