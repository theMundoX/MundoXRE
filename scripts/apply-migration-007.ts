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
    host: "207.244.225.239",
    port: 5432,
    database: "postgres",
    user: "postgres.your-tenant-id",
    password: "d6168ff6e8d9559d62642418bafb3d17",
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
