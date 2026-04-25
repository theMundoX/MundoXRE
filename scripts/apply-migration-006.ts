import { readFileSync } from "fs";
import { resolve } from "path";

const PG_META = process.env.SUPABASE_PG_META_URL || "${process.env.SUPABASE_URL}/pg/query";
const AUTH = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

async function runSQL(sql: string) {
  const r = await fetch(PG_META, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: AUTH,
      Authorization: `Bearer ${AUTH}`,
    },
    body: JSON.stringify({ query: sql }),
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

async function main() {
  const sql = readFileSync(
    resolve("src/db/migrations/006_hmda_rate_ingest.sql"),
    "utf8"
  );
  console.log(`Sending entire migration (${sql.length} bytes) as one query...`);
  await runSQL(sql);
  console.log("OK");
  console.log("\nMigration 006 applied.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
