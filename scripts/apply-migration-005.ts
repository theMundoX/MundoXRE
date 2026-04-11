#!/usr/bin/env tsx
/**
 * Apply migration 005 directly to self-hosted Supabase via pg-meta /pg/query.
 */
import "dotenv/config";
import { readFileSync } from "node:fs";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

const sqlPath = "C:/Users/msanc/mxre/src/db/migrations/005_mls_sales_history_cdc.sql";
const sql = readFileSync(sqlPath, "utf8");

console.log(`Applying migration 005 (${sql.length} chars) to ${SUPABASE_URL}/pg/query`);

async function exec(query: string, label: string) {
  const resp = await fetch(`${SUPABASE_URL}/pg/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SERVICE_KEY,
      Authorization: `Bearer ${SERVICE_KEY}`,
    },
    body: JSON.stringify({ query }),
  });
  const text = await resp.text();
  if (!resp.ok) {
    console.error(`  ❌ ${label}: HTTP ${resp.status}`);
    console.error(`     ${text.slice(0, 400)}`);
    return false;
  }
  console.log(`  ✅ ${label}`);
  return true;
}

async function main() {
  // pg-meta /pg/query handles multi-statement scripts in one shot.
  const ok = await exec(sql, "migration 005 (full)");
  if (!ok) process.exit(1);

  console.log("\nVerifying...");
  const verify = await fetch(`${SUPABASE_URL}/pg/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SERVICE_KEY,
      Authorization: `Bearer ${SERVICE_KEY}`,
    },
    body: JSON.stringify({
      query: `
        SELECT
          (SELECT count(*) FROM mortgage_records WHERE rate_source = 'estimated') AS est_count,
          (SELECT count(*) FROM mortgage_records WHERE rate_source = 'recorded')  AS rec_count,
          (SELECT count(*) FROM mortgage_records WHERE rate_source IS NULL)       AS null_count,
          (SELECT count(*) FROM mls_listings)                                     AS mls_listings,
          (SELECT count(*) FROM property_sales_history)                           AS sales_hist,
          (SELECT count(*) FROM property_history)                                 AS prop_hist,
          (SELECT count(*) FROM change_events)                                    AS events
      `,
    }),
  });
  const verifyJson = await verify.json();
  console.log("\n=== POST-MIGRATION COUNTS ===");
  console.log(JSON.stringify(verifyJson, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
