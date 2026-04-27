#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pg(q: string): Promise<any[]> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: q }),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
  return res.json();
}

async function main() {
  const rows = await pg(`
    SELECT year, state_code, COUNT(*)::int as cnt
    FROM hmda_originations
    GROUP BY year, state_code
    ORDER BY state_code, year
  `);
  if (rows.length === 0) {
    console.log("hmda_originations: NO ROWS — inserts likely failed");
  } else {
    console.log(`hmda_originations: ${rows.length} year/state combos`);
    for (const r of rows) console.log(`  ${r.state_code} ${r.year}: ${r.cnt.toLocaleString()} rows`);
  }

  // Also check if table even exists
  const check = await pg(`
    SELECT COUNT(*)::int as total FROM hmda_originations
  `);
  console.log(`Total rows: ${check[0]?.total?.toLocaleString() ?? "unknown"}`);
}

main().catch(e => { console.error(e); process.exit(1); });
