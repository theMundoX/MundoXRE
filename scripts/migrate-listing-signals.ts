/**
 * Migration: Create listing_signals and agent_licenses tables.
 * Run: npx tsx scripts/migrate-listing-signals.ts
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY/SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function runSQL(sql: string, label: string) {
  const { error } = await db.rpc("exec_sql", { sql_text: sql }).single();
  if (error) {
    // Try alternative: direct query via REST
    console.log(`  RPC exec_sql not available for "${label}", trying direct...`);
    const resp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ sql_text: sql }),
    });
    if (!resp.ok) {
      console.error(`  Failed: ${label} — ${error.message}`);
      console.log("  Run this SQL manually in Supabase SQL Editor:");
      console.log(sql);
      return false;
    }
  }
  console.log(`  ✓ ${label}`);
  return true;
}

const LISTING_SIGNALS_SQL = `
CREATE TABLE IF NOT EXISTS listing_signals (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  property_id bigint REFERENCES properties(id) ON DELETE SET NULL,
  address text NOT NULL,
  city text NOT NULL,
  state_code char(2) NOT NULL,
  zip text,
  is_on_market boolean NOT NULL DEFAULT true,
  mls_list_price integer,
  listing_agent_name text,
  listing_brokerage text,
  listing_source text NOT NULL,
  listing_url text,
  days_on_market integer,
  confidence text NOT NULL DEFAULT 'single',
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  delisted_at timestamptz,
  raw jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(address, city, state_code, listing_source)
);

CREATE INDEX IF NOT EXISTS idx_listing_signals_property_id ON listing_signals(property_id);
CREATE INDEX IF NOT EXISTS idx_listing_signals_area ON listing_signals(state_code, city, is_on_market);
CREATE INDEX IF NOT EXISTS idx_listing_signals_last_seen ON listing_signals(last_seen_at);
`;

const AGENT_LICENSES_SQL = `
CREATE TABLE IF NOT EXISTS agent_licenses (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  agent_name text NOT NULL,
  license_number text NOT NULL,
  license_state char(2) NOT NULL,
  license_status text NOT NULL,
  brokerage_name text,
  phone text,
  email text,
  license_type text,
  source_url text,
  observed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(license_number, license_state)
);

CREATE INDEX IF NOT EXISTS idx_agent_licenses_name ON agent_licenses(agent_name, license_state);
`;

async function main() {
  console.log("Creating listing_signals and agent_licenses tables...\n");

  const ok1 = await runSQL(LISTING_SIGNALS_SQL, "listing_signals table + indexes");
  const ok2 = await runSQL(AGENT_LICENSES_SQL, "agent_licenses table + indexes");

  if (!ok1 || !ok2) {
    console.log("\n── Manual SQL (copy to Supabase SQL Editor) ──\n");
    if (!ok1) console.log(LISTING_SIGNALS_SQL);
    if (!ok2) console.log(AGENT_LICENSES_SQL);
  }

  console.log("\nDone.");
}

main().catch(console.error);
