#!/usr/bin/env tsx
/**
 * Backfill county_fips = 39113 on Montgomery County OH records.
 * The single-call update timed out (1.3M rows). This splits by year.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(
  process.env.SUPABASE_URL as string,
  process.env.SUPABASE_SERVICE_KEY as string,
  { auth: { persistSession: false } },
);

const FIPS = "39113";
const PATTERN = "%riss.mcrecorder.org%";

async function main() {
  const { count: total } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("county_fips", null)
    .ilike("source_url", PATTERN);

  console.log(`Montgomery OH — ${total?.toLocaleString()} records to fix`);

  let grandTotal = 0;

  // Split by year 2020-2026
  const years = [2020, 2021, 2022, 2023, 2024, 2025, 2026];

  for (const year of years) {
    const start = `${year}-01-01`;
    const end   = `${year + 1}-01-01`;

    const { count } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .is("county_fips", null)
      .ilike("source_url", PATTERN)
      .gte("recording_date", start)
      .lt("recording_date", end);

    if (!count || count === 0) { console.log(`  ${year}: 0 — skip`); continue; }

    process.stdout.write(`  ${year}: ${count.toLocaleString()} updating...`);

    const { error, count: updated } = await db
      .from("mortgage_records")
      .update({ county_fips: FIPS }, { count: "exact" })
      .is("county_fips", null)
      .ilike("source_url", PATTERN)
      .gte("recording_date", start)
      .lt("recording_date", end);

    if (error) {
      console.log(` ERROR: ${error.message}`);
    } else {
      console.log(` done (${(updated ?? 0).toLocaleString()})`);
      grandTotal += updated ?? 0;
    }
  }

  // Catch any without a recording_date
  const { count: nullDate } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("county_fips", null)
    .ilike("source_url", PATTERN)
    .is("recording_date", null);

  if (nullDate && nullDate > 0) {
    process.stdout.write(`  (no date): ${nullDate.toLocaleString()} updating...`);
    const { error, count: updated } = await db
      .from("mortgage_records")
      .update({ county_fips: FIPS }, { count: "exact" })
      .is("county_fips", null)
      .ilike("source_url", PATTERN)
      .is("recording_date", null);
    if (error) console.log(` ERROR: ${error.message}`);
    else { console.log(` done (${(updated ?? 0).toLocaleString()})`); grandTotal += updated ?? 0; }
  }

  const { count: remaining } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("county_fips", null);

  console.log(`\nTotal updated: ${grandTotal.toLocaleString()}`);
  console.log(`Remaining missing county_fips: ${remaining?.toLocaleString()}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
