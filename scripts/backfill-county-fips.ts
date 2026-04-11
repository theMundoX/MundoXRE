#!/usr/bin/env tsx
/**
 * Backfill county_fips on mortgage_records that are missing it.
 * Maps source_url patterns to Ohio county FIPS codes.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(
  process.env.SUPABASE_URL as string,
  process.env.SUPABASE_SERVICE_KEY as string,
  { auth: { persistSession: false } },
);

const MAPPINGS: Array<{ pattern: string; fips: string; label: string }> = [
  // Ohio — PublicSearch.us (Kofile)
  { pattern: "%franklin.oh.publicsearch.us%",         fips: "39049", label: "Franklin, OH" },
  { pattern: "%butler.oh.publicsearch.us%",            fips: "39017", label: "Butler, OH" },
  { pattern: "%cuyahoga.oh.publicsearch.us%",          fips: "39035", label: "Cuyahoga, OH" },
  { pattern: "%stark.oh.publicsearch.us%",             fips: "39151", label: "Stark, OH" },
  // Ohio — other recorders
  { pattern: "%acclaim-web.hamiltoncountyohio.gov%",   fips: "39061", label: "Hamilton, OH" },
  { pattern: "%riss.mcrecorder.org%",                  fips: "39113", label: "Montgomery, OH" },
  { pattern: "%OHFairfield%",                          fips: "39045", label: "Fairfield, OH" },
  { pattern: "%OHGeauga%",                             fips: "39055", label: "Geauga, OH" },
  { pattern: "%OHPaulding%",                           fips: "39125", label: "Paulding, OH" },
  { pattern: "%OHWyandot%",                            fips: "39175", label: "Wyandot, OH" },
  // Texas — PublicSearch.us
  { pattern: "%dallas.tx.publicsearch.us%",            fips: "48113", label: "Dallas, TX" },
  { pattern: "%denton.tx.publicsearch.us%",            fips: "48121", label: "Denton, TX" },
  { pattern: "%tarrant.tx.publicsearch.us%",           fips: "48439", label: "Tarrant, TX" },
  // Florida — county clerk portals
  { pattern: "%or.martinclerk.com%",                   fips: "12085", label: "Martin, FL" },
  { pattern: "%clerkofcourts.co.walton.fl.us%",        fips: "12131", label: "Walton, FL" },
  { pattern: "%search.citrusclerk.org%",               fips: "12017", label: "Citrus, FL" },
  { pattern: "%online.levyclerk.com%",                 fips: "12075", label: "Levy, FL" },
  // Colorado
  { pattern: "%records.larimer.org%",                  fips: "08069", label: "Larimer, CO" },
  // Alabama
  { pattern: "%landmarkweb.jccal.org%",                fips: "01073", label: "Jefferson, AL" },
  // Indiana
  { pattern: "%inmarion.fidlar.com%",                  fips: "18097", label: "Marion, IN" },
  { pattern: "%inporter.fidlar.com%",                  fips: "18127", label: "Porter, IN" },
  { pattern: "%instjoseph.fidlar.com%",                fips: "18141", label: "St. Joseph, IN" },
  { pattern: "%inallen.fidlar.com%",                   fips: "18003", label: "Allen, IN" },
];

async function main() {
  console.log("MXRE — Backfill county_fips on mortgage_records");
  console.log("=".repeat(60));

  // Check total missing before
  const { count: missingBefore } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("county_fips", null);
  console.log(`Missing county_fips before: ${missingBefore?.toLocaleString()}\n`);

  for (const { pattern, fips, label } of MAPPINGS) {
    // Count how many will be updated
    const { count } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .is("county_fips", null)
      .ilike("source_url", pattern);

    if (!count || count === 0) {
      console.log(`  ${label} (${fips}): 0 to update — skipping`);
      continue;
    }

    console.log(`  ${label} (${fips}): ${count.toLocaleString()} to update...`);

    // Direct update using filter — avoids URI too long from passing thousands of IDs
    const { error: updateErr, count: updatedCount } = await db
      .from("mortgage_records")
      .update({ county_fips: fips }, { count: "exact" })
      .is("county_fips", null)
      .ilike("source_url", pattern);

    if (updateErr) {
      console.error(`    Update error: ${updateErr.message}`);
      continue;
    }

    console.log(`    Done: ${(updatedCount ?? 0).toLocaleString()} updated for ${label}`);
  }

  // Check total missing after
  const { count: missingAfter } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("county_fips", null);
  console.log(`\nMissing county_fips after: ${missingAfter?.toLocaleString()}`);
  console.log("Done.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
