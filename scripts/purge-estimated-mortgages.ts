#!/usr/bin/env tsx
/**
 * Purge ALL estimated/fabricated mortgage records from the database.
 * Only real recorded mortgage data should exist.
 *
 * Estimated records can be identified by:
 * - Having no document_number and no book_page (real records always have one)
 * - Having source_url that doesn't point to a county recorder portal
 * - Having interest_rate/term_months populated without a document_number
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Purge Estimated Mortgage Records");
  console.log("═══════════════════════════════════════════\n");

  // Count total mortgage records
  const { count: totalCount } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true });
  console.log(`Total mortgage_records: ${totalCount}`);

  // Count records that look estimated (no document_number AND no book_page)
  const { count: estimatedCount } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .is("document_number", null)
    .is("book_page", null);
  console.log(`Records with no doc_number AND no book_page (estimated): ${estimatedCount}`);

  // Count records with document_number (likely real)
  const { count: realCount } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .not("document_number", "is", null);
  console.log(`Records with document_number (likely real): ${realCount}`);

  if (!estimatedCount || estimatedCount === 0) {
    console.log("\nNo estimated records found. Nothing to purge.");
    return;
  }

  console.log(`\nPurging ${estimatedCount} estimated mortgage records...`);

  // Delete in batches to avoid timeout
  const BATCH_SIZE = 5000;
  let deleted = 0;

  while (deleted < estimatedCount) {
    // Get a batch of IDs to delete
    const { data: batch } = await db
      .from("mortgage_records")
      .select("id")
      .is("document_number", null)
      .is("book_page", null)
      .limit(BATCH_SIZE);

    if (!batch || batch.length === 0) break;

    const ids = batch.map(r => r.id);
    const { error } = await db
      .from("mortgage_records")
      .delete()
      .in("id", ids);

    if (error) {
      console.error(`Delete error: ${error.message}`);
      break;
    }

    deleted += ids.length;
    process.stdout.write(`\r  Deleted: ${deleted}/${estimatedCount}`);
  }

  console.log(`\n\nPurge complete. Deleted ${deleted} estimated records.`);

  // Final count
  const { count: remaining } = await db
    .from("mortgage_records")
    .select("*", { count: "exact", head: true });
  console.log(`Remaining mortgage_records: ${remaining}`);
}

main().catch(console.error);
