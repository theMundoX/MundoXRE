#!/usr/bin/env tsx
/**
 * Link Fairfield County OH mortgage records to the newly ingested properties.
 * Matches by borrower name → owner name (OWN1 field).
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

async function main() {
  console.log("MXRE — Link Fairfield OH Mortgage Records to Properties\n");

  // Get Fairfield OH county ID
  const { data: county } = await db.from("counties")
    .select("id").eq("county_name", "Fairfield").eq("state_code", "OH").single();
  if (!county) { console.log("No county found"); return; }
  console.log(`  County ID: ${county.id}`);

  // Get unlinked Fairfield OH mortgage records
  const { data: records } = await db.from("mortgage_records")
    .select("id, borrower_name, lender_name, document_type")
    .is("property_id", null)
    .eq("source_url", "https://ava.fidlar.com/OHFairfield/AvaWeb/")
    .not("borrower_name", "is", null)
    .neq("borrower_name", "")
    .limit(5000);

  if (!records || records.length === 0) {
    console.log("  No unlinked records found");
    return;
  }
  console.log(`  Found ${records.length} unlinked records`);

  let linked = 0, notFound = 0;

  for (const rec of records) {
    // Get first borrower name
    const borrower = rec.borrower_name.split(";")[0].trim();
    if (!borrower || borrower.length < 3) { notFound++; continue; }

    // Split into parts — Fidlar format is "LASTNAME FIRSTNAME"
    const parts = borrower.split(/\s+/).filter((p: string) => p.length > 1);
    if (parts.length === 0) { notFound++; continue; }

    // Search by last name (first word) in owner_name (OWN1 field)
    const lastName = parts[0];
    const { data: properties } = await db.from("properties")
      .select("id, owner_name")
      .eq("county_id", county.id)
      .ilike("owner_name", `${lastName}%`) // Fairfield OH format: "PONTIUS GREGORY L"
      .limit(20);

    if (!properties || properties.length === 0) { notFound++; continue; }

    // Score matches
    let bestMatch: { id: number; score: number } | null = null;
    for (const prop of properties) {
      const ownerUpper = (prop.owner_name || "").toUpperCase();
      let score = 0;
      for (const part of parts) {
        if (ownerUpper.includes(part)) score++;
      }
      // Need at least 2 matching parts (last name + first name)
      if (score >= Math.min(2, parts.length) && (!bestMatch || score > bestMatch.score)) {
        bestMatch = { id: prop.id, score };
      }
    }

    if (bestMatch) {
      await db.from("mortgage_records").update({ property_id: bestMatch.id }).eq("id", rec.id);
      linked++;
    } else {
      notFound++;
    }
  }

  console.log(`\n  Linked: ${linked} / ${records.length}`);
  console.log(`  Not found: ${notFound}`);

  const { count: totalLinked } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .not("property_id", "is", null);
  console.log(`  Total linked in DB: ${totalLinked}`);
}

main().catch(console.error);
