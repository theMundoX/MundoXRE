#!/usr/bin/env tsx
/**
 * Link mortgage_records to properties — only for counties that HAVE properties.
 * Matches by borrower name to owner name within the same county.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Only try counties we actually have property data for
const LINKABLE_SOURCES: Record<string, { county_name: string; state_code: string }> = {
  // Texas (we have Dallas, Tarrant, Comanche, Oklahoma)
  "https://ava.fidlar.com/TXAustin/AvaWeb/": { county_name: "Austin", state_code: "TX" },
  "https://ava.fidlar.com/TXFannin/AvaWeb/": { county_name: "Fannin", state_code: "TX" },
  "https://ava.fidlar.com/TXGalveston/AvaWeb/": { county_name: "Galveston", state_code: "TX" },
  "https://ava.fidlar.com/TXKerr/AvaWeb/": { county_name: "Kerr", state_code: "TX" },
  // Florida (we have Levy, Martin, Walton + more via NAL)
  "https://online.levyclerk.com": { county_name: "Levy", state_code: "FL" },
  "http://or.martinclerk.com": { county_name: "Martin", state_code: "FL" },
  "https://orsearch.clerkofcourts.co.walton.fl.us": { county_name: "Walton", state_code: "FL" },
  "https://search.citrusclerk.org": { county_name: "Citrus", state_code: "FL" },
};

async function main() {
  console.log("MXRE — Link Mortgage Records to Properties (v2 — TX/FL only)\n");

  // Get unlinked records from linkable sources
  let linked = 0, notFound = 0, total = 0;

  for (const [sourceUrl, info] of Object.entries(LINKABLE_SOURCES)) {
    // Get county_id
    const { data: county } = await db.from("counties")
      .select("id")
      .eq("county_name", info.county_name)
      .eq("state_code", info.state_code)
      .single();

    if (!county) {
      console.log(`  Skip ${info.county_name}, ${info.state_code} — no county in DB`);
      continue;
    }

    console.log(`\n  Processing ${info.county_name}, ${info.state_code} (county_id=${county.id})...`);

    // Get unlinked records for this source
    const { data: records } = await db.from("mortgage_records")
      .select("id, borrower_name, document_type")
      .is("property_id", null)
      .like("source_url", `${sourceUrl}%`)
      .not("borrower_name", "is", null)
      .neq("borrower_name", "")
      .limit(5000);

    if (!records || records.length === 0) {
      console.log(`    No unlinked records`);
      continue;
    }

    console.log(`    Found ${records.length} unlinked records`);
    let countyLinked = 0;

    for (const rec of records) {
      total++;
      const borrowerName = rec.borrower_name.split(";")[0].trim();
      if (!borrowerName || borrowerName.length < 3) { notFound++; continue; }

      // Split name into parts, take the longest parts for matching
      const nameParts = borrowerName.split(/\s+/).filter((p: string) => p.length > 2);
      if (nameParts.length === 0) { notFound++; continue; }

      // Search by first part (usually last name) — case insensitive
      const { data: properties } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", county.id)
        .ilike("owner_name", `%${nameParts[0]}%`)
        .limit(20);

      if (!properties || properties.length === 0) { notFound++; continue; }

      // Score matches — need at least 2 name parts matching
      let bestMatch: { id: number; score: number } | null = null;
      for (const prop of properties) {
        const ownerUpper = (prop.owner_name || "").toUpperCase();
        let score = 0;
        for (const part of nameParts) {
          if (ownerUpper.includes(part.toUpperCase())) score++;
        }
        if (score >= Math.min(2, nameParts.length) && (!bestMatch || score > bestMatch.score)) {
          bestMatch = { id: prop.id, score };
        }
      }

      if (bestMatch) {
        await db.from("mortgage_records").update({ property_id: bestMatch.id }).eq("id", rec.id);
        linked++;
        countyLinked++;
      } else {
        notFound++;
      }
    }

    console.log(`    Linked: ${countyLinked} / ${records.length}`);
  }

  const { count: totalLinked } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .not("property_id", "is", null);

  console.log(`\n═══════════════════════════════════`);
  console.log(`  Processed: ${total} | Newly linked: ${linked} | Not found: ${notFound}`);
  console.log(`  Total linked in DB: ${totalLinked}`);
}

main().catch(console.error);
