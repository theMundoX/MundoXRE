#!/usr/bin/env tsx
/**
 * Link unlinked mortgage_records to properties by matching:
 * 1. borrower_name contains property owner_name (or vice versa)
 * 2. Same county (inferred from source_url)
 *
 * For Fidlar AVA records, uses the legal description to match subdivision/lot.
 * For LandmarkWeb records, matches by borrower name to owner name.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const BATCH_SIZE = 200;

// Map source URLs to county info
const SOURCE_TO_COUNTY: Record<string, { county_name: string; state_code: string }> = {
  // Fidlar AVA counties
  "https://ava.fidlar.com/OHFairfield/AvaWeb/": { county_name: "Fairfield", state_code: "OH" },
  "https://ava.fidlar.com/ARSaline/AvaWeb/": { county_name: "Saline", state_code: "AR" },
  "https://ava.fidlar.com/IABlackHawk/AvaWeb/": { county_name: "Black Hawk", state_code: "IA" },
  "https://ava.fidlar.com/IABoone/AvaWeb/": { county_name: "Boone", state_code: "IA" },
  "https://ava.fidlar.com/IACalhoun/AvaWeb/": { county_name: "Calhoun", state_code: "IA" },
  "https://ava.fidlar.com/IAClayton/AvaWeb/": { county_name: "Clayton", state_code: "IA" },
  "https://ava.fidlar.com/IAJasper/AvaWeb/": { county_name: "Jasper", state_code: "IA" },
  "https://ava.fidlar.com/IALinn/AvaWeb/": { county_name: "Linn", state_code: "IA" },
  "https://ava.fidlar.com/IAScott/AvaWeb/": { county_name: "Scott", state_code: "IA" },
  "https://ava.fidlar.com/MIAntrim/AvaWeb/": { county_name: "Antrim", state_code: "MI" },
  "https://ava.fidlar.com/MIOakland/AvaWeb/": { county_name: "Oakland", state_code: "MI" },
  "https://ava.fidlar.com/NHBelknap/AvaWeb/": { county_name: "Belknap", state_code: "NH" },
  "https://ava.fidlar.com/NHCarroll/AvaWeb/": { county_name: "Carroll", state_code: "NH" },
  "https://ava.fidlar.com/NHCheshire/AvaWeb/": { county_name: "Cheshire", state_code: "NH" },
  "https://ava.fidlar.com/NHGrafton/AvaWeb/": { county_name: "Grafton", state_code: "NH" },
  "https://ava.fidlar.com/NHHillsborough/AvaWeb/": { county_name: "Hillsborough", state_code: "NH" },
  "https://ava.fidlar.com/NHRockingham/AvaWeb/": { county_name: "Rockingham", state_code: "NH" },
  "https://ava.fidlar.com/NHStrafford/AvaWeb/": { county_name: "Strafford", state_code: "NH" },
  "https://ava.fidlar.com/NHSullivan/AvaWeb/": { county_name: "Sullivan", state_code: "NH" },
  "https://ava.fidlar.com/OHGeauga/AvaWeb/": { county_name: "Geauga", state_code: "OH" },
  "https://ava.fidlar.com/OHPaulding/AvaWeb/": { county_name: "Paulding", state_code: "OH" },
  "https://ava.fidlar.com/OHWyandot/AvaWeb/": { county_name: "Wyandot", state_code: "OH" },
  "https://ava.fidlar.com/TXAustin/AvaWeb/": { county_name: "Austin", state_code: "TX" },
  "https://ava.fidlar.com/TXFannin/AvaWeb/": { county_name: "Fannin", state_code: "TX" },
  "https://ava.fidlar.com/TXGalveston/AvaWeb/": { county_name: "Galveston", state_code: "TX" },
  "https://ava.fidlar.com/TXKerr/AvaWeb/": { county_name: "Kerr", state_code: "TX" },
  "https://ava.fidlar.com/TXPanola/AvaWeb/": { county_name: "Panola", state_code: "TX" },
  "https://ava.fidlar.com/WAYakima/AvaWeb/": { county_name: "Yakima", state_code: "WA" },
  // Florida LandmarkWeb
  "https://officialrecords.levyclerk.com/LandmarkWeb": { county_name: "Levy", state_code: "FL" },
  "https://officialrecords.martinclerk.com/LandmarkWeb": { county_name: "Martin", state_code: "FL" },
  "https://orsearch.clerkofcourts.co.walton.fl.us/LandmarkWeb": { county_name: "Walton", state_code: "FL" },
  "https://citrusclerk.org/LandmarkWeb": { county_name: "Citrus", state_code: "FL" },
  // PublicSearch TX
  "https://txcountyrecords.com": { county_name: "Dallas", state_code: "TX" },
};

async function main() {
  console.log("MXRE — Link Mortgage Records to Properties\n");

  // Get unlinked mortgage records
  let linked = 0, notFound = 0, total = 0;
  let lastId = 0;

  while (true) {
    const { data: records } = await db.from("mortgage_records")
      .select("id, borrower_name, source_url, document_type")
      .is("property_id", null)
      .not("borrower_name", "is", null)
      .neq("borrower_name", "")
      .gt("id", lastId)
      .order("id")
      .limit(BATCH_SIZE);

    if (!records || records.length === 0) break;
    total += records.length;
    lastId = records[records.length - 1].id;

    for (const rec of records) {
      // Get county from source URL
      const countyInfo = SOURCE_TO_COUNTY[rec.source_url];
      if (!countyInfo) { notFound++; continue; }

      // Get county_id
      const { data: county } = await db.from("counties")
        .select("id")
        .eq("county_name", countyInfo.county_name)
        .eq("state_code", countyInfo.state_code)
        .single();

      if (!county) { notFound++; continue; }

      // Extract first borrower name (before semicolons)
      const borrowerName = rec.borrower_name.split(";")[0].trim();
      if (!borrowerName || borrowerName.length < 3) { notFound++; continue; }

      // Search for matching property by owner name in the same county
      // Use ILIKE for case-insensitive partial match
      const nameParts = borrowerName.split(/\s+/).filter((p: string) => p.length > 2);
      if (nameParts.length === 0) { notFound++; continue; }

      // Build search: last name first (most recorder data is LASTNAME FIRSTNAME)
      const searchName = nameParts[0]; // Usually the last name

      const { data: properties } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", county.id)
        .ilike("owner_name", `%${searchName}%`)
        .limit(10);

      if (!properties || properties.length === 0) { notFound++; continue; }

      // Find best match - check if more name parts match
      let bestMatch: { id: number; score: number } | null = null;
      for (const prop of properties) {
        const ownerUpper = (prop.owner_name || "").toUpperCase();
        let score = 0;
        for (const part of nameParts) {
          if (ownerUpper.includes(part)) score++;
        }
        if (score >= 2 && (!bestMatch || score > bestMatch.score)) {
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

    if (total % 1000 === 0) {
      console.log(`  Processed: ${total.toLocaleString()} | Linked: ${linked.toLocaleString()} | Not found: ${notFound.toLocaleString()}`);
    }
  }

  const { count: totalLinked } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  console.log(`\n  Done: ${total.toLocaleString()} processed | ${linked.toLocaleString()} newly linked`);
  console.log(`  Total linked in DB: ${totalLinked?.toLocaleString()}`);
}

main().catch(console.error);
