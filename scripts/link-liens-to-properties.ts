#!/usr/bin/env tsx
/**
 * Link orphan lien records to properties by matching:
 * 1. Source URL -> county -> county_id
 * 2. borrower_name fuzzy match to owner_name within that county
 *
 * Only processes records where property_id IS NULL.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Map source_url patterns to county info
const SOURCE_MAP: Record<string, { county: string; state: string }> = {
  "ava.fidlar.com/ARSaline": { county: "Saline", state: "AR" },
  "ava.fidlar.com/IABlackHawk": { county: "Black Hawk", state: "IA" },
  "ava.fidlar.com/IABoone": { county: "Boone", state: "IA" },
  "ava.fidlar.com/IACalhoun": { county: "Calhoun", state: "IA" },
  "ava.fidlar.com/IAClayton": { county: "Clayton", state: "IA" },
  "ava.fidlar.com/IAJasper": { county: "Jasper", state: "IA" },
  "ava.fidlar.com/IALinn": { county: "Linn", state: "IA" },
  "ava.fidlar.com/IAScott": { county: "Scott", state: "IA" },
  "ava.fidlar.com/MIAntrim": { county: "Antrim", state: "MI" },
  "ava.fidlar.com/MIOakland": { county: "Oakland", state: "MI" },
  "ava.fidlar.com/NHBelknap": { county: "Belknap", state: "NH" },
  "ava.fidlar.com/NHCarroll": { county: "Carroll", state: "NH" },
  "ava.fidlar.com/NHCheshire": { county: "Cheshire", state: "NH" },
  "ava.fidlar.com/NHGrafton": { county: "Grafton", state: "NH" },
  "ava.fidlar.com/NHHillsborough": { county: "Hillsborough", state: "NH" },
  "ava.fidlar.com/NHRockingham": { county: "Rockingham", state: "NH" },
  "ava.fidlar.com/NHStrafford": { county: "Strafford", state: "NH" },
  "ava.fidlar.com/NHSullivan": { county: "Sullivan", state: "NH" },
  "ava.fidlar.com/OHFairfield": { county: "Fairfield", state: "OH" },
  "ava.fidlar.com/OHGeauga": { county: "Geauga", state: "OH" },
  "ava.fidlar.com/OHPaulding": { county: "Paulding", state: "OH" },
  "ava.fidlar.com/OHWyandot": { county: "Wyandot", state: "OH" },
  "ava.fidlar.com/TXAustin": { county: "Austin", state: "TX" },
  "ava.fidlar.com/TXFannin": { county: "Fannin", state: "TX" },
  "ava.fidlar.com/TXGalveston": { county: "Galveston", state: "TX" },
  "ava.fidlar.com/TXKerr": { county: "Kerr", state: "TX" },
  "ava.fidlar.com/TXPanola": { county: "Panola", state: "TX" },
  "ava.fidlar.com/WAYakima": { county: "Yakima", state: "WA" },
  "levyclerk.com": { county: "Levy", state: "FL" },
  "martinclerk.com": { county: "Martin", state: "FL" },
  "clerkofcourts.co.walton.fl": { county: "Walton", state: "FL" },
  "citrusclerk.org": { county: "Citrus", state: "FL" },
  // OH PublicSearch.us counties
  "butler.oh.publicsearch": { county: "Butler", state: "OH" },
  "franklin.oh.publicsearch": { county: "Franklin", state: "OH" },
  "cuyahoga.oh.publicsearch": { county: "Cuyahoga", state: "OH" },
  "warren.oh.publicsearch": { county: "Warren", state: "OH" },
  "lake.oh.publicsearch": { county: "Lake", state: "OH" },
  "mahoning.oh.publicsearch": { county: "Mahoning", state: "OH" },
  "summit.oh.publicsearch": { county: "Summit", state: "OH" },
  "lorain.oh.publicsearch": { county: "Lorain", state: "OH" },
  "stark.oh.publicsearch": { county: "Stark", state: "OH" },
  // Hamilton County OH — Acclaim recorder
  "acclaim-web.hamiltoncountyohio.gov": { county: "Hamilton", state: "OH" },
  // TX PublicSearch
  "publicsearch.us": { county: "Dallas", state: "TX" }, // Default, but also Tarrant/Denton
};

function findCountyFromUrl(url: string): { county: string; state: string } | null {
  for (const [pattern, info] of Object.entries(SOURCE_MAP)) {
    if (url.includes(pattern)) return info;
  }
  return null;
}

function normalizeOwnerName(name: string): string {
  return name
    .toUpperCase()
    .replace(/[,.'"\-]/g, " ")
    .replace(/\s+(JR|SR|II|III|IV|LLC|INC|CORP|LTD|LP|CO)\s*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

async function main() {
  console.log("MXRE — Link Orphan Liens to Properties\n");

  // Get all unlinked mortgage records
  let offset = 0;
  let totalLinked = 0;
  let totalUnlinked = 0;
  let totalProcessed = 0;
  const BATCH = 500;

  while (true) {
    const { data: records, error } = await db.from("mortgage_records")
      .select("id, borrower_name, source_url, document_type")
      .is("property_id", null)
      .not("borrower_name", "is", null)
      .neq("borrower_name", "")
      .range(offset, offset + BATCH - 1)
      .order("id");

    if (error) {
      console.error("Query error:", error.message);
      break;
    }
    if (!records || records.length === 0) break;

    for (const rec of records) {
      totalProcessed++;
      const countyInfo = findCountyFromUrl(rec.source_url || "");
      if (!countyInfo) { totalUnlinked++; continue; }

      // Get county_id
      const { data: county } = await db.from("counties")
        .select("id")
        .eq("county_name", countyInfo.county)
        .eq("state_code", countyInfo.state)
        .single();
      if (!county) { totalUnlinked++; continue; }

      // Normalize borrower name and search for matching property owner
      const borrower = normalizeOwnerName(rec.borrower_name);
      if (borrower.length < 3) { totalUnlinked++; continue; }

      // Extract last name (first word before comma or space)
      const lastName = borrower.split(/[,\s]/)[0];
      if (lastName.length < 2) { totalUnlinked++; continue; }

      // Search properties by owner last name in this county
      const { data: matches } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", county.id)
        .ilike("owner_name", `%${lastName}%`)
        .limit(10);

      if (!matches || matches.length === 0) { totalUnlinked++; continue; }

      // Find best match — exact or close
      let bestMatch: { id: number; score: number } | null = null;
      for (const prop of matches) {
        const propOwner = normalizeOwnerName(prop.owner_name || "");
        if (propOwner === borrower) {
          bestMatch = { id: prop.id, score: 100 };
          break;
        }
        // Check if borrower contains property owner or vice versa
        if (propOwner.includes(borrower) || borrower.includes(propOwner)) {
          const score = Math.min(propOwner.length, borrower.length) / Math.max(propOwner.length, borrower.length) * 80;
          if (!bestMatch || score > bestMatch.score) {
            bestMatch = { id: prop.id, score };
          }
        }
      }

      if (bestMatch && bestMatch.score >= 50) {
        await db.from("mortgage_records").update({ property_id: bestMatch.id }).eq("id", rec.id);
        totalLinked++;
      } else {
        totalUnlinked++;
      }
    }

    offset += records.length;
    process.stdout.write(`\r  Processed: ${totalProcessed.toLocaleString()} | Linked: ${totalLinked.toLocaleString()} | Unlinked: ${totalUnlinked.toLocaleString()}`);
  }

  console.log(`\n\nDone: ${totalLinked.toLocaleString()} linked | ${totalUnlinked.toLocaleString()} unlinked`);

  const { count: linkedTotal } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  console.log(`Total linked mortgage records: ${linkedTotal?.toLocaleString()}`);
}

main().catch(console.error);
