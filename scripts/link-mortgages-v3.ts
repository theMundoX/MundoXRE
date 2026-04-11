#!/usr/bin/env tsx
/**
 * Link mortgage_records to properties v3 — handles swapped borrower/lender fields.
 * Fidlar AVA stores grantor in lender_name and grantee in borrower_name,
 * which means for mortgages: the PERSON is in lender_name, the BANK is in borrower_name.
 * This linker tries BOTH fields against property owner_name.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Map Fidlar source_url patterns to counties
const FIDLAR_COUNTIES: Record<string, { county_name: string; state_code: string }> = {
  "IABlackHawk": { county_name: "Black Hawk", state_code: "IA" },
  "IABoone": { county_name: "Boone", state_code: "IA" },
  "IACalhoun": { county_name: "Calhoun", state_code: "IA" },
  "IAClayton": { county_name: "Clayton", state_code: "IA" },
  "IAJasper": { county_name: "Jasper", state_code: "IA" },
  "IALinn": { county_name: "Linn", state_code: "IA" },
  "IAScott": { county_name: "Scott", state_code: "IA" },
  "ARSaline": { county_name: "Saline", state_code: "AR" },
  "MIAntrim": { county_name: "Antrim", state_code: "MI" },
  "NHBelknap": { county_name: "Belknap", state_code: "NH" },
  "NHCarroll": { county_name: "Carroll", state_code: "NH" },
  "NHCheshire": { county_name: "Cheshire", state_code: "NH" },
  "NHGrafton": { county_name: "Grafton", state_code: "NH" },
  "NHHillsborough": { county_name: "Hillsborough", state_code: "NH" },
  "NHRockingham": { county_name: "Rockingham", state_code: "NH" },
  "NHStrafford": { county_name: "Strafford", state_code: "NH" },
  "NHSullivan": { county_name: "Sullivan", state_code: "NH" },
  "OHFairfield": { county_name: "Fairfield", state_code: "OH" },
  "OHGeauga": { county_name: "Geauga", state_code: "OH" },
  "OHPaulding": { county_name: "Paulding", state_code: "OH" },
  "OHWyandot": { county_name: "Wyandot", state_code: "OH" },
  // PublicSearch.us counties (source_url = https://{subdomain}.oh.publicsearch.us/)
  "butler.oh.publicsearch": { county_name: "Butler", state_code: "OH" },
  "franklin.oh.publicsearch": { county_name: "Franklin", state_code: "OH" },
  "cuyahoga.oh.publicsearch": { county_name: "Cuyahoga", state_code: "OH" },
  "warren.oh.publicsearch": { county_name: "Warren", state_code: "OH" },
  "lake.oh.publicsearch": { county_name: "Lake", state_code: "OH" },
  "mahoning.oh.publicsearch": { county_name: "Mahoning", state_code: "OH" },
  "summit.oh.publicsearch": { county_name: "Summit", state_code: "OH" },
  "lorain.oh.publicsearch": { county_name: "Lorain", state_code: "OH" },
  "stark.oh.publicsearch": { county_name: "Stark", state_code: "OH" },
  // Hamilton County OH — Acclaim recorder
  "acclaim-web.hamiltoncountyohio.gov": { county_name: "Hamilton", state_code: "OH" },
  "TXAustin": { county_name: "Austin", state_code: "TX" },
  "TXFannin": { county_name: "Fannin", state_code: "TX" },
  "TXGalveston": { county_name: "Galveston", state_code: "TX" },
  "TXKerr": { county_name: "Kerr", state_code: "TX" },
  "WAYakima": { county_name: "Yakima", state_code: "WA" },
};

function isLikelyCompany(name: string): boolean {
  const upper = name.toUpperCase();
  return /\b(LLC|INC|CORP|BANK|MORTGAGE|CREDIT UNION|TRUST|NATIONAL|FEDERAL|LENDING|LOAN|FINANCIAL|SERVIC|ASSOC|INSURANCE|SAVINGS)\b/.test(upper);
}

function extractPersonName(name: string): string[] {
  // Clean and split into parts, filter short/common words
  const cleaned = name.replace(/[,;]/g, " ").replace(/\s+/g, " ").trim().toUpperCase();
  return cleaned.split(" ").filter(p => p.length > 2 && !["THE", "AND", "FOR", "JR", "SR", "III", "II"].includes(p));
}

async function main() {
  console.log("MXRE — Link Mortgage Records to Properties v3\n");
  console.log("Strategy: try BOTH borrower_name and lender_name (Fidlar swaps them)\n");

  let totalLinked = 0, totalProcessed = 0, totalSkipped = 0;

  for (const [sourceKey, info] of Object.entries(FIDLAR_COUNTIES)) {
    const { data: county } = await db.from("counties")
      .select("id")
      .eq("county_name", info.county_name)
      .eq("state_code", info.state_code)
      .single();

    if (!county) continue;

    // Count properties for this county
    const { count: propCount } = await db.from("properties")
      .select("*", { count: "exact", head: true })
      .eq("county_id", county.id);

    if (!propCount || propCount < 100) {
      console.log(`  Skip ${info.county_name} ${info.state_code} — only ${propCount} properties`);
      continue;
    }

    // Get unlinked records — try both name fields
    const { data: records } = await db.from("mortgage_records")
      .select("id, borrower_name, lender_name, document_type")
      .is("property_id", null)
      .ilike("source_url", `%${sourceKey}%`)
      .limit(10000);

    if (!records || records.length === 0) continue;

    console.log(`\n━━━ ${info.county_name}, ${info.state_code} (${propCount} props, ${records.length} unlinked) ━━━`);
    let countyLinked = 0;

    for (const rec of records) {
      totalProcessed++;

      // Try to find a person name from either field
      const borrower = (rec.borrower_name || "").trim();
      const lender = (rec.lender_name || "").trim();

      // Determine which field has the actual person (not company)
      let personName = "";
      if (lender && !isLikelyCompany(lender) && lender.length > 2) {
        personName = lender; // Fidlar typically puts person in lender_name
      } else if (borrower && !isLikelyCompany(borrower) && borrower.length > 2) {
        personName = borrower;
      }

      if (!personName) { totalSkipped++; continue; }

      const nameParts = extractPersonName(personName);
      if (nameParts.length === 0) { totalSkipped++; continue; }

      // Search by first name part (usually last name)
      const { data: properties } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", county.id)
        .ilike("owner_name", `%${nameParts[0]}%`)
        .limit(20);

      if (!properties || properties.length === 0) { totalSkipped++; continue; }

      // Score matches
      let bestMatch: { id: number; score: number } | null = null;
      for (const prop of properties) {
        const ownerUpper = (prop.owner_name || "").toUpperCase();
        let score = 0;
        for (const part of nameParts) {
          if (ownerUpper.includes(part)) score++;
        }
        const minRequired = Math.min(2, nameParts.length);
        if (score >= minRequired && (!bestMatch || score > bestMatch.score)) {
          bestMatch = { id: prop.id, score };
        }
      }

      if (bestMatch) {
        await db.from("mortgage_records").update({ property_id: bestMatch.id }).eq("id", rec.id);
        totalLinked++;
        countyLinked++;
      } else {
        totalSkipped++;
      }

      if (totalProcessed % 500 === 0) {
        process.stdout.write(`\r  Processed: ${totalProcessed} | Linked: ${totalLinked} | County: ${countyLinked}`);
      }
    }

    console.log(`\n  ${info.county_name}: linked ${countyLinked} / ${records.length}`);
  }

  // Refresh MVs
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_KEY!;
  for (const v of ["county_lien_counts", "county_stats_mv"]) {
    await fetch(`${url}/pg/query`, {
      method: "POST",
      headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW ${v}` }),
    });
  }

  console.log(`\n═══════════════════════════════════`);
  console.log(`  Total processed: ${totalProcessed}`);
  console.log(`  Total linked: ${totalLinked}`);
  console.log(`  Total skipped: ${totalSkipped}`);
  console.log(`  MVs refreshed`);
}

main().catch(console.error);
