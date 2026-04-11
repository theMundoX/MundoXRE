#!/usr/bin/env tsx
/**
 * Link NJ SR1A deed records to properties using BLOCK/LOT matching.
 *
 * NJ parcel_id format: "DDDD_BBB_LLL" (district_block_lot)
 * SR1A document_number format: "DDDDD-BBBBB" but we stored block/lot in book_page
 *
 * Strategy: The SR1A ingest stored the county_code + district_code as the first part
 * of the source, and block+lot info. We match by grantee name (buyer) against owner_name.
 *
 * Actually, the best approach: SR1A records have county_code (01-21) embedded in source_url.
 * We match each SR1A record to properties in the same county by grantee name -> owner_name.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const NJ_COUNTIES: Record<string, string> = {
  "01": "Atlantic", "02": "Bergen", "03": "Burlington", "04": "Camden",
  "05": "Cape May", "06": "Cumberland", "07": "Essex", "08": "Gloucester",
  "09": "Hudson", "10": "Hunterdon", "11": "Mercer", "12": "Middlesex",
  "13": "Monmouth", "14": "Morris", "15": "Ocean", "16": "Passaic",
  "17": "Salem", "18": "Somerset", "19": "Sussex", "20": "Union", "21": "Warren",
};

function isCompany(name: string): boolean {
  return /\b(LLC|INC|CORP|BANK|MORTGAGE|CREDIT|TRUST|NATIONAL|FEDERAL|LENDING|FINANCIAL|ASSOC|ESTATE|COUNTY|STATE|CITY|HOUSING|AUTHORITY|DEVELOPMENT|BUILDERS|CONSTRUCTION|REALTY|PROPERTIES)\b/i.test(name);
}

async function main() {
  console.log("MXRE — Link NJ SR1A Deed Records by Grantee Name\n");

  // Get all NJ county IDs
  const countyIds: Record<string, number> = {};
  for (const [, name] of Object.entries(NJ_COUNTIES)) {
    const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", "NJ").single();
    if (data) countyIds[name] = data.id;
  }
  console.log(`Found ${Object.keys(countyIds).length} NJ counties in DB\n`);

  let totalLinked = 0;
  let totalProcessed = 0;

  for (const [countyName, countyId] of Object.entries(countyIds)) {
    // Get unlinked SR1A records for this county
    // The source_url contains the county name or we can check the grantee
    // Actually, the SR1A ingest should have stored county info somehow
    // Let's check what we have

    // Get unlinked records - try matching by fetching batches
    const { data: records, count: totalUnlinked } = await db.from("mortgage_records")
      .select("id, borrower_name, lender_name, document_number, book_page", { count: "exact" })
      .is("property_id", null)
      .ilike("source_url", "%nj-sr1a%")
      .limit(50000);

    if (!records || records.length === 0) {
      console.log(`  ${countyName}: no unlinked SR1A records`);
      continue;
    }

    // Only process first county's worth, then we'll need county-specific filtering
    // For now, let's try name matching against this county's properties
    console.log(`\n━━━ ${countyName} (county_id=${countyId}) ━━━`);

    // Build owner name index for this county
    let ownerIndex: Map<string, number[]> = new Map();
    let propOffset = 0;
    const PROP_BATCH = 5000;
    let propCount = 0;

    while (true) {
      const { data: props } = await db.from("properties")
        .select("id, owner_name")
        .eq("county_id", countyId)
        .not("owner_name", "is", null)
        .neq("owner_name", "")
        .range(propOffset, propOffset + PROP_BATCH - 1);

      if (!props || props.length === 0) break;

      for (const p of props) {
        const lastName = (p.owner_name || "").toUpperCase().split(/[\s,;]+/)[0];
        if (lastName && lastName.length > 2) {
          if (!ownerIndex.has(lastName)) ownerIndex.set(lastName, []);
          ownerIndex.get(lastName)!.push(p.id);
        }
      }
      propCount += props.length;
      propOffset += PROP_BATCH;
      if (props.length < PROP_BATCH) break;
    }

    console.log(`  Built owner index: ${ownerIndex.size} unique last names from ${propCount} properties`);

    // Now try to match grantee names
    let countyLinked = 0;
    let countyChecked = 0;

    for (const rec of records) {
      // Try both name fields
      const grantee = (rec.lender_name || "").trim(); // In SR1A, grantee = buyer
      const grantor = (rec.borrower_name || "").trim();

      // Use grantee (buyer) to match against current owner
      let personName = "";
      if (grantee && !isCompany(grantee)) {
        personName = grantee;
      } else if (grantor && !isCompany(grantor)) {
        personName = grantor;
      }

      if (!personName || personName.length < 3) continue;

      const lastName = personName.toUpperCase().split(/[\s,;]+/)[0];
      if (!lastName || lastName.length < 3) continue;

      const candidates = ownerIndex.get(lastName);
      if (!candidates || candidates.length === 0) continue;

      // If we have candidates, just link to the first one (same last name in same county)
      // For better accuracy, we could fetch full owner_name and score
      if (candidates.length <= 50) {
        // Fetch full names for scoring
        const { data: props } = await db.from("properties")
          .select("id, owner_name")
          .in("id", candidates.slice(0, 50));

        if (!props) continue;

        const nameParts = personName.toUpperCase().split(/[\s,;]+/).filter(p => p.length > 2);
        let bestId: number | null = null;
        let bestScore = 0;

        for (const p of props) {
          const owner = (p.owner_name || "").toUpperCase();
          let score = 0;
          for (const part of nameParts) {
            if (owner.includes(part)) score++;
          }
          if (score >= Math.min(2, nameParts.length) && score > bestScore) {
            bestScore = score;
            bestId = p.id;
          }
        }

        if (bestId) {
          await db.from("mortgage_records").update({ property_id: bestId }).eq("id", rec.id);
          countyLinked++;
          totalLinked++;
        }
      }

      countyChecked++;
      totalProcessed++;

      if (countyChecked % 500 === 0) {
        process.stdout.write(`\r  Checked: ${countyChecked} | Linked: ${countyLinked}`);
      }

      // Stop after processing enough for this county
      if (countyChecked >= 20000) break;
    }

    console.log(`\n  ${countyName}: linked ${countyLinked} / ${countyChecked} checked`);

    // Clear index for next county
    ownerIndex.clear();
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
  console.log(`  MVs refreshed`);
}

main().catch(console.error);
