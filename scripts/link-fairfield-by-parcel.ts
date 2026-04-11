#!/usr/bin/env tsx
/**
 * Fetch and link ALL recorder documents for Fairfield OH properties
 * by searching Fidlar AVA with each property's parcel_id (TaxId).
 * This gets us to 100% lien coverage.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const COUNTY_ID = 31; // Fairfield OH
const BATCH_SIZE = 5000;

function classifyDocType(rawType: string): { document_type: string; deed_type?: string } {
  const upper = rawType.toUpperCase();
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT"))
    return { document_type: "mortgage" };
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

async function main() {
  console.log("MXRE — Fairfield OH: Link ALL Properties by Parcel Search\n");

  const config = FIDLAR_AVA_COUNTIES.find(c => c.county_name === "Fairfield" && c.state === "OH");
  if (!config) { console.error("Fairfield OH config not found"); return; }

  const adapter = new FidlarAvaAdapter();
  await adapter.init();

  // Get all properties that DON'T already have mortgage records
  let offset = 0;
  let totalProps = 0;
  let totalDocs = 0;
  let totalLinked = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  while (true) {
    // Fetch properties without linked mortgages
    const { data: properties, error } = await db.from("properties")
      .select("id, parcel_id, owner_name, address")
      .eq("county_id", COUNTY_ID)
      .not("parcel_id", "is", null)
      .neq("parcel_id", "")
      .range(offset, offset + BATCH_SIZE - 1);

    if (error) { console.error("DB error:", error.message); break; }
    if (!properties || properties.length === 0) break;

    console.log(`\nBatch ${Math.floor(offset / BATCH_SIZE) + 1}: ${properties.length} properties (offset ${offset})`);

    for (const prop of properties) {
      totalProps++;

      // Check if this property already has mortgage records
      const { count: existingCount } = await db.from("mortgage_records")
        .select("*", { count: "exact", head: true })
        .eq("property_id", prop.id);

      if (existingCount && existingCount > 0) {
        totalSkipped++;
        if (totalProps % 1000 === 0) {
          process.stdout.write(`\r  Props: ${totalProps} | Docs: ${totalDocs} | Linked: ${totalLinked} | Skipped: ${totalSkipped} | Errors: ${totalErrors}`);
        }
        continue;
      }

      // Re-auth every 50 properties to prevent token expiry
      if (totalProps % 50 === 0 && totalProps > 0) {
        try { await adapter.refreshToken?.(); } catch { /* ignore */ }
      }

      // Search Fidlar by parcel_id
      try {
        let docCount = 0;
        for await (const doc of adapter.fetchByParcel(config, prop.parcel_id)) {
          const classification = classifyDocType(doc.documentType || "");
          const amount = doc.consideration ? parseFloat(String(doc.consideration)) : null;

          const record: Record<string, unknown> = {
            property_id: prop.id,
            document_type: classification.document_type,
            deed_type: classification.deed_type || null,
            recording_date: doc.recordingDate || null,
            loan_amount: amount && amount > 0 && amount < 2147483647 ? Math.round(amount) : null,
            original_amount: amount && amount > 0 && amount < 2147483647 ? Math.round(amount) : null,
            borrower_name: doc.grantor || null,
            lender_name: doc.grantee || null,
            document_number: doc.instrumentNumber || doc.documentId || null,
            book_page: doc.book && doc.page ? `${doc.book}-${doc.page}` : null,
            source_url: `https://ava.fidlar.com/OHFairfield/AvaWeb/`,
          };

          // Compute mortgage fields if we have amount and date
          if (classification.document_type === "mortgage" && amount && amount > 0) {
            const calc = computeMortgageFields(amount, doc.recordingDate || "");
            Object.assign(record, calc);
          }

          const { error: insertErr } = await db.from("mortgage_records").upsert(record, {
            onConflict: "document_number,source_url",
            ignoreDuplicates: true,
          });

          if (!insertErr) {
            docCount++;
            totalDocs++;
            totalLinked++;
          }
        }
      } catch (err: any) {
        totalErrors++;
        if (totalErrors <= 5) console.error(`\n  Error on ${prop.parcel_id}: ${err.message?.slice(0, 100)}`);
      }

      if (totalProps % 100 === 0) {
        process.stdout.write(`\r  Props: ${totalProps} | Docs: ${totalDocs} | Linked: ${totalLinked} | Skipped: ${totalSkipped} | Errors: ${totalErrors}`);
      }

      // Rate limit — don't hammer the server
      await new Promise(r => setTimeout(r, 300));
    }

    offset += BATCH_SIZE;
    if (properties.length < BATCH_SIZE) break;
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

  console.log(`\n\n═══════════════════════════════════`);
  console.log(`  Properties checked: ${totalProps}`);
  console.log(`  Already had records: ${totalSkipped}`);
  console.log(`  New docs found: ${totalDocs}`);
  console.log(`  Linked: ${totalLinked}`);
  console.log(`  Errors: ${totalErrors}`);
  console.log(`  MVs refreshed`);
}

main().catch(console.error);
