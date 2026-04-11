#!/usr/bin/env tsx
/**
 * Ingest new FL LandmarkWeb counties — try each one, skip if blocked after 30s.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { LandmarkWebAdapter, type LandmarkCountyConfig } from "../src/discovery/adapters/landmark-web.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Try these in order — smaller/less protected first
const NEW_COUNTIES: LandmarkCountyConfig[] = [
  { county_name: "Hernando", state: "FL", base_url: "https://or.hernandoclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Leon", state: "FL", base_url: "https://www.leonclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Manatee", state: "FL", base_url: "https://records.manateeclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Lee", state: "FL", base_url: "https://or.leeclerk.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Pasco", state: "FL", base_url: "https://or.pascocounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Duval", state: "FL", base_url: "https://officialrecords.duvalclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Osceola", state: "FL", base_url: "https://or.osceolacounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Brevard", state: "FL", base_url: "https://officialrecords.brevardclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Broward", state: "FL", base_url: "https://or.browardclerk.org", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Clay", state: "FL", base_url: "https://landmark.clayclerk.com", path_prefix: "/LandmarkWeb", county_id: 0 },
  { county_name: "Palm Beach", state: "FL", base_url: "https://or.palmbeachcounty.org", path_prefix: "/LandmarkWeb", county_id: 0 },
];

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType.toUpperCase();
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT"))
    return { document_type: "mortgage", loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase" };
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

async function main() {
  console.log("MXRE — New FL Counties LandmarkWeb Ingestion\n");

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 30); // 30 days for speed
  const start = startDate.toISOString().split("T")[0];
  const end = endDate.toISOString().split("T")[0];

  let grandTotal = 0, grandAmounts = 0;

  for (const config of NEW_COUNTIES) {
    console.log(`\n━━━ ${config.county_name} County, FL ━━━`);

    const adapter = new LandmarkWebAdapter();

    try {
      await adapter.init();
    } catch (err: any) {
      console.log(`  Failed to init browser: ${err.message.slice(0, 60)}`);
      continue;
    }

    let inserted = 0, withAmount = 0;
    const timeout = setTimeout(async () => {
      console.log(`\n  TIMEOUT — skipping ${config.county_name} (took too long)`);
      await adapter.close();
    }, 900_000); // 15 min timeout per county

    try {
      const batch: Array<Record<string, unknown>> = [];

      for await (const doc of adapter.fetchDocuments(config, start, end, (p) => {
        process.stdout.write(`\r  ${p.current_date} | Found: ${p.total_found} | Inserted: ${inserted}`);
      })) {
        const classified = classifyDocType(doc.document_type);
        const record: Record<string, unknown> = {
          property_id: null,
          document_type: classified.document_type,
          recording_date: doc.recording_date,
          loan_amount: doc.consideration ? Math.round(doc.consideration) : null,
          original_amount: doc.consideration ? Math.round(doc.consideration) : null,
          lender_name: doc.grantee,
          borrower_name: doc.grantor,
          document_number: doc.instrument_number,
          book_page: doc.book_page,
          source_url: doc.source_url,
          loan_type: classified.loan_type,
          deed_type: classified.deed_type,
        };

        if (doc.consideration && doc.consideration > 0) {
          withAmount++;
          if (classified.document_type === "mortgage" || classified.document_type === "lien") {
            const fields = computeMortgageFields({ originalAmount: Math.round(doc.consideration), recordingDate: doc.recording_date });
            record.interest_rate = fields.interest_rate;
            record.term_months = fields.term_months;
            record.estimated_monthly_payment = fields.estimated_monthly_payment;
            record.estimated_current_balance = fields.estimated_current_balance;
            record.balance_as_of = fields.balance_as_of;
            record.maturity_date = fields.maturity_date;
          }
        }

        batch.push(record);
        if (batch.length >= 50) {
          const { error } = await db.from("mortgage_records").insert(batch);
          if (error) console.error(`\n  Insert: ${error.message.slice(0, 60)}`);
          else inserted += batch.length;
          batch.length = 0;
        }
      }

      if (batch.length > 0) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (!error) inserted += batch.length;
      }
    } catch (err: any) {
      console.log(`\n  Error: ${err.message.slice(0, 80)}`);
    }

    clearTimeout(timeout);
    await adapter.close().catch(() => {});

    console.log(`\n  Result: ${inserted} inserted | ${withAmount} with amounts`);
    grandTotal += inserted;
    grandAmounts += withAmount;
  }

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n═══════════════════════════════════`);
  console.log(`  Grand total: ${grandTotal} | With amounts: ${grandAmounts}`);
  console.log(`  DB total: ${count}`);
}

main().catch(console.error);
