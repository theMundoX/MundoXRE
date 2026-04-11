#!/usr/bin/env tsx
/**
 * Deep Fidlar AVA ingestion — uses 1-day chunks to bypass the 1,500 result limit.
 * Only runs for large counties that had truncated results in the initial run.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Counties that exceeded 1,500 results per week in the initial run
const LARGE_COUNTIES = [
  { name: "Oakland", state: "MI", slug: "MIOakland" },
  { name: "Hillsborough", state: "NH", slug: "NHHillsborough" },
  { name: "Rockingham", state: "NH", slug: "NHRockingham" },
  { name: "Galveston", state: "TX", slug: "TXGalveston" },
  { name: "Linn", state: "IA", slug: "IALinn" },
  { name: "Scott", state: "IA", slug: "IAScott" },
  { name: "Saline", state: "AR", slug: "ARSaline" },
  { name: "Black Hawk", state: "IA", slug: "IABlackHawk" },
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

async function getToken(apiBase: string): Promise<string> {
  const resp = await fetch(apiBase + "token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
  });
  const data = await resp.json();
  return data.access_token;
}

async function searchDay(apiBase: string, token: string, date: string): Promise<any[]> {
  const resp = await fetch(apiBase + "breeze/Search", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
    body: JSON.stringify({
      FirstName: "", LastBusinessName: "", StartDate: date, EndDate: date,
      DocumentName: "", DocumentType: "", SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
      MunicipalityName: "", TractSection: "", TractTownship: "", TractRange: "",
      TractQuarter: "", TractQuarterQuarter: "", Book: "", Page: "",
      LotOfRecord: "", BlockOfRecord: "", AddressNumber: "", AddressDirection: "", AddressStreetName: "", TaxId: "",
    }),
  });
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.DocResults || [];
}

async function isDuplicate(docNumber: string, source: string): Promise<boolean> {
  if (!docNumber) return false;
  const { data } = await db.from("mortgage_records").select("id").eq("document_number", docNumber).eq("source_url", source).limit(1);
  return (data?.length || 0) > 0;
}

async function main() {
  console.log("MXRE — Deep Fidlar AVA Ingestion (1-day chunks)\n");

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 90); // 90 days

  let grandTotal = 0, grandAmounts = 0;

  for (const county of LARGE_COUNTIES) {
    const apiBase = `https://ava.fidlar.com/${county.slug}/ScrapRelay.WebService.Ava/`;
    const sourceUrl = `https://ava.fidlar.com/${county.slug}/AvaWeb/`;

    console.log(`\n━━━ ${county.name}, ${county.state} ━━━`);

    let token: string;
    try { token = await getToken(apiBase); } catch { console.log("  Failed to get token"); continue; }

    let inserted = 0, withAmount = 0, duplicates = 0;
    const current = new Date(startDate);

    while (current <= endDate) {
      const dateStr = current.toISOString().split("T")[0];

      // Skip weekends for speed (fewer recordings)
      const dow = current.getDay();
      if (dow === 0 || dow === 6) { current.setDate(current.getDate() + 1); continue; }

      try {
        const results = await searchDay(apiBase, token, dateStr);

        for (const doc of results) {
          const docNum = doc.DocumentName || "";
          if (docNum && await isDuplicate(docNum, sourceUrl)) { duplicates++; continue; }

          const parties = doc.Parties || [];
          const grantors = parties.filter((p: any) => p.PartyTypeId === 1).map((p: any) => p.Name).filter(Boolean);
          const grantees = parties.filter((p: any) => p.PartyTypeId === 2).map((p: any) => p.Name).filter(Boolean);
          if (grantors.length === 0 && doc.Party1) grantors.push(doc.Party1.trim());
          if (grantees.length === 0 && doc.Party2) grantees.push(doc.Party2.trim());

          const classified = classifyDocType((doc.DocumentType || "").toUpperCase().trim());
          const record: Record<string, unknown> = {
            property_id: null,
            document_type: classified.document_type,
            recording_date: dateStr,
            loan_amount: doc.ConsiderationAmount > 0 ? Math.round(doc.ConsiderationAmount) : null,
            original_amount: doc.ConsiderationAmount > 0 ? Math.round(doc.ConsiderationAmount) : null,
            lender_name: grantees.join("; ").toUpperCase().trim().slice(0, 500),
            borrower_name: grantors.join("; ").toUpperCase().trim().slice(0, 500),
            document_number: docNum,
            source_url: sourceUrl,
            loan_type: classified.loan_type,
            deed_type: classified.deed_type,
          };

          if (doc.ConsiderationAmount > 0) {
            withAmount++;
            if (classified.document_type === "mortgage" || classified.document_type === "lien") {
              const fields = computeMortgageFields({ originalAmount: Math.round(doc.ConsiderationAmount), recordingDate: dateStr });
              record.interest_rate = fields.interest_rate;
              record.term_months = fields.term_months;
              record.estimated_monthly_payment = fields.estimated_monthly_payment;
              record.estimated_current_balance = fields.estimated_current_balance;
              record.balance_as_of = fields.balance_as_of;
              record.maturity_date = fields.maturity_date;
            }
          }

          const { error } = await db.from("mortgage_records").insert(record);
          if (!error) inserted++;
        }

        if (results.length > 0) {
          process.stdout.write(`\r  ${dateStr}: ${results.length} docs | Inserted: ${inserted} | Dupes: ${duplicates} | Amounts: ${withAmount}`);
        }
      } catch (err: any) {
        // Token might have expired
        try { token = await getToken(apiBase); } catch {}
      }

      current.setDate(current.getDate() + 1);
    }

    console.log(`\n  Total: ${inserted} inserted | ${withAmount} with amounts | ${duplicates} dupes`);
    grandTotal += inserted;
    grandAmounts += withAmount;
  }

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\n═══════════════════════════════════`);
  console.log(`  Grand total: ${grandTotal} | With amounts: ${grandAmounts}`);
  console.log(`  DB total: ${count}`);
}

main().catch(console.error);
