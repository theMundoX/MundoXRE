#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { Client } from "pg";
import {
  FidlarDirectSearchAdapter,
  DIRECT_SEARCH_COUNTIES,
} from "../src/discovery/adapters/fidlar-direct-search.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const limit = Number(valueArg("limit") ?? "500");
const from = valueArg("from") ?? "2000-01-01";
const to = valueArg("to") ?? new Date().toISOString().slice(0, 10);
const dryRun = args.includes("--dry-run");
const onlyOnMarket = args.includes("--on-market");
const batchSleepMs = Number(valueArg("delay-ms") ?? "250");
const afterParcel = valueArg("after-parcel");
const maxRunMs = Number(valueArg("max-run-ms") ?? "0");

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});
const directPgUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.DATABASE_URL ?? process.env.POSTGRES_URL;

const marion = DIRECT_SEARCH_COUNTIES.find((county) => county.county_name === "Marion" && county.state === "IN");
if (!marion) throw new Error("Marion DirectSearch config not found.");

const adapter = new FidlarDirectSearchAdapter();

function classifyDocType(raw: string): string {
  const upper = raw.toUpperCase();
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE") || upper.includes("DISCHARGE")) return "satisfaction";
  if (upper.includes("ASSIGNMENT")) return "assignment";
  if (upper.includes("MECHANIC") || upper.includes("MATERIALMAN")) return "mechanics_lien";
  if (upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return "judgment";
  if (upper.includes("TAX") && upper.includes("LIEN")) return "tax_lien";
  if (upper.includes("MORTGAGE")) return "mortgage";
  if (upper.includes("LIEN")) return "lien";
  return raw.toLowerCase().trim() || "other";
}

function isOpenLienType(type: string): boolean {
  return !["satisfaction", "assignment"].includes(type);
}

async function main() {
  console.log(`Marion property-address lien backfill | limit=${limit} | ${from} to ${to} | dry=${dryRun}`);
  const startedAt = Date.now();

  const candidates = await loadCandidates();
  console.log(`Searching ${candidates.length} properties without linked recorder rows.`);

  let searched = 0;
  let docsFound = 0;
  let inserted = 0;
  let noDocs = 0;
  let errors = 0;

  for (const property of candidates) {
    if (maxRunMs > 0 && Date.now() - startedAt > maxRunMs) {
      console.log(`\nReached max runtime ${maxRunMs}ms; stopping cleanly.`);
      break;
    }

    searched++;
    try {
      const docs = await adapter.fetchAddressDocuments(marion, property.address, from, to);
      if (docs.length === 0) {
        noDocs++;
        process.stdout.write(`\rsearched=${searched} no_docs=${noDocs} docs=${docsFound} inserted=${inserted} errors=${errors}`);
        await sleep(batchSleepMs);
        continue;
      }

      docsFound += docs.length;
      for (const doc of docs) {
        const docType = classifyDocType(doc.document_type);
        const amount = doc.consideration ? Math.round(doc.consideration) : null;
        const computed = amount && doc.recording_date && isOpenLienType(docType)
          ? computeMortgageFields({ originalAmount: amount, recordingDate: doc.recording_date })
          : null;

        if (!dryRun) {
          const { error: upsertError } = await db.from("mortgage_records").upsert({
            property_id: property.id,
            document_type: docType,
            recording_date: doc.recording_date || null,
            loan_amount: amount,
            original_amount: amount,
            lender_name: doc.grantee?.slice(0, 500) || null,
            borrower_name: doc.grantor?.slice(0, 500) || null,
            document_number: doc.instrument_number || null,
            book_page: doc.book_page || null,
            source_url: marion.base_url,
            open: isOpenLienType(docType),
            interest_rate: computed?.interest_rate ?? null,
            term_months: computed?.term_months ?? null,
            estimated_monthly_payment: computed?.estimated_monthly_payment ?? null,
            estimated_current_balance: computed?.estimated_current_balance ?? null,
            balance_as_of: computed?.balance_as_of ?? null,
            maturity_date: computed?.maturity_date ?? null,
            rate_source: computed?.rate_source ?? null,
          }, { onConflict: "document_number,source_url", ignoreDuplicates: true });
          if (upsertError) throw upsertError;
        }
        inserted++;
      }
    } catch (error) {
      errors++;
    }

    process.stdout.write(`\rsearched=${searched} no_docs=${noDocs} docs=${docsFound} inserted=${inserted} errors=${errors}`);
    await sleep(batchSleepMs);
  }

  console.log();
  const last = candidates[candidates.length - 1];
  console.log(JSON.stringify({
    searched,
    noDocs,
    docsFound,
    inserted,
    errors,
    dryRun,
    lastPropertyId: last?.id ?? null,
    lastParcel: last?.parcel_id ?? null,
  }, null, 2));
}

async function loadCandidates(): Promise<Array<{ id: number; parcel_id: string | null; address: string; city: string; state_code: string; zip: string }>> {
  if (directPgUrl) {
    const client = new Client({ connectionString: directPgUrl });
    await client.connect();
    try {
      if (onlyOnMarket) {
        const listingLimit = Math.max(limit * 10, 100);
        const { data: listingRows, error: listingError } = await db
          .from("listing_signals")
          .select("property_id")
          .eq("is_on_market", true)
          .eq("state_code", "IN")
          .eq("city", "INDIANAPOLIS")
          .not("property_id", "is", null)
          .limit(listingLimit);

        if (listingError) throw listingError;

        const propertyIds = Array.from(new Set(
          (listingRows ?? [])
            .map((row) => Number(row.property_id))
            .filter((id) => Number.isFinite(id) && id > 0),
        )).slice(0, listingLimit);

        if (propertyIds.length === 0) return [];

        const params: Array<string | number | number[]> = [limit, propertyIds];
        let cursorClause = "";
        if (afterParcel) {
          params.push(afterParcel);
          cursorClause = "and p.parcel_id > $3";
        }

        const result = await client.query(`
          select p.id, p.parcel_id, p.address, p.city, p.state_code, p.zip
          from properties p
          where p.id = any($2::int[])
            and p.county_id = 797583
            and p.state_code = 'IN'
            and p.address ~ '^[0-9]'
            ${cursorClause}
          limit $1
        `, params);
        return result.rows;
      }

      const result = await client.query(`
        select p.id, p.parcel_id, p.address, p.city, p.state_code, p.zip
        from properties p
        where p.county_id = 797583
          and p.state_code = 'IN'
          and p.address ~ '^[0-9]'
          ${afterParcel ? "and p.parcel_id > $2" : ""}
        order by p.parcel_id
        limit $1
      `, afterParcel ? [limit, afterParcel] : [limit]);
      return result.rows;
    } finally {
      await client.end();
    }
  }

  const { data, error } = await db.from("properties")
    .select("id,parcel_id,address,city,state_code,zip")
    .eq("county_id", 797583)
    .eq("state_code", "IN")
    .not("address", "is", null)
    .order("id")
    .limit(limit);
  if (error) throw error;
  return data ?? [];
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
