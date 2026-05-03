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
const perSearchTimeoutMs = Number(valueArg("per-search-timeout-ms") ?? "45000");
const verboseErrors = args.includes("--verbose-errors");

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});
const directPgUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.DATABASE_URL ?? process.env.POSTGRES_URL;
const pgQueryUrl = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const pgQueryKey = process.env.SUPABASE_SERVICE_KEY ?? "";

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
      const docs = await withTimeout(
        adapter.fetchAddressDocuments(marion, property.address, from, to),
        perSearchTimeoutMs,
        `Timed out searching ${property.address}`,
      );
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
      if (verboseErrors || errors <= 5) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`\n[error] property_id=${property.id} address="${property.address}" ${message}`);
      }
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
  if (onlyOnMarket && pgQueryKey) {
    return loadOnMarketCandidatesViaPgQuery();
  }

  if (directPgUrl) {
    const client = new Client({ connectionString: directPgUrl });
    await client.connect();
    try {
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

async function loadOnMarketCandidatesViaPgQuery(): Promise<Array<{ id: number; parcel_id: string | null; address: string; city: string; state_code: string; zip: string }>> {
  const listingLimit = Math.max(limit * 10, 100);
  const afterClause = afterParcel ? `and p.parcel_id > ${sqlString(afterParcel)}` : "";
  const query = `
    with active as (
      select distinct property_id
      from listing_signals
      where is_on_market = true
        and state_code = 'IN'
        and upper(city) = 'INDIANAPOLIS'
        and property_id is not null
      limit ${listingLimit}
    )
    select p.id, p.parcel_id, p.address, p.city, p.state_code, p.zip
    from active a
    join properties p on p.id = a.property_id
    where p.county_id = 797583
      and p.state_code = 'IN'
      and p.address ~ '^[0-9]'
      ${afterClause}
    limit ${limit};
  `;

  const response = await fetch(pgQueryUrl, {
    method: "POST",
    headers: {
      apikey: pgQueryKey,
      Authorization: `Bearer ${pgQueryKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(60_000),
  });

  if (!response.ok) {
    throw new Error(`pg/query candidate load failed ${response.status}: ${await response.text()}`);
  }

  return response.json() as Promise<Array<{ id: number; parcel_id: string | null; address: string; city: string; state_code: string; zip: string }>>;
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
