#!/usr/bin/env tsx
/**
 * Enrich Fidlar AVA mortgage records that have NULL loan_amount.
 *
 * Strategy: For each record missing a loan amount, re-query the Fidlar API
 * by document number (instrument number) to retrieve ConsiderationAmount.
 * Then update the DB record with the found amount and computed mortgage fields.
 *
 * Some Fidlar counties (all NH, Linn IA) never index consideration amounts
 * for mortgages — those will remain NULL. The script still tries them in case
 * the county starts indexing amounts.
 *
 * Usage: npx tsx scripts/enrich-mortgage-amounts.ts [--dry-run] [--county OHFairfield]
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

// ─── Config ────────────────────────────────────────────────────────────

const BATCH_SIZE = 50;          // Records to fetch from DB at a time
const API_DELAY_MS = 300;       // Delay between API calls (rate limiting)
const TOKEN_REFRESH_INTERVAL = 25 * 60 * 1000; // Refresh token every 25 min

const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const COUNTY_FILTER = args.find((_, i) => args[i - 1] === "--county") || null;

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── Fidlar API helpers ────────────────────────────────────────────────

/** Map source_url to API base URL */
function getApiBase(sourceUrl: string): string {
  return sourceUrl.replace("/AvaWeb/", "/ScrapRelay.WebService.Ava/");
}

/** Get anonymous bearer token */
async function getToken(apiBase: string): Promise<string> {
  const resp = await fetch(apiBase + "token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
  });
  if (!resp.ok) throw new Error(`Token failed: ${resp.status}`);
  const data = (await resp.json()) as any;
  return data.access_token;
}

/** Search Fidlar API by document number, return ConsiderationAmount */
async function lookupConsideration(
  apiBase: string,
  token: string,
  documentNumber: string,
): Promise<{ amount: number; id: number | null } | null> {
  const resp = await fetch(apiBase + "breeze/Search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer " + token,
    },
    body: JSON.stringify({
      FirstName: "", LastBusinessName: "",
      StartDate: "", EndDate: "",
      DocumentName: documentNumber,
      DocumentType: "",
      SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
      MunicipalityName: "",
      TractSection: "", TractTownship: "", TractRange: "",
      TractQuarter: "", TractQuarterQuarter: "",
      Book: "", Page: "",
      LotOfRecord: "", BlockOfRecord: "",
      AddressNumber: "", AddressDirection: "", AddressStreetName: "",
      TaxId: "",
    }),
  });

  if (!resp.ok) {
    if (resp.status === 401) throw new Error("TOKEN_EXPIRED");
    throw new Error(`Search failed: ${resp.status}`);
  }

  const data = (await resp.json()) as any;
  if (!data.DocResults || data.DocResults.length === 0) return null;

  const doc = data.DocResults[0];
  return {
    amount: doc.ConsiderationAmount || 0,
    id: doc.Id || null,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Main ──────────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Enrich Fidlar AVA Mortgage Amounts");
  console.log(`Mode: ${DRY_RUN ? "DRY RUN" : "LIVE"}`);
  if (COUNTY_FILTER) console.log(`County filter: ${COUNTY_FILTER}`);
  console.log();

  // Step 1: Get distinct source_urls that need enrichment
  let query = db
    .from("mortgage_records")
    .select("source_url")
    .is("loan_amount", null)
    .eq("document_type", "mortgage")
    .like("source_url", "%fidlar%");

  if (COUNTY_FILTER) {
    query = query.like("source_url", `%${COUNTY_FILTER}%`);
  }

  const { data: sourceUrls, error: srcErr } = await query;
  if (srcErr) { console.error("Error fetching source URLs:", srcErr.message); return; }

  // Deduplicate
  const uniqueUrls = [...new Set((sourceUrls || []).map((r: any) => r.source_url))];
  console.log(`Found ${uniqueUrls.length} Fidlar county source(s) with unenriched mortgages.\n`);

  let grandTotal = 0;
  let grandEnriched = 0;
  let grandSkipped = 0;
  let grandErrors = 0;

  for (const sourceUrl of uniqueUrls) {
    const countySlug = sourceUrl.match(/fidlar\.com\/(\w+)\//)?.[1] || sourceUrl;
    console.log(`━━━ ${countySlug} ━━━`);

    const apiBase = getApiBase(sourceUrl);
    let token: string;
    let tokenTime = Date.now();

    try {
      token = await getToken(apiBase);
    } catch (err: any) {
      console.error(`  Failed to get token: ${err.message}`);
      grandErrors++;
      continue;
    }

    // Count records to process
    const { count } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .is("loan_amount", null)
      .eq("document_type", "mortgage")
      .eq("source_url", sourceUrl);

    console.log(`  Records to process: ${count || 0}`);
    let processed = 0;
    let enriched = 0;
    let notFound = 0;
    let zeroAmount = 0;
    let errors = 0;

    // Process in batches
    let offset = 0;
    while (true) {
      const { data: records, error: batchErr } = await db
        .from("mortgage_records")
        .select("id, document_number, recording_date, source_url")
        .is("loan_amount", null)
        .eq("document_type", "mortgage")
        .eq("source_url", sourceUrl)
        .order("id", { ascending: true })
        .range(offset, offset + BATCH_SIZE - 1);

      if (batchErr) {
        console.error(`  Batch error: ${batchErr.message}`);
        errors++;
        break;
      }

      if (!records || records.length === 0) break;

      for (const record of records) {
        if (!record.document_number) {
          notFound++;
          processed++;
          continue;
        }

        // Refresh token if needed
        if (Date.now() - tokenTime > TOKEN_REFRESH_INTERVAL) {
          try {
            token = await getToken(apiBase);
            tokenTime = Date.now();
          } catch {
            console.error("  Token refresh failed, continuing with old token...");
          }
        }

        try {
          const result = await lookupConsideration(apiBase, token, record.document_number);

          if (!result) {
            notFound++;
          } else if (result.amount > 0) {
            const amount = Math.round(result.amount);

            // Compute mortgage fields
            const fields = computeMortgageFields({
              originalAmount: amount,
              recordingDate: record.recording_date,
            });

            if (!DRY_RUN) {
              const { error: updateErr } = await db
                .from("mortgage_records")
                .update({
                  loan_amount: amount,
                  original_amount: amount,
                  interest_rate: fields.interest_rate,
                  term_months: fields.term_months,
                  estimated_monthly_payment: fields.estimated_monthly_payment,
                  estimated_current_balance: fields.estimated_current_balance,
                  balance_as_of: fields.balance_as_of,
                  maturity_date: fields.maturity_date,
                })
                .eq("id", record.id);

              if (updateErr) {
                console.error(`  Update error for ${record.document_number}: ${updateErr.message}`);
                errors++;
              } else {
                enriched++;
              }
            } else {
              console.log(`  [DRY] ${record.document_number}: $${amount.toLocaleString()}`);
              enriched++;
            }
          } else {
            zeroAmount++;
          }
        } catch (err: any) {
          if (err.message === "TOKEN_EXPIRED") {
            // Refresh and retry once
            try {
              token = await getToken(apiBase);
              tokenTime = Date.now();
              // Retry
              const result = await lookupConsideration(apiBase, token, record.document_number);
              if (result && result.amount > 0) {
                const amount = Math.round(result.amount);
                const fields = computeMortgageFields({
                  originalAmount: amount,
                  recordingDate: record.recording_date,
                });
                if (!DRY_RUN) {
                  await db
                    .from("mortgage_records")
                    .update({
                      loan_amount: amount,
                      original_amount: amount,
                      interest_rate: fields.interest_rate,
                      term_months: fields.term_months,
                      estimated_monthly_payment: fields.estimated_monthly_payment,
                      estimated_current_balance: fields.estimated_current_balance,
                      balance_as_of: fields.balance_as_of,
                      maturity_date: fields.maturity_date,
                    })
                    .eq("id", record.id);
                }
                enriched++;
              } else {
                zeroAmount++;
              }
            } catch (retryErr: any) {
              console.error(`  Retry failed for ${record.document_number}: ${retryErr.message?.slice(0, 60)}`);
              errors++;
            }
          } else {
            console.error(`  Error for ${record.document_number}: ${err.message?.slice(0, 60)}`);
            errors++;
          }
        }

        processed++;
        await sleep(API_DELAY_MS);

        // Progress update every 50 records
        if (processed % 50 === 0) {
          process.stdout.write(
            `\r  Processed: ${processed}/${count || "?"} | Enriched: ${enriched} | $0: ${zeroAmount} | Errors: ${errors}`,
          );
        }
      }

      // If we've checked a good sample and found zero amounts,
      // this county likely doesn't index consideration amounts — skip early.
      // Use a higher threshold (100) to avoid missing sparse data.
      if (processed >= 100 && enriched === 0 && zeroAmount >= 100) {
        console.log(`\n  Early exit: ${countySlug} appears to not index consideration amounts (0/${processed} had amounts).`);
        break;
      }

      offset += records.length;
    }

    console.log(
      `\n  Done: processed=${processed} enriched=${enriched} $0=${zeroAmount} notFound=${notFound} errors=${errors}`,
    );

    grandTotal += processed;
    grandEnriched += enriched;
    grandSkipped += zeroAmount;
    grandErrors += errors;
  }

  console.log("\n═══════════════════════════════════════");
  console.log(`  Total processed: ${grandTotal}`);
  console.log(`  Enriched: ${grandEnriched}`);
  console.log(`  Skipped ($0 in Fidlar): ${grandSkipped}`);
  console.log(`  Errors: ${grandErrors}`);

  if (DRY_RUN) {
    console.log("\n  (DRY RUN — no records were updated)");
  }
}

main().catch(console.error);
