#!/usr/bin/env tsx
/**
 * MXRE Recorder Ingest — Texas publicsearch.us
 *
 * Pulls real deed/mortgage records from Texas county clerk portals on publicsearch.us.
 *
 * Usage:
 *   npx tsx scripts/ingest-recorder-tx.ts --county=Dallas --days=7
 *   npx tsx scripts/ingest-recorder-tx.ts --county=Dallas --from=2024-01-01 --to=2024-12-31
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { PublicSearchAdapter, PUBLICSEARCH_COUNTIES } from "../src/discovery/adapters/publicsearch.js";
import type { RecorderDocument } from "../src/discovery/adapters/landmark-web.js";

// ─── CLI Args ──────────────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const arg = args.find((a) => a.startsWith(`--${name}=`));
  return arg?.split("=")[1];
}

const countyFilter = getArg("county");
const fromDate = getArg("from");
const toDate = getArg("to");
const daysBack = parseInt(getArg("days") || "7", 10);
const runAll = args.includes("--all");
const dryRun = args.includes("--dry-run");
const maxDocs = Math.max(1, parseInt(getArg("max-docs") || "1000000", 10));

if (!countyFilter && !runAll) {
  console.log("Usage: npx tsx scripts/ingest-recorder-tx.ts --county=Dallas [--from=2024-01-01] [--to=2024-12-31] [--days=7] [--max-docs=1000] [--dry-run]");
  console.log("       npx tsx scripts/ingest-recorder-tx.ts --all --days=730");
  console.log("\nAvailable counties:");
  for (const c of PUBLICSEARCH_COUNTIES) console.log(`  ${c.county_name} — ${c.base_url}`);
  process.exit(1);
}

// ─── Database ──────────────────────────────────────────────────────

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── Date Range ────────────────────────────────────────────────────

const endDate = toDate || new Date().toISOString().split("T")[0];
let startDate: string;
if (fromDate) {
  startDate = fromDate;
} else {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  startDate = d.toISOString().split("T")[0];
}

// ─── Helpers ───────────────────────────────────────────────────────

async function findPropertyByOwner(ownerName: string, stateCode: string): Promise<number | null> {
  if (!ownerName || ownerName.length < 3) return null;
  const cleanName = ownerName.replace(/\s+/g, " ").trim();
  const { data } = await db.from("properties").select("id").eq("state_code", stateCode).ilike("owner_name", cleanName).limit(1);
  if (data?.length) return data[0].id;
  const lastName = cleanName.split(/[,\s]+/)[0];
  if (lastName.length >= 3) {
    const { data: partial } = await db.from("properties").select("id").eq("state_code", stateCode).ilike("owner_name", `${lastName}%`).limit(1);
    if (partial?.length) return partial[0].id;
  }
  return null;
}

async function isDuplicate(doc: RecorderDocument): Promise<boolean> {
  if (doc.instrument_number) {
    const { data } = await db.from("mortgage_records").select("id").eq("document_number", doc.instrument_number).eq("source_url", doc.source_url).limit(1);
    if (data?.length) return true;
  }
  if (doc.book_page && doc.book_page !== "--/--/--") {
    const { data } = await db.from("mortgage_records").select("id").eq("book_page", doc.book_page).eq("source_url", doc.source_url).limit(1);
    if (data?.length) return true;
  }
  return false;
}

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType.toUpperCase();
  if (upper.includes("DEED OF TRUST") || upper.includes("DOT")) return { document_type: "mortgage", loan_type: "purchase" };
  if (upper.includes("MORTGAGE") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) return { document_type: "mortgage", loan_type: "purchase" };
  if (upper.includes("RELEASE") || upper.includes("SATISFACTION")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("WARRANTY DEED") || upper === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (upper.includes("QUIT CLAIM") || upper === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (upper.includes("SPECIAL WARRANTY")) return { document_type: "deed", deed_type: "special_warranty" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase() };
}

// ─── Main ──────────────────────────────────────────────────────────

async function ingestCounty(
  adapter: PublicSearchAdapter,
  config: (typeof PUBLICSEARCH_COUNTIES)[0],
): Promise<{ inserted: number; duplicates: number; unmatched: number }> {
  const { data: dbCounty } = await db.from("counties").select("id").eq("county_name", config.county_name).eq("state_code", "TX").single();
  if (dbCounty) config.county_id = dbCounty.id;

  let inserted = 0, duplicates = 0, unmatched = 0;
  const batch: Array<Record<string, unknown>> = [];
  const BATCH_SIZE = 50;

  let seen = 0;
  for await (const doc of adapter.fetchDocuments(config, startDate, endDate)) {
    seen++;
    if (await isDuplicate(doc)) { duplicates++; continue; }

    const classified = classifyDocType(doc.document_type);
    const propertyId = await findPropertyByOwner(doc.grantor, "TX") || await findPropertyByOwner(doc.grantee, "TX");
    if (!propertyId) unmatched++;

    batch.push({
      property_id: propertyId || null,
      document_type: classified.document_type,
      recording_date: doc.recording_date,
      lender_name: doc.grantee,
      borrower_name: doc.grantor,
      document_number: doc.instrument_number,
      book_page: doc.book_page !== "--/--/--" ? doc.book_page : null,
      source_url: doc.source_url,
      loan_type: classified.loan_type,
      deed_type: classified.deed_type,
    });

    if (batch.length >= BATCH_SIZE) {
      if (dryRun) {
        inserted += batch.length;
        batch.length = 0;
        if (seen >= maxDocs) break;
        continue;
      }
      const { error } = await db.from("mortgage_records").insert(batch);
      if (error) console.error(`Batch error: ${error.message}`);
      else inserted += batch.length;
      batch.length = 0;
    }
    if (seen >= maxDocs) break;
  }

  if (batch.length > 0) {
    if (dryRun) {
      inserted += batch.length;
    } else {
      const { error } = await db.from("mortgage_records").insert(batch);
      if (error) console.error(`Final batch error: ${error.message}`);
      else inserted += batch.length;
    }
  }

  return { inserted, duplicates, unmatched };
}

async function main() {
  console.log(`\nMXRE Recorder Ingest — Texas publicsearch.us`);
  console.log(`${"─".repeat(55)}`);
  console.log(`Date range: ${startDate} to ${endDate}\n`);
  console.log(`Dry run: ${dryRun}; max docs per county: ${maxDocs.toLocaleString()}\n`);

  const countiesToRun = runAll
    ? PUBLICSEARCH_COUNTIES
    : PUBLICSEARCH_COUNTIES.filter(c => c.county_name.toLowerCase() === (countyFilter ?? "").toLowerCase());

  if (countiesToRun.length === 0) {
    console.error(`County "${countyFilter}" not found.`);
    process.exit(1);
  }

  const adapter = new PublicSearchAdapter();
  console.log("Launching browser...");
  await adapter.init();

  let grandInserted = 0, grandDupes = 0, grandUnmatched = 0;

  try {
    for (const config of countiesToRun) {
      console.log(`\n── ${config.county_name} County ──`);
      const result = await ingestCounty(adapter, config);
      grandInserted += result.inserted;
      grandDupes += result.duplicates;
      grandUnmatched += result.unmatched;
      console.log(`  Inserted: ${result.inserted} | Dupes: ${result.duplicates} | Unmatched: ${result.unmatched}`);
    }
  } finally {
    await adapter.close();
  }

  console.log(`\n${"─".repeat(55)}`);
  console.log(`Total: Inserted: ${grandInserted} | Dupes: ${grandDupes} | Unmatched: ${grandUnmatched}`);
  console.log("Done.\n");
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
