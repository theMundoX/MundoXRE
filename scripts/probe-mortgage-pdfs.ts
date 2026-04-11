#!/usr/bin/env tsx
/**
 * Probe a sample of mortgage_records to see if we can actually download the PDFs.
 * Goal: figure out if the labeling pipeline is feasible from what's already in Supabase.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PROBE_DIR = "C:/Users/msanc/mxre/data/probe-pdfs";
if (!existsSync(PROBE_DIR)) mkdirSync(PROBE_DIR, { recursive: true });

async function main() {
  console.log("=== MORTGAGE PDF PROBE ===\n");

  // 1. Sample 25 mortgage records WITH source_url + actual loan amount
  // (loan_amount filter biases toward complete records that came from real APIs)
  console.log("[1/4] Sampling 25 mortgage_records with source_url...");
  const { data: withUrl, error: e1 } = await db
    .from("mortgage_records")
    .select("id, document_type, recording_date, lender_name, borrower_name, loan_amount, document_number, source_url")
    .not("source_url", "is", null)
    .gt("loan_amount", 0)
    .limit(25);

  if (e1) {
    console.error("Query failed:", e1);
    process.exit(1);
  }

  console.log(`  ${withUrl?.length || 0} records returned\n`);

  if (!withUrl || withUrl.length === 0) {
    console.log("  No records with source_url. Trying a broader query...");
    const { data: anyMort } = await db
      .from("mortgage_records")
      .select("id, document_type, recording_date, lender_name, borrower_name, loan_amount, document_number, source_url")
      .limit(10);
    console.log("  Sample (any source_url status):");
    for (const r of anyMort || []) {
      console.log(`    id=${r.id}  src=${r.source_url || "(null)"}  type=${r.document_type}  $${r.loan_amount || "?"}`);
    }
    process.exit(0);
  }

  // 2. Show what the URLs look like
  console.log("[2/4] URL patterns:");
  const hosts = new Map<string, number>();
  for (const r of withUrl) {
    if (!r.source_url) continue;
    try {
      const u = new URL(r.source_url);
      hosts.set(u.host, (hosts.get(u.host) || 0) + 1);
    } catch {
      hosts.set("(invalid url)", (hosts.get("(invalid url)") || 0) + 1);
    }
  }
  for (const [host, count] of [...hosts.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${count}× ${host}`);
  }

  console.log("\n[3/4] Sample URLs (first 5):");
  for (const r of withUrl.slice(0, 5)) {
    console.log(`    ${r.document_type || "?"} | ${r.lender_name || "?"} | ${r.source_url}`);
  }

  // 3. Try downloading the first 5 to see what they actually return
  console.log("\n[4/4] Attempting downloads of first 5...");
  const results = [];
  for (let i = 0; i < Math.min(5, withUrl.length); i++) {
    const r = withUrl[i];
    if (!r.source_url) continue;
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 15_000);
      const resp = await fetch(r.source_url, {
        signal: ctrl.signal,
        headers: { "User-Agent": "MXRE-Probe/1.0" },
      });
      clearTimeout(timer);

      const ct = resp.headers.get("content-type") || "";
      const cl = resp.headers.get("content-length") || "?";
      const status = resp.status;

      let savedPath = null;
      if (resp.ok && (ct.includes("pdf") || ct.includes("octet-stream") || ct.includes("application/"))) {
        const buf = Buffer.from(await resp.arrayBuffer());
        savedPath = `${PROBE_DIR}/probe-${r.id}.pdf`;
        writeFileSync(savedPath, buf);
      } else if (resp.ok) {
        // Probably an HTML page, not a direct PDF
        const text = await resp.text();
        savedPath = `${PROBE_DIR}/probe-${r.id}.html`;
        writeFileSync(savedPath, text);
      }

      const result = {
        id: r.id,
        url: r.source_url,
        status,
        contentType: ct,
        contentLength: cl,
        savedAs: savedPath ? savedPath.split("/").pop() : null,
        isPdf: ct.includes("pdf"),
      };
      results.push(result);
      console.log(`    [${i + 1}/5] ${status} ${ct.slice(0, 30)} ${cl}b → ${result.savedAs || "(not saved)"}`);
    } catch (err) {
      console.log(`    [${i + 1}/5] ERROR: ${err instanceof Error ? err.message : err}`);
      results.push({ id: r.id, url: r.source_url, error: String(err) });
    }
  }

  // Summary
  console.log("\n=== SUMMARY ===");
  const downloaded = results.filter((r) => "savedAs" in r && r.savedAs).length;
  const pdfCount = results.filter((r) => "isPdf" in r && r.isPdf).length;
  console.log(`Total records with URLs:  ${withUrl.length}`);
  console.log(`Distinct hosts:           ${hosts.size}`);
  console.log(`Sample downloads tried:   ${results.length}`);
  console.log(`Successfully fetched:     ${downloaded}`);
  console.log(`Direct PDF responses:     ${pdfCount}`);
  console.log(`Saved to:                 ${PROBE_DIR}`);

  if (pdfCount === 0) {
    console.log("\n⚠️  None of the URLs returned a PDF directly.");
    console.log("    They're probably landing pages or require navigation.");
    console.log("    We'll need a different strategy (recorder-specific download scripts).");
  } else if (pdfCount < results.length / 2) {
    console.log("\n⚠️  Mixed results — some URLs are direct PDFs, others aren't.");
    console.log("    We'd need per-host handling to download reliably.");
  } else {
    console.log("\n✅ Most URLs return PDFs directly. Bulk download is feasible.");
  }
}

main().catch((err) => {
  console.error("Probe failed:", err);
  process.exit(1);
});
