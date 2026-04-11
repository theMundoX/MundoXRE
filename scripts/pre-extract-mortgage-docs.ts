#!/usr/bin/env tsx
/**
 * Pre-extract draft JSON from downloaded mortgage document images
 * using the local MundoX 27B vision model.
 *
 * Input:  docs/FL/levy/{docnum}/page*.png  (from download-free-docs.ts)
 * Output: data/labeling-sample/{docnum}/page1.png, draft.json, meta.json
 *
 * Each doc gets its first page sent to MundoX with a structured extraction
 * prompt. The response is parsed as JSON and saved for human review in the
 * labeling UI.
 *
 * Usage:
 *   npx tsx scripts/pre-extract-mortgage-docs.ts
 *   npx tsx scripts/pre-extract-mortgage-docs.ts --limit 10
 */

import "dotenv/config";
import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync, statSync, copyFileSync } from "node:fs";
import { join, basename } from "node:path";
import { createClient } from "@supabase/supabase-js";

const args = process.argv.slice(2);
const LIMIT_ARG = args.find((a) => a.startsWith("--limit"));
const LIMIT = LIMIT_ARG ? parseInt(args[args.indexOf(LIMIT_ARG) + 1] || "0", 10) : 0;

const MXRE_ROOT = "C:/Users/msanc/mxre";
const SRC_BASE = join(MXRE_ROOT, "docs", "FL", "levy");
const OUT_BASE = join(MXRE_ROOT, "data", "labeling-sample");
const MUNDOX_URL = process.env.MUNDOX_URL || "http://127.0.0.1:18791/v1/chat/completions";
const MODEL = "mundox";

if (!existsSync(OUT_BASE)) mkdirSync(OUT_BASE, { recursive: true });

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const EXTRACTION_PROMPT = `You are extracting structured data from a scanned county recorder mortgage document image.

Return ONLY a JSON object with these fields (use null if a field is not visible or unclear):

{
  "document_type": "mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "document_number": "the instrument/document number as shown, or null",
  "book_page": "book/page reference if shown, or null",
  "borrower_name": "primary borrower(s) / mortgagor(s) full name as written, or null",
  "lender_name": "mortgagee / lender full name as written, or null",
  "loan_amount": "principal amount in cents as an integer (e.g., 25000000 for $250,000), or null",
  "interest_rate": "annual rate as decimal percent (e.g., 6.875 for 6.875%), or null",
  "term_months": "loan term in months as integer (e.g., 360 for 30-year), or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "subject property address as written, or null",
  "notes": "any important observations about the document, or null"
}

Rules:
- Return ONLY the JSON object, no preamble, no markdown fences, no commentary.
- If a field is partially visible but unclear, use null.
- loan_amount must be in CENTS (multiply dollars by 100).
- interest_rate is the rate stamped on the document, NOT an estimate.
- If the document is not a mortgage-related instrument, set document_type="other" and fill what you can.`;

async function extract(imageB64: string): Promise<{ raw: string; parsed: any | null; elapsed_ms: number }> {
  const start = Date.now();
  const body = {
    model: MODEL,
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text: EXTRACTION_PROMPT },
          { type: "image_url", image_url: { url: `data:image/png;base64,${imageB64}` } },
        ],
      },
    ],
    max_tokens: 600,
    temperature: 0.1,
    chat_template_kwargs: { enable_thinking: false },
  };

  const resp = await fetch(MUNDOX_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data: any = await resp.json();
  const text: string = data.choices?.[0]?.message?.content || "";
  const elapsed_ms = Date.now() - start;

  // Try to parse the first {...} block
  let parsed: any = null;
  const match = text.match(/\{[\s\S]*\}/);
  if (match) {
    try {
      parsed = JSON.parse(match[0]);
    } catch {}
  }
  return { raw: text, parsed, elapsed_ms };
}

async function lookupGroundTruth(docNumber: string): Promise<any | null> {
  try {
    const { data } = await db
      .from("mortgage_records")
      .select("id, document_type, recording_date, document_number, lender_name, borrower_name, loan_amount, original_amount, interest_rate, rate_source, term_months, source_url")
      .eq("document_number", docNumber)
      .limit(1);
    return data && data.length > 0 ? data[0] : null;
  } catch {
    return null;
  }
}

async function main() {
  console.log("MXRE — Pre-extract mortgage doc drafts using MundoX 27B\n");

  if (!existsSync(SRC_BASE)) {
    console.log(`Source dir ${SRC_BASE} does not exist yet.`);
    console.log("Waiting for download-free-docs.ts to produce files first.");
    return;
  }

  const docDirs = readdirSync(SRC_BASE)
    .filter((d) => {
      try {
        return statSync(join(SRC_BASE, d)).isDirectory();
      } catch {
        return false;
      }
    })
    .map((d) => join(SRC_BASE, d));

  console.log(`Found ${docDirs.length} doc dirs in ${SRC_BASE}`);

  if (docDirs.length === 0) {
    console.log("Nothing to extract yet.");
    return;
  }

  let processed = 0;
  let skipped = 0;
  let errors = 0;
  const durations: number[] = [];

  for (const dir of docDirs) {
    if (LIMIT && processed >= LIMIT) break;

    const docNumber = basename(dir);
    const outDir = join(OUT_BASE, docNumber);
    const draftPath = join(outDir, "draft.json");
    const imgDest = join(outDir, "page1.png");

    if (existsSync(draftPath)) {
      skipped++;
      continue;
    }

    const page1 = join(dir, "page1.png");
    if (!existsSync(page1)) {
      console.log(`  ${docNumber}: no page1.png`);
      errors++;
      continue;
    }

    try {
      mkdirSync(outDir, { recursive: true });
      copyFileSync(page1, imgDest);

      const buf = readFileSync(page1);
      const b64 = buf.toString("base64");

      console.log(`  ${docNumber}: extracting (${(buf.length / 1024).toFixed(0)} KB)...`);
      const { raw, parsed, elapsed_ms } = await extract(b64);
      durations.push(elapsed_ms);

      const groundTruth = await lookupGroundTruth(docNumber);

      writeFileSync(
        draftPath,
        JSON.stringify(
          {
            doc_number: docNumber,
            extracted_at: new Date().toISOString(),
            elapsed_ms,
            mundox_raw_response: raw,
            draft: parsed,
            ground_truth_from_db: groundTruth,
          },
          null,
          2,
        ),
      );

      writeFileSync(
        join(outDir, "meta.json"),
        JSON.stringify(
          {
            doc_number: docNumber,
            source_dir: dir,
            created_at: new Date().toISOString(),
            status: "draft", // draft | approved | skipped
            corrected: null,
          },
          null,
          2,
        ),
      );

      if (parsed) {
        console.log(`    ✓ parsed (${elapsed_ms}ms) — lender: ${parsed.lender_name?.slice(0, 30) || "?"}`);
      } else {
        console.log(`    ⚠ raw returned but no JSON parseable:\n    ${raw.slice(0, 200)}`);
      }

      processed++;
    } catch (err: any) {
      console.log(`  ${docNumber}: error — ${err?.message || err}`);
      errors++;
    }
  }

  const avgMs = durations.length ? durations.reduce((a, b) => a + b, 0) / durations.length : 0;
  console.log(`\nProcessed:  ${processed}`);
  console.log(`Skipped:    ${skipped}`);
  console.log(`Errors:     ${errors}`);
  console.log(`Avg time:   ${avgMs.toFixed(0)}ms per doc`);
  console.log(`Output dir: ${OUT_BASE}`);
}

main().catch((e) => {
  console.error("extract failed:", e);
  process.exit(1);
});
