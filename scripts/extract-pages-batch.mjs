#!/usr/bin/env node
/**
 * Batch extract structured JSON from already-captured page images.
 *
 * Reads data/labeling-sample/{doc_dir}/page*.png
 * Skips pages < 50 KB (blank separators)
 * Runs MundoX extraction with retry on connection failures
 * Rate-limits to 1 request every 4 seconds to avoid crashing llama-server
 * Merges per-page results into a single doc record
 */
import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const DOC_DIR = process.argv[2] || "C:/Users/msanc/mxre/data/labeling-sample/fidlar-201900009659-multipage";
const MUNDOX_URL = "http://127.0.0.1:18791/v1/chat/completions";
const MIN_PAGE_BYTES = 50_000; // skip blank separator pages
const RATE_LIMIT_MS = 4000;
const MAX_RETRIES = 3;

const PROMPT = `You are extracting structured data from a scanned recorded mortgage document page. Focus on the actual document content — ignore the yellow viewer UI around the edges.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field you cannot clearly read on THIS page.

{
  "document_type": "mortgage" | "open_end_mortgage" | "deed_of_trust" | "heloc" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "book_page": "book/page ref if shown, or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "borrower / mortgagor full name(s), or null",
  "lender_name": "lender / mortgagee full name, or null",
  "lender_address": "lender address, or null",
  "loan_amount_cents": "principal amount in CENTS (dollars*100), or null",
  "max_principal_cents": "for open-end: credit line max in CENTS, or null",
  "interest_rate": "annual rate as decimal percent (e.g. 6.875), or null",
  "interest_rate_type": "fixed | adjustable | variable | null",
  "term_months": "loan term in months, or null",
  "maturity_date": "YYYY-MM-DD, or null",
  "monthly_payment_cents": "monthly P&I payment in CENTS, or null",
  "property_address": "full subject property address, or null",
  "property_county": "county, or null",
  "property_state": "2-letter state, or null",
  "parcel_id": "tax parcel ID, or null",
  "signing_date": "YYYY-MM-DD, or null",
  "notes": "1-sentence summary of this specific page"
}

Rules:
- loan_amount_cents and monthly_payment_cents are in CENTS.
- interest_rate is the rate PRINTED on the doc, not estimated.
- Only fill in what you can clearly see on THIS specific page.
- Return ONLY the JSON object.`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function callOnce(imgB64) {
  const resp = await fetch(MUNDOX_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "mundox",
      messages: [{
        role: "user",
        content: [
          { type: "text", text: PROMPT },
          { type: "image_url", image_url: { url: `data:image/png;base64,${imgB64}` } },
        ],
      }],
      max_tokens: 1500,
      temperature: 0.1,
    }),
    signal: AbortSignal.timeout(60000),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text().then((t) => t.slice(0, 200))}`);
  const data = await resp.json();
  return data.choices?.[0]?.message?.content || "";
}

async function extractWithRetry(imgBuf, pageNum) {
  const b64 = imgBuf.toString("base64");
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const t0 = Date.now();
      const raw = await callOnce(b64);
      const elapsed = Date.now() - t0;
      let parsed = null;
      const m = raw.match(/\{[\s\S]*\}/);
      if (m) {
        try { parsed = JSON.parse(m[0]); } catch {}
      }
      return { raw, parsed, elapsed };
    } catch (e) {
      console.log(`  page ${pageNum} attempt ${attempt}/${MAX_RETRIES} failed: ${e.message || e}`);
      if (attempt < MAX_RETRIES) {
        await sleep(5000 * attempt);
      } else {
        return { raw: `error: ${e.message}`, parsed: null, elapsed: 0, error: true };
      }
    }
  }
}

function mergeExtractions(perPage) {
  const merged = {};
  const keys = new Set();
  for (const p of perPage) if (p) for (const k of Object.keys(p)) keys.add(k);
  for (const k of keys) {
    for (const p of perPage) {
      if (p && p[k] !== null && p[k] !== undefined && p[k] !== "") {
        merged[k] = p[k];
        break;
      }
    }
    if (merged[k] === undefined) merged[k] = null;
  }
  return merged;
}

async function main() {
  console.log(`Extracting from ${DOC_DIR}\n`);

  // Find all page*.png files
  const allFiles = readdirSync(DOC_DIR).filter((f) => /^page\d+\.png$/.test(f)).sort();
  console.log(`Found ${allFiles.length} page files`);

  const perPage = [];
  const perPageDetails = [];

  for (const fname of allFiles) {
    const p = join(DOC_DIR, fname);
    const sz = statSync(p).size;
    const pageNum = parseInt(fname.match(/\d+/)[0]);

    if (sz < MIN_PAGE_BYTES) {
      console.log(`page${pageNum}: skip (${(sz/1024).toFixed(0)} KB = blank separator)`);
      perPage.push(null);
      perPageDetails.push({ page: pageNum, skipped: true, size: sz });
      continue;
    }

    console.log(`page${pageNum}: extracting (${(sz/1024).toFixed(0)} KB) ...`);
    const imgBuf = readFileSync(p);
    const { raw, parsed, elapsed, error } = await extractWithRetry(imgBuf, pageNum);
    perPage.push(parsed);
    perPageDetails.push({ page: pageNum, size: sz, elapsed_ms: elapsed, raw, parsed, error });

    if (parsed) {
      const hits = Object.entries(parsed).filter(([k, v]) => v !== null && v !== "" && !["document_type", "notes"].includes(k));
      const summary = hits.map(([k, v]) => `${k}=${String(v).slice(0, 30)}`).join(" ");
      console.log(`         ${elapsed}ms  ${summary.slice(0, 200)}`);
    } else if (error) {
      console.log(`         ERROR after retries`);
    } else {
      console.log(`         no JSON parsed`);
    }

    await sleep(RATE_LIMIT_MS);
  }

  const merged = mergeExtractions(perPage);

  console.log("\n═══ MERGED ═══");
  console.log(JSON.stringify(merged, null, 2));

  const out = {
    doc_dir: DOC_DIR,
    total_files: allFiles.length,
    extracted_pages: perPage.filter(Boolean).length,
    per_page: perPageDetails,
    merged,
    generated_at: new Date().toISOString(),
  };
  writeFileSync(join(DOC_DIR, "merged.json"), JSON.stringify(out, null, 2));
  console.log(`\nSaved → ${join(DOC_DIR, "merged.json")}`);
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
