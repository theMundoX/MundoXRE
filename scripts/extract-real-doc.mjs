#!/usr/bin/env node
/**
 * Process a real mortgage/foreclosure PDF end-to-end:
 *   1. Render ALL pages to PNG (not just page 1)
 *   2. Send page 1 to MundoX 27B vision for extraction
 *   3. Save image paths + JSON response for user review
 */
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pdf } from "pdf-to-img";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-001";
const PDF_PATH = join(OUT, "source.pdf");
const DRAFT_PATH = join(OUT, "draft.json");
const MUNDOX_URL = "http://127.0.0.1:18791/v1/chat/completions";

if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

const EXTRACTION_PROMPT = `You are extracting structured data from a real estate / mortgage document. This may be a recorded mortgage, deed of trust, court filing that references a mortgage, foreclosure complaint, lien, or similar instrument.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field that isn't clearly visible in this page.

{
  "document_type": "mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "foreclosure_complaint" | "court_order" | "lien" | "other",
  "is_court_filing": true | false,
  "recording_date": "YYYY-MM-DD or null",
  "document_number": "instrument / recording number if shown, or null",
  "borrower_name": "borrower / mortgagor / defendant name as written, or null",
  "lender_name": "lender / mortgagee / plaintiff name as written, or null",
  "loan_amount_cents": "principal amount in CENTS as integer (e.g., 25000000 for $250,000), or null",
  "interest_rate": "annual rate as decimal percent (e.g., 6.875), or null",
  "term_months": "loan term in months as integer (e.g., 360 for 30-year), or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "subject property address as written, or null",
  "case_number": "court case number if this is a court filing, or null",
  "notes": "key observations about this document in 1-2 sentences"
}

Rules:
- loan_amount_cents is in CENTS (dollars * 100).
- interest_rate is the rate shown on the doc, not an estimate.
- If a field is blank or not visible on this page, use null.
- Return ONLY the JSON object.`;

async function renderAllPages(pdfPath) {
  const doc = await pdf(pdfPath, { scale: 2 });
  const paths = [];
  let i = 0;
  for await (const pageBuf of doc) {
    i++;
    const p = join(OUT, `page${i}.png`);
    writeFileSync(p, pageBuf);
    paths.push({ page: i, path: p, size: pageBuf.length });
    console.log(`    page${i}: ${(pageBuf.length / 1024).toFixed(0)} KB → ${p}`);
  }
  return { paths, total: doc.length };
}

async function callMundox(prompt, imageB64, maxTokens = 1500) {
  const resp = await fetch(MUNDOX_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "mundox",
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: prompt },
            { type: "image_url", image_url: { url: `data:image/png;base64,${imageB64}` } },
          ],
        },
      ],
      max_tokens: maxTokens,
      temperature: 0.1,
      chat_template_kwargs: { enable_thinking: false },
    }),
  });
  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`MundoX HTTP ${resp.status}: ${errText.slice(0, 300)}`);
  }
  const data = await resp.json();
  return {
    content: data.choices?.[0]?.message?.content || "",
    finish_reason: data.choices?.[0]?.finish_reason || "?",
    usage: data.usage || {},
  };
}

async function mundoxExtract(imagePath) {
  const imgBuf = readFileSync(imagePath);
  const b64 = imgBuf.toString("base64");

  const t0 = Date.now();
  // First try: structured JSON extraction prompt with 1500 tokens
  let r = await callMundox(EXTRACTION_PROMPT, b64, 1500);
  let raw = r.content;
  let finish = r.finish_reason;

  // Retry path 1: if empty, try with a simpler "describe this page" prompt
  if (!raw || raw.length < 20) {
    console.log(`    empty response on first try (finish=${finish}), retrying with simpler prompt`);
    const simple = "Describe what's visible on this document page in 150 words or less. Then list any names, dates, dollar amounts, and percentages you see.";
    const r2 = await callMundox(simple, b64, 600);
    raw = r2.content;
    finish = r2.finish_reason;
  }

  // Retry path 2: if still empty, try a 1-line tiny prompt
  if (!raw || raw.length < 20) {
    console.log(`    still empty, retrying with minimal prompt`);
    const r3 = await callMundox("What is on this page? Answer in one sentence.", b64, 150);
    raw = r3.content;
    finish = r3.finish_reason;
  }

  const elapsed = Date.now() - t0;

  // Try to parse JSON out of whatever we got
  let parsed = null;
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      parsed = JSON.parse(m[0]);
    } catch (e) {
      // not a JSON response, that's fine
    }
  }
  return { raw, parsed, elapsed, finish_reason: finish };
}

async function main() {
  console.log("== Real doc extraction ==\n");

  if (!existsSync(PDF_PATH)) {
    console.error(`No PDF at ${PDF_PATH}`);
    process.exit(1);
  }

  console.log(`[1] Rendering all pages of ${PDF_PATH}`);
  const { paths, total } = await renderAllPages(PDF_PATH);
  console.log(`    total pages rendered: ${paths.length}`);

  const extractions = [];
  for (const p of paths) {
    console.log(`\n[2] MundoX extract from page ${p.page}`);
    const { raw, parsed, elapsed } = await mundoxExtract(p.path);
    console.log(`    ${elapsed}ms`);
    extractions.push({
      page: p.page,
      image_path: p.path,
      mundox_elapsed_ms: elapsed,
      raw_response: raw,
      parsed,
    });
    if (parsed) {
      // Short summary line
      const summary = [
        parsed.document_type && `type=${parsed.document_type}`,
        parsed.borrower_name && `borrower=${String(parsed.borrower_name).slice(0, 30)}`,
        parsed.lender_name && `lender=${String(parsed.lender_name).slice(0, 30)}`,
        parsed.loan_amount_cents && `$${(Number(parsed.loan_amount_cents) / 100).toLocaleString()}`,
        parsed.interest_rate && `${parsed.interest_rate}%`,
        parsed.case_number && `case=${parsed.case_number}`,
      ].filter(Boolean).join("  ");
      console.log(`    parsed: ${summary}`);
    } else {
      console.log(`    (no JSON parsed, raw head: ${raw.slice(0, 120)})`);
    }
  }

  writeFileSync(
    DRAFT_PATH,
    JSON.stringify(
      {
        doc_id: "real-001",
        source: "US Bank Trust NA v. Mary E. Dudla Family Trust (CourtListener)",
        pdf_path: PDF_PATH,
        total_pages: paths.length,
        page_images: paths.map((p) => p.path),
        extractions,
      },
      null,
      2,
    ),
  );

  console.log(`\n═══════════════════════════════════════`);
  console.log(`  PDF:     ${PDF_PATH}`);
  console.log(`  Pages:   ${paths.length}`);
  for (const p of paths) console.log(`    ${p.path}`);
  console.log(`  JSON:    ${DRAFT_PATH}`);
  console.log(`═══════════════════════════════════════`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
