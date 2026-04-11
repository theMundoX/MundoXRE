#!/usr/bin/env node
/**
 * Render one sample mortgage PDF → PNG via pdf-to-img,
 * send it to MundoX 27B vision, save image and JSON for user review.
 *
 * Single-doc smoke test to prove the extraction pipeline works.
 */
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { pdf } from "pdf-to-img";

const OUT_DIR = "C:/Users/msanc/mxre/data/labeling-sample/sample-001";
const PDF_PATH = join(OUT_DIR, "source.pdf");
const PNG_PATH = join(OUT_DIR, "page1.png");
const DRAFT_PATH = join(OUT_DIR, "draft.json");
const MUNDOX_URL = "http://127.0.0.1:18791/v1/chat/completions";

if (!existsSync(OUT_DIR)) mkdirSync(OUT_DIR, { recursive: true });

const EXTRACTION_PROMPT = `You are extracting structured data from a mortgage document image.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field that isn't clearly visible.

{
  "document_type": "mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "full borrower / trustor / mortgagor name as written, or null",
  "lender_name": "full lender / beneficiary / mortgagee name as written, or null",
  "loan_amount_cents": "principal amount in CENTS as integer (e.g. 25000000 = $250,000), or null",
  "interest_rate": "annual rate as decimal percent (e.g. 6.875), or null",
  "term_months": "loan term in months as integer (e.g. 360), or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "property address as written on the document, or null",
  "notes": "any important observations about the document, or null"
}

Rules:
- loan_amount_cents = dollars * 100. If the doc shows $250,000 return 25000000.
- interest_rate is the rate printed on the doc, not an estimate.
- If a field is blank or unfilled on the doc, use null.
- Return ONLY the JSON object.`;

async function renderPage1(pdfPath, outPath) {
  const doc = await pdf(pdfPath, { scale: 2 });
  let i = 0;
  for await (const pageBuf of doc) {
    i++;
    if (i === 1) {
      writeFileSync(outPath, pageBuf);
      return { size: pageBuf.length, pages: doc.length };
    }
  }
  throw new Error("No pages rendered");
}

async function mundoxExtract(imagePath) {
  const imgBuf = readFileSync(imagePath);
  const b64 = imgBuf.toString("base64");

  const t0 = Date.now();
  const resp = await fetch(MUNDOX_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "mundox",
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: EXTRACTION_PROMPT },
            { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
          ],
        },
      ],
      max_tokens: 800,
      temperature: 0.1,
      chat_template_kwargs: { enable_thinking: false },
    }),
  });
  const elapsed = Date.now() - t0;

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`MundoX HTTP ${resp.status}: ${errText.slice(0, 300)}`);
  }

  const data = await resp.json();
  const raw = data.choices?.[0]?.message?.content || "";
  let parsed = null;
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      parsed = JSON.parse(m[0]);
    } catch (e) {
      console.log(`    JSON parse failed: ${e.message}`);
    }
  }
  return { raw, parsed, elapsed };
}

async function main() {
  console.log("== MundoX single-doc smoke test ==\n");

  if (!existsSync(PDF_PATH)) {
    console.error(`PDF not found at ${PDF_PATH}`);
    process.exit(1);
  }

  console.log(`[1] Rendering page 1 of ${PDF_PATH} -> ${PNG_PATH}`);
  const info = await renderPage1(PDF_PATH, PNG_PATH);
  console.log(`    ${info.pages} pages total, page 1 = ${(info.size / 1024).toFixed(0)} KB`);

  if (info.size < 10_000) {
    console.log("    WARNING: page 1 is suspiciously small. Probably blank render.");
  }

  console.log(`\n[2] Sending to MundoX 27B vision at ${MUNDOX_URL}`);
  const { raw, parsed, elapsed } = await mundoxExtract(PNG_PATH);
  console.log(`    ${elapsed}ms`);

  console.log("\n─── MUNDOX RAW RESPONSE ───");
  console.log(raw);
  console.log("─── END ───\n");

  if (parsed) {
    console.log("─── PARSED JSON ───");
    console.log(JSON.stringify(parsed, null, 2));
    console.log("─── END ───\n");
  } else {
    console.log("(no JSON parsed)\n");
  }

  writeFileSync(
    DRAFT_PATH,
    JSON.stringify(
      {
        doc_id: "sample-001",
        pdf_path: PDF_PATH,
        image_path: PNG_PATH,
        image_size_bytes: info.size,
        mundox_elapsed_ms: elapsed,
        raw_response: raw,
        parsed,
      },
      null,
      2,
    ),
  );

  console.log("═══════════════════════════════════════");
  console.log(`  Image: ${PNG_PATH}`);
  console.log(`  JSON:  ${DRAFT_PATH}`);
  console.log("═══════════════════════════════════════");
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
