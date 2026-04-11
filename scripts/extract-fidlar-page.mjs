import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const DIR = "C:/Users/msanc/mxre/data/labeling-sample/fidlar-201900009659";
const IMG = join(DIR, "page1.png");

const PROMPT = `You are extracting structured data from a scanned recorded mortgage document. The image is a screenshot of a Fidlar AVA document viewer showing one page of the mortgage — ignore the yellow header/footer UI controls and focus on the actual document content in the middle.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field you cannot clearly read.

{
  "document_type": "mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null (from the Filed for Record stamp)",
  "book_page": "book and page reference if shown (e.g. 'OR Book 1793 Page 1804'), or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "borrower / mortgagor full name as written, or null",
  "lender_name": "lender / mortgagee full name as written, or null",
  "lender_address": "lender mailing address as written, or null",
  "loan_amount_cents": "principal amount in CENTS as integer, or null",
  "interest_rate": "annual rate as decimal percent (e.g., 6.875), or null",
  "term_months": "loan term in months as integer, or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "subject property address as written, or null",
  "property_county": "county the property is in, or null",
  "property_state": "2-letter state code, or null",
  "va_case_number": "VA case number if shown, or null",
  "fha_case_number": "FHA case number if shown, or null",
  "recorder_name": "county recorder name (e.g. 'Lisa McKenzie'), or null",
  "notes": "1-2 sentence summary of what's visible on this page"
}

Rules:
- loan_amount_cents = dollars * 100. Read very carefully — it's usually on page 1 in the "Note" or "Security Instrument" section.
- interest_rate is the rate printed on the doc, not estimated.
- If the page shows only the cover/first page, many fields (rate, term, maturity) will legitimately be null. That's fine.
- Return ONLY the JSON object.`;

const img = readFileSync(IMG);
const b64 = img.toString("base64");
const t0 = Date.now();

const resp = await fetch("http://127.0.0.1:18791/v1/chat/completions", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    model: "mundox",
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text: PROMPT },
          { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
        ],
      },
    ],
    max_tokens: 1200,
    temperature: 0.1,
  }),
});
const data = await resp.json();
const raw = data.choices?.[0]?.message?.content || "";
const elapsed = Date.now() - t0;

console.log(`\n─── RAW MUNDOX RESPONSE (${elapsed}ms) ───`);
console.log(raw);
console.log("─── END ───\n");

const m = raw.match(/\{[\s\S]*\}/);
let parsed = null;
if (m) {
  try {
    parsed = JSON.parse(m[0]);
    console.log("─── PARSED JSON ───");
    console.log(JSON.stringify(parsed, null, 2));
  } catch (e) {
    console.log(`Parse failed: ${e.message}`);
  }
}

writeFileSync(
  join(DIR, "draft.json"),
  JSON.stringify(
    {
      source: "Fidlar AVA — Fairfield County OH — doc 201900009659",
      image_path: IMG,
      mundox_elapsed_ms: elapsed,
      raw_response: raw,
      parsed,
    },
    null,
    2,
  ),
);

console.log(`\nSaved → ${join(DIR, "draft.json")}`);
