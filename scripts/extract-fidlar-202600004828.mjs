import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const DIR = "C:/Users/msanc/mxre/data/labeling-sample/fidlar-202600004828-full";
const IMG = join(DIR, "page01.png");

const PROMPT = `You are extracting structured data from a scanned recorded mortgage document. Focus on all the real content — this is a real page from a recorded instrument.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field you cannot clearly read.

{
  "document_type": "mortgage" | "open_end_mortgage" | "construction_mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "book_page": "book/page reference if shown, or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "borrower / mortgagor full name as written (all borrowers), or null",
  "lender_name": "lender / mortgagee name as written, or null",
  "lender_address": "lender address as written, or null",
  "loan_amount_cents": "principal amount in CENTS as integer (dollars*100), or null",
  "max_principal_cents": "for open-end: maximum principal in CENTS, or null",
  "interest_rate": "annual rate as decimal percent, or null",
  "term_months": "loan term in months as integer, or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "subject property address as written, or null",
  "property_county": "county name, or null",
  "property_state": "2-letter state code, or null",
  "trustee_name": "trustee name if deed of trust, or null",
  "signing_date": "YYYY-MM-DD the doc was signed, or null",
  "notes": "1-2 sentence summary of what this page shows"
}

Rules:
- loan_amount_cents = dollars * 100. Read carefully.
- For an OPEN-END mortgage, max_principal_cents is the credit line max (e.g., HELOC limit).
- If this is the cover page, many fields (rate, term, maturity) may be on later pages — use null.
- Return ONLY the JSON object.`;

const img = readFileSync(IMG);
const b64 = img.toString("base64");
const t0 = Date.now();

const resp = await fetch("http://127.0.0.1:18791/v1/chat/completions", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    model: "mundox",
    messages: [{
      role: "user",
      content: [
        { type: "text", text: PROMPT },
        { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
      ],
    }],
    max_tokens: 1200,
    temperature: 0.1,
  }),
});
const data = await resp.json();
const raw = data.choices?.[0]?.message?.content || "";
const elapsed = Date.now() - t0;

console.log(`\n─── MUNDOX RESPONSE (${elapsed}ms) ───`);
console.log(raw);
console.log("─── END ───\n");

const m = raw.match(/\{[\s\S]*\}/);
let parsed = null;
if (m) {
  try { parsed = JSON.parse(m[0]); } catch {}
}

writeFileSync(
  join(DIR, "draft.json"),
  JSON.stringify({ source: "Fidlar Fairfield OH 202600004828", image_path: IMG, mundox_elapsed_ms: elapsed, raw_response: raw, parsed }, null, 2)
);
console.log(`Saved → ${join(DIR, "draft.json")}`);
