#!/usr/bin/env tsx
/**
 * Pull N mortgage doc samples from Fidlar AVA + run baseline extraction.
 *
 * For each record that has loan_amount + source_url on ava.fidlar.com:
 *   1. Auth to the county's ScrapRelay API (anonymous)
 *   2. Search by document_number to get the internal Fidlar docId
 *   3. Download pages 1-3 as PNGs
 *   4. Run Tesseract OCR on each page
 *   5. Apply regex extractors for rate/term/maturity
 *   6. Also call MundoX 27B vision (if reachable) for a richer draft JSON
 *   7. Save everything into data/labeling-sample/{doc_number}/
 *       ├── page1.png, page2.png, ...
 *       ├── ocr_raw.txt              (full tesseract output)
 *       ├── tesseract_extract.json   (regex-extracted fields)
 *       ├── mundox_extract.json      (vision-LLM draft)
 *       ├── draft.json               (merged best-guess for labeling UI)
 *       └── meta.json                (source metadata, status='draft')
 *
 * This script does NOT update mortgage_records.
 *
 * Usage:
 *   npx tsx scripts/pull-mortgage-samples.ts            # default 20 docs
 *   npx tsx scripts/pull-mortgage-samples.ts --limit 5  # small smoke test
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import Tesseract from "tesseract.js";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

// ─── Config ────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const LIMIT_IDX = args.indexOf("--limit");
const LIMIT = LIMIT_IDX >= 0 ? parseInt(args[LIMIT_IDX + 1] || "20", 10) : 20;

const OUT_DIR = "C:/Users/msanc/mxre/data/labeling-sample";
if (!existsSync(OUT_DIR)) mkdirSync(OUT_DIR, { recursive: true });

const MUNDOX_URL = process.env.MUNDOX_URL || "http://127.0.0.1:18791/v1/chat/completions";
const MUNDOX_MODEL = "mundox";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── Regex extractors (from extract-rates-from-docs.ts) ────────────

function extractRate(text: string): number | null {
  const patterns = [
    /(?:interest\s*rate|annual\s*rate|note\s*rate|rate\s*of)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*%/i,
    /(\d{1,2}\.\d{2,4})\s*%\s*(?:per\s*(?:annum|year))/i,
    /(\d{1,2}\.\d{3})\s*%/,
    /(?:rate|interest)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*(?:%|percent)/i,
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m) {
      const r = parseFloat(m[1]);
      if (r >= 0.5 && r <= 20) return r;
    }
  }
  return null;
}

function extractTerm(text: string): number | null {
  const patterns = [
    /(\d{2,3})\s*(?:monthly\s*payments|monthly\s*installments|consecutive)/i,
    /term\s*(?:of|:)\s*(\d{2,3})\s*months/i,
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m) {
      const n = parseInt(m[1]);
      if (n >= 12 && n <= 480) return n;
    }
  }
  const y = text.match(/(\d{1,2})\s*(?:-?\s*year|yr)\s*(?:term|mortgage|loan|fixed|adjustable)/i);
  if (y) {
    const years = parseInt(y[1]);
    if (years >= 1 && years <= 40) return years * 12;
  }
  return null;
}

function extractMaturityDate(text: string): string | null {
  const m = text.match(/(?:maturity|final\s*payment|due)\s*(?:date)?\s*[:;]?\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/i);
  if (m) {
    const mo = m[1].padStart(2, "0");
    const d = m[2].padStart(2, "0");
    let y = m[3];
    if (y.length === 2) y = (parseInt(y) > 50 ? "19" : "20") + y;
    return `${y}-${mo}-${d}`;
  }
  return null;
}

// ─── MundoX vision call ───────────────────────────────────────────

const MUNDOX_PROMPT = `You are extracting structured data from a scanned county recorder mortgage document image.

Return ONLY a JSON object (no markdown fences, no commentary) with these fields. Use null if not visible.

{
  "document_type": "mortgage" | "deed_of_trust" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "mortgagor(s) full name as written, or null",
  "lender_name": "mortgagee / lender full name as written, or null",
  "loan_amount_cents": "principal amount in CENTS as integer (e.g. 25000000 = $250,000), or null",
  "interest_rate": "annual rate as decimal percent (e.g., 6.875), or null",
  "term_months": "loan term in months as integer (e.g., 360), or null",
  "maturity_date": "YYYY-MM-DD or null",
  "property_address": "subject property address as written, or null",
  "notes": "any important observations, or null"
}

Rules:
- loan_amount_cents is in CENTS (dollars × 100).
- interest_rate is the rate STAMPED on the doc, not an estimate.
- If a field is partial or unclear, use null.
- Return ONLY the JSON object.`;

async function mundoxExtract(imageBuffer: Buffer): Promise<{ raw: string; parsed: any | null; ms: number }> {
  const b64 = imageBuffer.toString("base64");
  const start = Date.now();
  try {
    const resp = await fetch(MUNDOX_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: MUNDOX_MODEL,
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: MUNDOX_PROMPT },
              { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
            ],
          },
        ],
        max_tokens: 700,
        temperature: 0.1,
        chat_template_kwargs: { enable_thinking: false },
      }),
    });
    const data: any = await resp.json();
    const raw = data.choices?.[0]?.message?.content || "";
    let parsed: any = null;
    const m = raw.match(/\{[\s\S]*\}/);
    if (m) {
      try {
        parsed = JSON.parse(m[0]);
      } catch {}
    }
    return { raw, parsed, ms: Date.now() - start };
  } catch (e: any) {
    return { raw: `error: ${e?.message || e}`, parsed: null, ms: Date.now() - start };
  }
}

// ─── Main ─────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Pull mortgage samples + baseline extract\n");
  console.log(`Target: ${LIMIT} docs from Fidlar AVA\n`);

  // Get records with loan_amount on ava.fidlar.com
  const { data: records, error } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date, property_id")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .like("source_url", "%ava.fidlar.com%")
    .order("loan_amount", { ascending: false })
    .limit(LIMIT);

  if (error) {
    console.error("DB error:", error);
    process.exit(1);
  }
  if (!records?.length) {
    console.log("No eligible records.");
    return;
  }

  console.log(`Got ${records.length} candidate records from Supabase.\n`);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });

  let ok = 0;
  let failed = 0;

  for (const rec of records) {
    const countySlug = rec.source_url?.match(/ava\.fidlar\.com\/(\w+)\//)?.[1];
    if (!countySlug) {
      console.log(`  ${rec.document_number}: no county slug`);
      failed++;
      continue;
    }

    const docDir = join(OUT_DIR, `${rec.document_number}`);
    if (existsSync(join(docDir, "draft.json"))) {
      console.log(`  ${rec.document_number}: already processed, skip`);
      ok++;
      continue;
    }

    const apiBase = `https://ava.fidlar.com/${countySlug}/ScrapRelay.WebService.Ava/`;
    console.log(`\n─── ${rec.document_number} | ${countySlug} | $${rec.loan_amount?.toLocaleString()} | ${rec.borrower_name?.slice(0, 30) || "?"}`);

    try {
      // 1. Get anonymous token
      const tokenResp = await fetch(apiBase + "token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "grant_type=password&username=anonymous&password=anonymous",
      });
      if (!tokenResp.ok) {
        console.log(`  token http ${tokenResp.status}`);
        failed++;
        continue;
      }
      const { access_token: token } = (await tokenResp.json()) as any;

      // 2. Search for doc by number
      const searchResp = await fetch(apiBase + "breeze/Search", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          FirstName: "", LastBusinessName: "", StartDate: "", EndDate: "",
          DocumentName: rec.document_number, DocumentType: "",
          SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
          MunicipalityName: "", TractSection: "", TractTownship: "", TractRange: "",
          TractQuarter: "", TractQuarterQuarter: "", Book: "", Page: "",
          LotOfRecord: "", BlockOfRecord: "", AddressNumber: "", AddressDirection: "",
          AddressStreetName: "", TaxId: "",
        }),
      });
      if (!searchResp.ok) {
        console.log(`  search http ${searchResp.status}`);
        failed++;
        continue;
      }
      const searchData: any = await searchResp.json();
      const doc = searchData.DocResults?.[0];
      if (!doc) {
        console.log("  no doc in search results");
        failed++;
        continue;
      }

      const docId = doc.Id;
      const pageCount = doc.ImagePageCount || 1;
      if (!doc.CanViewImage) {
        console.log(`  ${countySlug} county does not allow free image view`);
        failed++;
        continue;
      }
      console.log(`  docId=${docId}  pages=${pageCount}  viewable=yes`);

      // 3. Establish session for images
      const page = await ctx.newPage();
      await page.goto(`https://ava.fidlar.com/${countySlug}/AvaWeb/`, {
        waitUntil: "networkidle",
        timeout: 15000,
      });
      await page.waitForTimeout(800);

      // 4. Download up to 3 pages
      mkdirSync(docDir, { recursive: true });
      const maxPages = Math.min(pageCount, 3);
      const savedPages: string[] = [];
      let fullOcrText = "";

      for (let pg = 0; pg < maxPages; pg++) {
        const imgUrl = `https://ava.fidlar.com/${countySlug}/ScrapRelay.WebService.Ava/breeze/DocumentImage?documentId=${docId}&pageIndex=${pg}`;
        const imgResp = await fetch(imgUrl, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!imgResp.ok) {
          console.log(`    page${pg + 1}: http ${imgResp.status}`);
          continue;
        }
        const imgBuf = Buffer.from(await imgResp.arrayBuffer());
        if (imgBuf.length < 1000) {
          console.log(`    page${pg + 1}: ${imgBuf.length}b (too small)`);
          continue;
        }

        const pagePath = join(docDir, `page${pg + 1}.png`);
        writeFileSync(pagePath, imgBuf);
        savedPages.push(pagePath);
        console.log(`    page${pg + 1}: ${(imgBuf.length / 1024).toFixed(0)}KB saved`);

        // OCR
        try {
          const { data: { text } } = await Tesseract.recognize(imgBuf, "eng", { logger: () => {} });
          fullOcrText += text + "\n";
        } catch (e: any) {
          console.log(`    page${pg + 1}: ocr failed ${e?.message}`);
        }
      }

      await page.close();

      if (savedPages.length === 0) {
        console.log("  no pages saved");
        failed++;
        continue;
      }

      // 5. Regex extract from OCR text
      const tesseractExtract = {
        interest_rate: extractRate(fullOcrText),
        term_months: extractTerm(fullOcrText),
        maturity_date: extractMaturityDate(fullOcrText),
      };
      writeFileSync(join(docDir, "ocr_raw.txt"), fullOcrText);
      writeFileSync(join(docDir, "tesseract_extract.json"), JSON.stringify(tesseractExtract, null, 2));
      console.log(`  tesseract: rate=${tesseractExtract.interest_rate ?? "?"} term=${tesseractExtract.term_months ?? "?"}`);

      // 6. MundoX vision extract on page 1
      const page1Buf = Buffer.from(require("node:fs").readFileSync(savedPages[0]));
      const mundox = await mundoxExtract(page1Buf);
      writeFileSync(join(docDir, "mundox_extract.json"), JSON.stringify(mundox, null, 2));
      if (mundox.parsed) {
        console.log(`  mundox (${mundox.ms}ms): rate=${mundox.parsed.interest_rate ?? "?"} lender=${(mundox.parsed.lender_name || "").slice(0, 25)}`);
      } else {
        console.log(`  mundox (${mundox.ms}ms): no JSON parseable from response`);
      }

      // 7. Merged draft — MundoX fields with tesseract as fallback
      const mx = mundox.parsed || {};
      const draft = {
        document_type: mx.document_type ?? "mortgage",
        recording_date: mx.recording_date ?? (rec.recording_date as string | null),
        document_number: mx.document_number ?? rec.document_number,
        borrower_name: mx.borrower_name ?? rec.borrower_name,
        lender_name: mx.lender_name ?? rec.lender_name,
        loan_amount_cents: mx.loan_amount_cents ?? (rec.loan_amount ? rec.loan_amount * 100 : null),
        interest_rate: mx.interest_rate ?? tesseractExtract.interest_rate,
        term_months: mx.term_months ?? tesseractExtract.term_months,
        maturity_date: mx.maturity_date ?? tesseractExtract.maturity_date,
        property_address: mx.property_address ?? null,
        notes: mx.notes ?? null,
      };
      writeFileSync(join(docDir, "draft.json"), JSON.stringify(draft, null, 2));

      // 8. Meta (for the labeling UI)
      writeFileSync(
        join(docDir, "meta.json"),
        JSON.stringify(
          {
            doc_number: rec.document_number,
            record_id: rec.id,
            county: countySlug,
            source_url: rec.source_url,
            total_pages: pageCount,
            saved_pages: savedPages.length,
            fidlar_doc_id: docId,
            status: "draft",
            labeled_at: null,
            labeled_by: null,
            corrected: null,
            created_at: new Date().toISOString(),
            baseline_sources: {
              supabase_metadata: {
                borrower_name: rec.borrower_name,
                lender_name: rec.lender_name,
                loan_amount: rec.loan_amount,
                recording_date: rec.recording_date,
              },
              tesseract: tesseractExtract,
              mundox_raw_len: mundox.raw.length,
              mundox_ms: mundox.ms,
            },
          },
          null,
          2,
        ),
      );

      ok++;
      console.log(`  ✓ draft saved to ${docDir}`);
    } catch (err: any) {
      console.log(`  error: ${(err?.message || String(err)).slice(0, 100)}`);
      failed++;
    }
  }

  await browser.close();

  console.log(`\n═══════════════════════════════════════`);
  console.log(`  ok:     ${ok}`);
  console.log(`  failed: ${failed}`);
  console.log(`  output: ${OUT_DIR}`);
  console.log(`\n  next: build labeling UI + open in browser`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
