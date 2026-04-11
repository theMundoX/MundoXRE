#!/usr/bin/env tsx
/**
 * Extract interest rates from FL LandmarkWeb documents via direct API calls.
 * No form interaction — just API calls for search + document image.
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import Tesseract from "tesseract.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

function extractRate(text: string): number | null {
  const patterns = [
    /(?:interest\s*rate|annual\s*rate|note\s*rate|rate\s*of)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*%/i,
    /(\d{1,2}\.\d{2,4})\s*%\s*(?:per\s*(?:annum|year))/i,
    /(\d{1,2}\.\d{3})\s*%/,
    /(?:rate|interest)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*(?:%|percent)/i,
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m) { const r = parseFloat(m[1]); if (r >= 0.5 && r <= 20) return r; }
  }
  return null;
}

function extractTerm(text: string): number | null {
  const m = text.match(/(\d{2,3})\s*(?:monthly\s*payments|monthly\s*installments|consecutive)/i);
  if (m) { const n = parseInt(m[1]); if (n >= 12 && n <= 480) return n; }
  return null;
}

async function main() {
  console.log("MXRE — Extract Interest Rates (LandmarkWeb Direct API)\n");

  const { data: records } = await db.from("mortgage_records")
    .select("id, document_number, loan_amount, borrower_name, lender_name, recording_date, source_url")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 50000)
    .like("source_url", "%levyclerk%")
    .order("loan_amount", { ascending: false })
    .limit(10);

  if (!records?.length) { console.log("No records."); return; }
  console.log(`${records.length} documents to process.\n`);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  // Setup session — navigate and accept disclaimer
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => { try { (window as any).SetDisclaimer(); } catch {} });
  await page.waitForTimeout(3000);

  // Navigate to main search page to establish cookies
  await page.goto("https://online.levyclerk.com/LandmarkWeb/search/index", { waitUntil: "networkidle", timeout: 15000 });
  await page.waitForTimeout(1000);

  let extracted = 0, failed = 0;

  for (const rec of records) {
    console.log(`─── CFN ${rec.document_number} | $${rec.loan_amount?.toLocaleString()} | ${rec.borrower_name?.slice(0, 25)} ───`);

    try {
      // Use the InstrumentNumber search API directly
      const searchResult = await page.evaluate(async (cfn) => {
        // Set the search criteria
        const r1 = await fetch("/LandmarkWeb/search/SetSearchCriteria", {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          credentials: "include",
          body: `SrchType=instrumentNumber&CaseKey=&TotalRows=&searchCriteria=${encodeURIComponent(cfn)}`,
        });

        // Get results
        const r2 = await fetch("/LandmarkWeb/search/GetSearchResults", {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          credentials: "include",
          body: "page=1",
        });
        return await r2.text();
      }, rec.document_number);

      // Find the internal document ID
      const idMatch = searchResult.match(/hidden_(\d+)/);
      if (!idMatch) {
        console.log("  No doc ID in results");
        failed++;
        continue;
      }

      const docId = idMatch[1];

      // Set up document navigation session
      await page.evaluate(async (id) => {
        await fetch("/LandmarkWeb/Document/SetSessionNavigation", { method: "POST", credentials: "include" });
        await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
      }, docId);
      await page.waitForTimeout(500);

      // Download first page image
      const imgData = await page.evaluate(async (id) => {
        const url = `/LandmarkWeb/Document/GetDocumentImage/?documentId=${id}&index=0&pageNum=0&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
        const r = await fetch(url, { credentials: "include" });
        if (!r.ok) return null;
        const buf = await r.arrayBuffer();
        return Array.from(new Uint8Array(buf));
      }, docId);

      if (!imgData || imgData.length < 1000) {
        console.log("  Image download failed");
        failed++;
        continue;
      }

      const buffer = Buffer.from(imgData);
      console.log(`  Page 1: ${(buffer.length / 1024).toFixed(0)}KB — OCR...`);

      const { data: { text } } = await Tesseract.recognize(buffer, "eng");
      let rate = extractRate(text);
      let term = extractTerm(text);

      // If not found on page 1, try page 2
      if (!rate) {
        const imgData2 = await page.evaluate(async (id) => {
          const url = `/LandmarkWeb/Document/GetDocumentImage/?documentId=${id}&index=0&pageNum=1&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
          const r = await fetch(url, { credentials: "include" });
          if (!r.ok) return null;
          const buf = await r.arrayBuffer();
          return Array.from(new Uint8Array(buf));
        }, docId);

        if (imgData2 && imgData2.length > 1000) {
          console.log(`  Page 2: ${(Buffer.from(imgData2).length / 1024).toFixed(0)}KB — OCR...`);
          const { data: { text: text2 } } = await Tesseract.recognize(Buffer.from(imgData2), "eng");
          rate = extractRate(text2);
          if (!term) term = extractTerm(text2);
        }
      }

      if (rate) {
        const monthlyRate = rate / 100 / 12;
        const termMonths = term || 360;
        const payment = Math.round(rec.loan_amount * (monthlyRate * Math.pow(1 + monthlyRate, termMonths)) / (Math.pow(1 + monthlyRate, termMonths) - 1));

        await db.from("mortgage_records").update({
          interest_rate: rate,
          term_months: termMonths,
          estimated_monthly_payment: payment,
        }).eq("id", rec.id);

        console.log(`  ACTUAL RATE: ${rate}%${term ? ` | Term: ${term}mo` : ""} | Payment: $${payment.toLocaleString()}/mo`);
        extracted++;
      } else {
        const snippet = text.replace(/\n/g, " ").slice(0, 150);
        console.log(`  No rate found. OCR: "${snippet}..."`);
        failed++;
      }
    } catch (err: any) {
      console.log(`  Error: ${err.message.slice(0, 60)}`);
      failed++;
    }
  }

  await browser.close();
  console.log(`\n  Extracted: ${extracted} | Failed: ${failed}`);
}

main().catch(console.error);
