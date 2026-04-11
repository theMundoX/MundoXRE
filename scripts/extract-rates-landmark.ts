#!/usr/bin/env tsx
/**
 * Extract actual interest rates from FL LandmarkWeb documents.
 * LandmarkWeb has free document viewing (no reCAPTCHA, no login required).
 *
 * Flow:
 * 1. Navigate to LandmarkWeb portal
 * 2. Accept disclaimer
 * 3. Search by document number
 * 4. Open document viewer
 * 5. Capture page image
 * 6. OCR to extract interest rate
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
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const rate = parseFloat(match[1]);
      if (rate >= 0.5 && rate <= 20) return rate;
    }
  }
  return null;
}

function extractTerm(text: string): number | null {
  const patterns = [
    /(\d{2,3})\s*(?:monthly\s*payments|monthly\s*installments)/i,
    /term\s*(?:of|:)\s*(\d{2,3})\s*months/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const num = parseInt(match[1]);
      if (num >= 12 && num <= 480) return num;
    }
  }
  return null;
}

async function main() {
  console.log("MXRE — Extract Interest Rates from FL LandmarkWeb Documents\n");

  // Get FL mortgage records with amounts
  const { data: records } = await db.from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 50000)
    .like("source_url", "%levyclerk%")
    .order("loan_amount", { ascending: false })
    .limit(10);

  if (!records?.length) {
    console.log("No Levy County mortgage records to process.");
    return;
  }

  console.log(`Processing ${records.length} documents from Levy County FL...\n`);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Navigate to LandmarkWeb and accept disclaimer
  console.log("Setting up LandmarkWeb session...");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(2000);

  // Accept disclaimer
  try {
    await page.evaluate(() => {
      if (typeof (window as any).SetDisclaimer === "function") {
        (window as any).SetDisclaimer();
      }
    });
    await page.waitForTimeout(2000);
  } catch {}

  let extracted = 0, failed = 0;

  for (const rec of records) {
    console.log(`\n─── Doc# ${rec.document_number} | $${rec.loan_amount?.toLocaleString()} | ${rec.borrower_name?.slice(0, 30)} ───`);

    try {
      // Navigate to document search
      await page.goto("https://online.levyclerk.com/LandmarkWeb/search/index?theme=.blue&section=searchCriteriaInstrumentNumber", { waitUntil: "networkidle", timeout: 15000 });
      await page.waitForTimeout(1000);

      // Fill instrument number
      const cfnInput = page.locator("#instrumentNumber, #instNum, input[name*='instrument']").first();
      if (await cfnInput.count() > 0) {
        await cfnInput.fill(rec.document_number);
      } else {
        console.log("  No instrument number input found");
        failed++;
        continue;
      }

      // Submit search and capture response
      const responsePromise = new Promise<string>((resolve) => {
        const handler = async (resp: any) => {
          if (resp.url().includes("GetSearchResults") || resp.url().includes("SearchResults")) {
            try { resolve(await resp.text()); } catch { resolve(""); }
            page.off("response", handler);
          }
        };
        page.on("response", handler);
        setTimeout(() => resolve(""), 15000);
      });

      await page.locator("input[type='submit'], button[type='submit'], #submitRow button").first().click();
      const resultsHtml = await responsePromise;
      await page.waitForTimeout(2000);

      // Find internal document ID from results
      const idMatch = resultsHtml.match(/hidden_(\d+)/);
      if (!idMatch) {
        console.log("  No document ID in search results");
        failed++;
        continue;
      }

      const docId = idMatch[1];
      console.log(`  Internal ID: ${docId}`);

      // Set up document navigation
      await page.evaluate(async (id) => {
        try {
          await fetch(`/LandmarkWeb/Document/SetSessionNavigation`, { method: "POST", credentials: "include" });
          await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
        } catch {}
      }, docId);
      await page.waitForTimeout(1000);

      // Get document image (page 0 = first page)
      const imgUrl = `https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/?documentId=${docId}&index=0&pageNum=0&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;

      const imgBuffer = await page.evaluate(async (url) => {
        const r = await fetch(url, { credentials: "include" });
        if (!r.ok) return null;
        const buffer = await r.arrayBuffer();
        return Array.from(new Uint8Array(buffer));
      }, imgUrl);

      if (!imgBuffer) {
        console.log("  Failed to download page image");
        failed++;
        continue;
      }

      const buffer = Buffer.from(imgBuffer);
      console.log(`  Page 1: ${(buffer.length / 1024).toFixed(0)}KB — OCR...`);

      if (buffer.length < 1000) {
        console.log("  Image too small, likely error page");
        failed++;
        continue;
      }

      // OCR the image
      const { data: { text } } = await Tesseract.recognize(buffer, "eng");

      const rate = extractRate(text);
      const term = extractTerm(text);

      if (rate) {
        console.log(`  FOUND: Rate = ${rate}%${term ? ` | Term = ${term} months` : ""}`);

        // Calculate real payment
        const monthlyRate = rate / 100 / 12;
        const termMonths = term || 360;
        const payment = Math.round(rec.loan_amount * (monthlyRate * Math.pow(1 + monthlyRate, termMonths)) / (Math.pow(1 + monthlyRate, termMonths) - 1));

        await db.from("mortgage_records").update({
          interest_rate: rate,
          term_months: termMonths,
          estimated_monthly_payment: payment,
        }).eq("id", rec.id);

        console.log(`  Updated: ${rate}% → $${payment.toLocaleString()}/mo`);
        extracted++;
      } else {
        // Show OCR snippet for debugging
        const snippet = text.replace(/\n/g, " ").slice(0, 200);
        console.log(`  No rate found. OCR: "${snippet}..."`);

        // Try page 2 — sometimes the Note starts on page 2
        const imgUrl2 = `https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/?documentId=${docId}&index=0&pageNum=1&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
        const imgBuffer2 = await page.evaluate(async (url) => {
          const r = await fetch(url, { credentials: "include" });
          if (!r.ok) return null;
          const buffer = await r.arrayBuffer();
          return Array.from(new Uint8Array(buffer));
        }, imgUrl2);

        if (imgBuffer2) {
          const buffer2 = Buffer.from(imgBuffer2);
          if (buffer2.length > 1000) {
            console.log(`  Page 2: ${(buffer2.length / 1024).toFixed(0)}KB — OCR...`);
            const { data: { text: text2 } } = await Tesseract.recognize(buffer2, "eng");
            const rate2 = extractRate(text2);
            if (rate2) {
              console.log(`  FOUND on page 2: Rate = ${rate2}%`);
              const monthlyRate = rate2 / 100 / 12;
              const payment = Math.round(rec.loan_amount * (monthlyRate * Math.pow(1 + monthlyRate, 360)) / (Math.pow(1 + monthlyRate, 360) - 1));
              await db.from("mortgage_records").update({
                interest_rate: rate2,
                estimated_monthly_payment: payment,
              }).eq("id", rec.id);
              console.log(`  Updated: ${rate2}% → $${payment.toLocaleString()}/mo`);
              extracted++;
              continue;
            }
          }
        }
        failed++;
      }
    } catch (err: any) {
      console.log(`  Error: ${err.message.slice(0, 80)}`);
      failed++;
    }
  }

  await browser.close();

  console.log(`\n═══════════════════════════════════════`);
  console.log(`  Extracted: ${extracted} | Failed: ${failed}`);
}

main().catch(console.error);
