#!/usr/bin/env tsx
/**
 * Download mortgage document images from Fidlar AVA and extract actual interest rates via OCR.
 * Uses TapestryLink or direct document image API.
 *
 * Flow:
 * 1. Get mortgage records with loan_amount but no actual interest rate
 * 2. Download first 2-3 pages of the document (rate is usually on page 1)
 * 3. OCR the image
 * 4. Extract interest rate, term, and any other fields
 * 5. Update the mortgage_records table
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import Tesseract from "tesseract.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// Extract interest rate from OCR text
function extractRate(text: string): number | null {
  // Look for patterns like "6.500%", "6.5 %", "Interest Rate: 6.5%", "rate of 6.500 percent"
  const patterns = [
    /(?:interest\s*rate|annual\s*rate|note\s*rate|rate\s*of)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*%/i,
    /(\d{1,2}\.\d{2,4})\s*%\s*(?:per\s*(?:annum|year))/i,
    /(\d{1,2}\.\d{3})\s*%/,  // 6.500% format (3 decimal places = likely interest rate)
    /(?:rate|interest)\s*[:;]?\s*(\d{1,2}\.\d{1,4})\s*(?:%|percent)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const rate = parseFloat(match[1]);
      if (rate >= 0.5 && rate <= 20) return rate; // Sanity check
    }
  }
  return null;
}

// Extract loan term from OCR text
function extractTerm(text: string): number | null {
  const patterns = [
    /(\d{2,3})\s*(?:monthly\s*payments|monthly\s*installments)/i,
    /term\s*(?:of|:)\s*(\d{2,3})\s*months/i,
    /(\d{2,3})\s*months/i,
    /(?:30|15|20|10)\s*(?:-?\s*year|yr)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const num = parseInt(match[1]);
      if (num >= 12 && num <= 480) return num;
    }
  }

  // Check for year-based terms
  const yearMatch = text.match(/(\d{1,2})\s*(?:-?\s*year|yr)\s*(?:term|mortgage|loan|fixed|adjustable)/i);
  if (yearMatch) {
    const years = parseInt(yearMatch[1]);
    if (years >= 1 && years <= 40) return years * 12;
  }

  return null;
}

// Extract maturity date from OCR text
function extractMaturityDate(text: string): string | null {
  const match = text.match(/(?:maturity|final\s*payment|due)\s*(?:date)?\s*[:;]?\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/i);
  if (match) {
    const month = match[1].padStart(2, "0");
    const day = match[2].padStart(2, "0");
    let year = match[3];
    if (year.length === 2) year = (parseInt(year) > 50 ? "19" : "20") + year;
    return `${year}-${month}-${day}`;
  }
  return null;
}

async function main() {
  console.log("MXRE — Extract Actual Interest Rates from Recorded Documents\n");

  // Get Fidlar AVA mortgage records with amounts but no verified rate
  const { data: records } = await db.from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .like("source_url", "%ava.fidlar.com%")
    .order("loan_amount", { ascending: false })
    .limit(20); // Start with 20 documents

  if (!records?.length) {
    console.log("No records to process.");
    return;
  }

  console.log(`Processing ${records.length} mortgage documents...\n`);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });

  let extracted = 0, failed = 0;

  for (const rec of records) {
    const countySlug = rec.source_url.match(/ava\.fidlar\.com\/(\w+)\//)?.[1];
    if (!countySlug) { failed++; continue; }

    const apiBase = `https://ava.fidlar.com/${countySlug}/ScrapRelay.WebService.Ava/`;

    console.log(`\n─── Doc# ${rec.document_number} | $${rec.loan_amount?.toLocaleString()} | ${rec.borrower_name?.slice(0, 30)} ───`);

    try {
      // Get auth token
      const tokenResp = await fetch(apiBase + "token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "grant_type=password&username=anonymous&password=anonymous",
      });
      if (!tokenResp.ok) { console.log("  Token failed"); failed++; continue; }
      const { access_token: token } = await tokenResp.json();

      // Search for the document by number to get the internal ID
      const searchResp = await fetch(apiBase + "breeze/Search", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
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

      if (!searchResp.ok) { console.log("  Search failed"); failed++; continue; }
      const searchData = await searchResp.json();

      if (!searchData.DocResults?.length) { console.log("  No results"); failed++; continue; }

      const doc = searchData.DocResults[0];
      const docId = doc.Id;
      const pageCount = doc.ImagePageCount || 1;

      console.log(`  Internal ID: ${docId} | Pages: ${pageCount} | CanView: ${doc.CanViewImage}`);

      if (!doc.CanViewImage) { console.log("  Document not viewable"); failed++; continue; }

      // Download first 2 pages and OCR them
      const page = await ctx.newPage();
      let fullText = "";

      for (let pg = 0; pg < Math.min(pageCount, 3); pg++) {
        // Navigate to document viewer to establish session
        if (pg === 0) {
          await page.goto(`https://ava.fidlar.com/${countySlug}/AvaWeb/`, { waitUntil: "networkidle", timeout: 15000 });
          await page.waitForTimeout(1000);
        }

        // Get document image via the API
        const imgUrl = `https://ava.fidlar.com/${countySlug}/ScrapRelay.WebService.Ava/breeze/DocumentImage?documentId=${docId}&pageIndex=${pg}`;
        const imgResp = await fetch(imgUrl, {
          headers: { "Authorization": `Bearer ${token}` },
        });

        if (!imgResp.ok) {
          console.log(`  Page ${pg + 1}: fetch failed (${imgResp.status})`);
          continue;
        }

        const imgBuffer = Buffer.from(await imgResp.arrayBuffer());
        if (imgBuffer.length < 1000) {
          console.log(`  Page ${pg + 1}: too small (${imgBuffer.length} bytes)`);
          continue;
        }

        console.log(`  Page ${pg + 1}: ${(imgBuffer.length / 1024).toFixed(0)}KB — OCR...`);

        // OCR the image
        const { data: { text } } = await Tesseract.recognize(imgBuffer, "eng", {
          logger: () => {}, // Suppress progress logs
        });

        fullText += text + "\n";

        // Check if we found a rate on this page
        const rate = extractRate(text);
        if (rate) {
          console.log(`  Found rate on page ${pg + 1}: ${rate}%`);
          break; // No need to OCR more pages
        }
      }

      await page.close();

      // Extract fields from OCR text
      const rate = extractRate(fullText);
      const term = extractTerm(fullText);
      const maturity = extractMaturityDate(fullText);

      if (rate) {
        console.log(`  EXTRACTED: Rate=${rate}% | Term=${term || "N/A"} months | Maturity=${maturity || "N/A"}`);

        // Calculate actual monthly payment with real rate
        const monthlyRate = rate / 100 / 12;
        const termMonths = term || 360;
        const payment = Math.round(rec.loan_amount * (monthlyRate * Math.pow(1 + monthlyRate, termMonths)) / (Math.pow(1 + monthlyRate, termMonths) - 1));

        // Update the record
        const update: Record<string, unknown> = {
          interest_rate: rate,
          term_months: term || 360,
          estimated_monthly_payment: payment,
          maturity_date: maturity,
        };

        const { error } = await db.from("mortgage_records").update(update).eq("id", rec.id);
        if (error) {
          console.log(`  DB update failed: ${error.message.slice(0, 60)}`);
        } else {
          console.log(`  Updated: $${payment}/mo payment`);
          extracted++;
        }
      } else {
        console.log(`  No rate found in OCR text`);
        // Show a snippet of what we got
        const snippet = fullText.slice(0, 200).replace(/\n/g, " ");
        console.log(`  OCR snippet: "${snippet}..."`);
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
