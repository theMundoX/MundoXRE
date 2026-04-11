#!/usr/bin/env tsx
/**
 * Extract interest rates from FL LandmarkWeb — use page interaction to navigate.
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

async function main() {
  console.log("MXRE — Extract Interest Rates v3\n");

  const { data: records } = await db.from("mortgage_records")
    .select("id, document_number, loan_amount, borrower_name")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 50000)
    .like("source_url", "%levyclerk%")
    .order("loan_amount", { ascending: false })
    .limit(5);

  if (!records?.length) { console.log("No records."); return; }

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  // Setup session
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => { try { (window as any).SetDisclaimer(); } catch {} });
  await page.waitForTimeout(3000);

  // Go to search — navigate to instrument number section via URL
  await page.goto("https://online.levyclerk.com/LandmarkWeb/search/index?theme=.blue&section=searchCriteriaInstrumentNumber", { waitUntil: "networkidle", timeout: 15000 });
  await page.waitForTimeout(2000);

  // Check what inputs are visible now
  const inputs = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("input, select, textarea"))
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(i => ({
        id: i.id,
        name: (i as HTMLInputElement).name,
        type: (i as HTMLInputElement).type,
        placeholder: (i as HTMLInputElement).placeholder,
      }));
  });
  console.log("Visible inputs:", inputs.slice(0, 10));

  // Look for the instrument number input
  const cfnInput = page.locator("#instrumentNumber, #instNum, #cfnNumber, input[name*='instrumentNumber'], input[name*='cfn']").first();
  const cfnCount = await cfnInput.count();
  console.log("CFN input found:", cfnCount);

  if (cfnCount === 0) {
    // Try the search navs
    console.log("\nTrying search nav links...");
    const navs = await page.evaluate(() => {
      return Array.from(document.querySelectorAll(".searchNav, a[class*='searchNav']"))
        .map(el => el.textContent?.trim());
    });
    console.log("Nav tabs:", navs);

    // Click the one that says Instrument or CFN
    for (const navText of navs) {
      if (navText?.match(/instrument|cfn|clerk\s*file/i)) {
        console.log(`Clicking "${navText}"...`);
        await page.locator(`.searchNav:has-text("${navText}")`).click();
        await page.waitForTimeout(1000);
        break;
      }
    }

    // Check inputs again
    const inputs2 = await page.evaluate(() => {
      return Array.from(document.querySelectorAll("input"))
        .filter(el => (el as HTMLElement).offsetHeight > 0)
        .map(i => ({ id: i.id, name: i.name, type: i.type, placeholder: i.placeholder }));
    });
    console.log("Inputs after nav click:", inputs2);
  }

  // Now try to find ANY text input and try each one
  const allTextInputs = page.locator("input[type='text']:visible");
  const textCount = await allTextInputs.count();
  console.log(`\nVisible text inputs: ${textCount}`);
  for (let i = 0; i < Math.min(textCount, 5); i++) {
    const input = allTextInputs.nth(i);
    const id = await input.getAttribute("id");
    const name = await input.getAttribute("name");
    console.log(`  Input ${i}: id="${id}" name="${name}"`);
  }

  // For each record, try to search
  let extracted = 0;
  for (const rec of records) {
    console.log(`\n─── CFN ${rec.document_number} | $${rec.loan_amount?.toLocaleString()} ───`);

    // Intercept search results
    const resultPromise = new Promise<string>((resolve) => {
      const handler = async (resp: any) => {
        if (resp.url().includes("GetSearchResults") || resp.url().includes("SearchResults")) {
          try { resolve(await resp.text()); } catch { resolve(""); }
          page.off("response", handler);
        }
      };
      page.on("response", handler);
      setTimeout(() => resolve(""), 15000);
    });

    // Try filling the first visible text input with the CFN
    const firstInput = allTextInputs.first();
    await firstInput.fill(rec.document_number);
    console.log("  Filled input with CFN");

    // Find and click submit
    const submitBtn = page.locator("input[type='submit'], button[type='submit']").first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      console.log("  Clicked submit");
    } else {
      // Try pressing Enter
      await page.keyboard.press("Enter");
      console.log("  Pressed Enter");
    }

    const result = await resultPromise;
    console.log(`  Result length: ${result.length}`);
    if (result) {
      const idMatch = result.match(/hidden_(\d+)/);
      console.log(`  Doc ID: ${idMatch?.[1] || "none"}`);

      if (idMatch) {
        const docId = idMatch[1];

        // Set up document session and get image
        await page.evaluate(async (id) => {
          await fetch("/LandmarkWeb/Document/SetSessionNavigation", { method: "POST", credentials: "include" });
          await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
        }, docId);

        const imgData = await page.evaluate(async (id) => {
          const url = `/LandmarkWeb/Document/GetDocumentImage/?documentId=${id}&index=0&pageNum=0&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
          const r = await fetch(url, { credentials: "include" });
          if (!r.ok) return null;
          const buf = await r.arrayBuffer();
          return Array.from(new Uint8Array(buf));
        }, docId);

        // Scan up to 5 pages to find the interest rate
        let foundRate: number | null = null;
        for (let pg = 0; pg < 5; pg++) {
          const pgData = await page.evaluate(async ({ id, pg }) => {
            const url = `/LandmarkWeb/Document/GetDocumentImage/?documentId=${id}&index=0&pageNum=${pg}&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
            const r = await fetch(url, { credentials: "include" });
            if (!r.ok) return null;
            const buf = await r.arrayBuffer();
            return Array.from(new Uint8Array(buf));
          }, { id: docId, pg });

          if (!pgData || pgData.length < 1000) break;

          const buf = Buffer.from(pgData);
          console.log(`  Page ${pg + 1}: ${(buf.length / 1024).toFixed(0)}KB — OCR...`);
          const { data: { text } } = await Tesseract.recognize(buf, "eng");
          const rate = extractRate(text);
          if (rate) {
            console.log(`  ACTUAL RATE FOUND on page ${pg + 1}: ${rate}%`);
            foundRate = rate;

            // Update DB
            const monthlyRate = rate / 100 / 12;
            const payment = Math.round(rec.loan_amount * (monthlyRate * Math.pow(1 + monthlyRate, 360)) / (Math.pow(1 + monthlyRate, 360) - 1));
            await db.from("mortgage_records").update({
              interest_rate: rate,
              estimated_monthly_payment: payment,
            }).eq("id", rec.id);
            console.log(`  Updated: ${rate}% → $${payment.toLocaleString()}/mo`);
            extracted++;
            break;
          } else {
            const snippet = text.replace(/\n/g, " ").slice(0, 100);
            console.log(`    snippet: "${snippet}..."`);
          }
        }
        if (!foundRate) console.log("  No rate found in first 5 pages");
      }
    }
  }

  await browser.close();
  console.log(`\nExtracted: ${extracted}`);
}

main().catch(console.error);
