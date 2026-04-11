#!/usr/bin/env tsx
/**
 * Download a mortgage document image from LandmarkWeb and extract the interest rate.
 * Uses the free document viewer to get page images, then searches for rate patterns.
 */
import { chromium } from "playwright";
import { writeFileSync } from "node:fs";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Levy County — setup session
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Search for a mortgage
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim()?.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(500);
  await page.fill("#lowerBound", "100000");
  await page.fill("#upperBound", "500000");
  await page.fill("#beginDate-Consideration", "03/19/2026");
  await page.fill("#endDate-Consideration", "03/20/2026");

  const resultPromise = new Promise<string>((resolve) => {
    page.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 25000);
  });
  await page.click("#submit-Consideration");
  const jsonStr = await resultPromise;

  if (!jsonStr) { console.log("No results"); await browser.close(); return; }
  const data = JSON.parse(jsonStr);

  // Find a mortgage doc
  let docId = "";
  let docInfo = { grantor: "", grantee: "", amount: "" };
  for (const row of data.data) {
    const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";
    const cols: string[] = [];
    for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");
    const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
    if (dateIdx < 0) continue;
    if (!cols[dateIdx + 1]?.includes("MORTGAGE")) continue;

    // Get doc ID
    for (let i = 0; i < 30; i++) {
      const raw = row[String(i)] || "";
      const match = raw.match(/hidden_(\d+)/);
      if (match) { docId = match[1]; break; }
    }
    docInfo = { grantor: cols[dateIdx - 2], grantee: cols[dateIdx - 1], amount: cols[dateIdx - 3] };
    if (docId) break;
  }

  if (!docId) { console.log("No mortgage doc found"); await browser.close(); return; }

  console.log(`Found mortgage doc: ${docId}`);
  console.log(`  Borrower: ${docInfo.grantor}`);
  console.log(`  Lender: ${docInfo.grantee}`);
  console.log(`  Amount: ${docInfo.amount}\n`);

  // Navigate to document viewer
  await page.goto(`https://online.levyclerk.com/LandmarkWeb/Document/Index`, { waitUntil: "networkidle", timeout: 30000 });

  // Get page count
  const navUrl = `https://online.levyclerk.com/LandmarkWeb/Document/DocumentNavigation?id=${docId}&row=1&time=${encodeURIComponent(new Date().toString())}`;
  const navResp = await page.evaluate(async (url) => {
    const r = await fetch(url, { credentials: "include" });
    return await r.text();
  }, navUrl);

  // Find total pages
  const pageCountMatch = navResp.match(/(\d+)\s*(?:of|\/)\s*(\d+)/);
  const totalPages = pageCountMatch ? parseInt(pageCountMatch[2]) : 1;
  console.log(`Document has ${totalPages} pages\n`);

  // Download first few pages as images and look for interest rate
  const maxPages = Math.min(totalPages, 3); // First 3 pages usually have the rate
  for (let pg = 0; pg < maxPages; pg++) {
    const imgUrl = `https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/?documentId=${docId}&index=0&pageNum=${pg}&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;

    console.log(`Downloading page ${pg + 1}...`);

    // Download the image
    const imgResp = await page.evaluate(async (url) => {
      const r = await fetch(url, { credentials: "include" });
      const blob = await r.blob();
      const buffer = await blob.arrayBuffer();
      return { size: buffer.byteLength, type: r.headers.get("content-type") };
    }, imgUrl);

    console.log(`  Size: ${imgResp.size} bytes, Type: ${imgResp.type}`);

    // Download actual bytes via page context
    const imgBuffer = await page.evaluate(async (url) => {
      const r = await fetch(url, { credentials: "include" });
      const buffer = await r.arrayBuffer();
      return Array.from(new Uint8Array(buffer));
    }, imgUrl);

    const buffer = Buffer.from(imgBuffer);
    const path = `/tmp/mortgage-doc-page${pg + 1}.png`;
    writeFileSync(path, buffer);
    console.log(`  Saved to: ${path}`);
  }

  console.log("\nDocument images downloaded. To extract interest rate, these would need OCR.");
  console.log("The images are available for visual inspection or Tesseract OCR processing.");

  // Let's also check if the document navigation response has any structured data
  console.log(`\nNavigation response preview: ${navResp.slice(0, 500)}`);

  await browser.close();
}

main().catch(console.error);
