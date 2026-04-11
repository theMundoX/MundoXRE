#!/usr/bin/env tsx
/**
 * Download page 2 of the mortgage document to find the interest rate.
 * Page 1 = Mortgage (security instrument). Page 2+ = the actual Note with rate/terms.
 * But this doc is 21 pages — the rate is likely on page ~15-17 (Note section).
 * Actually, for Fannie/Freddie uniform instruments, the Mortgage references the Note
 * but the Note is a SEPARATE document. The rate might be stated in the Mortgage body.
 * Let's check pages 2-3.
 */
import { chromium } from "playwright";
import { writeFileSync } from "node:fs";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  const docId = "928288"; // The mortgage we found earlier
  const baseImgUrl = "https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/";

  // First set up the session by navigating to the doc
  await page.goto("https://online.levyclerk.com/LandmarkWeb/Document/Index", { waitUntil: "networkidle", timeout: 15000 });

  // Set the document in the session
  await page.evaluate(async (id) => {
    await fetch(`/LandmarkWeb/Document/SetSessionNavigation`, { method: "POST", credentials: "include" });
    await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
  }, docId);
  await page.waitForTimeout(1000);

  // Download pages 2-5 (rate is usually early in the document)
  for (let pg = 1; pg <= 5; pg++) {
    const imgUrl = `${baseImgUrl}?documentId=${docId}&index=0&pageNum=${pg}&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;

    const imgBuffer = await page.evaluate(async (url) => {
      const r = await fetch(url, { credentials: "include" });
      if (!r.ok) return null;
      const buffer = await r.arrayBuffer();
      return Array.from(new Uint8Array(buffer));
    }, imgUrl);

    if (!imgBuffer) {
      console.log(`Page ${pg + 1}: not available`);
      continue;
    }

    const buffer = Buffer.from(imgBuffer);
    const path = `/tmp/mortgage-doc-page${pg + 1}.png`;
    writeFileSync(path, buffer);
    console.log(`Page ${pg + 1}: ${buffer.length} bytes → ${path}`);
  }

  console.log("\nDone. Check /tmp/mortgage-doc-page*.png for interest rate.");
  await browser.close();
}

main().catch(console.error);
