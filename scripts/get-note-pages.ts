#!/usr/bin/env tsx
import { chromium } from "playwright";
import { writeFileSync } from "node:fs";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" });
  const page = await ctx.newPage();

  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  const docId = "928288";
  await page.goto("https://online.levyclerk.com/LandmarkWeb/Document/Index", { waitUntil: "networkidle", timeout: 15000 });
  await page.evaluate(async (id) => {
    await fetch(`/LandmarkWeb/Document/SetSessionNavigation`, { method: "POST", credentials: "include" });
    await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
  }, docId);
  await page.waitForTimeout(1000);

  // Get pages 14-20 (Note section in 21-page document)
  for (let pg = 14; pg <= 20; pg++) {
    const imgUrl = `https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/?documentId=${docId}&index=0&pageNum=${pg}&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;
    const imgBuffer = await page.evaluate(async (url) => {
      const r = await fetch(url, { credentials: "include" });
      if (!r.ok) return null;
      const buffer = await r.arrayBuffer();
      return Array.from(new Uint8Array(buffer));
    }, imgUrl);

    if (!imgBuffer) { console.log(`Page ${pg + 1}: not available`); continue; }
    const buffer = Buffer.from(imgBuffer);
    writeFileSync(`/tmp/mortgage-note-page${pg + 1}.png`, buffer);
    console.log(`Page ${pg + 1}: ${buffer.length} bytes`);
  }

  await browser.close();
}
main().catch(console.error);
