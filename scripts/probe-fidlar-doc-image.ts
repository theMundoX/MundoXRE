#!/usr/bin/env tsx
/**
 * Find the correct document image URL by watching network requests
 * when viewing a document in the Fidlar AVA portal.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Use Fairfield County OH — we know doc 202601044784 exists (Perkins Brian P mortgage)
  const baseUrl = "https://ava.fidlar.com/OHFairfield/AvaWeb/";

  // Track ALL network requests
  const requests: string[] = [];
  page.on("request", (req) => {
    const url = req.url();
    if (url.includes("image") || url.includes("Image") || url.includes("document") || url.includes("Document") || url.includes("page") || url.includes("Page") || url.includes("view") || url.includes("View") || url.includes("tiff") || url.includes("png") || url.includes("jpg") || url.includes("pdf")) {
      requests.push(`${req.method()} ${url}`);
    }
  });

  page.on("response", async (resp) => {
    const url = resp.url();
    const ct = resp.headers()["content-type"] || "";
    if (ct.includes("image") || ct.includes("pdf") || ct.includes("tiff")) {
      console.log(`  IMAGE RESPONSE: [${resp.status()}] ${ct} ${url.slice(0, 120)}`);
    }
  });

  await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(2000);

  // Search for a specific document
  console.log("Searching for doc 202601044784...");
  const docInput = page.locator('input[placeholder="Document Number"]');
  await docInput.fill("202601044784");
  await page.locator('button:has-text("Search")').first().click();
  await page.waitForTimeout(5000);

  // Click the first result to open document viewer
  console.log("Looking for result to click...");
  const resultRow = page.locator('mat-row, tr, [class*="result"], [class*="row"]').first();
  if (await resultRow.count() > 0) {
    await resultRow.click();
    await page.waitForTimeout(3000);
  }

  // Check for a document viewer or image
  const viewerInfo = await page.evaluate(() => {
    const imgs = document.querySelectorAll("img");
    const iframes = document.querySelectorAll("iframe");
    const canvases = document.querySelectorAll("canvas");
    return {
      images: Array.from(imgs).map(i => ({ src: i.src?.slice(0, 100), w: i.width, h: i.height })).filter(i => i.w > 100),
      iframes: Array.from(iframes).map(i => i.src?.slice(0, 100)),
      canvases: canvases.length,
      url: window.location.href,
    };
  });

  console.log("\nViewer state:");
  console.log("  URL:", viewerInfo.url);
  console.log("  Large images:", viewerInfo.images);
  console.log("  Iframes:", viewerInfo.iframes);
  console.log("  Canvases:", viewerInfo.canvases);

  console.log("\nDocument/image related requests:");
  for (const r of requests) {
    console.log("  " + r.slice(0, 120));
  }

  // Also check TapestryLink
  console.log("\nTrying TapestryLink...");
  const tapestryUrl = "https://tapestry.fidlar.com/Tapestry2/LinkToTapestry.aspx?County=Fairfield&State=OH&DocNum=202601044784";
  await page.goto(tapestryUrl, { waitUntil: "networkidle", timeout: 15000 });
  console.log("Tapestry URL:", page.url());
  console.log("Tapestry title:", await page.title());

  const tapestryInfo = await page.evaluate(() => {
    const imgs = document.querySelectorAll("img");
    const iframes = document.querySelectorAll("iframe");
    return {
      images: Array.from(imgs).map(i => ({ src: i.src?.slice(0, 120), w: i.width, h: i.height })).filter(i => i.w > 50),
      iframes: Array.from(iframes).map(i => i.src?.slice(0, 120)),
      bodyText: document.body.innerText.slice(0, 300),
    };
  });
  console.log("Tapestry images:", tapestryInfo.images);
  console.log("Tapestry iframes:", tapestryInfo.iframes);
  console.log("Tapestry text:", tapestryInfo.bodyText.slice(0, 200));

  await browser.close();
}

main().catch(console.error);
