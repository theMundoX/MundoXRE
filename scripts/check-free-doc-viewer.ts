#!/usr/bin/env tsx
/**
 * Check if LandmarkWeb portals offer free document viewing.
 * Navigate to a document and see if we can view the PDF/image without paying.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Levy County
  console.log("═══ LEVY COUNTY — Checking free document access ═══\n");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Get search results first
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim() === "Record Date Search" && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(500);
  await page.fill("#beginDate-RecordDate", "03/20/2026");
  await page.fill("#endDate-RecordDate", "03/20/2026");
  await page.click("#submit-RecordDate");

  // Wait for results
  await page.waitForFunction(() => document.querySelectorAll("#resultsTable tbody tr").length > 0, { timeout: 20000 });
  await page.waitForTimeout(2000);

  // Monitor all network requests to find document image URLs
  const docRequests: string[] = [];
  page.on("response", async (resp) => {
    const url = resp.url();
    const ct = resp.headers()["content-type"] || "";
    if (ct.includes("pdf") || ct.includes("image/tiff") || ct.includes("image/png") ||
        url.includes("Document") || url.includes("Image") || url.includes("View")) {
      docRequests.push(`[${resp.status()}] ${ct} ${url.slice(0, 120)}`);
    }
  });

  // Try clicking the "eye" icon or view link on the first result
  console.log("Attempting to view first document...");

  // Find and click view button
  const clicked = await page.evaluate(() => {
    // Look for view/eye icons in the results table
    const icons = document.querySelectorAll("#resultsTable tbody tr:first-child i, #resultsTable tbody tr:first-child a, #resultsTable tbody tr:first-child button");
    for (const icon of icons) {
      const title = icon.getAttribute("title") || icon.textContent || "";
      const cls = icon.className || "";
      if (title.includes("View") || title.includes("eye") || cls.includes("view") || cls.includes("eye")) {
        (icon as HTMLElement).click();
        return `Clicked: ${icon.tagName} title="${title}" class="${cls}"`;
      }
    }
    // Try clicking any link in the first result
    const link = document.querySelector("#resultsTable tbody tr:first-child a");
    if (link) {
      (link as HTMLElement).click();
      return `Clicked link: ${link.getAttribute("href")?.slice(0, 60)}`;
    }
    // Try clicking the row itself
    const row = document.querySelector("#resultsTable tbody tr:first-child");
    if (row) {
      (row as HTMLElement).click();
      return "Clicked row";
    }
    return "Nothing to click";
  });
  console.log(`  ${clicked}`);

  await page.waitForTimeout(5000);

  // Check if a viewer opened
  const viewerState = await page.evaluate(() => {
    // Check for iframe (PDF viewer)
    const iframes = document.querySelectorAll("iframe");
    const iframeSrcs = Array.from(iframes).map(f => f.src).filter(s => s);

    // Check for image elements that might be document pages
    const images = document.querySelectorAll("img[src*='Document'], img[src*='Image'], img[src*='page']");
    const imgSrcs = Array.from(images).map(i => (i as HTMLImageElement).src).filter(s => s);

    // Check for canvas elements (PDF.js viewer)
    const canvases = document.querySelectorAll("canvas");

    // Check for any modal or overlay
    const modals = document.querySelectorAll('.modal.show, [class*="viewer"], [class*="Viewer"], [id*="viewer"]');
    const modalTexts = Array.from(modals).map(m => m.textContent?.trim().slice(0, 100));

    // Check for payment/login required messages
    const bodyText = document.body.innerText;
    const needsPayment = bodyText.match(/(payment|purchase|buy|escrow|balance|login|sign in|register|subscription)/i);

    return {
      iframes: iframeSrcs,
      images: imgSrcs,
      canvasCount: canvases.length,
      modals: modalTexts,
      paymentRequired: needsPayment?.[0] || null,
      url: window.location.href,
    };
  });

  console.log("\nViewer state after click:");
  console.log(`  URL: ${viewerState.url}`);
  console.log(`  Iframes: ${viewerState.iframes.length > 0 ? viewerState.iframes.join(", ") : "none"}`);
  console.log(`  Doc images: ${viewerState.images.length > 0 ? viewerState.images.join(", ") : "none"}`);
  console.log(`  Canvas elements: ${viewerState.canvasCount}`);
  console.log(`  Modals: ${viewerState.modals.length > 0 ? viewerState.modals.join(" | ") : "none"}`);
  console.log(`  Payment indicator: ${viewerState.paymentRequired || "none detected"}`);

  if (docRequests.length > 0) {
    console.log("\n  Document-related network requests:");
    for (const r of docRequests) console.log(`    ${r}`);
  }

  // Screenshot
  await page.screenshot({ path: "/tmp/levy-doc-viewer.png" });
  console.log("\n  Screenshot: /tmp/levy-doc-viewer.png");

  await browser.close();
}

main().catch(console.error);
