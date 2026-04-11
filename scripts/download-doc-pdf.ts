#!/usr/bin/env tsx
/**
 * Download a recorded document PDF from LandmarkWeb and extract lien amount + interest rate.
 *
 * LandmarkWeb document images are served at:
 *   {base_url}/LandmarkWeb/Document/GetDocumentForViewing?documentId={docId}
 * or via their image viewer API.
 */
import { chromium } from "playwright";
import { writeFileSync, readFileSync } from "node:fs";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Martin County FL
  const baseUrl = "http://or.martinclerk.com";
  const pathPrefix = "/LandmarkWeb";

  await page.goto(`${baseUrl}${pathPrefix}`, { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Search for a mortgage
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

  // Capture search results
  const resultPromise = new Promise<string>((resolve) => {
    page.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 20000);
  });
  await page.click("#submit-RecordDate");
  const jsonStr = await resultPromise;

  if (!jsonStr) { console.log("No results"); await browser.close(); return; }
  const data = JSON.parse(jsonStr);

  // Find a mortgage and get its internal doc ID
  let docId = "";
  for (const row of data.data) {
    const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";
    const cols: string[] = [];
    for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");
    const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
    if (dateIdx < 0) continue;
    if (!cols[dateIdx + 1]?.includes("MORTGAGE")) continue;

    // Extract doc ID from the raw HTML columns
    for (let i = 0; i < 30; i++) {
      const raw = row[String(i)] || "";
      const match = raw.match(/hidden_(\d+)/);
      if (match) { docId = match[1]; break; }
    }
    if (docId) {
      console.log(`Found mortgage doc ID: ${docId}`);
      console.log(`Grantor: ${cols[dateIdx - 2]}`);
      console.log(`Grantee: ${cols[dateIdx - 1]}`);
      break;
    }
  }

  if (!docId) { console.log("No doc ID found"); await browser.close(); return; }

  // Try to access the document image/PDF
  // Common LandmarkWeb document URLs:
  const docUrls = [
    `${baseUrl}${pathPrefix}/Document/GetDocumentForViewing?documentId=${docId}`,
    `${baseUrl}${pathPrefix}/Document/GetDocument?documentId=${docId}`,
    `${baseUrl}${pathPrefix}/Search/GetDocumentDetail?documentId=${docId}`,
    `${baseUrl}${pathPrefix}/Document/ViewDocument?docId=${docId}`,
    `${baseUrl}${pathPrefix}/Image/GetImage?documentId=${docId}`,
  ];

  console.log("\nTrying document URLs...\n");

  for (const url of docUrls) {
    try {
      const resp = await page.evaluate(async (fetchUrl) => {
        const r = await fetch(fetchUrl, { credentials: "include" });
        const contentType = r.headers.get("content-type") || "";
        const status = r.status;
        let bodyPreview = "";
        if (contentType.includes("json")) {
          bodyPreview = await r.text();
        } else if (contentType.includes("html")) {
          bodyPreview = (await r.text()).slice(0, 500);
        } else if (contentType.includes("pdf") || contentType.includes("image")) {
          bodyPreview = `[Binary: ${contentType}, size unknown]`;
        } else {
          bodyPreview = (await r.text()).slice(0, 300);
        }
        return { status, contentType, bodyPreview };
      }, url);

      const icon = resp.status === 200 ? "✓" : "✗";
      console.log(`${icon} [${resp.status}] ${url}`);
      console.log(`  Content-Type: ${resp.contentType}`);
      if (resp.bodyPreview.length < 500) {
        console.log(`  Body: ${resp.bodyPreview.slice(0, 200)}`);
      }
      console.log();

      // If we got a JSON response with document details, parse it
      if (resp.contentType.includes("json") && resp.bodyPreview.startsWith("{")) {
        try {
          const detail = JSON.parse(resp.bodyPreview);
          console.log("  Document detail keys:", Object.keys(detail).join(", "));
          // Look for consideration/amount
          for (const [k, v] of Object.entries(detail)) {
            if (/consider|amount|principal|value|rate/i.test(k)) {
              console.log(`  *** ${k}: ${v} ***`);
            }
          }
        } catch {}
      }
    } catch (err: any) {
      console.log(`✗ Error: ${url} — ${err.message.slice(0, 80)}`);
    }
  }

  await browser.close();
}

main().catch(console.error);
