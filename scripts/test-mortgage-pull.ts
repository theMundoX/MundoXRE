#!/usr/bin/env tsx
/**
 * Quick test: Pull a few REAL mortgage records from Dallas County TX.
 * No database writes — just fetch and print.
 */

import { chromium } from "playwright";

const BASE = "https://dallas.tx.publicsearch.us";

async function main() {
  console.log("Launching browser...");
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();

  try {
    // Navigate to search results for Deed of Trust, last few days
    const url = `${BASE}/results?department=RP&recordedDateRange=2026-03-20to2026-03-25&docType=DEED+OF+TRUST`;
    console.log(`Navigating to: ${url}`);
    await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });

    // Wait for results to render
    console.log("Waiting for results...");
    await page.waitForTimeout(3000);

    // Check what's on the page
    const title = await page.title();
    console.log(`Page title: ${title}`);

    // Look for result rows
    const resultCount = await page.locator('[data-testid="searchResultRow"], .result-row, tr.result, .search-result, .document-row').count();
    console.log(`Result elements found: ${resultCount}`);

    // Try to find any table or list of results
    const tableRows = await page.locator('table tbody tr').count();
    console.log(`Table rows found: ${tableRows}`);

    // Get page content to understand structure
    const content = await page.content();

    // Look for __data in the rendered page
    const dataStr = await page.evaluate(() => {
      return (window as any).__data ? JSON.stringify((window as any).__data, null, 2).slice(0, 5000) : "no __data";
    });

    if (dataStr !== "no __data") {
      console.log("\n__data preview:");
      console.log(dataStr);
    }

    // Try to find results via the DOM
    const allText = await page.evaluate(() => {
      // Find any elements that look like search results
      const elements = document.querySelectorAll('[class*="result"], [class*="Result"], [class*="record"], [class*="Record"], [class*="document"], [class*="Document"]');
      return Array.from(elements).slice(0, 5).map(el => ({
        tag: el.tagName,
        class: el.className,
        text: el.textContent?.slice(0, 200),
      }));
    });

    if (allText.length > 0) {
      console.log("\nResult-like elements:");
      for (const el of allText) {
        console.log(`  <${el.tag} class="${el.class}"> ${el.text?.trim()}`);
      }
    }

    // Screenshot for debug
    await page.screenshot({ path: "/tmp/dallas-search.png" });
    console.log("\nScreenshot saved to /tmp/dallas-search.png");

    // Also try to intercept XHR/fetch requests the page makes
    // Navigate again with network monitoring
    const requests: string[] = [];
    page.on("response", (response) => {
      const url = response.url();
      if (url.includes("api") || url.includes("search") || url.includes("result")) {
        requests.push(`${response.status()} ${url}`);
      }
    });

    // Reload to catch API calls
    console.log("\nReloading to capture network requests...");
    await page.reload({ waitUntil: "networkidle", timeout: 30000 });
    await page.waitForTimeout(2000);

    if (requests.length > 0) {
      console.log("\nAPI-like requests captured:");
      for (const r of requests) console.log(`  ${r}`);
    }

  } catch (err: any) {
    console.error("Error:", err.message);
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
