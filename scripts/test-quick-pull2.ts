#!/usr/bin/env tsx
/**
 * Quick pull with proper SPA wait — waits for actual table content to render.
 */

import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  const url = "https://dallas.tx.publicsearch.us/results?department=RP&limit=50&offset=0&recordedDateRange=custom&recordedDateFrom=2026-03-24&recordedDateTo=2026-03-24&searchOcrText=false&searchType=quickSearch";

  console.log("Navigating to Dallas County PublicSearch...");

  // Don't wait for networkidle — the SPA keeps making requests
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

  // Wait for the React app to finish rendering results
  console.log("Waiting for search results to render...");

  // Wait for either results table or "no results" message
  try {
    await page.waitForFunction(() => {
      const rows = document.querySelectorAll("table tbody tr");
      const noResults = document.querySelector('[class*="no-results"], [class*="empty"]');
      const loading = document.querySelector('[class*="loading"]');
      // Results loaded when we have rows OR no-results shown AND loading is gone
      return (rows.length > 0 || noResults) && !loading?.textContent?.includes("Loading");
    }, { timeout: 30000 });
  } catch {
    // Fallback: just wait a fixed time
    console.log("Timeout waiting for results, trying with fixed delay...");
    await page.waitForTimeout(10000);
  }

  const rowCount = await page.locator("table tbody tr").count();
  console.log(`Table rows: ${rowCount}`);

  if (rowCount === 0) {
    // Try a different date that we know has data
    console.log("\nNo results for 2026-03-24. Trying March 20, 2026...");
    const url2 = "https://dallas.tx.publicsearch.us/results?department=RP&limit=50&offset=0&recordedDateRange=custom&recordedDateFrom=2026-03-20&recordedDateTo=2026-03-20&searchOcrText=false&searchType=quickSearch";
    await page.goto(url2, { waitUntil: "domcontentloaded", timeout: 30000 });

    try {
      await page.waitForFunction(() => {
        return document.querySelectorAll("table tbody tr").length > 0;
      }, { timeout: 30000 });
    } catch {
      await page.waitForTimeout(10000);
    }

    const count2 = await page.locator("table tbody tr").count();
    console.log(`Table rows for March 20: ${count2}`);

    if (count2 === 0) {
      // Try a very recent weekday — maybe today is ahead of their indexing
      console.log("\nTrying March 14, 2026 (a week and a half back)...");
      const url3 = "https://dallas.tx.publicsearch.us/results?department=RP&limit=50&offset=0&recordedDateRange=custom&recordedDateFrom=2026-03-14&recordedDateTo=2026-03-14&searchOcrText=false&searchType=quickSearch";
      await page.goto(url3, { waitUntil: "domcontentloaded", timeout: 30000 });
      try {
        await page.waitForFunction(() => document.querySelectorAll("table tbody tr").length > 0, { timeout: 30000 });
      } catch {
        await page.waitForTimeout(10000);
      }
      const count3 = await page.locator("table tbody tr").count();
      console.log(`Table rows for March 14: ${count3}`);
    }
  }

  // Now extract whatever we have
  const finalRows = await page.locator("table tbody tr").count();
  if (finalRows > 0) {
    const records = await page.evaluate(() => {
      const rows = document.querySelectorAll("table tbody tr");
      return Array.from(rows).map(row => {
        const tds = row.querySelectorAll("td");
        return {
          grantor: tds[3]?.textContent?.trim() || "",
          grantee: tds[4]?.textContent?.trim() || "",
          doc_type: tds[5]?.textContent?.trim() || "",
          date: tds[6]?.textContent?.trim() || "",
          doc_number: tds[7]?.textContent?.trim() || "",
          book_page: tds[8]?.textContent?.trim() || "",
          town: tds[9]?.textContent?.trim() || "",
          legal: tds[10]?.textContent?.trim() || "",
        };
      });
    });

    console.log(`\n═══════════════════════════════════════════════════════════════`);
    console.log(`  REAL RECORDED DOCUMENTS — Dallas County, TX`);
    console.log(`  ${records.length} total records`);
    console.log(`═══════════════════════════════════════════════════════════════\n`);

    const typeCounts = new Map<string, number>();
    for (const r of records) {
      typeCounts.set(r.doc_type, (typeCounts.get(r.doc_type) || 0) + 1);
    }
    console.log("  Document types:");
    for (const [type, count] of [...typeCounts.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`    ${type.padEnd(35)} ${count}`);
    }

    const mortgages = records.filter(r =>
      r.doc_type.toUpperCase().includes("DEED OF TRUST") ||
      r.doc_type.toUpperCase().includes("DOT")
    );

    if (mortgages.length > 0) {
      console.log(`\n  ─── Deed of Trust Records (${mortgages.length}) ───\n`);
      for (const r of mortgages.slice(0, 10)) {
        console.log(`  Borrower:   ${r.grantor}`);
        console.log(`  Lender:     ${r.grantee}`);
        console.log(`  Type:       ${r.doc_type}`);
        console.log(`  Recorded:   ${r.date}`);
        console.log(`  Doc #:      ${r.doc_number}`);
        console.log(`  Town:       ${r.town}`);
        console.log(`  Legal:      ${r.legal.slice(0, 80)}`);
        console.log();
      }
    }
  } else {
    console.log("No results found. Taking screenshot for debug...");
    await page.screenshot({ path: "/tmp/dallas-debug.png" });
    const bodyText = await page.textContent("body");
    console.log("Page text:", bodyText?.slice(0, 500));
  }

  await browser.close();
}

main().catch(console.error);
