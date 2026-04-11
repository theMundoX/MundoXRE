#!/usr/bin/env tsx
/**
 * Pull REAL mortgage records from Dallas County TX and display them.
 * Extracts data from the server-rendered HTML table on publicsearch.us.
 */

import { chromium } from "playwright";

const BASE = "https://dallas.tx.publicsearch.us";

async function main() {
  console.log("Launching browser...");
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await context.newPage();

  try {
    const url = `${BASE}/results?department=RP&recordedDateRange=2026-03-17to2026-03-25&docType=DEED+OF+TRUST`;
    console.log(`Fetching Deed of Trust records from Dallas County...`);
    console.log(`URL: ${url}\n`);

    await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
    await page.waitForTimeout(2000);

    // Extract all table rows
    const records = await page.evaluate(() => {
      const rows = document.querySelectorAll("table tbody tr");
      return Array.from(rows).map(row => {
        const cells = row.querySelectorAll("td");
        return {
          grantor: cells[0]?.textContent?.trim() || "",
          grantee: cells[1]?.textContent?.trim() || "",
          doc_type: cells[2]?.textContent?.trim() || "",
          recorded_date: cells[3]?.textContent?.trim() || "",
          doc_number: cells[4]?.textContent?.trim() || "",
          book_page: cells[5]?.textContent?.trim() || "",
          town: cells[6]?.textContent?.trim() || "",
          legal: cells[7]?.textContent?.trim() || "",
        };
      });
    });

    console.log(`Found ${records.length} Deed of Trust records\n`);
    console.log("═══════════════════════════════════════════════════════════════");
    console.log("  REAL MORTGAGE RECORDS — Dallas County TX (Last 7 Days)");
    console.log("═══════════════════════════════════════════════════════════════\n");

    for (let i = 0; i < Math.min(records.length, 15); i++) {
      const r = records[i];
      console.log(`  Record #${i + 1}`);
      console.log(`  ─────────────────────────────────────────────────`);
      console.log(`  Borrower (Grantor): ${r.grantor}`);
      console.log(`  Lender (Grantee):   ${r.grantee}`);
      console.log(`  Doc Type:           ${r.doc_type}`);
      console.log(`  Recorded Date:      ${r.recorded_date}`);
      console.log(`  Doc Number:         ${r.doc_number}`);
      console.log(`  Book/Page:          ${r.book_page}`);
      console.log(`  Legal Description:  ${r.legal?.slice(0, 80)}`);
      console.log();
    }

    if (records.length > 15) {
      console.log(`  ... and ${records.length - 15} more records.`);
    }

    // Get the total count shown on the page
    const totalText = await page.evaluate(() => {
      const el = document.querySelector('[class*="count"], [class*="total"], [class*="showing"]');
      return el?.textContent?.trim() || "N/A";
    });
    console.log(`\nPage status: ${totalText}`);

  } catch (err: any) {
    console.error("Error:", err.message);
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
