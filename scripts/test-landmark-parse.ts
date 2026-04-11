#!/usr/bin/env tsx
/**
 * Parse real LandmarkWeb JSON response and show mortgage records.
 */

import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Levy County — confirmed working
  console.log("Loading Levy County, FL LandmarkWeb...");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Click Record Date Search
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

  // Capture the JSON response
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
  console.log(`Response: ${jsonStr.length} chars\n`);

  if (!jsonStr) {
    console.log("No response");
    await browser.close();
    return;
  }

  // Parse the DataTables JSON
  const data = JSON.parse(jsonStr);
  console.log(`Total records: ${data.recordsTotal}`);
  console.log(`Records in response: ${data.data.length}\n`);

  // Parse each row — the data is HTML-escaped column values
  const records = data.data.map((row: Record<string, string>) => {
    // Extract text from HTML in each column
    const strip = (html: string) => {
      return html
        .replace(/<[^>]+>/g, "")
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/nobreak_\s*/g, "")
        .replace(/unclickable_/g, "")
        .trim();
    };

    // Column mapping varies — let's just dump all columns
    const cols = Object.keys(row).sort((a, b) => parseInt(a) - parseInt(b));
    return cols.map(k => strip(row[k]));
  });

  // Show column headers by looking at first few records
  console.log("Column dump (first record):");
  if (records.length > 0) {
    records[0].forEach((val: string, i: number) => {
      console.log(`  [${i}] = "${val.slice(0, 60)}"`);
    });
  }

  console.log("\n═══════════════════════════════════════════════════════════════");
  console.log("  REAL RECORDED DOCUMENTS — Levy County, FL — March 20, 2026");
  console.log("═══════════════════════════════════════════════════════════════\n");

  // Identify column mapping from content patterns
  // Typical LandmarkWeb columns: #, eye, checkbox, Grantor, Grantee, Date, DocType, DocSubType, Book, Page, CFN, Legal
  for (const row of records.slice(0, 15)) {
    // Find the date column (MM/DD/YYYY pattern)
    const dateIdx = row.findIndex((c: string) => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
    if (dateIdx < 0) continue;

    const grantor = row[dateIdx - 2] || "?";
    const grantee = row[dateIdx - 1] || "?";
    const date = row[dateIdx];
    const docType = row[dateIdx + 1] || "?";
    const docSubType = row[dateIdx + 2] || "";
    const book = row[dateIdx + 3] || "";
    const pg = row[dateIdx + 4] || "";
    const cfn = row[dateIdx + 5] || "";
    const legal = row[dateIdx + 6] || "";

    console.log(`  ${docType}${docSubType ? ` / ${docSubType}` : ""}`);
    console.log(`    Grantor:  ${grantor}`);
    console.log(`    Grantee:  ${grantee}`);
    console.log(`    Date:     ${date}`);
    console.log(`    Book/Pg:  ${book}/${pg}`);
    console.log(`    CFN:      ${cfn}`);
    console.log(`    Legal:    ${legal.slice(0, 60)}`);
    console.log();
  }

  await browser.close();
}

main().catch(console.error);
