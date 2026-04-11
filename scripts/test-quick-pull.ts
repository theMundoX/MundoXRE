#!/usr/bin/env tsx
/**
 * Quick pull: one day of Dallas County records using the correct URL format.
 */

import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
  });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Use the CORRECT URL format from the existing adapter
  const url = "https://dallas.tx.publicsearch.us/results?department=RP&limit=50&offset=0&recordedDateRange=custom&recordedDateFrom=2026-03-24&recordedDateTo=2026-03-24&searchOcrText=false&searchType=quickSearch";

  console.log("Navigating to Dallas County PublicSearch...");
  console.log(`URL: ${url}\n`);

  await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
  console.log(`Title: ${await page.title()}`);

  try {
    await page.waitForSelector("table tbody tr", { timeout: 15000 });
  } catch {
    console.log("No table rows found. Checking page content...");
    const body = await page.textContent("body");
    console.log("Body:", body?.slice(0, 500));
    await browser.close();
    return;
  }

  await page.waitForTimeout(2000);

  // Extract with correct column mapping: [0-2]=empty, [3]=Grantor, [4]=Grantee, [5]=DocType, [6]=Date, [7]=DocNum, [8]=BookPage, [9]=Town, [10]=Legal
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

  console.log(`\nTotal records for 2026-03-24: ${records.length}\n`);

  // Filter to mortgage-related docs
  const mortgages = records.filter(r =>
    r.doc_type.toUpperCase().includes("DEED OF TRUST") ||
    r.doc_type.toUpperCase().includes("MORTGAGE") ||
    r.doc_type.toUpperCase().includes("ASSIGNMENT") ||
    r.doc_type.toUpperCase().includes("RELEASE")
  );

  console.log("═══════════════════════════════════════════════════════════════");
  console.log(`  REAL RECORDED DOCUMENTS — Dallas County, TX — March 24, 2026`);
  console.log(`  ${records.length} total records, ${mortgages.length} mortgage-related`);
  console.log("═══════════════════════════════════════════════════════════════\n");

  // Show all doc types found
  const typeCounts = new Map<string, number>();
  for (const r of records) {
    typeCounts.set(r.doc_type, (typeCounts.get(r.doc_type) || 0) + 1);
  }
  console.log("  Document types found:");
  for (const [type, count] of [...typeCounts.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${type.padEnd(35)} ${count}`);
  }

  // Show mortgage records
  if (mortgages.length > 0) {
    console.log(`\n  ─── Mortgage-Related Records ───\n`);
    for (const r of mortgages.slice(0, 10)) {
      console.log(`  Borrower:   ${r.grantor}`);
      console.log(`  Lender:     ${r.grantee}`);
      console.log(`  Type:       ${r.doc_type}`);
      console.log(`  Recorded:   ${r.date}`);
      console.log(`  Doc #:      ${r.doc_number}`);
      console.log(`  Location:   ${r.town}`);
      console.log(`  Legal:      ${r.legal.slice(0, 80)}`);
      console.log();
    }
  }

  await browser.close();
  console.log("Done.");
}

main().catch(console.error);
