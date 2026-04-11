#!/usr/bin/env tsx
/**
 * Use LandmarkWeb Consideration search to find mortgages WITH dollar amounts.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  await page.goto("http://or.martinclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Click Consideration tab
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim()?.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(1000);

  // Fill the correct field IDs
  await page.fill("#lowerBound", "100000");
  await page.fill("#upperBound", "5000000");
  await page.fill("#beginDate-Consideration", "03/19/2026");
  await page.fill("#endDate-Consideration", "03/21/2026");

  // Capture response
  const resultPromise = new Promise<string>((resolve) => {
    page.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 25000);
  });

  // Submit
  await page.click("#submit-Consideration");
  const jsonStr = await resultPromise;

  if (!jsonStr || !jsonStr.startsWith("{")) {
    console.log("No results. Response:", jsonStr?.slice(0, 200));
    await browser.close();
    return;
  }

  const data = JSON.parse(jsonStr);
  console.log(`Results: ${data.recordsTotal} documents with consideration $100K-$5M\n`);

  console.log("═══════════════════════════════════════════════════════════════════════");
  console.log("  RECORDED DOCUMENTS WITH LIEN AMOUNTS — Martin County, FL");
  console.log("═══════════════════════════════════════════════════════════════════════\n");

  for (const row of (data.data || []).slice(0, 15)) {
    const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";

    const cols: string[] = [];
    for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");

    // Find the date column
    const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
    if (dateIdx < 0) continue;

    const grantor = cols[dateIdx - 2] || "";
    const grantee = cols[dateIdx - 1] || "";
    const date = cols[dateIdx];
    const docType = cols[dateIdx + 1] || "";
    const subType = cols[dateIdx + 2] || "";
    const book = cols[dateIdx + 3] || "";
    const pg = cols[dateIdx + 4] || "";
    const cfn = cols[dateIdx + 5] || "";
    const legal = cols[dateIdx + 6] || "";

    // The consideration might be in a different column position for this search
    // Check column 13 or the column after legal
    const consideration = cols[dateIdx + 7] || cols[13] || "";

    // Also check if any column contains a dollar amount pattern
    let amount = "";
    for (const c of cols) {
      if (/^\$?[\d,]+(?:\.\d{2})?$/.test(c) && c.replace(/[$,]/g, "").length > 3) {
        amount = c;
        break;
      }
    }

    console.log(`  ${docType} ${subType ? "/ " + subType : ""}`);
    console.log(`    Grantor:       ${grantor.slice(0, 50)}`);
    console.log(`    Grantee:       ${grantee.slice(0, 50)}`);
    console.log(`    Date:          ${date}`);
    console.log(`    Doc #:         ${cfn}`);
    console.log(`    Book/Page:     ${book}/${pg}`);
    console.log(`    Consideration: ${consideration || amount || "NOT FOUND"}`);
    console.log(`    Legal:         ${legal.slice(0, 60)}`);

    // Dump all non-empty cols for debugging
    const extras = cols.filter((c, i) => c && i > dateIdx + 6 && !c.startsWith("doc_") && c !== "result" && !c.startsWith("eye_"));
    if (extras.length > 0) {
      console.log(`    Extra cols:    ${extras.join(" | ")}`);
    }
    console.log();
  }

  await browser.close();
}

main().catch(console.error);
