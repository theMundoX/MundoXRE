#!/usr/bin/env tsx
/**
 * Click into a LandmarkWeb document to see the detail view with consideration.
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

  // Search record date
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

  // Wait for results
  await page.waitForFunction(() => document.querySelectorAll("#resultsTable tbody tr").length === 0, { timeout: 5000 }).catch(() => {});
  await page.click("#submit-RecordDate");

  // Wait for results table
  await page.waitForFunction(() => document.querySelectorAll("#resultsTable tbody tr").length > 0, { timeout: 20000 });
  await page.waitForTimeout(2000);

  // Find a mortgage row and click it
  const mortgageRow = await page.evaluate(() => {
    const rows = document.querySelectorAll("#resultsTable tbody tr");
    for (const row of rows) {
      if (row.textContent?.includes("MORTGAGE")) {
        // Get the doc ID from the row
        const link = row.querySelector("a[href*='javascript']");
        const onclick = link?.getAttribute("onclick") || "";
        return { text: row.textContent?.trim().slice(0, 100), onclick };
      }
    }
    return null;
  });

  console.log("Found mortgage row:", mortgageRow?.text?.slice(0, 80));

  // Capture any detail API calls
  const detailResponses: Array<{ url: string; body: string }> = [];
  page.on("response", async (resp) => {
    const url = resp.url();
    if (url.includes("Detail") || url.includes("detail") || url.includes("Document") || url.includes("GetDocument")) {
      try {
        const body = await resp.text();
        detailResponses.push({ url, body: body.slice(0, 2000) });
      } catch {}
    }
  });

  // Click the first mortgage row
  await page.evaluate(() => {
    const rows = document.querySelectorAll("#resultsTable tbody tr");
    for (const row of rows) {
      if (row.textContent?.includes("MORTGAGE")) {
        (row as HTMLElement).click();
        break;
      }
    }
  });

  await page.waitForTimeout(5000);

  // Check for detail responses
  if (detailResponses.length > 0) {
    console.log("\nDetail API responses:");
    for (const r of detailResponses) {
      console.log(`  URL: ${r.url}`);
      console.log(`  Body: ${r.body.slice(0, 500)}`);
    }
  }

  // Check the current page for detail content
  const detailContent = await page.evaluate(() => {
    // Look for consideration, amount, or dollar values
    const allText = document.body.innerText;
    const lines = allText.split("\n").filter(l => l.trim());

    // Find lines with dollar amounts or key terms
    const relevant = lines.filter(l =>
      /consider|amount|value|price|principal|\$[\d,]+/i.test(l)
    );

    // Also look for any modal/overlay/detail panel
    const panels = document.querySelectorAll('.modal, .overlay, [class*="detail"], [class*="viewer"], [id*="detail"]');
    const panelTexts = Array.from(panels).map(p => p.textContent?.trim().slice(0, 300));

    return { relevant, panelTexts, url: window.location.href };
  });

  console.log("\nRelevant lines on page:");
  for (const line of detailContent.relevant.slice(0, 10)) {
    console.log(`  ${line.slice(0, 100)}`);
  }

  if (detailContent.panelTexts.length > 0) {
    console.log("\nDetail panels:");
    for (const p of detailContent.panelTexts) {
      console.log(`  ${p}`);
    }
  }

  console.log(`\nCurrent URL: ${detailContent.url}`);

  // Take a screenshot
  await page.screenshot({ path: "/tmp/landmark-detail.png" });
  console.log("\nScreenshot saved to /tmp/landmark-detail.png");

  await browser.close();
}

main().catch(console.error);
