#!/usr/bin/env tsx
/**
 * Check if LandmarkWeb JSON response includes consideration/amount for mortgages.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Martin County — bigger county, has mortgages
  await page.goto("http://or.martinclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Use the "Consideration" search tab to find mortgages with dollar amounts
  // But first, let's check if Record Date search includes consideration in results
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

  if (!jsonStr) { console.log("No response"); await browser.close(); return; }

  const data = JSON.parse(jsonStr);
  console.log(`Total records: ${data.recordsTotal}\n`);

  // Parse and look for consideration/amount columns
  for (const row of data.data.slice(0, 5)) {
    const strip = (val: string) => val?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";

    // Dump ALL columns to find the consideration/amount
    console.log("─── Row ───");
    for (let i = 0; i < 30; i++) {
      const val = strip(row[String(i)] || "");
      if (val && val !== "result" && !val.startsWith("doc_") && !val.startsWith("eye_")) {
        console.log(`  [${i}] = "${val.slice(0, 80)}"`);
      }
    }
    console.log();
  }

  // Also check if there's a "Consideration" search option
  const searchTabs = await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav, [class*='nav'], [class*='tab']");
    return Array.from(navs).map(n => n.textContent?.trim()).filter(Boolean);
  });
  console.log("Search tabs:", searchTabs.slice(0, 15).join(" | "));

  await browser.close();
}

main().catch(console.error);
