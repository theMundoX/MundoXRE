#!/usr/bin/env tsx
/**
 * Probe Fidlar AVA by intercepting XHR calls when search is executed.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  const apiBase = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

  // Intercept ALL API calls
  const apiCalls: Array<{ url: string; reqBody: string; respBody: string; status: number }> = [];

  page.on("response", async (resp) => {
    if (resp.url().includes("ScrapRelay")) {
      let body = "";
      try { body = (await resp.text()).slice(0, 1000); } catch {}
      apiCalls.push({
        url: resp.url().replace(apiBase, ""),
        reqBody: resp.request().postData()?.slice(0, 500) || "",
        respBody: body,
        status: resp.status(),
      });
    }
  });

  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(3000);

  // Clear old calls
  apiCalls.length = 0;

  // Fill date range
  console.log("Filling search form...");
  const dateInputs = page.locator('input[placeholder="MM/DD/YYYY"]');
  const count = await dateInputs.count();
  console.log(`Found ${count} date inputs`);

  if (count >= 2) {
    await dateInputs.nth(0).click();
    await dateInputs.nth(0).fill("03/19/2026");
    await page.keyboard.press("Tab");
    await dateInputs.nth(1).click();
    await dateInputs.nth(1).fill("03/20/2026");
    await page.keyboard.press("Tab");
  }

  await page.waitForTimeout(500);

  // Click search
  console.log("Clicking search...");
  const searchBtn = page.locator('button:has-text("Search")').first();
  await searchBtn.click();
  await page.waitForTimeout(8000);

  // Show API calls
  console.log(`\n${apiCalls.length} API calls captured:\n`);
  for (const call of apiCalls) {
    console.log(`  ${call.url}`);
    console.log(`    Status: ${call.status}`);
    if (call.reqBody) console.log(`    Request: ${call.reqBody.slice(0, 300)}`);
    if (call.respBody) console.log(`    Response: ${call.respBody.slice(0, 500)}`);
    console.log();
  }

  // Check results on page
  const pageContent = await page.evaluate(() => {
    const body = document.body.innerText;
    const lines = body.split("\n").filter(l => l.trim());
    // Find lines with dates, amounts, or document types
    return lines
      .filter(l => /mortgage|deed|lien|\$[\d,]+|\d{2}\/\d{2}\/\d{4}/i.test(l))
      .slice(0, 20);
  });

  if (pageContent.length > 0) {
    console.log("Relevant page content:");
    for (const line of pageContent) {
      console.log(`  ${line.slice(0, 100)}`);
    }
  }

  await browser.close();
}

main().catch(console.error);
