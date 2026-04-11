#!/usr/bin/env tsx
/**
 * Test the Fidlar AVA search API to find the document search endpoint.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  const baseUrl = "https://ava.fidlar.com/OHFairfield/AvaWeb/";
  const apiBase = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

  await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(2000);

  // Get the auth token
  const token = await page.evaluate(async (api) => {
    const r = await fetch(api + "token", { method: "POST", credentials: "include" });
    const data = await r.json();
    return data.access_token;
  }, apiBase);

  console.log(`Got token: ${token?.slice(0, 50)}...\n`);

  // Monitor API calls while we search
  const apiCalls: Array<{ method: string; url: string; body: string; response: string }> = [];

  page.on("request", (req) => {
    const url = req.url();
    if (url.includes("ScrapRelay") && req.method() === "POST") {
      apiCalls.push({
        method: req.method(),
        url: url.replace(apiBase, "breeze/"),
        body: req.postData()?.slice(0, 500) || "",
        response: "",
      });
    }
  });

  page.on("response", async (resp) => {
    const url = resp.url();
    if (url.includes("ScrapRelay") && resp.request().method() === "POST") {
      const call = apiCalls.find(c => c.url === url.replace(apiBase, "breeze/") && !c.response);
      if (call) {
        try { call.response = (await resp.text()).slice(0, 500); } catch {}
      }
    }
  });

  // Fill date range and click search
  console.log("Filling date range search...");

  // Fill start date
  const dateInputs = page.locator('input[placeholder="MM/DD/YYYY"]');
  await dateInputs.nth(0).fill("03/19/2026");
  await dateInputs.nth(1).fill("03/20/2026");

  // Click search button
  const searchBtns = page.locator('button:has-text("Search")');
  await searchBtns.first().click();
  await page.waitForTimeout(5000);

  // Show API calls
  console.log(`\nAPI calls during search (${apiCalls.length}):\n`);
  for (const call of apiCalls) {
    console.log(`  POST ${call.url}`);
    if (call.body) console.log(`    Request: ${call.body.slice(0, 300)}`);
    if (call.response) console.log(`    Response: ${call.response.slice(0, 300)}`);
    console.log();
  }

  // Check what's on the page now
  const results = await page.evaluate(() => {
    // Look for result table/list
    const rows = document.querySelectorAll("table tbody tr, mat-row, [class*='result'], [class*='row']");
    return {
      count: rows.length,
      firstRow: rows[0]?.textContent?.trim().slice(0, 200) || "no rows",
    };
  });
  console.log(`Results on page: ${results.count}`);
  console.log(`First row: ${results.firstRow}`);

  await browser.close();
}

main().catch(console.error);
