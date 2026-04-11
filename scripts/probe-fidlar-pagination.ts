#!/usr/bin/env tsx
/**
 * Test Fidlar AVA pagination — find how to get page 2+ of results.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();
  const apiBase = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

  // Monitor ALL API calls
  page.on("response", async (resp) => {
    if (resp.url().includes("ScrapRelay") && !resp.url().includes(".css") && !resp.url().includes(".js")) {
      const method = resp.request().method();
      const url = resp.url().replace(apiBase, "");
      const body = resp.request().postData()?.slice(0, 200) || "";
      let respBody = "";
      try { respBody = (await resp.text()).slice(0, 200); } catch {}
      console.log(`  ${method} ${url}`);
      if (body) console.log(`    req: ${body}`);
      if (respBody) console.log(`    res: ${respBody}`);
    }
  });

  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(2000);

  // Search with a wide range to get lots of results
  const dateInputs = page.locator('input[placeholder="MM/DD/YYYY"]');
  await dateInputs.nth(0).click();
  await dateInputs.nth(0).fill("02/01/2026");
  await page.keyboard.press("Tab");
  await dateInputs.nth(1).click();
  await dateInputs.nth(1).fill("03/27/2026");
  await page.keyboard.press("Tab");
  await page.waitForTimeout(500);

  console.log("\n=== Clicking Search ===\n");
  await page.locator('button:has-text("Search")').first().click();
  await page.waitForTimeout(5000);

  // Now scroll down or click "next page" to trigger pagination
  console.log("\n=== Looking for pagination controls ===\n");
  const pagInfo = await page.evaluate(() => {
    const btns = document.querySelectorAll("button, a, [class*='pag'], [class*='next'], [class*='more']");
    return Array.from(btns)
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(el => ({
        tag: el.tagName,
        text: el.textContent?.trim().slice(0, 30),
        class: el.className?.slice(0, 50),
      }))
      .filter(b => b.text?.match(/next|more|page|>>|›|\d+/i) || b.class?.match(/pag|next/i));
  });

  console.log("Pagination elements:");
  for (const b of pagInfo) {
    console.log(`  <${b.tag} class="${b.class}"> ${b.text}`);
  }

  // Try scrolling the result list
  console.log("\n=== Scrolling result list ===\n");
  await page.evaluate(() => {
    const container = document.querySelector('[class*="result"], [class*="list"], [class*="scroll"], mat-list, cdk-virtual-scroll-viewport');
    if (container) {
      container.scrollTop = container.scrollHeight;
      return "scrolled " + container.className;
    }
    window.scrollTo(0, document.body.scrollHeight);
    return "scrolled window";
  });
  await page.waitForTimeout(3000);

  await browser.close();
}

main().catch(console.error);
