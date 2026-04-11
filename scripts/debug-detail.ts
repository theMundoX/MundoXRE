#!/usr/bin/env tsx
import "dotenv/config";
import { chromium } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";

async function main() {
  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ userAgent: stealth.userAgent, viewport: stealth.viewport });
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  await page.goto("https://www.actdatascout.com/RealProperty/Oklahoma/Comanche", { waitUntil: "networkidle", timeout: 30000 });
  await page.click('a[href*="rpparcel"]');
  await page.waitForTimeout(500);
  await page.fill("#ParcelNumber", "01N12W-01-2-18800-008-0001");
  await page.click("#RPParcelSubmit");
  await page.waitForTimeout(4000);

  console.log("Results page URL:", page.url());

  // Click View
  const viewLink = page.locator("a:has-text('View')").first();
  console.log("View visible:", await viewLink.isVisible());
  await viewLink.click();
  await page.waitForTimeout(5000);

  console.log("Detail URL:", page.url());
  const text = await page.evaluate(`document.body.innerText.substring(0, 6000)`);
  console.log("\n--- DETAIL PAGE ---\n");
  console.log(text);

  await browser.close();
}

main().catch((err) => { console.error(err.message); process.exit(1); });
