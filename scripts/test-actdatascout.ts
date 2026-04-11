#!/usr/bin/env tsx
import "dotenv/config";
import { chromium } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";

async function main() {
  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
  });
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  console.log("Navigating to ActDataScout...");
  const response = await page.goto(
    "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
    { waitUntil: "domcontentloaded", timeout: 30000 },
  );
  console.log("Status:", response?.status());
  console.log("Title:", await page.title());

  const html = await page.content();
  console.log("Page length:", html.length);
  console.log("Has search form:", html.includes("search") || html.includes("Search"));

  // Take a text snapshot of the page
  const text = await page.evaluate(`document.body.innerText.substring(0, 2000)`);
  console.log("\nPage text preview:\n", text);

  await browser.close();
  console.log("\nDone");
}

main().catch((err) => {
  console.error("Test failed:", err.message);
  process.exit(1);
});
