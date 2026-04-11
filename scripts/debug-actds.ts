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

  console.log("1. Loading page...");
  await page.goto(
    "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
    { waitUntil: "networkidle", timeout: 30000 },
  );

  // Try address search for Country Club View Apartments (4635 W Gore Blvd)
  console.log("2. Clicking Physical Address tab...");
  await page.click('a[href*="rpaddress"]');
  await page.waitForTimeout(1000);

  console.log("3. Filling address: 4635 GORE...");
  await page.fill("#StreetNumber", "4635");
  await page.fill("#StreetName", "GORE");
  await page.waitForTimeout(500);

  console.log("4. Submitting...");
  await page.click("#RPAddressSubmit");
  await page.waitForTimeout(5000);

  console.log("5. Current URL:", page.url());

  // Get the page text
  const text = await page.evaluate(`document.body.innerText.substring(0, 5000)`);
  console.log("\n--- PAGE TEXT ---");
  console.log(text);

  // Check for result links
  const links = await page.evaluate(`
    (() => {
      const links = document.querySelectorAll("a");
      return Array.from(links)
        .filter(a => a.href && (a.href.includes("Detail") || a.href.includes("detail")))
        .map(a => ({ text: a.textContent?.trim().substring(0, 80), href: a.href }))
        .slice(0, 10);
    })()
  `);
  console.log("\n--- DETAIL LINKS ---");
  console.log(JSON.stringify(links, null, 2));

  // Also try parcel search
  console.log("\n6. Trying parcel search...");
  await page.goto(
    "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
    { waitUntil: "networkidle", timeout: 30000 },
  );
  await page.click('a[href*="rpparcel"]');
  await page.waitForTimeout(1000);
  await page.fill("#ParcelNumber", "01N12W-01-2-18800-008-0001");
  await page.click("#RPParcelSubmit");
  await page.waitForTimeout(5000);

  console.log("7. Parcel URL:", page.url());
  const parcelText = await page.evaluate(`document.body.innerText.substring(0, 3000)`);
  console.log("\n--- PARCEL RESULTS ---");
  console.log(parcelText);

  await browser.close();
}

main().catch((err) => {
  console.error("Debug failed:", err.message);
  process.exit(1);
});
