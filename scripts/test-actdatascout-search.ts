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

  console.log("Loading ActDataScout Comanche County...");
  await page.goto(
    "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
    { waitUntil: "networkidle", timeout: 30000 },
  );

  // Look for search inputs
  const inputs = await page.evaluate(`
    (() => {
      const els = document.querySelectorAll("input, select, button");
      return Array.from(els).map(el => ({
        tag: el.tagName,
        type: el.getAttribute("type"),
        name: el.getAttribute("name"),
        id: el.id,
        placeholder: el.getAttribute("placeholder"),
        value: el.getAttribute("value"),
        text: el.textContent?.trim().substring(0, 50),
      }));
    })()
  `);
  console.log("\nForm elements found:");
  for (const input of inputs as Array<Record<string, string>>) {
    if (input.tag === "INPUT" || input.tag === "SELECT" || (input.tag === "BUTTON" && input.text)) {
      console.log(`  ${input.tag} name="${input.name}" id="${input.id}" type="${input.type}" placeholder="${input.placeholder}" text="${input.text}"`);
    }
  }

  // Look for search-related links
  const links = await page.evaluate(`
    (() => {
      const els = document.querySelectorAll("a");
      return Array.from(els)
        .filter(a => {
          const text = a.textContent?.toLowerCase() || "";
          const href = a.href || "";
          return text.includes("search") || text.includes("property") ||
                 text.includes("parcel") || text.includes("address") ||
                 href.includes("Search") || href.includes("Property");
        })
        .map(a => ({ text: a.textContent?.trim().substring(0, 50), href: a.href }));
    })()
  `);
  console.log("\nSearch-related links:");
  for (const link of links as Array<{text: string, href: string}>) {
    console.log(`  "${link.text}" → ${link.href}`);
  }

  // Try a known address search - Gore Blvd (where Country Club View is)
  console.log("\nSearching for 'GORE' street...");

  // Find the search input and try it
  const searchInput = page.locator("input[name*='search'], input[placeholder*='search'], input[placeholder*='Search'], #txtSearch, #searchBox, input[type='text']").first();
  if (await searchInput.isVisible().catch(() => false)) {
    await searchInput.fill("4635 GORE");
    console.log("Filled search with '4635 GORE'");

    // Look for a search button
    const searchBtn = page.locator("button[type='submit'], input[type='submit'], button:has-text('Search'), .btn-search").first();
    if (await searchBtn.isVisible().catch(() => false)) {
      await searchBtn.click();
      console.log("Clicked search button");
      await page.waitForTimeout(3000);

      // Get the results
      const resultText = await page.evaluate(`document.body.innerText.substring(0, 3000)`);
      console.log("\nResults preview:\n", resultText);
    } else {
      console.log("No search button found");
    }
  } else {
    console.log("No search input found - page may need interaction first");
    // Try clicking on a tab or link
    const propertySearch = page.locator("a:has-text('Property'), a:has-text('Search'), a:has-text('Real Property')").first();
    if (await propertySearch.isVisible().catch(() => false)) {
      await propertySearch.click();
      await page.waitForTimeout(2000);
      console.log("Clicked property search link");
      const text = await page.evaluate(`document.body.innerText.substring(0, 2000)`);
      console.log(text);
    }
  }

  await browser.close();
}

main().catch((err) => {
  console.error("Failed:", err.message);
  process.exit(1);
});
