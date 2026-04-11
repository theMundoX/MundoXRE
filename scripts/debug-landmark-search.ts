#!/usr/bin/env tsx
/**
 * Debug LandmarkWeb search to understand the correct API format.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => { try { (window as any).SetDisclaimer(); } catch {} });
  await page.waitForTimeout(3000);

  // Go to search page
  await page.goto("https://online.levyclerk.com/LandmarkWeb/search/index", { waitUntil: "networkidle", timeout: 15000 });
  await page.waitForTimeout(1000);

  // Try different search API formats
  const cfn = "765305";

  console.log("Testing search formats for CFN:", cfn);

  // Format 1: SetSearchCriteria
  const r1 = await page.evaluate(async (cfn) => {
    const r = await fetch("/LandmarkWeb/search/SetSearchCriteria", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      credentials: "include",
      body: `SrchType=instrumentNumber&CaseKey=&TotalRows=&searchCriteria=${cfn}`,
    });
    return { status: r.status, text: (await r.text()).slice(0, 200) };
  }, cfn);
  console.log("  SetSearchCriteria:", r1);

  // Format 2: Try getting results
  const r2 = await page.evaluate(async () => {
    const r = await fetch("/LandmarkWeb/search/GetSearchResults", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      credentials: "include",
      body: "page=1",
    });
    return { status: r.status, text: (await r.text()).slice(0, 500) };
  });
  console.log("  GetSearchResults:", r2.status, r2.text.slice(0, 300));

  // Let's also check what the search page looks like
  const pageContent = await page.evaluate(() => {
    const forms = document.querySelectorAll("form");
    const inputs = document.querySelectorAll("input, select");
    return {
      forms: forms.length,
      inputs: Array.from(inputs).map(i => ({
        id: i.id,
        name: (i as HTMLInputElement).name,
        type: (i as HTMLInputElement).type,
        value: (i as HTMLInputElement).value?.slice(0, 20),
      })).filter(i => i.id || i.name),
    };
  });
  console.log("\nPage forms:", pageContent.forms);
  console.log("Inputs:", pageContent.inputs.slice(0, 10));

  // Try using the form directly
  console.log("\nTrying to fill and submit the form via page interaction...");

  // Navigate to the instrument number search section
  const searchNavs = page.locator(".searchNav, a[href*='instrument'], [data-section*='instrument']");
  const navCount = await searchNavs.count();
  console.log("Search nav links:", navCount);

  // Try clicking on Instrument Number search tab
  const instrumentLink = page.locator("text=Instrument Number, text=CFN, text=Clerk File");
  if (await instrumentLink.count() > 0) {
    await instrumentLink.first().click();
    await page.waitForTimeout(1000);
    console.log("Clicked instrument number tab");
  }

  // Show all visible inputs now
  const visibleInputs = await page.evaluate(() => {
    const inputs = document.querySelectorAll("input, select, textarea");
    return Array.from(inputs)
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(i => ({
        id: i.id,
        name: (i as HTMLInputElement).name,
        type: (i as HTMLInputElement).type,
        placeholder: (i as HTMLInputElement).placeholder,
      }));
  });
  console.log("Visible inputs:", visibleInputs);

  await browser.close();
}

main().catch(console.error);
