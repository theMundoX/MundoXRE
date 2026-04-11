#!/usr/bin/env tsx
/**
 * Debug LandmarkWeb — check if the portal actually loads and renders.
 */

import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });

  const portals = [
    { name: "Martin", url: "http://or.martinclerk.com/LandmarkWeb" },
    { name: "Citrus", url: "https://search.citrusclerk.org/LandmarkWeb" },
    { name: "Walton", url: "https://orsearch.clerkofcourts.co.walton.fl.us/LandmarkWeb" },
    { name: "Levy", url: "https://online.levyclerk.com/landmarkweb" },
  ];

  for (const portal of portals) {
    console.log(`\n━━━ ${portal.name} County ━━━`);
    console.log(`URL: ${portal.url}`);

    const page = await ctx.newPage();
    try {
      await page.goto(portal.url, { waitUntil: "networkidle", timeout: 15000 });
      console.log(`Title: ${await page.title()}`);
      console.log(`URL after load: ${page.url()}`);

      // Check for disclaimer
      const hasDisclaimer = await page.evaluate(() => {
        return typeof (window as any).SetDisclaimer === "function";
      });
      console.log(`Has SetDisclaimer: ${hasDisclaimer}`);

      if (hasDisclaimer) {
        await page.evaluate(() => (window as any).SetDisclaimer());
        await page.waitForTimeout(2000);
        console.log(`After disclaimer: ${await page.title()}`);
      }

      // Check for Record Date Search tab
      const hasDateSearch = await page.evaluate(() => {
        const navs = document.querySelectorAll(".searchNav");
        const texts = Array.from(navs).map(n => n.textContent?.trim());
        return texts;
      });
      console.log(`Nav tabs: ${hasDateSearch.join(", ")}`);

      // Check for the date input fields
      const hasBeginDate = await page.locator("#beginDate-RecordDate").count();
      console.log(`Has beginDate-RecordDate: ${hasBeginDate > 0}`);

      // Try filling date and searching
      if (hasBeginDate > 0) {
        try {
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
          console.log("Filled dates: 03/20/2026");

          // Set up response listener
          let gotResponse = false;
          page.on("response", async (resp) => {
            if (resp.url().includes("GetSearchResults")) {
              gotResponse = true;
              try {
                const text = await resp.text();
                console.log(`GetSearchResults response: ${text.length} chars`);
                console.log(`Preview: ${text.slice(0, 300)}`);
              } catch {}
            }
          });

          await page.click("#submit-RecordDate");
          await page.waitForTimeout(5000);

          if (!gotResponse) {
            console.log("No GetSearchResults response intercepted");
          }

          // Check for results table
          const resultRows = await page.locator("#resultsTable tbody tr").count();
          console.log(`Result rows in DOM: ${resultRows}`);
        } catch (err: any) {
          console.log(`Search error: ${err.message.slice(0, 100)}`);
        }
      }
    } catch (err: any) {
      console.log(`Load error: ${err.message.slice(0, 100)}`);
    } finally {
      await page.close();
    }
  }

  await browser.close();
}

main().catch(console.error);
