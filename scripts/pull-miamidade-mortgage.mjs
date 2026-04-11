#!/usr/bin/env node
/**
 * Pull ONE real recorded mortgage from Miami-Dade Clerk's official records.
 * Public access, no login, no reCAPTCHA (hopefully).
 *
 * Flow:
 *   1. Open the standard search page
 *   2. Search for "MOR" (mortgage) document type in a recent date range
 *   3. Click the first result to open the doc viewer
 *   4. Find the PDF/image link and download it
 *   5. Save to data/labeling-sample/real-001/source.pdf
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-001";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

const SEARCH_URL = "https://onlineservices.miamidadeclerk.gov/officialrecords/standardsearch.aspx";

async function main() {
  console.log("Miami-Dade real mortgage pull\n");

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    acceptDownloads: true,
    viewport: { width: 1400, height: 900 },
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Log all requests that look like PDFs or images
  const imageRequests = [];
  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const url = resp.url();
    if (
      ct.includes("pdf") ||
      ct.includes("tiff") ||
      (ct.includes("image") && !url.includes("logo") && !url.includes("icon") && !url.includes("banner"))
    ) {
      imageRequests.push({ url, status: resp.status(), ct, size: Number(resp.headers()["content-length"] || 0) });
      console.log(`  [resp] ${resp.status()} ${ct} ${url.slice(0, 120)}`);
    }
  });

  console.log("[1] navigating to search page");
  await page.goto(SEARCH_URL, { waitUntil: "domcontentloaded", timeout: 40000 });
  await page.waitForTimeout(3000);
  await page.screenshot({ path: join(OUT, "1-search-page.png"), fullPage: true });

  // Handle disclaimer if present
  const disclaimerBtn = page.locator("text=/I Accept|Accept|Continue|OK/i").first();
  if ((await disclaimerBtn.count()) > 0) {
    try {
      await disclaimerBtn.click({ timeout: 2000 });
      console.log("    clicked disclaimer");
      await page.waitForTimeout(2000);
    } catch {}
  }

  console.log("[2] inspecting search form");
  const formInfo = await page.evaluate(() => {
    return {
      inputs: Array.from(document.querySelectorAll("input, select")).map((el) => ({
        tag: el.tagName.toLowerCase(),
        type: el.type,
        id: el.id,
        name: el.name,
        placeholder: el.placeholder,
        visible: el.offsetHeight > 0,
      })).filter((e) => e.visible && e.id),
      buttons: Array.from(document.querySelectorAll("button, input[type='submit']")).map((el) => ({
        id: el.id,
        text: (el.textContent || el.value || "").trim().slice(0, 30),
        visible: el.offsetHeight > 0,
      })).filter((b) => b.visible),
    };
  });
  console.log("    inputs:", JSON.stringify(formInfo.inputs.slice(0, 15), null, 2));
  console.log("    buttons:", JSON.stringify(formInfo.buttons.slice(0, 10), null, 2));

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
