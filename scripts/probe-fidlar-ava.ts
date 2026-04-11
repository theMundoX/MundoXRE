#!/usr/bin/env tsx
/**
 * Probe Fidlar AVA recorder portal to understand the API and data structure.
 * AVA is an Angular SPA — it likely has a JSON API underneath.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Use Fairfield County OH — validated working
  const baseUrl = "https://ava.fidlar.com/OHFairfield/AvaWeb/";
  console.log(`Probing: ${baseUrl}\n`);

  // Monitor API calls
  const apiCalls: Array<{ method: string; url: string; status: number; contentType: string; bodyPreview: string }> = [];

  page.on("response", async (resp) => {
    const url = resp.url();
    // Skip static assets
    if (url.match(/\.(js|css|png|jpg|svg|woff|ttf|ico)(\?|$)/)) return;
    if (url.includes("fonts.googleapis") || url.includes("material")) return;

    const ct = resp.headers()["content-type"] || "";
    let body = "";
    try {
      if (ct.includes("json")) {
        body = (await resp.text()).slice(0, 300);
      }
    } catch {}

    apiCalls.push({
      method: resp.request().method(),
      url: url.replace(baseUrl, "/"),
      status: resp.status(),
      contentType: ct.split(";")[0],
      bodyPreview: body,
    });
  });

  await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(3000);

  console.log("Page title:", await page.title());
  console.log("URL:", page.url());

  // Show API calls made during page load
  console.log(`\nAPI calls during load (${apiCalls.length}):\n`);
  for (const call of apiCalls) {
    if (call.contentType.includes("json") || call.method === "POST") {
      console.log(`  ${call.method} ${call.url.slice(0, 80)}`);
      console.log(`    [${call.status}] ${call.contentType}`);
      if (call.bodyPreview) console.log(`    ${call.bodyPreview.slice(0, 200)}`);
      console.log();
    }
  }

  // Check for search forms or API endpoints
  const pageStructure = await page.evaluate(() => {
    // Look for search inputs
    const inputs = document.querySelectorAll("input, select, textarea");
    const inputInfo = Array.from(inputs)
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(el => ({
        tag: el.tagName,
        type: (el as HTMLInputElement).type,
        id: el.id,
        name: (el as HTMLInputElement).name,
        placeholder: (el as HTMLInputElement).placeholder,
      }));

    // Look for buttons
    const buttons = document.querySelectorAll("button, [type='submit'], a[class*='btn']");
    const btnInfo = Array.from(buttons)
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(el => ({
        text: el.textContent?.trim().slice(0, 40),
        id: el.id,
        class: el.className?.slice(0, 40),
      }));

    // Check for Angular API patterns
    const bodyText = document.body.innerText;
    const hasSearch = bodyText.includes("Search") || bodyText.includes("search");
    const hasRecorder = bodyText.includes("Recorder") || bodyText.includes("recorder");

    return { inputs: inputInfo.slice(0, 15), buttons: btnInfo.slice(0, 10), hasSearch, hasRecorder };
  });

  console.log("Visible inputs:");
  for (const input of pageStructure.inputs) {
    console.log(`  ${input.tag} id=${input.id} name=${input.name} type=${input.type} placeholder="${input.placeholder}"`);
  }

  console.log("\nButtons:");
  for (const btn of pageStructure.buttons) {
    console.log(`  "${btn.text}" id=${btn.id}`);
  }

  // Try to find the search API by looking at the Angular app's HTTP interceptors
  const angularInfo = await page.evaluate(() => {
    // Check for API base URL in Angular's environment
    const scripts = document.querySelectorAll("script");
    let apiBase = "";
    for (const s of scripts) {
      const src = s.src || "";
      if (src.includes("main")) {
        return { mainBundle: src };
      }
    }
    return { apiBase };
  });
  console.log("\nAngular info:", angularInfo);

  // Try common AVA API patterns
  console.log("\nTesting API patterns...");
  const patterns = [
    "api/search",
    "api/documents",
    "api/Document/Search",
    "Search/GetSearchResults",
    "api/v1/search",
    "Search",
  ];

  for (const pattern of patterns) {
    try {
      const resp = await page.evaluate(async (url) => {
        const r = await fetch(url, { credentials: "include" });
        return { status: r.status, type: r.headers.get("content-type"), body: (await r.text()).slice(0, 200) };
      }, baseUrl + pattern);
      if (resp.status !== 404) {
        console.log(`  ${pattern}: [${resp.status}] ${resp.type} — ${resp.body.slice(0, 100)}`);
      }
    } catch {}
  }

  await browser.close();
}

main().catch(console.error);
