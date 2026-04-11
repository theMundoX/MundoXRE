#!/usr/bin/env node
/**
 * Cook County IL Clerk Recordings search — find 1 mortgage, capture
 * the document viewer URL pattern, download the page image.
 *
 * Timeboxed: if this doesn't produce a real mortgage PDF in 5 min, give up.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-002-cook";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  console.log("Cook County IL probe\n");

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1400, height: 900 },
    acceptDownloads: true,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  const docRequests = [];
  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const url = resp.url();
    if (ct.includes("pdf") || ct.includes("tiff") || (ct.includes("image") && !/logo|icon|banner|sprite|bg|header|footer/i.test(url))) {
      const size = Number(resp.headers()["content-length"] || 0);
      if (size > 5000 || ct.includes("pdf")) {
        docRequests.push({ url, status: resp.status(), ct, size });
        console.log(`  [DOC-LIKE] ${resp.status()} ${ct} ${size}b ${url.slice(0, 130)}`);
      }
    }
  });

  console.log("[1] landing page (disclaimer)");
  await page.goto("https://ccrd.cookcountyclerkil.gov/Search/DisclaimerSearch.aspx", {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });
  await page.waitForTimeout(2000);
  console.log(`    now at: ${page.url()}`);
  await page.screenshot({ path: join(OUT, "1-landing.png"), fullPage: true });

  // Find visible buttons / inputs
  const pageInfo = await page.evaluate(() => {
    return {
      title: document.title,
      url: window.location.href,
      buttons: Array.from(document.querySelectorAll("button, input[type='submit'], input[type='button'], a")).map((el) => ({
        tag: el.tagName.toLowerCase(),
        id: el.id,
        text: (el.textContent || el.value || "").trim().slice(0, 40),
        visible: el.offsetHeight > 0,
      })).filter((b) => b.visible && b.text.length > 0).slice(0, 30),
      inputs: Array.from(document.querySelectorAll("input[type='text'], input[type='search'], select")).map((el) => ({
        tag: el.tagName.toLowerCase(),
        type: el.type,
        id: el.id,
        name: el.name,
        placeholder: el.placeholder,
        visible: el.offsetHeight > 0,
      })).filter((i) => i.visible).slice(0, 15),
    };
  });
  console.log(`    title: ${pageInfo.title}`);
  console.log(`    inputs: ${JSON.stringify(pageInfo.inputs.slice(0, 8))}`);
  console.log(`    buttons: ${JSON.stringify(pageInfo.buttons.slice(0, 15))}`);

  // Try clicking "I Agree" / "Accept" / "Continue" disclaimer
  const acceptClicked = await page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll("button, input[type='submit'], input[type='button'], a"));
    for (const el of candidates) {
      const txt = (el.textContent || el.value || "").trim();
      if (/agree|accept|continue|enter|i understand|proceed/i.test(txt) && el.offsetHeight > 0) {
        el.click();
        return txt;
      }
    }
    return null;
  });
  console.log(`    clicked: ${acceptClicked || "(none)"}`);
  if (acceptClicked) {
    await page.waitForTimeout(3000);
    console.log(`    after click, url: ${page.url()}`);
    await page.screenshot({ path: join(OUT, "2-after-accept.png"), fullPage: true });
  }

  // Dump forms on the search page now
  const searchPage = await page.evaluate(() => {
    return {
      title: document.title,
      url: window.location.href,
      inputs: Array.from(document.querySelectorAll("input[type='text'], input[type='search'], select, input:not([type])")).map((el) => ({
        tag: el.tagName.toLowerCase(),
        type: el.type,
        id: el.id,
        name: el.name,
        placeholder: el.placeholder,
        visible: el.offsetHeight > 0,
      })).filter((i) => i.visible).slice(0, 20),
      buttons: Array.from(document.querySelectorAll("button, input[type='submit'], input[type='button']")).map((el) => ({
        id: el.id,
        text: (el.textContent || el.value || "").trim().slice(0, 40),
        visible: el.offsetHeight > 0,
      })).filter((b) => b.visible).slice(0, 15),
    };
  });
  console.log(`\n[2] search page: ${searchPage.title}`);
  console.log(`    url: ${searchPage.url}`);
  console.log(`    inputs (${searchPage.inputs.length}):`);
  for (const i of searchPage.inputs) console.log(`      ${i.type || i.tag}  id=${i.id || "?"}  ph="${i.placeholder || ""}"`);
  console.log(`    buttons:`);
  for (const b of searchPage.buttons) console.log(`      id=${b.id || "?"}  "${b.text}"`);

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
