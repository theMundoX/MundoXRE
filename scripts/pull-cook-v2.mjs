#!/usr/bin/env node
/**
 * Cook County IL — navigate Advanced Search → find recent Mortgage →
 * click first result → capture doc viewer URL + download image.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-002-cook";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  console.log("Cook County IL — advanced search\n");

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Log all image/pdf responses
  const docHits = [];
  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const url = resp.url();
    const size = Number(resp.headers()["content-length"] || 0);
    if ((ct.includes("pdf") || ct.includes("tiff") || (ct.includes("image") && size > 50000)) && !/seal|logo|icon|sprite|header|footer|banner/i.test(url)) {
      docHits.push({ url, status: resp.status(), ct, size });
      console.log(`  [HIT] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 140)}`);
    }
  });

  console.log("[1] loading search page");
  await page.goto("https://crs.cookcountyclerkil.gov/Search", { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2500);

  console.log("[2] clicking Advanced Search");
  const advanced = await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll("a, button"));
    for (const el of links) {
      const t = (el.textContent || "").trim();
      if (t === "Advanced Search" && el.offsetHeight > 0) {
        el.click();
        return true;
      }
    }
    return false;
  });
  console.log(`    clicked: ${advanced}`);
  await page.waitForTimeout(3000);
  await page.screenshot({ path: join(OUT, "1-advanced.png"), fullPage: true });
  console.log(`    url now: ${page.url()}`);

  // Dump the advanced search form elements
  const form = await page.evaluate(() => {
    const visible = (el) => el.offsetHeight > 0;
    return {
      url: window.location.href,
      title: document.title,
      inputs: Array.from(document.querySelectorAll("input, select")).filter(visible).map((el) => ({
        tag: el.tagName.toLowerCase(),
        type: el.type || "",
        id: el.id || "",
        name: el.name || "",
        placeholder: el.placeholder || "",
        optCount: el.tagName === "SELECT" ? el.options.length : null,
        firstOpts: el.tagName === "SELECT" ? Array.from(el.options).slice(0, 10).map((o) => o.text.trim().slice(0, 30)) : null,
      })).slice(0, 25),
      buttons: Array.from(document.querySelectorAll("button, input[type='submit'], input[type='button']")).filter(visible).map((el) => ({
        id: el.id || "",
        text: (el.textContent || el.value || "").trim().slice(0, 40),
      })).slice(0, 20),
    };
  });
  console.log(`    title: ${form.title}`);
  console.log(`    inputs (${form.inputs.length}):`);
  for (const i of form.inputs) {
    const base = `      ${(i.type || i.tag).padEnd(10)} id=${(i.id || "?").padEnd(30)} name=${i.name || "?"}`;
    if (i.optCount) console.log(`${base}  options=${i.optCount}  first=${JSON.stringify(i.firstOpts)}`);
    else console.log(`${base}  ph="${i.placeholder}"`);
  }
  console.log(`    buttons:`);
  for (const b of form.buttons) console.log(`      id=${b.id || "?"}  "${b.text}"`);

  writeFileSync(join(OUT, "advanced-page-info.json"), JSON.stringify(form, null, 2));

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
