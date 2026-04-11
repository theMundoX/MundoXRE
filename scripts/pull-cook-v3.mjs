#!/usr/bin/env node
/**
 * Cook County IL — full flow: doc type search → MORTGAGE → recent date →
 * submit → click first result → capture and save doc image.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-002-cook";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function dumpForm(page, label) {
  const info = await page.evaluate(() => {
    const vis = (el) => el.offsetHeight > 0;
    return {
      url: location.href,
      title: document.title,
      inputs: Array.from(document.querySelectorAll("input, select")).filter(vis).map((el) => ({
        type: el.type || el.tagName.toLowerCase(),
        id: el.id || "",
        name: el.name || "",
        placeholder: el.placeholder || "",
        optCount: el.tagName === "SELECT" ? el.options.length : null,
      })),
      buttons: Array.from(document.querySelectorAll("button, input[type='submit']")).filter(vis).map((el) => ({
        id: el.id || "",
        text: (el.textContent || el.value || "").trim().slice(0, 40),
      })),
    };
  });
  console.log(`    [${label}] ${info.inputs.length} inputs, ${info.buttons.length} buttons`);
  for (const i of info.inputs.slice(0, 15)) {
    console.log(`       in: type=${i.type} id=${i.id} name=${i.name} opts=${i.optCount || "-"} ph="${i.placeholder}"`);
  }
  for (const b of info.buttons.slice(0, 15)) {
    console.log(`       btn: id=${b.id || "?"} "${b.text}"`);
  }
  return info;
}

async function main() {
  console.log("Cook County IL — full flow\n");

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  });
  const page = await ctx.newPage();

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

  console.log("[1] go to advanced search");
  await page.goto("https://crs.cookcountyclerkil.gov/Search", { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);
  await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll("a"));
    for (const el of links) {
      if ((el.textContent || "").trim() === "Advanced Search") { el.click(); return; }
    }
  });
  await page.waitForTimeout(3000);

  console.log("[2] click 'Document Type Search' button");
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button, a"));
    for (const el of btns) {
      if ((el.textContent || "").trim() === "Document Type Search") { el.click(); return; }
    }
  });
  await page.waitForTimeout(2500);
  await page.screenshot({ path: join(OUT, "2-doctype-form.png"), fullPage: true });
  await dumpForm(page, "doctype-form");

  console.log("\n[3] fill form: type=MORTGAGE, date range");
  // Select document type MORTGAGE
  const mortgageSelected = await page.evaluate(() => {
    const selects = Array.from(document.querySelectorAll("select")).filter((s) => s.offsetHeight > 0);
    for (const s of selects) {
      const opts = Array.from(s.options);
      for (const o of opts) {
        if (/^MORTGAGE$|^MTG$/i.test(o.text.trim())) {
          s.value = o.value;
          s.dispatchEvent(new Event("change", { bubbles: true }));
          return { selectId: s.id || s.name, optText: o.text, optVal: o.value };
        }
      }
    }
    return null;
  });
  console.log(`    mortgage selected: ${JSON.stringify(mortgageSelected)}`);

  // Fill date range — 2024-01-01 to 2024-01-07 (small window to limit results)
  const dateSet = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    const result = { filled: [] };
    for (const inp of inputs) {
      const id = (inp.id || "").toLowerCase();
      const name = (inp.name || "").toLowerCase();
      if (id.includes("begin") || name.includes("begin") || id.includes("from") || name.includes("from") || id.includes("start")) {
        inp.value = "01/01/2024";
        inp.dispatchEvent(new Event("input", { bubbles: true }));
        inp.dispatchEvent(new Event("change", { bubbles: true }));
        result.filled.push({ kind: "begin", id: inp.id });
      } else if (id.includes("end") || name.includes("end") || id.includes("to") || name.includes("to") || id.includes("stop")) {
        inp.value = "01/07/2024";
        inp.dispatchEvent(new Event("input", { bubbles: true }));
        inp.dispatchEvent(new Event("change", { bubbles: true }));
        result.filled.push({ kind: "end", id: inp.id });
      }
    }
    return result;
  });
  console.log(`    dates filled: ${JSON.stringify(dateSet)}`);
  await page.screenshot({ path: join(OUT, "3-form-filled.png"), fullPage: true });

  console.log("\n[4] submit");
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button, input[type='submit']")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      const t = (b.textContent || b.value || "").trim();
      if (/^search$|^submit$|search\s*documents/i.test(t)) {
        b.click();
        return t;
      }
    }
  });
  await page.waitForTimeout(6000);
  await page.screenshot({ path: join(OUT, "4-results.png"), fullPage: true });

  console.log("\n[5] looking for first result to click");
  const firstResult = await page.evaluate(() => {
    // Results are usually in a table or grid
    const rowCandidates = ["tr[role='row']", "tbody tr", "[class*='result-row']", "[class*='grid-row']", "button[data-doc-id]"];
    for (const sel of rowCandidates) {
      const rows = Array.from(document.querySelectorAll(sel)).filter((r) => r.offsetHeight > 10);
      if (rows.length > 1) { // skip header row
        const first = rows[1] || rows[0];
        const text = (first.textContent || "").trim().slice(0, 120);
        first.click();
        return { selector: sel, text };
      }
    }
    return null;
  });
  console.log(`    first result: ${JSON.stringify(firstResult)}`);
  await page.waitForTimeout(5000);
  console.log(`    url now: ${page.url()}`);
  await page.screenshot({ path: join(OUT, "5-after-result-click.png"), fullPage: true });

  // Final screenshot + doc hits summary
  console.log(`\n[6] document-like responses captured during flow: ${docHits.length}`);
  for (const h of docHits) console.log(`  ${h.status} ${h.ct} ${(h.size/1024).toFixed(0)}KB ${h.url}`);

  writeFileSync(join(OUT, "doc-hits.json"), JSON.stringify(docHits, null, 2));

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
