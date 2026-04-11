#!/usr/bin/env tsx
/**
 * Probe the RIGHT page — the landing page after the disclaimer,
 * which IS the search form. Dump every form element with its id,
 * so we can build an exact scraper selector list.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/levy-debug";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36",
    viewport: { width: 1280, height: 900 },
  });
  const page = await ctx.newPage();

  console.log("[1] landing");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1000);
  await page.evaluate(() => (window as any).SetDisclaimer?.());
  await page.waitForTimeout(2000);

  console.log(`URL after disclaimer: ${page.url()}`);
  console.log("");

  // Dump EVERY form-related element on the page
  const elements = await page.evaluate(() => {
    const result: any = { inputs: [], selects: [], buttons: [], links: [], divs_with_id: [] };
    document.querySelectorAll("input").forEach((el: any) => {
      if (el.offsetHeight > 0 || el.type === "hidden") {
        result.inputs.push({
          type: el.type, id: el.id, name: el.name,
          placeholder: el.placeholder, value: (el.value || "").slice(0, 40),
          visible: el.offsetHeight > 0,
        });
      }
    });
    document.querySelectorAll("select").forEach((el: any) => {
      result.selects.push({
        id: el.id, name: el.name, visible: el.offsetHeight > 0,
        options: Array.from(el.options).map((o: any) => ({ value: o.value, text: o.text.trim().slice(0, 30) })),
      });
    });
    document.querySelectorAll("button, input[type='submit'], input[type='button']").forEach((el: any) => {
      result.buttons.push({
        id: el.id, type: el.type, name: el.name,
        text: (el.textContent || el.value || "").trim().slice(0, 40),
        onclick: (el.getAttribute("onclick") || "").slice(0, 60),
        visible: el.offsetHeight > 0,
      });
    });
    document.querySelectorAll("a[onclick], a[href^='javascript']").forEach((el: any) => {
      result.links.push({
        text: (el.textContent || "").trim().slice(0, 40),
        id: el.id, href: el.href,
        onclick: (el.getAttribute("onclick") || "").slice(0, 80),
        visible: el.offsetHeight > 0,
      });
    });
    // Divs with recognizable IDs (for category selectors etc.)
    document.querySelectorAll("div[id]").forEach((el: any) => {
      if (el.id.match(/doc|category|type|mortgage|search|submit/i) && el.offsetHeight > 0) {
        result.divs_with_id.push({
          id: el.id, text: (el.textContent || "").trim().slice(0, 60),
        });
      }
    });
    return result;
  });

  console.log(`[2] ${elements.inputs.length} inputs:`);
  for (const i of elements.inputs.slice(0, 30)) {
    console.log(`  type=${i.type} id=${i.id || "?"} name=${i.name || "?"} placeholder=${i.placeholder || ""} visible=${i.visible}`);
  }
  console.log("");
  console.log(`[3] ${elements.selects.length} selects:`);
  for (const s of elements.selects.slice(0, 15)) {
    console.log(`  id=${s.id} name=${s.name} opts=${s.options.length}`);
    for (const o of s.options.slice(0, 5)) console.log(`    ${o.value} → ${o.text}`);
  }
  console.log("");
  console.log(`[4] ${elements.buttons.length} buttons:`);
  for (const b of elements.buttons.slice(0, 20)) {
    console.log(`  id=${b.id || "?"} text="${b.text}" onclick=${b.onclick ? b.onclick.slice(0, 50) : ""}`);
  }
  console.log("");
  console.log(`[5] ${elements.links.length} js links:`);
  for (const l of elements.links.slice(0, 30)) {
    console.log(`  id=${l.id || "?"} "${l.text}" onclick=${l.onclick}`);
  }
  console.log("");
  console.log(`[6] ${elements.divs_with_id.length} relevant divs:`);
  for (const d of elements.divs_with_id.slice(0, 20)) {
    console.log(`  id=${d.id}`);
  }

  writeFileSync(join(OUT, "element-dump.json"), JSON.stringify(elements, null, 2));
  console.log(`\nSaved to element-dump.json`);

  await browser.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
