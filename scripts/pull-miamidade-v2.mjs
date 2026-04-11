#!/usr/bin/env node
/**
 * Smarter Miami-Dade probe. Handles disclaimer, waits for content,
 * dumps page title / URL / iframes so we know what we're working with.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-001";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    acceptDownloads: true,
    viewport: { width: 1400, height: 900 },
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    ignoreHTTPSErrors: true,
  });
  const page = await ctx.newPage();

  console.log("[1] navigate to Miami-Dade standard search");
  const resp = await page.goto(
    "https://onlineservices.miamidadeclerk.gov/officialrecords/standardsearch.aspx",
    { waitUntil: "networkidle", timeout: 60000 },
  );
  console.log(`    HTTP ${resp?.status()}`);
  console.log(`    URL:   ${page.url()}`);
  console.log(`    Title: ${await page.title()}`);

  // Check for disclaimer — often a checkbox + button
  console.log("\n[2] looking for disclaimer / accept buttons");
  const disclaimerElements = await page.evaluate(() => {
    const found = [];
    // Any checkbox
    document.querySelectorAll("input[type='checkbox']").forEach((c) => {
      found.push({ kind: "checkbox", id: c.id, name: c.name });
    });
    // Any button with "accept"/"agree"/"continue"
    document.querySelectorAll("button, input[type='submit'], input[type='button'], a").forEach((b) => {
      const txt = (b.textContent || b.value || "").trim();
      if (/accept|agree|continue|enter|proceed|i understand/i.test(txt)) {
        found.push({ kind: "button", id: b.id, text: txt.slice(0, 40), href: b.href });
      }
    });
    return found;
  });
  console.log("    found:", JSON.stringify(disclaimerElements, null, 2));

  // Check any frames/iframes on the page
  const frames = page.frames();
  console.log(`\n[3] frames on the page: ${frames.length}`);
  for (const f of frames) {
    console.log(`    ${f.name()} → ${f.url()}`);
  }

  // Get full body innerText (first 2000 chars)
  const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "(no body)");
  console.log(`\n[4] body innertext (first 2000 chars):\n${bodyText}`);

  // Full screenshot to see what's really rendered
  await page.screenshot({ path: join(OUT, "miamidade-raw.png"), fullPage: true });

  // Also save the HTML
  const html = await page.content();
  writeFileSync(join(OUT, "miamidade-raw.html"), html);
  console.log(`\n[5] saved screenshot + html to ${OUT}`);

  // Try clicking any "Accept" button we found
  const accept = disclaimerElements.find((e) => e.kind === "button" && /accept|agree|continue|enter|proceed/i.test(e.text));
  if (accept) {
    console.log(`\n[6] clicking accept: "${accept.text}"`);
    try {
      if (accept.id) {
        await page.click(`#${accept.id}`, { timeout: 5000 });
      } else {
        await page.click(`text=${accept.text}`, { timeout: 5000 });
      }
      await page.waitForTimeout(3000);
      console.log(`    now at: ${page.url()}`);
      await page.screenshot({ path: join(OUT, "after-accept.png"), fullPage: true });
    } catch (e) {
      console.log(`    click failed: ${e.message}`);
    }
  }

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
