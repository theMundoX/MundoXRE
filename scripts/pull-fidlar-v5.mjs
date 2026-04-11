#!/usr/bin/env node
/**
 * Fidlar v5 — FINAL SCRAPER.
 * Uses Playwright stealth to get to the #/image viewer, then for each
 * page of the doc: screenshots the viewer area. Works around the bot-
 * detection of the underlying image URL by capturing rendered pixels.
 */
import { chromium as playExtra } from "playwright-extra";
import stealth from "puppeteer-extra-plugin-stealth";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

playExtra.use(stealth());

const OUT_BASE = "C:/Users/msanc/mxre/data/labeling-sample";
const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

async function pullOne(page, ctx, record) {
  const docDir = join(OUT_BASE, `fidlar-${record.document_number}`);
  mkdirSync(docDir, { recursive: true });

  console.log(`\n═══ ${record.document_number} ═══`);
  console.log(`  Borrower: ${record.borrower_name || "?"}`);
  console.log(`  Lender:   ${record.lender_name || "?"}`);
  console.log(`  Amount:   $${record.loan_amount?.toLocaleString() || "?"}`);

  // Start from landing page each time
  console.log("  [1] landing");
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 60000 });
  await page.waitForTimeout(3500);

  // Fill doc number
  await page.evaluate((dn) => {
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      if ((inp.placeholder || "").toLowerCase().includes("document")) {
        inp.value = dn;
        inp.dispatchEvent(new Event("input", { bubbles: true }));
      }
    }
  }, record.document_number);

  // Click Search
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      if ((b.textContent || "").trim() === "Search") { b.click(); return; }
    }
  });
  console.log("  [2] search submitted");
  await page.waitForTimeout(9000);

  // Click the doc number button to open the viewer
  const openedViewer = await page.evaluate((dn) => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      if ((b.textContent || "").trim() === dn) { b.click(); return true; }
    }
    return false;
  }, record.document_number);
  if (!openedViewer) {
    console.log("  ✗ could not open viewer");
    return { ok: false, reason: "no-viewer-btn" };
  }
  console.log("  [3] viewer open");
  await page.waitForTimeout(8000);

  // Confirm we're on the /#/image route
  if (!page.url().includes("/image")) {
    console.log(`  ✗ wrong route: ${page.url()}`);
    return { ok: false, reason: "wrong-route" };
  }

  // Find the actual doc image element — it's usually an <img> in a viewer container
  // Wait for image to load
  await page.waitForTimeout(3000);

  // Find the image viewer container and how many pages
  const viewerInfo = await page.evaluate(() => {
    // Try to find the page count from the "Go" input or a counter label
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    let totalPages = 1;
    for (const inp of inputs) {
      // A page-navigator input usually has a small numeric max
      const max = inp.getAttribute("max");
      if (max) totalPages = Math.max(totalPages, parseInt(max));
    }
    // Look for a "Page X of Y" label
    const labels = Array.from(document.querySelectorAll("*")).filter((e) => e.children.length === 0);
    for (const l of labels) {
      const t = (l.textContent || "").trim();
      const m = t.match(/page\s*\d+\s*of\s*(\d+)/i) || t.match(/^(\d+)\s*\/\s*\d+$/);
      if (m) { totalPages = Math.max(totalPages, parseInt(m[1])); break; }
    }

    // Find the largest image on the page — that's the doc
    const imgs = Array.from(document.querySelectorAll("img")).filter((i) => i.offsetHeight > 100 && i.offsetWidth > 100);
    imgs.sort((a, b) => b.offsetWidth * b.offsetHeight - a.offsetWidth * a.offsetHeight);
    const docImg = imgs[0];
    return {
      totalPages,
      imgCount: imgs.length,
      docImgSrc: docImg?.src || null,
      docImgSize: docImg ? { w: docImg.offsetWidth, h: docImg.offsetHeight } : null,
    };
  });
  console.log(`  [4] viewer info: ${JSON.stringify(viewerInfo)}`);

  // Screenshot each page: capture viewport, then click Next, repeat
  const maxPages = Math.min(viewerInfo.totalPages || 1, 5); // cap at 5 for now
  const savedPages = [];
  for (let i = 1; i <= maxPages; i++) {
    // Try to find and screenshot just the image element; fallback to full page
    const imgBox = await page.evaluate(() => {
      const imgs = Array.from(document.querySelectorAll("img")).filter((i) => i.offsetHeight > 200 && i.offsetWidth > 200);
      imgs.sort((a, b) => b.offsetWidth * b.offsetHeight - a.offsetWidth * a.offsetHeight);
      const el = imgs[0];
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return { x: r.x, y: r.y, width: r.width, height: r.height };
    });

    const pagePath = join(docDir, `page${i}.png`);
    if (imgBox && imgBox.width > 200 && imgBox.height > 200) {
      await page.screenshot({
        path: pagePath,
        clip: { x: Math.max(0, imgBox.x), y: Math.max(0, imgBox.y), width: imgBox.width, height: imgBox.height },
      });
    } else {
      await page.screenshot({ path: pagePath, fullPage: false });
    }
    savedPages.push(pagePath);
    console.log(`  [5] page ${i}/${maxPages} → ${pagePath}`);

    if (i < maxPages) {
      // Click Next Page
      await page.evaluate(() => {
        const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
        for (const b of btns) {
          if ((b.textContent || "").trim() === "Next Page") { b.click(); return; }
        }
      });
      await page.waitForTimeout(2500);
    }
  }

  // Save metadata
  writeFileSync(
    join(docDir, "meta.json"),
    JSON.stringify(
      {
        doc_number: record.document_number,
        record_id: record.id,
        county: "Fairfield OH",
        source: "Fidlar AVA (stealth-scraped)",
        loan_amount: record.loan_amount,
        borrower_name_from_db: record.borrower_name,
        lender_name_from_db: record.lender_name,
        total_pages: viewerInfo.totalPages,
        saved_pages: savedPages,
        captured_at: new Date().toISOString(),
      },
      null,
      2,
    ),
  );

  return { ok: true, pages: savedPages.length, dir: docDir };
}

async function main() {
  // Pick 1 record to start
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name")
    .eq("document_type", "mortgage")
    .gt("loan_amount", 50000)
    .lt("loan_amount", 500000)
    .like("source_url", "%OHFairfield%")
    .limit(1);
  if (!records?.length) { console.error("no records"); process.exit(1); }

  const browser = await playExtra.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 1200 },
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/New_York",
  });
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    // @ts-ignore
    window.chrome = { runtime: {} };
  });
  const page = await ctx.newPage();

  for (const rec of records) {
    const result = await pullOne(page, ctx, rec);
    console.log(`  result: ${JSON.stringify(result)}`);
  }

  await browser.close();
  console.log("\nDone.");
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
