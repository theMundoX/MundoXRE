#!/usr/bin/env node
/**
 * BULK FIDLAR SCRAPER
 *
 * Reads data/fidlar-queue.json (produced by fetch-fidlar-queue.mjs), scrapes
 * every document listed, saves pages as PNGs under data/labeling-sample/
 * fidlar-{docnum}/page##.png. Does NOT run extraction — the
 * mundox-autonomous-runner picks those pages up in parallel and extracts them.
 *
 * Runs one doc at a time (Playwright is heavy) but never sleeps — as soon as
 * one doc is done, the next starts. The MundoX runner watches the output dir
 * and processes pages concurrently, so as this scraper drops pages, GPU stays
 * saturated.
 */
import { chromium as playExtra } from "playwright-extra";
import stealth from "puppeteer-extra-plugin-stealth";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";

playExtra.use(stealth());

const OUT_BASE = "C:/Users/msanc/mxre/data/labeling-sample";
const QUEUE_PATH = "C:/Users/msanc/mxre/data/fidlar-queue.json";
const MAX_PAGES = 50;

function sha(buf) {
  return createHash("sha256").update(buf).digest("hex").slice(0, 16);
}

async function scrapeOne(doc) {
  const { document_number, source_url } = doc;
  const outDir = join(OUT_BASE, `fidlar-${document_number}`);
  mkdirSync(outDir, { recursive: true });
  if (existsSync(join(outDir, "DONE"))) {
    console.log(`[skip] ${document_number} (DONE marker exists)`);
    return;
  }

  const browser = await playExtra.launch({ headless: true });
  try {
    const ctx = await browser.newContext({
      viewport: { width: 1440, height: 1200 },
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    });
    const page = await ctx.newPage();
    await page.goto(source_url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await page.waitForTimeout(3500);

    // Search for doc number
    const searchBox = await page
      .locator('input[placeholder*="Search" i], input[type="search"], input[name*="search" i]')
      .first();
    await searchBox.fill(document_number);
    await searchBox.press("Enter");
    await page.waitForTimeout(3500);

    // Click the first result row's doc number button
    const docBtn = page.locator(`button:has-text("${document_number}"), a:has-text("${document_number}")`).first();
    await docBtn.click({ timeout: 20000 });
    await page.waitForTimeout(4000);

    // Wait for viewer iframe or canvas
    await page.waitForSelector("canvas, iframe", { timeout: 30000 });
    await page.waitForTimeout(2500);

    const pages = [];
    let lastHash = "";
    for (let i = 0; i < MAX_PAGES; i++) {
      // Capture the largest visible canvas
      const dataUrl = await page.evaluate(() => {
        const canvases = Array.from(document.querySelectorAll("canvas"));
        if (!canvases.length) return null;
        canvases.sort((a, b) => b.width * b.height - a.width * a.height);
        try {
          return canvases[0].toDataURL("image/png");
        } catch {
          return null;
        }
      });
      if (!dataUrl || !dataUrl.startsWith("data:image/png;base64,")) {
        console.log(`  page ${i + 1}: no canvas data, stopping`);
        break;
      }
      const buf = Buffer.from(dataUrl.split(",")[1], "base64");
      const h = sha(buf);
      if (h === lastHash) {
        console.log(`  page ${i + 1}: duplicate hash, stopping`);
        break;
      }
      lastHash = h;
      const fname = `page${String(i + 1).padStart(2, "0")}.png`;
      writeFileSync(join(outDir, fname), buf);
      pages.push(fname);
      console.log(`  ${document_number}/${fname}  ${(buf.length / 1024).toFixed(0)}KB`);

      // Click Next Page via title attribute (the v7 fix)
      const clickResult = await page.evaluate(() => {
        const allBtns = Array.from(document.querySelectorAll("button"));
        for (const b of allBtns) {
          const title = (b.getAttribute("title") || "").trim();
          if (title === "Next Page" || title.toLowerCase() === "next page") {
            if (b.disabled) return "disabled";
            b.click();
            return "clicked";
          }
        }
        return "not_found";
      });
      if (clickResult !== "clicked") {
        console.log(`  next page button: ${clickResult}, stopping`);
        break;
      }
      await page.waitForTimeout(1800);
    }

    writeFileSync(join(outDir, "DONE"), JSON.stringify({ pages, completedAt: new Date().toISOString() }));
    console.log(`[done] ${document_number}: ${pages.length} pages`);
  } catch (e) {
    console.error(`[err] ${document_number}: ${e.message}`);
    writeFileSync(join(outDir, "ERROR.txt"), `${new Date().toISOString()} ${e.message}\n${e.stack}`);
  } finally {
    await browser.close().catch(() => {});
  }
}

async function main() {
  const queue = JSON.parse(readFileSync(QUEUE_PATH, "utf8"));
  console.log(`Queue: ${queue.length} docs`);
  for (const [i, doc] of queue.entries()) {
    console.log(`\n[${i + 1}/${queue.length}] ${doc.document_number} @ ${doc.source_url}`);
    try {
      await scrapeOne(doc);
    } catch (e) {
      console.error("FATAL:", e.message);
    }
  }
  console.log("\nBulk scrape complete.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
