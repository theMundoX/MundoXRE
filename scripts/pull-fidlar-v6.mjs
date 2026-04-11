#!/usr/bin/env node
/**
 * Fidlar stealth v6 — FULL RESOLUTION page capture.
 *
 * Strategy:
 *   - After landing on /#/image, find the actual doc image element in the DOM
 *     (it's either an <img> with a src URL, or a <canvas>).
 *   - For <img>: fetch the src directly from inside the browser context
 *     (so auth/cookies/blob URLs work) and save the bytes at full resolution.
 *   - For <canvas>: export via canvas.toDataURL("image/png").
 *   - Fallback: fullPage screenshot of the entire scrollable viewer.
 *   - Click "Next Page" and repeat for all pages in the doc.
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

/** Extract the currently-displayed document page as a PNG Buffer. */
async function capturePageImage(page) {
  // Try to find the doc image or canvas at full resolution.
  const info = await page.evaluate(async () => {
    // 1. Any <img> with natural dimensions > 500 (full doc size, not thumb)
    const imgs = Array.from(document.querySelectorAll("img"));
    const bigImg = imgs
      .filter((i) => i.naturalWidth > 500 && i.naturalHeight > 500)
      .sort((a, b) => b.naturalWidth * b.naturalHeight - a.naturalWidth * a.naturalHeight)[0];

    if (bigImg) {
      // Try to fetch the src from inside the page context (has cookies/auth)
      try {
        const r = await fetch(bigImg.src, { credentials: "include" });
        if (r.ok) {
          const blob = await r.blob();
          const buf = await blob.arrayBuffer();
          const arr = Array.from(new Uint8Array(buf));
          return {
            kind: "img-fetched",
            src: bigImg.src.slice(0, 200),
            natural: { w: bigImg.naturalWidth, h: bigImg.naturalHeight },
            bytes: arr,
            contentType: blob.type,
          };
        }
      } catch (e) {
        // fallthrough
      }
      // If fetch failed, draw the img onto a canvas and export
      try {
        const canvas = document.createElement("canvas");
        canvas.width = bigImg.naturalWidth;
        canvas.height = bigImg.naturalHeight;
        const ctx = canvas.getContext("2d");
        ctx.drawImage(bigImg, 0, 0);
        const dataUrl = canvas.toDataURL("image/png");
        return {
          kind: "img-canvas",
          src: bigImg.src.slice(0, 200),
          natural: { w: bigImg.naturalWidth, h: bigImg.naturalHeight },
          dataUrl,
        };
      } catch (e) {
        return { kind: "img-tainted", error: String(e), src: bigImg.src.slice(0, 200) };
      }
    }

    // 2. Largest <canvas>
    const canvases = Array.from(document.querySelectorAll("canvas"))
      .filter((c) => c.width > 300 && c.height > 300)
      .sort((a, b) => b.width * b.height - a.width * a.height);
    if (canvases[0]) {
      const c = canvases[0];
      try {
        const dataUrl = c.toDataURL("image/png");
        return { kind: "canvas", natural: { w: c.width, h: c.height }, dataUrl };
      } catch (e) {
        return { kind: "canvas-tainted", error: String(e) };
      }
    }

    // 3. Nothing found
    return { kind: "none", imgCount: imgs.length, imgSizes: imgs.map((i) => `${i.naturalWidth}x${i.naturalHeight}`) };
  });

  if (info.kind === "img-fetched") {
    const buf = Buffer.from(info.bytes);
    return { buf, info };
  }
  if (info.kind === "img-canvas" || info.kind === "canvas") {
    const b64 = info.dataUrl.split(",")[1];
    return { buf: Buffer.from(b64, "base64"), info };
  }
  return { buf: null, info };
}

async function pullDoc(page, record) {
  const docDir = join(OUT_BASE, `fidlar-${record.document_number}-full`);
  mkdirSync(docDir, { recursive: true });

  console.log(`\n═══ ${record.document_number} ═══`);

  // 1. search
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 60000 });
  await page.waitForTimeout(3500);
  await page.evaluate((dn) => {
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      if ((inp.placeholder || "").toLowerCase().includes("document")) {
        inp.value = dn;
        inp.dispatchEvent(new Event("input", { bubbles: true }));
      }
    }
  }, record.document_number);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) if ((b.textContent || "").trim() === "Search") { b.click(); return; }
  });
  await page.waitForTimeout(9000);

  // 2. open viewer
  await page.evaluate((dn) => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) if ((b.textContent || "").trim() === dn) { b.click(); return; }
  }, record.document_number);
  await page.waitForTimeout(8000);

  if (!page.url().includes("/image")) {
    console.log("  ✗ viewer didn't open");
    return { ok: false };
  }
  console.log("  [viewer open]");

  // 3. detect total page count from the viewer UI
  const totalPages = await page.evaluate(() => {
    // Page count is often shown as "1 of N" or in a max attr on a number input
    const bodyText = document.body?.innerText || "";
    const m = bodyText.match(/page\s*\d+\s*of\s*(\d+)/i) || bodyText.match(/(\d+)\s*\/\s*(\d+)/);
    if (m) return parseInt(m[m.length - 1]);
    const inputs = Array.from(document.querySelectorAll("input[type='number'], input[type='text']")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      const max = parseInt(inp.getAttribute("max") || "0");
      if (max > 1) return max;
    }
    return 1;
  });
  console.log(`  total pages: ${totalPages}`);

  // 4. loop: capture each page
  const pages = [];
  const cap = Math.min(totalPages, 25);
  for (let i = 1; i <= cap; i++) {
    // Wait a bit for the new page image to render
    await page.waitForTimeout(2500);
    const { buf, info } = await capturePageImage(page);
    const outPath = join(docDir, `page${String(i).padStart(2, "0")}.png`);

    if (buf) {
      writeFileSync(outPath, buf);
      console.log(`  page ${i}/${cap}: ${info.kind} ${info.natural?.w}x${info.natural?.h} → ${(buf.length/1024).toFixed(0)}KB  ${outPath}`);
      pages.push({ page: i, path: outPath, kind: info.kind, size: buf.length, dims: info.natural });
    } else {
      // Fallback: fullPage screenshot (captures scrollable)
      await page.screenshot({ path: outPath, fullPage: true });
      console.log(`  page ${i}/${cap}: FALLBACK fullPage screenshot (info: ${JSON.stringify(info).slice(0, 200)}) → ${outPath}`);
      pages.push({ page: i, path: outPath, kind: "fullPage-screenshot" });
    }

    // Click Next Page if not last
    if (i < cap) {
      const clicked = await page.evaluate(() => {
        const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
        for (const b of btns) if ((b.textContent || "").trim() === "Next Page") { b.click(); return true; }
        return false;
      });
      if (!clicked) { console.log("  (no Next Page button — stopping)"); break; }
    }
  }

  writeFileSync(
    join(docDir, "meta.json"),
    JSON.stringify(
      {
        doc_number: record.document_number,
        record_id: record.id,
        county: "Fairfield OH",
        source: "Fidlar AVA (stealth)",
        loan_amount: record.loan_amount,
        borrower_name_db: record.borrower_name,
        lender_name_db: record.lender_name,
        total_pages: totalPages,
        captured_pages: pages,
        captured_at: new Date().toISOString(),
      },
      null,
      2,
    ),
  );
  return { ok: true, pages: pages.length, dir: docDir };
}

async function main() {
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name")
    .eq("document_type", "mortgage")
    .gt("loan_amount", 50000)
    .lt("loan_amount", 500000)
    .like("source_url", "%OHFairfield%")
    .limit(1);
  const rec = records[0];

  const browser = await playExtra.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1600, height: 1200 },
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

  const result = await pullDoc(page, rec);
  console.log(`\nresult: ${JSON.stringify(result)}`);

  await browser.close();
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
