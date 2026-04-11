#!/usr/bin/env node
/**
 * Fidlar stealth v3 — click PRINTER FRIENDLY which opens a printable
 * page that embeds the document images. Capture those images.
 */
import { chromium as playExtra } from "playwright-extra";
import stealth from "puppeteer-extra-plugin-stealth";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

playExtra.use(stealth());

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-003-fidlar";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

async function main() {
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date")
    .eq("document_type", "mortgage")
    .gt("loan_amount", 50000)
    .lt("loan_amount", 500000)
    .like("source_url", "%OHFairfield%")
    .limit(1);
  const rec = records[0];
  console.log(`Target: ${rec.document_number} ($${rec.loan_amount?.toLocaleString()})\n`);

  const browser = await playExtra.launch({
    headless: true,
    args: ["--disable-blink-features=AutomationControlled"],
  });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
    acceptDownloads: true,
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

  const docResponses = [];
  page.on("response", async (resp) => {
    const url = resp.url();
    const ct = resp.headers()["content-type"] || "";
    const size = Number(resp.headers()["content-length"] || 0);
    const isImage =
      ct.includes("pdf") ||
      ct.includes("tiff") ||
      ct.includes("octet-stream") ||
      (ct.includes("image") && size > 30000);
    if (isImage && !/textwithgradient|backgroundrepeat|header\.jpg|logo|icon|sprite|favicon|fontawesome|woff/i.test(url)) {
      docResponses.push({ url, status: resp.status(), ct, size });
      console.log(`  [DOC] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 150)}`);
      try {
        const buf = await resp.body();
        const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
        const fname = `doc-${String(docResponses.length).padStart(2, "0")}.${ext}`;
        writeFileSync(join(OUT, fname), buf);
        console.log(`        saved → ${fname}`);
      } catch (e) {
        console.log(`        save failed: ${e.message}`);
      }
    }
  });

  // Catch window.open → new tab/popup for printer-friendly
  ctx.on("page", async (newPage) => {
    console.log(`  [NEW PAGE] ${newPage.url()}`);
    // Hook responses on the new page too
    newPage.on("response", async (resp) => {
      const url = resp.url();
      const ct = resp.headers()["content-type"] || "";
      const size = Number(resp.headers()["content-length"] || 0);
      if ((ct.includes("pdf") || ct.includes("tiff") || (ct.includes("image") && size > 30000)) &&
          !/logo|icon|sprite|banner|header|fontawesome|woff/i.test(url)) {
        docResponses.push({ url, status: resp.status(), ct, size });
        console.log(`  [DOC(new)] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 150)}`);
        try {
          const buf = await resp.body();
          const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
          const fname = `doc-${String(docResponses.length).padStart(2, "0")}.${ext}`;
          writeFileSync(join(OUT, fname), buf);
          console.log(`        saved → ${fname}`);
        } catch {}
      }
    });
  });

  console.log("[1] search for doc");
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 60000 });
  await page.waitForTimeout(4000);
  await page.evaluate((dn) => {
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      if ((inp.placeholder || "").toLowerCase().includes("document")) {
        inp.value = dn;
        inp.dispatchEvent(new Event("input", { bubbles: true }));
      }
    }
  }, rec.document_number);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      if ((b.textContent || "").trim() === "Search") { b.click(); return; }
    }
  });
  await page.waitForTimeout(10000);
  await page.screenshot({ path: join(OUT, "v3-1-results.png"), fullPage: true });

  console.log("\n[2] dump all visible interactables on results page");
  const interactables = await page.evaluate(() => {
    const result = [];
    Array.from(document.querySelectorAll("button, a, [role='button']")).forEach((el) => {
      if (el.offsetHeight > 0 && el.offsetWidth > 0) {
        const txt = (el.textContent || el.getAttribute("aria-label") || el.getAttribute("title") || "").trim().slice(0, 40);
        const cls = el.className || "";
        const mi = el.querySelector("mat-icon, .mat-icon, i.fa");
        const icon = mi ? (mi.textContent || mi.className || "").trim().slice(0, 20) : "";
        result.push({ tag: el.tagName.toLowerCase(), text: txt, icon, cls: cls.slice(0, 50), href: el.href || "" });
      }
    });
    return result.filter((x) => x.text || x.icon || /fa-|mat-icon-button/.test(x.cls));
  });
  console.log(`    found ${interactables.length} interactables:`);
  for (const i of interactables.slice(0, 40)) {
    console.log(`      ${i.tag}  text="${i.text}"  icon="${i.icon}"  href="${i.href}"`);
  }

  console.log("\n[3] clicking PRINTER FRIENDLY");
  const printerClicked = await page.evaluate(() => {
    const els = Array.from(document.querySelectorAll("button, a")).filter((e) => e.offsetHeight > 0);
    for (const e of els) {
      const t = (e.textContent || e.getAttribute("aria-label") || "").trim();
      if (/printer\s*friendly|print.*friendly/i.test(t)) {
        e.click();
        return t;
      }
    }
    return null;
  });
  console.log(`    clicked: ${printerClicked || "(none)"}`);
  if (printerClicked) {
    await page.waitForTimeout(12000);
    await page.screenshot({ path: join(OUT, "v3-2-after-print.png"), fullPage: true });
  }

  // Also check all pages open (printer-friendly often opens a new tab)
  const allPages = ctx.pages();
  console.log(`\n[4] ${allPages.length} pages open:`);
  for (const p of allPages) {
    console.log(`    ${p.url()}`);
    if (p !== page) {
      await p.waitForTimeout(8000);
      await p.screenshot({ path: join(OUT, `v3-3-popup-${Date.now()}.png`), fullPage: true });
    }
  }

  console.log(`\n[5] total doc responses: ${docResponses.length}`);
  writeFileSync(join(OUT, "v3-doc-hits.json"), JSON.stringify(docResponses, null, 2));

  await browser.close();
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
