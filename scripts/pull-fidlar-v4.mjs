#!/usr/bin/env node
/**
 * Fidlar v4 — click the DOC NUMBER button (not Printer Friendly)
 * to open the document details view, then find the image trigger.
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
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name")
    .eq("document_type", "mortgage")
    .gt("loan_amount", 50000)
    .lt("loan_amount", 500000)
    .like("source_url", "%OHFairfield%")
    .limit(1);
  const rec = records[0];
  console.log(`Target: ${rec.document_number}\n`);

  const browser = await playExtra.launch({ headless: true });
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
  async function hookResponses(p) {
    p.on("response", async (resp) => {
      const url = resp.url();
      const ct = resp.headers()["content-type"] || "";
      const size = Number(resp.headers()["content-length"] || 0);
      const isDoc =
        ct.includes("pdf") ||
        ct.includes("tiff") ||
        ct.includes("octet-stream") ||
        (ct.includes("image") && size > 30000);
      if (isDoc && !/textwithgradient|backgroundrepeat|header\.jpg|logo|icon|sprite|favicon|fontawesome|woff/i.test(url)) {
        docResponses.push({ url, status: resp.status(), ct, size });
        console.log(`  [DOC] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 150)}`);
        try {
          const buf = await resp.body();
          const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
          const fname = `v4-doc-${String(docResponses.length).padStart(2, "0")}.${ext}`;
          writeFileSync(join(OUT, fname), buf);
          console.log(`        saved → ${fname} (${buf.length}b)`);
        } catch (e) {
          console.log(`        save failed: ${e.message}`);
        }
      }
    });
  }
  hookResponses(page);
  ctx.on("page", async (newPage) => {
    console.log(`  [NEW TAB] ${newPage.url()}`);
    hookResponses(newPage);
  });

  console.log("[1] search");
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

  console.log("[2] click the DOC NUMBER button (opens details)");
  const clicked = await page.evaluate((dn) => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      const t = (b.textContent || "").trim();
      if (t === dn) { b.click(); return t; }
    }
    return null;
  }, rec.document_number);
  console.log(`    clicked: ${clicked}`);
  await page.waitForTimeout(8000);
  await page.screenshot({ path: join(OUT, "v4-1-details.png"), fullPage: true });
  console.log(`    url: ${page.url()}`);

  console.log("\n[3] dump interactables on details page");
  const details = await page.evaluate(() => {
    const out = [];
    Array.from(document.querySelectorAll("button, a, [role='button'], .mat-mdc-button, mat-icon-button")).forEach((el) => {
      if (el.offsetHeight > 0 && el.offsetWidth > 0) {
        const txt = (el.textContent || el.getAttribute("aria-label") || el.getAttribute("title") || "").trim().slice(0, 40);
        const cls = (el.className || "").toString().slice(0, 50);
        const mi = el.querySelector("mat-icon, .mat-icon, i.fa");
        const icon = mi ? (mi.textContent || mi.className || "").trim().slice(0, 30) : "";
        out.push({ tag: el.tagName.toLowerCase(), text: txt, icon, cls, href: el.href || "" });
      }
    });
    return out;
  });
  console.log(`    found ${details.length} interactables:`);
  for (const i of details.slice(0, 40)) {
    console.log(`      ${i.tag}  text="${i.text}"  icon="${i.icon}"  cls="${i.cls.slice(0, 30)}"`);
  }

  console.log("\n[4] looking for 'View Image' / 'Page 1' / eye-icon trigger");
  const viewTriggered = await page.evaluate(() => {
    const all = Array.from(document.querySelectorAll("button, a, [role='button']")).filter((e) => e.offsetHeight > 0);
    for (const el of all) {
      const txt = (el.textContent || el.getAttribute("aria-label") || el.getAttribute("title") || "").trim();
      const icon = (el.querySelector("mat-icon")?.textContent || "").trim();
      if (
        /view.*image|image|visibility|eye|page\s*1|show\s*doc|open\s*doc/i.test(txt) ||
        /visibility|image|remove_red_eye/i.test(icon)
      ) {
        el.scrollIntoView();
        el.click();
        return { text: txt, icon };
      }
    }
    return null;
  });
  console.log(`    clicked: ${JSON.stringify(viewTriggered)}`);
  await page.waitForTimeout(10000);
  await page.screenshot({ path: join(OUT, "v4-2-after-view.png"), fullPage: true });

  const allPages = ctx.pages();
  console.log(`\n[5] ${allPages.length} tabs open:`);
  for (const p of allPages) console.log(`    ${p.url()}`);
  for (const p of allPages) {
    if (p !== page) {
      await p.waitForTimeout(5000);
      try {
        await p.screenshot({ path: join(OUT, `v4-popup-${Date.now()}.png`), fullPage: true });
      } catch {}
    }
  }

  console.log(`\n[6] doc responses: ${docResponses.length}`);
  writeFileSync(join(OUT, "v4-doc-hits.json"), JSON.stringify(docResponses, null, 2));
  await browser.close();
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
