#!/usr/bin/env node
/**
 * Fidlar AVA with Playwright stealth to defeat reCAPTCHA v3.
 * Once we get ONE image through, we have the URL pattern and can
 * scrape at scale.
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
  console.log("Fidlar stealth probe\n");

  // Pick one known Fairfield OH mortgage with loan_amount
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .like("source_url", "%OHFairfield%")
    .order("loan_amount", { ascending: false })
    .limit(1);

  const rec = records?.[0];
  if (!rec) {
    console.error("no record");
    process.exit(1);
  }
  console.log(`Target: ${rec.document_number} ($${rec.loan_amount?.toLocaleString()})`);
  console.log(`  Borrower: ${rec.borrower_name || "?"}`);
  console.log(`  Lender:   ${rec.lender_name || "?"}\n`);

  const browser = await playExtra.launch({
    headless: true,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
      "--no-sandbox",
    ],
  });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/New_York",
    javaScriptEnabled: true,
  });

  // Remove webdriver flag
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    // @ts-ignore
    window.chrome = { runtime: {} };
  });

  const page = await ctx.newPage();

  // Capture every doc-like response
  const docResponses = [];
  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const url = resp.url();
    const size = Number(resp.headers()["content-length"] || 0);
    if (
      (ct.includes("pdf") ||
       ct.includes("tiff") ||
       ct.includes("octet-stream") ||
       (ct.includes("image") && size > 30000)) &&
      !/textwithgradient|backgroundrepeat|header\.jpg|logo|icon|sprite|favicon/i.test(url)
    ) {
      docResponses.push({ url, status: resp.status(), ct, size });
      console.log(`  [HIT] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 150)}`);
      try {
        if (size > 10_000 || ct.includes("pdf")) {
          const buf = await resp.body();
          const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
          const fname = `doc-${docResponses.length}.${ext}`;
          writeFileSync(join(OUT, fname), buf);
          console.log(`         saved → ${fname}`);
        }
      } catch {}
    }
  });

  console.log("[1] loading Fairfield AvaWeb");
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", {
    waitUntil: "networkidle",
    timeout: 45000,
  });
  await page.waitForTimeout(3000);
  await page.screenshot({ path: join(OUT, "1-landing.png"), fullPage: true });

  console.log("[2] filling document number input");
  const filled = await page.evaluate((docNum) => {
    // Find all text inputs and pick the one that looks right
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      const ph = (inp.placeholder || "").toLowerCase();
      if (ph.includes("document") || ph.includes("instrument")) {
        inp.value = docNum;
        inp.dispatchEvent(new Event("input", { bubbles: true }));
        return { placeholder: inp.placeholder, id: inp.id };
      }
    }
    return null;
  }, rec.document_number);
  console.log(`    filled: ${JSON.stringify(filled)}`);

  console.log("[3] clicking Search");
  const searched = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      if ((b.textContent || "").trim() === "Search") {
        b.click();
        return true;
      }
    }
    return false;
  });
  console.log(`    clicked search: ${searched}`);
  await page.waitForTimeout(7000);
  await page.screenshot({ path: join(OUT, "2-after-search.png"), fullPage: true });

  console.log("[4] inspecting search results");
  const resultInfo = await page.evaluate(() => {
    // Angular Material rows
    const rows = Array.from(document.querySelectorAll("mat-row, tr, [class*='row'], [role='row']")).filter(
      (r) => r.offsetHeight > 20 && !r.matches("mat-header-row, thead tr, [class*='header'][role='row']")
    );
    return {
      count: rows.length,
      firstText: rows[0] ? (rows[0].textContent || "").trim().slice(0, 200) : null,
    };
  });
  console.log(`    result rows: ${resultInfo.count}, first: ${resultInfo.firstText?.slice(0, 100)}`);

  console.log("[5] clicking first result row");
  await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("mat-row, tr, [class*='row'], [role='row']")).filter(
      (r) => r.offsetHeight > 20 && !r.matches("mat-header-row, thead tr, [class*='header'][role='row']")
    );
    if (rows[0]) rows[0].click();
  });
  await page.waitForTimeout(6000);
  await page.screenshot({ path: join(OUT, "3-after-row-click.png"), fullPage: true });
  console.log(`    url now: ${page.url()}`);

  // Look for a "View Image" button in the doc details
  console.log("[6] looking for View Image button");
  const viewBtnText = await page.evaluate(() => {
    const all = Array.from(document.querySelectorAll("button, a")).filter((b) => b.offsetHeight > 0);
    for (const b of all) {
      const t = (b.textContent || "").trim();
      if (/view\s*image|view\s*doc|image|view|open/i.test(t) && t.length < 30) {
        b.scrollIntoView();
        return t;
      }
    }
    return null;
  });
  console.log(`    view button text: ${viewBtnText || "(none)"}`);

  if (viewBtnText) {
    await page.evaluate((txt) => {
      const all = Array.from(document.querySelectorAll("button, a")).filter((b) => b.offsetHeight > 0);
      for (const b of all) {
        if ((b.textContent || "").trim() === txt) {
          b.click();
          return;
        }
      }
    }, viewBtnText);
    await page.waitForTimeout(8000);
    await page.screenshot({ path: join(OUT, "4-after-view.png"), fullPage: true });
  }

  console.log(`\n[7] final: ${docResponses.length} doc-like responses captured`);
  for (const h of docResponses) console.log(`  ${h.status} ${h.ct} ${(h.size/1024).toFixed(0)}KB ${h.url}`);
  writeFileSync(join(OUT, "doc-hits.json"), JSON.stringify(docResponses, null, 2));

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
