#!/usr/bin/env node
/**
 * Fidlar stealth v2 — more patience, dump full HTML, inspect the
 * real Angular Material DOM after results load.
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
  // Pick a saner record — $50k-$500k range, Fairfield OH
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date")
    .eq("document_type", "mortgage")
    .gt("loan_amount", 50000)
    .lt("loan_amount", 500000)
    .like("source_url", "%OHFairfield%")
    .limit(1);

  const rec = records?.[0];
  if (!rec) { console.error("no record"); process.exit(1); }
  console.log(`Target: ${rec.document_number}`);
  console.log(`  Amount: $${rec.loan_amount?.toLocaleString()}`);
  console.log(`  Recorded: ${rec.recording_date}`);
  console.log(`  Borrower: ${rec.borrower_name || "?"}`);
  console.log(`  Lender:   ${rec.lender_name || "?"}\n`);

  const browser = await playExtra.launch({
    headless: true,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
    ],
  });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
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

  // Hook ALL network traffic
  const docResponses = [];
  const searchResponses = [];
  page.on("response", async (resp) => {
    const url = resp.url();
    const ct = resp.headers()["content-type"] || "";
    const size = Number(resp.headers()["content-length"] || 0);
    if (url.includes("breeze/Search") || url.includes("ViewImages") || url.includes("PrintDocument")) {
      const body = await resp.text().catch(() => "");
      searchResponses.push({ url, status: resp.status(), ct, bodyLen: body.length, bodyHead: body.slice(0, 400) });
      console.log(`  [API] ${resp.status()} ${url.split("/").pop()} body=${body.length}b`);
    }
    if (
      (ct.includes("pdf") || ct.includes("tiff") || (ct.includes("image") && size > 30000)) &&
      !/textwithgradient|backgroundrepeat|header\.jpg|logo|icon|sprite|favicon/i.test(url)
    ) {
      docResponses.push({ url, status: resp.status(), ct, size });
      console.log(`  [DOC] ${resp.status()} ${ct} ${(size/1024).toFixed(0)}KB  ${url.slice(0, 140)}`);
      try {
        const buf = await resp.body();
        const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
        const fname = `doc-${docResponses.length}.${ext}`;
        writeFileSync(join(OUT, fname), buf);
        console.log(`        saved → ${fname}`);
      } catch {}
    }
  });

  console.log("[1] load AvaWeb landing");
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 60000 });
  console.log(`    url: ${page.url()}`);
  await page.waitForTimeout(5000);

  console.log("\n[2] fill document number");
  await page.evaluate((docNum) => {
    const inputs = Array.from(document.querySelectorAll("input")).filter((i) => i.offsetHeight > 0);
    for (const inp of inputs) {
      if ((inp.placeholder || "").toLowerCase().includes("document")) {
        inp.value = docNum;
        inp.dispatchEvent(new Event("input", { bubbles: true }));
        inp.dispatchEvent(new Event("change", { bubbles: true }));
        return;
      }
    }
  }, rec.document_number);

  console.log("[3] click Search");
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      if ((b.textContent || "").trim() === "Search") { b.click(); return; }
    }
  });

  console.log("[4] waiting 15s for results to render");
  await page.waitForTimeout(15000);
  await page.screenshot({ path: join(OUT, "1-results-loaded.png"), fullPage: true });
  console.log(`    url: ${page.url()}`);

  // Dump the whole results area DOM structure
  const resultInfo = await page.evaluate(() => {
    return {
      title: document.title,
      bodyText: (document.body?.innerText || "").slice(0, 2500),
      allRowSelectors: {
        "mat-row": document.querySelectorAll("mat-row").length,
        "tr": document.querySelectorAll("tr").length,
        "[role='row']": document.querySelectorAll("[role='row']").length,
        ".result-row": document.querySelectorAll(".result-row").length,
        ".search-result": document.querySelectorAll(".search-result").length,
        "button[mat-button]": document.querySelectorAll("button[mat-button]").length,
      },
      matTables: document.querySelectorAll("mat-table, table.mat-table").length,
      cards: document.querySelectorAll("mat-card, [class*='card']").length,
    };
  });
  console.log(`    title: ${resultInfo.title}`);
  console.log(`    row counts: ${JSON.stringify(resultInfo.allRowSelectors)}`);
  console.log(`    mat-tables: ${resultInfo.matTables}, cards: ${resultInfo.cards}`);
  console.log(`\n    body innertext:\n${resultInfo.bodyText}\n`);

  console.log(`[5] search API responses: ${searchResponses.length}`);
  for (const r of searchResponses) {
    console.log(`  ${r.status} ${r.url}`);
    console.log(`    body (${r.bodyLen}b): ${r.bodyHead}`);
  }

  // Save full HTML for diagnosis
  const html = await page.content();
  writeFileSync(join(OUT, "results.html"), html);
  console.log(`\n  saved full HTML (${html.length}b)`);

  console.log(`\n[6] doc responses: ${docResponses.length}`);
  await browser.close();
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
