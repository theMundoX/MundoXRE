#!/usr/bin/env node
/**
 * Cook County IL v4 — click the "View" button in the result row,
 * not the row itself. Then capture the document image.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample/real-002-cook";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  console.log("Cook County v4 — click the View link\n");

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1400, height: 1000 },
    acceptDownloads: true,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Capture everything that looks like a document
  const docResponses = [];
  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const url = resp.url();
    const size = Number(resp.headers()["content-length"] || 0);
    if (
      (ct.includes("pdf") ||
       ct.includes("tiff") ||
       ct.includes("octet-stream") ||
       (ct.includes("image") && size > 50000)) &&
      !/seal|logo|icon|sprite|header|footer|banner|favicon/i.test(url)
    ) {
      docResponses.push({ url, status: resp.status(), ct, size });
      console.log(`  [HIT] ${resp.status()} ${ct} ${(size / 1024).toFixed(0)}KB  ${url.slice(0, 140)}`);
      // Save the PDF/image directly from the response
      try {
        if (size > 10_000 || ct.includes("pdf")) {
          const buf = await resp.body();
          const ext = ct.includes("pdf") ? "pdf" : ct.includes("tiff") ? "tif" : "png";
          const fname = `doc-${docResponses.length}.${ext}`;
          writeFileSync(join(OUT, fname), buf);
          console.log(`         saved → ${fname} (${buf.length} bytes)`);
        }
      } catch (e) {
        console.log(`         save failed: ${e.message}`);
      }
    }
  });

  // Capture downloads via the download event as a backup
  page.on("download", async (dl) => {
    const fname = `download-${Date.now()}-${dl.suggestedFilename()}`;
    const p = join(OUT, fname);
    await dl.saveAs(p);
    console.log(`  [DL] saved download → ${p}`);
  });

  console.log("[1] navigating to advanced doc-type search");
  await page.goto("https://crs.cookcountyclerkil.gov/Search", { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);
  await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll("a"));
    for (const el of links) {
      if ((el.textContent || "").trim() === "Advanced Search") { el.click(); return; }
    }
  });
  await page.waitForTimeout(2500);

  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button, a"));
    for (const el of btns) {
      if ((el.textContent || "").trim() === "Document Type Search") { el.click(); return; }
    }
  });
  await page.waitForTimeout(2000);

  console.log("[2] filling form");
  await page.evaluate(() => {
    const sel = document.querySelector("select#DocumentType");
    if (sel) {
      const mort = Array.from(sel.options).find((o) => /^MORTGAGE$/i.test(o.text.trim()));
      if (mort) {
        sel.value = mort.value;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }
    const from = document.querySelector("#RecordedFromDate");
    if (from) {
      from.value = "01/01/2024";
      from.dispatchEvent(new Event("change", { bubbles: true }));
    }
    const to = document.querySelector("#RecordedToDate");
    if (to) {
      to.value = "01/07/2024";
      to.dispatchEvent(new Event("change", { bubbles: true }));
    }
  });
  await page.waitForTimeout(500);

  console.log("[3] submit");
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button, input[type='submit']")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) {
      const t = (b.textContent || b.value || "").trim();
      if (t === "Search") { b.click(); return; }
    }
  });
  await page.waitForTimeout(7000);
  await page.screenshot({ path: join(OUT, "results.png"), fullPage: true });

  console.log("[4] inspecting results row structure");
  const rowInfo = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("tbody tr")).filter((r) => r.offsetHeight > 10);
    if (rows.length === 0) return null;
    const first = rows[0];
    // Dump all anchors and buttons inside the first row
    const interactables = Array.from(first.querySelectorAll("a, button")).map((el) => ({
      tag: el.tagName.toLowerCase(),
      text: (el.textContent || "").trim().slice(0, 40),
      href: el.href || "",
      onclick: (el.getAttribute("onclick") || "").slice(0, 80),
      id: el.id,
      dataAttrs: Object.fromEntries(Array.from(el.attributes).filter((a) => a.name.startsWith("data-")).map((a) => [a.name, a.value])),
    }));
    const rowText = (first.textContent || "").trim().slice(0, 200);
    return { count: rows.length, rowText, interactables };
  });
  console.log(`    rows: ${rowInfo?.count}`);
  console.log(`    first row text: ${rowInfo?.rowText}`);
  console.log(`    interactables in first row:`);
  for (const i of rowInfo?.interactables || []) {
    console.log(`      ${i.tag} "${i.text}" href="${i.href}" onclick="${i.onclick}" data=${JSON.stringify(i.dataAttrs)}`);
  }

  console.log("\n[5] clicking the View anchor specifically");
  const viewClicked = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("tbody tr")).filter((r) => r.offsetHeight > 10);
    if (rows.length === 0) return null;
    const first = rows[0];
    const viewLink = Array.from(first.querySelectorAll("a, button")).find((el) => (el.textContent || "").trim() === "View");
    if (!viewLink) return "no View link";
    viewLink.click();
    return "clicked";
  });
  console.log(`    ${viewClicked}`);
  await page.waitForTimeout(8000); // give it time to open the viewer
  console.log(`    url now: ${page.url()}`);
  await page.screenshot({ path: join(OUT, "after-view.png"), fullPage: true });

  // Scroll in case doc viewer is below
  await page.keyboard.press("End");
  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(OUT, "after-scroll.png"), fullPage: true });

  console.log(`\n[6] doc responses captured: ${docResponses.length}`);
  for (const h of docResponses) console.log(`  ${h.status} ${h.ct} ${(h.size/1024).toFixed(0)}KB ${h.url}`);

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
