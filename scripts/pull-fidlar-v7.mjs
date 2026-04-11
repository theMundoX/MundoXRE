#!/usr/bin/env node
/**
 * Fidlar v7 — multi-page full capture.
 *
 * Flow:
 *   1. Search for the doc
 *   2. Open viewer via doc number button → /#/image
 *   3. Loop: capture current page (canvas.toDataURL) → click Next Page → repeat
 *   4. Stop when the captured image is identical to the previous one (= at end)
 *      OR when the Next Page button disappears/disables
 *      OR safety limit of 50 pages
 *   5. Run MundoX extraction on each page separately
 *   6. Merge all page JSONs into one combined record, preferring the first
 *      non-null value for each field
 */
import { chromium as playExtra } from "playwright-extra";
import stealth from "puppeteer-extra-plugin-stealth";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

playExtra.use(stealth());

const OUT_BASE = "C:/Users/msanc/mxre/data/labeling-sample";
const MUNDOX_URL = "http://127.0.0.1:18791/v1/chat/completions";
const MAX_PAGES = 50;

const db = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const PROMPT = `You are extracting structured data from a scanned recorded mortgage document page. Focus on the actual document content — ignore the yellow viewer UI.

Return ONLY a JSON object (no markdown, no preamble). Use null for any field you cannot clearly read on THIS page.

{
  "document_type": "mortgage" | "open_end_mortgage" | "deed_of_trust" | "heloc" | "assignment" | "release" | "modification" | "other",
  "recording_date": "YYYY-MM-DD or null",
  "book_page": "book/page ref if shown, or null",
  "document_number": "instrument number as shown, or null",
  "borrower_name": "borrower / mortgagor full name(s), or null",
  "lender_name": "lender / mortgagee full name, or null",
  "lender_address": "lender address, or null",
  "loan_amount_cents": "principal amount in CENTS (dollars*100), or null",
  "max_principal_cents": "for open-end: credit line max in CENTS, or null",
  "interest_rate": "annual rate as decimal percent (e.g. 6.875), or null",
  "interest_rate_type": "fixed | adjustable | variable | null",
  "term_months": "loan term in months, or null",
  "maturity_date": "YYYY-MM-DD, or null",
  "monthly_payment_cents": "monthly P&I payment in CENTS, or null",
  "property_address": "full subject property address, or null",
  "property_county": "county, or null",
  "property_state": "2-letter state, or null",
  "parcel_id": "tax parcel ID if shown, or null",
  "signing_date": "YYYY-MM-DD the doc was signed, or null",
  "notes": "1-sentence summary of this specific page"
}

Rules:
- loan_amount_cents and monthly_payment_cents are CENTS (dollars*100).
- interest_rate is the rate printed on the doc, not estimated.
- Only fill in what you can CLEARLY see on this specific page. Null is correct for missing fields.
- Return ONLY the JSON object.`;

async function capturePage(page) {
  return await page.evaluate(async () => {
    const imgs = Array.from(document.querySelectorAll("img")).filter((i) => i.naturalWidth > 500 && i.naturalHeight > 500);
    if (imgs[0]) {
      try {
        const r = await fetch(imgs[0].src, { credentials: "include" });
        if (r.ok) {
          const buf = await r.arrayBuffer();
          return { kind: "img-fetched", bytes: Array.from(new Uint8Array(buf)), w: imgs[0].naturalWidth, h: imgs[0].naturalHeight };
        }
      } catch {}
      try {
        const c = document.createElement("canvas");
        c.width = imgs[0].naturalWidth;
        c.height = imgs[0].naturalHeight;
        c.getContext("2d").drawImage(imgs[0], 0, 0);
        const url = c.toDataURL("image/png");
        return { kind: "img-canvas", dataUrl: url, w: c.width, h: c.height };
      } catch {}
    }
    const canvases = Array.from(document.querySelectorAll("canvas")).filter((c) => c.width > 300 && c.height > 300).sort((a, b) => b.width * b.height - a.width * a.height);
    if (canvases[0]) {
      const c = canvases[0];
      try {
        return { kind: "canvas", dataUrl: c.toDataURL("image/png"), w: c.width, h: c.height };
      } catch (e) {
        return { kind: "canvas-tainted", error: String(e) };
      }
    }
    return { kind: "none" };
  });
}

function infoToBuffer(info) {
  if (info.kind === "img-fetched") return Buffer.from(info.bytes);
  if (info.kind === "img-canvas" || info.kind === "canvas") {
    return Buffer.from(info.dataUrl.split(",")[1], "base64");
  }
  return null;
}

async function mundoxExtract(imgBuf) {
  const b64 = imgBuf.toString("base64");
  const t0 = Date.now();
  const resp = await fetch(MUNDOX_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "mundox",
      messages: [{
        role: "user",
        content: [
          { type: "text", text: PROMPT },
          { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
        ],
      }],
      max_tokens: 1500,
      temperature: 0.1,
    }),
  });
  const d = await resp.json();
  const raw = d.choices?.[0]?.message?.content || "";
  const elapsed = Date.now() - t0;
  let parsed = null;
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) {
    try { parsed = JSON.parse(m[0]); } catch {}
  }
  return { raw, parsed, elapsed };
}

/** Merge a list of per-page JSONs: first non-null wins for each field. */
function mergeExtractions(perPage) {
  const merged = {};
  const keys = new Set();
  for (const p of perPage) if (p) for (const k of Object.keys(p)) keys.add(k);
  for (const k of keys) {
    for (const p of perPage) {
      if (p && p[k] !== null && p[k] !== undefined && p[k] !== "") {
        merged[k] = p[k];
        break;
      }
    }
    if (merged[k] === undefined) merged[k] = null;
  }
  return merged;
}

async function main() {
  // Use the 22-page VA loan we already verified in v5
  const docNumber = process.argv[2] || "201900009659";
  const { data } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name, recording_date")
    .eq("document_number", docNumber)
    .limit(1);
  const rec = data?.[0];
  if (!rec) { console.error(`no record ${docNumber}`); process.exit(1); }

  const docDir = join(OUT_BASE, `fidlar-${docNumber}-multipage`);
  mkdirSync(docDir, { recursive: true });

  console.log(`\n═══ ${docNumber} ═══`);
  console.log(`  Amount: $${rec.loan_amount?.toLocaleString()}`);

  const browser = await playExtra.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1600, height: 1400 },
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    locale: "en-US",
    timezoneId: "America/New_York",
  });
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
    window.chrome = { runtime: {} };
  });
  const page = await ctx.newPage();

  // 1. search
  console.log("\n[1] search");
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
  }, docNumber);
  await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) if ((b.textContent || "").trim() === "Search") { b.click(); return; }
  });
  await page.waitForTimeout(9000);

  // 2. open viewer
  console.log("[2] open viewer");
  await page.evaluate((dn) => {
    const btns = Array.from(document.querySelectorAll("button")).filter((b) => b.offsetHeight > 0);
    for (const b of btns) if ((b.textContent || "").trim() === dn) { b.click(); return; }
  }, docNumber);
  await page.waitForTimeout(8000);

  if (!page.url().includes("/image")) {
    console.error("  viewer didn't open");
    await browser.close();
    process.exit(1);
  }

  // 3. loop: capture pages until we see repeats or hit safety
  console.log("[3] capturing all pages");
  const savedPages = [];
  let prevHash = "";
  let repeatCount = 0;

  for (let i = 1; i <= MAX_PAGES; i++) {
    await page.waitForTimeout(2500);
    const info = await capturePage(page);
    const buf = infoToBuffer(info);

    if (!buf) {
      console.log(`  page ${i}: capture failed (${info.kind})`);
      break;
    }

    const hash = createHash("sha256").update(buf).digest("hex").slice(0, 16);
    if (hash === prevHash) {
      repeatCount++;
      console.log(`  page ${i}: same hash as previous (repeat ${repeatCount})`);
      if (repeatCount >= 2) {
        console.log(`  hit repeat limit → we're at the end (real doc has ${i - 1} pages)`);
        break;
      }
    } else {
      repeatCount = 0;
    }
    prevHash = hash;

    const outPath = join(docDir, `page${String(i).padStart(2, "0")}.png`);
    writeFileSync(outPath, buf);
    console.log(`  page ${i}: ${info.kind} ${info.w}x${info.h} → ${(buf.length/1024).toFixed(0)}KB  hash=${hash}`);
    savedPages.push({ idx: i, path: outPath, hash, w: info.w, h: info.h, size: buf.length });

    // Click Next Page — try multiple selectors / variants
    const nextResult = await page.evaluate(() => {
      const allBtns = Array.from(document.querySelectorAll("button, a, [role='button']")).filter((b) => b.offsetHeight > 0);
      const btnList = allBtns.map((b) => ({
        tag: b.tagName.toLowerCase(),
        text: (b.textContent || "").trim().slice(0, 40),
        aria: b.getAttribute("aria-label") || "",
        title: b.getAttribute("title") || "",
        disabled: b.disabled || b.getAttribute("aria-disabled") === "true",
      }));

      // Strategy 1: exact text match
      for (const b of allBtns) {
        const t = (b.textContent || "").trim();
        if (t === "Next Page" || t === "Next" || t === "›" || t === ">") {
          if (b.disabled || b.getAttribute("aria-disabled") === "true") return { result: "disabled", matched: t, btnList };
          b.click();
          return { result: "clicked", matched: t, btnList };
        }
      }
      // Strategy 2: title attribute (Fidlar uses icon buttons with title="Next Page")
      for (const b of allBtns) {
        const title = (b.getAttribute("title") || "").trim();
        if (title === "Next Page" || title.toLowerCase() === "next") {
          if (b.disabled || b.getAttribute("aria-disabled") === "true") return { result: "disabled", matched: `title:${title}`, btnList };
          b.click();
          return { result: "clicked", matched: `title:${title}`, btnList };
        }
      }
      // Strategy 3: aria-label match
      for (const b of allBtns) {
        const a = (b.getAttribute("aria-label") || "").toLowerCase();
        if (a.includes("next")) {
          if (b.disabled) return { result: "disabled", matched: `aria:${a}`, btnList };
          b.click();
          return { result: "clicked", matched: `aria:${a}`, btnList };
        }
      }
      return { result: "not-found", matched: null, btnList };
    });

    if (nextResult.result === "not-found") {
      console.log(`  no Next Page button → reached end at page ${i}`);
      console.log(`  (buttons seen: ${JSON.stringify(nextResult.btnList.slice(0, 15))})`);
      break;
    }
    if (nextResult.result === "disabled") {
      console.log(`  Next Page disabled (${nextResult.matched}) → reached end at page ${i}`);
      break;
    }
    console.log(`    clicked: "${nextResult.matched}"`);
  }

  await browser.close();

  // Dedupe: drop the duplicate final repeated page
  while (savedPages.length > 1 && savedPages[savedPages.length - 1].hash === savedPages[savedPages.length - 2].hash) {
    const drop = savedPages.pop();
    console.log(`  [dedupe] dropping duplicate ${drop.path}`);
  }

  console.log(`\n[4] captured ${savedPages.length} unique pages`);

  // 4. run MundoX extraction on each page
  console.log("\n[5] extracting fields from each page");
  const perPage = [];
  for (const p of savedPages) {
    const imgBuf = readFileSync(p.path);
    const { raw, parsed, elapsed } = await mundoxExtract(imgBuf);
    perPage.push(parsed);
    const summary = parsed ? Object.entries(parsed).filter(([k, v]) => v !== null && v !== "" && !["notes", "document_type"].includes(k)).map(([k, v]) => `${k}=${String(v).slice(0, 25)}`).join(" ") : "(no JSON)";
    console.log(`  page ${p.idx} (${elapsed}ms): ${summary.slice(0, 250)}`);
    writeFileSync(join(docDir, `page${String(p.idx).padStart(2, "0")}.extract.json`), JSON.stringify({ raw, parsed, elapsed_ms: elapsed }, null, 2));
  }

  // 5. merge
  const merged = mergeExtractions(perPage);
  console.log("\n[6] MERGED RESULT:");
  console.log(JSON.stringify(merged, null, 2));

  writeFileSync(
    join(docDir, "merged.json"),
    JSON.stringify({
      source: `Fidlar Fairfield OH ${docNumber}`,
      doc_dir: docDir,
      total_pages_captured: savedPages.length,
      per_page_extractions: perPage,
      merged,
      created_at: new Date().toISOString(),
    }, null, 2)
  );

  console.log(`\nSaved → ${join(docDir, "merged.json")}`);
}

main().catch((e) => { console.error("fatal:", e); process.exit(1); });
