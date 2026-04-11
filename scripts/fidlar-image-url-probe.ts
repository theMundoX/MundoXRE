#!/usr/bin/env tsx
/**
 * Find the REAL current Fidlar AVA document image URL pattern.
 *
 * Strategy:
 *   1. Open ava.fidlar.com in a real Chromium session
 *   2. Log every single network request the browser makes
 *   3. Search for a known mortgage doc (Fairfield County, OH)
 *   4. Click the result to open the document viewer
 *   5. Capture every image/pdf/stream request — that's where the real URL lives
 *
 * Output: prints the URL patterns that returned image content.
 */
import { chromium } from "playwright";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/fidlar-probe";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

interface ImageReq {
  url: string;
  status: number;
  contentType: string;
  contentLength: number;
}

async function main() {
  console.log("FIDLAR AVA — Image URL reverse-engineering probe\n");

  // Pick ONE real Fairfield OH mortgage record
  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number, loan_amount, source_url, borrower_name, lender_name")
    .eq("document_type", "mortgage")
    .not("loan_amount", "is", null)
    .gt("loan_amount", 0)
    .like("source_url", "%OHFairfield%")
    .limit(1);

  const rec = records?.[0];
  if (!rec) {
    console.error("no Fairfield OH record found");
    process.exit(1);
  }

  console.log(`Target doc: ${rec.document_number}`);
  console.log(`  Borrower: ${rec.borrower_name}`);
  console.log(`  Lender:   ${rec.lender_name}`);
  console.log(`  Amount:   $${rec.loan_amount?.toLocaleString()}\n`);

  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
    viewport: { width: 1400, height: 900 },
  });
  const page = await ctx.newPage();

  // Log ALL requests
  const allRequests: string[] = [];
  const imageResponses: ImageReq[] = [];

  page.on("request", (req) => {
    allRequests.push(`${req.method()} ${req.url()}`);
  });

  page.on("response", async (resp) => {
    const ct = resp.headers()["content-type"] || "";
    const cl = Number(resp.headers()["content-length"] || "0");
    const url = resp.url();
    if (
      ct.includes("image") ||
      ct.includes("pdf") ||
      ct.includes("tiff") ||
      ct.includes("octet-stream") ||
      /\.(png|jpg|jpeg|tiff|pdf)(\?|$)/i.test(url)
    ) {
      imageResponses.push({
        url,
        status: resp.status(),
        contentType: ct,
        contentLength: cl,
      });
    }
  });

  console.log("[1] loading ava.fidlar.com/OHFairfield/AvaWeb/");
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", {
    waitUntil: "networkidle",
    timeout: 30000,
  });
  await page.waitForTimeout(3000);
  await page.screenshot({ path: join(OUT, "1-landing.png"), fullPage: true });

  console.log("[2] trying to search for doc number");
  // Look for any input that accepts a document number
  const searchInputFound = await page.evaluate((docNum) => {
    const inputs = Array.from(document.querySelectorAll("input"));
    for (const i of inputs as any[]) {
      if (i.offsetHeight > 0 && (i.type === "text" || i.type === "search" || !i.type)) {
        const ph = (i.placeholder || "").toLowerCase();
        const name = (i.name || "").toLowerCase();
        const id = (i.id || "").toLowerCase();
        if (
          ph.includes("document") ||
          ph.includes("instrument") ||
          ph.includes("file") ||
          name.includes("document") ||
          id.includes("document") ||
          ph.includes("search")
        ) {
          i.value = docNum;
          i.dispatchEvent(new Event("input", { bubbles: true }));
          return { id: i.id, name: i.name, placeholder: i.placeholder };
        }
      }
    }
    return null;
  }, rec.document_number);

  console.log(`  search input used: ${JSON.stringify(searchInputFound)}`);
  await page.waitForTimeout(1000);

  // Try Enter key to submit
  await page.keyboard.press("Enter");
  await page.waitForTimeout(4000);
  await page.screenshot({ path: join(OUT, "2-after-search.png"), fullPage: true });

  console.log("[3] looking for search result to click");
  const resultInfo = await page.evaluate(() => {
    // Look for mat-row (Angular Material), tr, or any row-like structure
    const rowSelectors = ["mat-row", "tr[role='row']", "[class*='row']:not(header)", "button[role='row']"];
    for (const sel of rowSelectors) {
      const rows = document.querySelectorAll(sel);
      for (const r of rows as any) {
        if (r.offsetHeight > 10) {
          const text = (r.textContent || "").trim().slice(0, 100);
          return { selector: sel, text };
        }
      }
    }
    return null;
  });
  console.log(`  first result: ${JSON.stringify(resultInfo)}`);

  // Click the first visible row
  try {
    await page.evaluate((sel) => {
      if (!sel) return;
      const el = document.querySelector(sel) as HTMLElement | null;
      if (el) el.click();
    }, resultInfo?.selector);
    await page.waitForTimeout(5000);
    await page.screenshot({ path: join(OUT, "3-after-click.png"), fullPage: true });
  } catch (e: any) {
    console.log(`  click failed: ${e.message}`);
  }

  // Force a few scrolls to trigger any lazy-loaded images
  await page.keyboard.press("PageDown");
  await page.waitForTimeout(1500);
  await page.keyboard.press("PageDown");
  await page.waitForTimeout(1500);
  await page.screenshot({ path: join(OUT, "4-after-scroll.png"), fullPage: true });

  console.log("\n=== IMAGE/PDF RESPONSES CAPTURED ===");
  if (imageResponses.length === 0) {
    console.log("(none — the document viewer may not have loaded)");
  }
  for (const r of imageResponses) {
    console.log(`  ${r.status}  ${r.contentType.padEnd(22)} ${r.contentLength}b  ${r.url.slice(0, 140)}`);
  }

  console.log(`\n=== ALL REQUESTS (${allRequests.length} total) — filtering for interesting ones ===`);
  for (const r of allRequests) {
    if (/image|Image|document|Document|page|Page|view|View|breeze|ScrapRelay|GetDoc|doc\//i.test(r)) {
      console.log(`  ${r.slice(0, 150)}`);
    }
  }

  writeFileSync(join(OUT, "all-requests.txt"), allRequests.join("\n"));
  writeFileSync(join(OUT, "image-responses.json"), JSON.stringify(imageResponses, null, 2));
  console.log(`\nFull log saved to ${OUT}/all-requests.txt`);

  await browser.close();
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
