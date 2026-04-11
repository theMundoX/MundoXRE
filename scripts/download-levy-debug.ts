#!/usr/bin/env tsx
/**
 * Headful debug version of the Levy County downloader.
 * Takes a screenshot at each step so we can see where it breaks.
 */
import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const DEBUG_DIR = "C:/Users/msanc/mxre/data/levy-debug";
if (!existsSync(DEBUG_DIR)) mkdirSync(DEBUG_DIR, { recursive: true });

async function shoot(page: any, name: string) {
  const p = join(DEBUG_DIR, `${String(Date.now()).slice(-6)}-${name}.png`);
  await page.screenshot({ path: p, fullPage: false });
  console.log(`  [screenshot] ${p}`);
}

async function main() {
  console.log("MXRE — Levy County debug downloader\n");

  const { data: records } = await db
    .from("mortgage_records")
    .select("id, document_number")
    .like("source_url", "%levyclerk%")
    .eq("document_type", "mortgage")
    .not("document_number", "is", null)
    .limit(2);

  if (!records || records.length === 0) {
    console.log("No Levy records.");
    return;
  }

  console.log(`Trying ${records.length} docs:`);
  for (const r of records) console.log(`  ${r.document_number}`);

  const browser = await chromium.launch({ headless: true, slowMo: 300 });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36",
    viewport: { width: 1280, height: 900 },
  });
  const page = await ctx.newPage();

  try {
    console.log("\n[1] landing page");
    await page.goto("https://online.levyclerk.com/landmarkweb", {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    });
    await shoot(page, "1-landing");

    // Disclaimer
    try {
      await page.evaluate(() => (window as any).SetDisclaimer?.());
      await page.waitForTimeout(1500);
    } catch (e: any) {
      console.log(`    SetDisclaimer err: ${e.message}`);
    }
    await shoot(page, "2-after-disclaimer");

    console.log("\n[2] navigate to document search");
    await page.goto("https://online.levyclerk.com/LandmarkWeb/Document/Index", {
      waitUntil: "domcontentloaded",
      timeout: 20_000,
    });
    await page.waitForTimeout(1500);
    await shoot(page, "3-doc-index");

    // What search nav buttons exist?
    const navs = await page.evaluate(() => {
      const els = Array.from(document.querySelectorAll(".searchNav"));
      return els.map((el: any) => ({
        text: el.textContent?.trim() || "",
        visible: el.offsetHeight > 0,
        id: el.id,
        cls: el.className,
      }));
    });
    console.log("\n[3] .searchNav elements:");
    console.log(JSON.stringify(navs, null, 2));

    // What submit buttons exist?
    const submits = await page.evaluate(() => {
      const els = Array.from(document.querySelectorAll('[id^="submit-"], button[type="submit"], input[type="submit"]'));
      return els.map((el: any) => ({
        id: el.id,
        type: el.type,
        text: (el.innerText || el.value || "").trim(),
        visible: el.offsetHeight > 0,
      }));
    });
    console.log("\n[4] submit buttons in DOM:");
    console.log(JSON.stringify(submits, null, 2));

    // Try to click an Instrument Number search
    const clicked = await page.evaluate(() => {
      const navs = Array.from(document.querySelectorAll(".searchNav"));
      for (const n of navs as any[]) {
        const t = (n.textContent || "").trim();
        if (/(Instrument|Clerk File)/.test(t) && n.offsetHeight > 0) {
          n.click();
          return t;
        }
      }
      return null;
    });
    console.log(`\n[5] clicked nav: ${clicked || "(none)"}`);
    await page.waitForTimeout(1500);
    await shoot(page, "5-after-nav-click");

    // What inputs exist now?
    const inputs = await page.evaluate(() => {
      const els = Array.from(document.querySelectorAll("input[type='text'], input:not([type])"));
      return els
        .map((el: any) => ({ id: el.id, name: el.name, placeholder: el.placeholder, visible: el.offsetHeight > 0 }))
        .filter((e: any) => e.visible);
    });
    console.log("\n[6] visible inputs:");
    console.log(JSON.stringify(inputs, null, 2));

    // Try to fill the first doc number
    const docNum = records[0].document_number;
    const filled = await page.evaluate((dn) => {
      const candidates = ["#instrumentNumber", "#clerkFileNumber", "#docNumber", "input[name*='instrument' i]"];
      for (const sel of candidates) {
        const el = document.querySelector(sel) as HTMLInputElement | null;
        if (el && el.offsetHeight > 0) {
          el.value = dn;
          el.dispatchEvent(new Event("input", { bubbles: true }));
          return sel;
        }
      }
      return null;
    }, docNum);
    console.log(`\n[7] filled doc number with selector: ${filled || "(none)"}`);
    await shoot(page, "7-after-fill");

    // Submit via whatever submit button is visible
    const submitted = await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('[id^="submit-"], button[type="submit"], input[type="submit"]'));
      for (const b of btns as any[]) {
        if (b.offsetHeight > 0) {
          b.click();
          return b.id || b.type || "?";
        }
      }
      return null;
    });
    console.log(`\n[8] clicked submit: ${submitted || "(none)"}`);
    await page.waitForTimeout(3000);
    await shoot(page, "8-after-submit");

    console.log(`\nAll screenshots in ${DEBUG_DIR}`);
  } catch (e: any) {
    console.log(`error: ${e?.message || e}`);
    await shoot(page, "error");
  } finally {
    await page.waitForTimeout(2000);
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
