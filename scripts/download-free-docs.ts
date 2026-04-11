#!/usr/bin/env tsx
/**
 * Download free document images from LandmarkWeb portals.
 * Saves to filesystem: /opt/mxre/docs/{state}/{county}/{docId}/page{N}.png
 * Stores document_path in the mortgage_records table.
 */
import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const DOCS_DIR = join(process.cwd(), "docs");

async function main() {
  console.log("MXRE — Download Free Document Images\n");

  // Get mortgage records from Levy County that don't have documents yet
  const { data: records } = await db.from("mortgage_records")
    .select("id, document_number, document_type, source_url, borrower_name")
    .like("source_url", "%levyclerk%")
    .eq("document_type", "mortgage")
    .not("document_number", "is", null)
    .limit(20);

  if (!records || records.length === 0) {
    console.log("No Levy County mortgage records to download.");
    return;
  }

  console.log(`Found ${records.length} mortgage records to download.\n`);

  // Setup browser
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Setup LandmarkWeb session
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);
  await page.goto("https://online.levyclerk.com/LandmarkWeb/Document/Index", { waitUntil: "networkidle", timeout: 15000 });

  let downloaded = 0;
  let failed = 0;

  for (const record of records) {
    const docDir = join(DOCS_DIR, "FL", "levy", record.document_number);

    // Skip if already downloaded
    if (existsSync(join(docDir, "page1.png"))) {
      console.log(`  Skip ${record.document_number} (already exists)`);
      continue;
    }

    // We need to find the internal docId from the document_number (CFN)
    // Search by CFN using the Instrument Number search
    try {
      // Navigate to instrument number search
      await page.evaluate(() => {
        const navs = document.querySelectorAll(".searchNav");
        for (const nav of navs) {
          const text = nav.textContent?.trim() || "";
          if ((text.includes("Instrument") || text.includes("Clerk File")) && (nav as HTMLElement).offsetHeight > 0) {
            (nav as HTMLElement).click();
            break;
          }
        }
      });
      await page.waitForTimeout(500);

      // Fill CFN
      const cfnInput = page.locator("#instrumentNumber, #clerkFileNumber");
      if (await cfnInput.count() > 0) {
        await cfnInput.fill(record.document_number);
      }

      // Capture search results
      const resultPromise = new Promise<string>((resolve) => {
        const handler = async (resp: any) => {
          if (resp.url().includes("GetSearchResults")) {
            try { resolve(await resp.text()); } catch { resolve(""); }
            page.off("response", handler);
          }
        };
        page.on("response", handler);
        setTimeout(() => resolve(""), 15000);
      });

      // Submit
      const submitBtn = page.locator("[id^='submit-']").first();
      await submitBtn.click();
      const jsonStr = await resultPromise;

      if (!jsonStr || !jsonStr.startsWith("{")) {
        console.log(`  ${record.document_number}: no search results`);
        failed++;
        continue;
      }

      const data = JSON.parse(jsonStr);
      if (!data.data || data.data.length === 0) {
        console.log(`  ${record.document_number}: no results`);
        failed++;
        continue;
      }

      // Extract internal doc ID
      let internalId = "";
      const row = data.data[0];
      for (let i = 0; i < 30; i++) {
        const raw = row[String(i)] || "";
        const match = raw.match(/hidden_(\d+)/);
        if (match) { internalId = match[1]; break; }
      }

      if (!internalId) {
        console.log(`  ${record.document_number}: no internal ID found`);
        failed++;
        continue;
      }

      // Set up document session
      await page.evaluate(async (id) => {
        await fetch(`/LandmarkWeb/Document/SetSessionNavigation`, { method: "POST", credentials: "include" });
        await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
      }, internalId);
      await page.waitForTimeout(500);

      // Get page count from nav response
      const navHtml = await page.evaluate(async (id) => {
        const r = await fetch(`/LandmarkWeb/Document/DocumentNavigation?id=${id}&row=1&time=${new Date()}`, { credentials: "include" });
        return await r.text();
      }, internalId);

      // Count pages from carousel
      const carouselHtml = await page.evaluate(async () => {
        const r = await fetch(`/LandmarkWeb/Document/DocumentImageCarouselCount?type=normal&time=${new Date()}`, { credentials: "include" });
        return await r.text();
      });
      const pageCountMatch = carouselHtml.match(/(\d+)/g);
      const totalPages = pageCountMatch ? Math.max(...pageCountMatch.map(Number)) : 1;

      // Create directory
      mkdirSync(docDir, { recursive: true });

      // Download all pages
      const maxPages = Math.min(totalPages, 30); // Cap at 30 pages
      let savedPages = 0;

      for (let pg = 0; pg < maxPages; pg++) {
        const imgUrl = `https://online.levyclerk.com/LandmarkWeb/Document/GetDocumentImage/?documentId=${internalId}&index=0&pageNum=${pg}&type=normal&time=${encodeURIComponent(new Date().toString())}&rotate=0`;

        const imgBuffer = await page.evaluate(async (url) => {
          const r = await fetch(url, { credentials: "include" });
          if (!r.ok) return null;
          const buffer = await r.arrayBuffer();
          return Array.from(new Uint8Array(buffer));
        }, imgUrl);

        if (!imgBuffer) break;

        const buffer = Buffer.from(imgBuffer);
        if (buffer.length < 1000) break; // Probably an error page

        writeFileSync(join(docDir, `page${pg + 1}.png`), buffer);
        savedPages++;
      }

      if (savedPages > 0) {
        console.log(`  ✓ ${record.document_number}: ${savedPages} pages saved to ${docDir}`);
        downloaded++;
      } else {
        console.log(`  ✗ ${record.document_number}: no pages downloaded`);
        failed++;
      }
    } catch (err: any) {
      console.log(`  ✗ ${record.document_number}: ${err.message.slice(0, 60)}`);
      failed++;
    }
  }

  await browser.close();

  console.log(`\n  Downloaded: ${downloaded} | Failed: ${failed}`);
}

main().catch(console.error);
