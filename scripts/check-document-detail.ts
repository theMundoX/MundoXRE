#!/usr/bin/env tsx
/**
 * Check what fields are available on document detail pages
 * for both LandmarkWeb and PublicSearch portals.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });

  // ─── Test 1: LandmarkWeb (Levy County FL) — document detail ───
  console.log("═══ LANDMARKWEB (Levy County FL) — Document Detail ═══\n");

  const page1 = await ctx.newPage();
  await page1.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page1.evaluate(() => (window as any).SetDisclaimer());
  await page1.waitForTimeout(2000);

  // Search by record date
  await page1.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim() === "Record Date Search" && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page1.waitForTimeout(500);
  await page1.fill("#beginDate-RecordDate", "03/20/2026");
  await page1.fill("#endDate-RecordDate", "03/20/2026");

  // Capture search results
  const resultPromise = new Promise<string>((resolve) => {
    page1.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 20000);
  });
  await page1.click("#submit-RecordDate");
  const jsonStr = await resultPromise;

  if (jsonStr) {
    const data = JSON.parse(jsonStr);
    // Find a mortgage record
    for (const row of data.data) {
      const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";
      const cols: string[] = [];
      for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");

      const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
      if (dateIdx < 0) continue;
      const docType = cols[dateIdx + 1] || "";
      if (!docType.toUpperCase().includes("MORTGAGE")) continue;

      // Found a mortgage — extract the doc ID from the raw HTML
      const rawHtml = row["25"] || row["27"] || "";
      const docIdMatch = rawHtml.match(/hidden_(\d+)/);
      const docId = docIdMatch?.[1];
      console.log(`Found mortgage — Doc ID: ${docId}`);
      console.log(`Grantor: ${cols[dateIdx - 2]}`);
      console.log(`Grantee: ${cols[dateIdx - 1]}`);

      if (docId) {
        // Navigate to document detail page
        console.log(`\nNavigating to detail page...`);

        // Capture the detail API response
        const detailPromise = new Promise<string>((resolve) => {
          page1.on("response", async (resp) => {
            const url = resp.url();
            if (url.includes("GetDocumentDetail") || url.includes("Detail") || url.includes("document")) {
              try { resolve(await resp.text()); } catch { resolve(""); }
            }
          });
          setTimeout(() => resolve(""), 10000);
        });

        // Try clicking the document link/row
        await page1.evaluate((id) => {
          // Look for clickable elements with the doc ID
          const links = document.querySelectorAll(`a[href*="${id}"], [onclick*="${id}"], tr[data-id="${id}"]`);
          if (links.length > 0) (links[0] as HTMLElement).click();
          // Also try the view icon
          const eye = document.getElementById(`eye_${id}_1`);
          if (eye) eye.click();
        }, docId);

        await page1.waitForTimeout(3000);
        const detailResponse = await detailPromise;

        if (detailResponse) {
          console.log(`Detail response: ${detailResponse.length} chars`);
          console.log(`Preview: ${detailResponse.slice(0, 500)}`);
        }

        // Check the current page for detail info
        const pageText = await page1.evaluate(() => {
          // Look for consideration/amount fields
          const labels = document.querySelectorAll("label, th, td, span, div");
          const fields: string[] = [];
          for (const el of labels) {
            const text = el.textContent?.trim() || "";
            if (text.match(/consider|amount|value|price|rate|interest|principal|loan/i)) {
              const next = el.nextElementSibling?.textContent?.trim() || "";
              fields.push(`${text}: ${next}`);
            }
          }
          return fields;
        });

        if (pageText.length > 0) {
          console.log(`\nDetail page fields:`);
          for (const f of pageText) console.log(`  ${f}`);
        }

        // Also get the full URL and page content
        console.log(`\nCurrent URL: ${page1.url()}`);

        // Check for a document viewer or detail panel
        const detailContent = await page1.evaluate(() => {
          const panel = document.querySelector('#detailPanel, .detail-panel, .document-detail, [class*="detail"]');
          return panel?.textContent?.trim()?.slice(0, 500) || "no detail panel found";
        });
        console.log(`\nDetail panel: ${detailContent}`);
      }
      break;
    }
  }

  // ─── Test 2: PublicSearch (Dallas TX) — document detail ───
  console.log("\n\n═══ PUBLICSEARCH (Dallas TX) — Document Detail ═══\n");

  const page2 = await ctx.newPage();
  await page2.goto("https://dallas.tx.publicsearch.us/results?department=RP&recordedDateRange=2026-03-01to2026-03-26&docType=DEED+OF+TRUST", { waitUntil: "domcontentloaded", timeout: 30000 });

  await page2.waitForFunction(() => document.querySelectorAll("table tbody tr").length > 0, { timeout: 30000 });
  await page2.waitForTimeout(2000);

  // Click the first result to see detail
  const firstRow = page2.locator("table tbody tr").first();
  await firstRow.click();
  await page2.waitForTimeout(3000);

  // Check what's in the detail view
  console.log(`URL after click: ${page2.url()}`);

  const detailFields = await page2.evaluate(() => {
    const body = document.body.textContent || "";
    // Look for dollar amounts or interest rates
    const amounts = body.match(/\$[\d,]+\.?\d*/g) || [];
    const rates = body.match(/\d+\.?\d*\s*%/g) || [];

    // Look for specific field labels
    const labels = document.querySelectorAll("dt, label, th, .field-label, [class*='label']");
    const fieldPairs: string[] = [];
    for (const label of labels) {
      const text = label.textContent?.trim();
      if (text && text.length < 50) {
        const value = label.nextElementSibling?.textContent?.trim() || "";
        if (value) fieldPairs.push(`${text}: ${value.slice(0, 60)}`);
      }
    }

    return { amounts: amounts.slice(0, 10), rates: rates.slice(0, 5), fields: fieldPairs.slice(0, 20) };
  });

  console.log("Dollar amounts found:", detailFields.amounts);
  console.log("Percentages found:", detailFields.rates);
  console.log("Field pairs:");
  for (const f of detailFields.fields) console.log(`  ${f}`);

  await browser.close();
}

main().catch(console.error);
