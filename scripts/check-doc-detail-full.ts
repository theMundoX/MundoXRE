#!/usr/bin/env tsx
/**
 * Get ALL fields from a PublicSearch document detail page.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Go directly to a known document detail page
  // Use a deed of trust doc number from our Dallas data
  console.log("Navigating to Dallas PublicSearch document detail...\n");
  await page.goto("https://dallas.tx.publicsearch.us/doc/66449723", { waitUntil: "domcontentloaded", timeout: 30000 });

  await page.waitForFunction(() => {
    return document.body.textContent?.includes("GRANTOR") || document.body.textContent?.includes("Grantor");
  }, { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(3000);

  // Get ALL text content organized by structure
  const content = await page.evaluate(() => {
    // Get all label-value pairs
    const pairs: Array<{ label: string; value: string }> = [];

    // Method 1: dt/dd pairs
    const dts = document.querySelectorAll("dt");
    for (const dt of dts) {
      const dd = dt.nextElementSibling;
      if (dd) pairs.push({ label: dt.textContent?.trim() || "", value: dd.textContent?.trim() || "" });
    }

    // Method 2: label elements
    const labels = document.querySelectorAll("label");
    for (const label of labels) {
      const input = document.getElementById(label.htmlFor || "");
      const next = label.nextElementSibling;
      const value = input?.textContent?.trim() || next?.textContent?.trim() || "";
      if (value) pairs.push({ label: label.textContent?.trim() || "", value });
    }

    // Method 3: table rows
    const rows = document.querySelectorAll("table tr");
    for (const row of rows) {
      const cells = row.querySelectorAll("th, td");
      if (cells.length === 2) {
        pairs.push({ label: cells[0].textContent?.trim() || "", value: cells[1].textContent?.trim() || "" });
      }
    }

    // Method 4: any element with class containing "label", "field", "key"
    const fieldLabels = document.querySelectorAll('[class*="label"], [class*="field"], [class*="key"], [class*="header"]');
    for (const el of fieldLabels) {
      const text = el.textContent?.trim() || "";
      if (text.length > 0 && text.length < 40) {
        const sibling = el.nextElementSibling?.textContent?.trim() || "";
        if (sibling && sibling.length < 200) {
          pairs.push({ label: text, value: sibling });
        }
      }
    }

    // Also get the full body text to find dollar amounts and rates
    const bodyText = document.body.textContent || "";
    const dollars = bodyText.match(/\$[\d,]+(?:\.\d{2})?/g) || [];
    const rates = bodyText.match(/\d+\.\d+\s*%/g) || [];

    // Get the __data for this page
    const pageData = (window as any).__data;
    let docData = null;
    if (pageData?.docPreview?.document) {
      docData = pageData.docPreview.document;
    }

    return { pairs, dollars, rates, docData: docData ? JSON.stringify(docData).slice(0, 3000) : null };
  });

  console.log("Field pairs found:");
  const seen = new Set<string>();
  for (const p of content.pairs) {
    const key = `${p.label}:${p.value}`;
    if (seen.has(key) || !p.label || !p.value) continue;
    seen.add(key);
    console.log(`  ${p.label.padEnd(30)} ${p.value.slice(0, 60)}`);
  }

  console.log(`\nDollar amounts: ${content.dollars.join(", ") || "none"}`);
  console.log(`Interest rates: ${content.rates.join(", ") || "none"}`);

  if (content.docData) {
    console.log(`\n__data.docPreview.document:`);
    console.log(content.docData);
  }

  await browser.close();
}

main().catch(console.error);
