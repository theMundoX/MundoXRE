#!/usr/bin/env tsx
/**
 * Pull REAL mortgage records from Dallas County TX — fixed extraction.
 */

import { chromium } from "playwright";

const BASE = "https://dallas.tx.publicsearch.us";

async function main() {
  console.log("Launching browser...");
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await context.newPage();

  try {
    const url = `${BASE}/results?department=RP&recordedDateRange=2026-03-17to2026-03-26&docType=DEED+OF+TRUST`;
    console.log(`Fetching Deed of Trust records...`);
    await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
    await page.waitForTimeout(3000);

    // First, let's understand the table structure
    const headers = await page.evaluate(() => {
      const ths = document.querySelectorAll("table thead th");
      return Array.from(ths).map((th, i) => `[${i}] ${th.textContent?.trim()}`);
    });
    console.log("Table headers:", headers.join(" | "));

    // Now check how the cells are structured in a row
    const rowStructure = await page.evaluate(() => {
      const firstRow = document.querySelector("table tbody tr");
      if (!firstRow) return "no rows";
      const cells = firstRow.querySelectorAll("td");
      return Array.from(cells).map((td, i) => {
        // Check for nested elements
        const spans = td.querySelectorAll("span, div, a");
        const nested = Array.from(spans).map(s => s.textContent?.trim()).filter(Boolean);
        return `[${i}] "${td.textContent?.trim().slice(0, 50)}" (${nested.length} nested)`;
      });
    });
    console.log("\nFirst row structure:");
    for (const r of rowStructure) console.log(`  ${r}`);

    // Get the actual __data which should have structured records
    const records = await page.evaluate(() => {
      const data = (window as any).__data;
      if (!data) return null;

      // Look for search results in all keys
      const keys = Object.keys(data);
      for (const key of keys) {
        const val = data[key];
        if (val && typeof val === "object") {
          // Check for results arrays
          for (const subKey of Object.keys(val)) {
            if (Array.isArray(val[subKey]) && val[subKey].length > 0) {
              const first = val[subKey][0];
              if (first && (first.docType || first.recordedDate || first.grantor || first.instrumentNumber)) {
                return {
                  path: `${key}.${subKey}`,
                  count: val[subKey].length,
                  sample: val[subKey].slice(0, 3),
                  keys: Object.keys(first),
                };
              }
            }
          }

          // Check nested objects
          for (const subKey of Object.keys(val)) {
            if (val[subKey] && typeof val[subKey] === "object" && !Array.isArray(val[subKey])) {
              for (const subSubKey of Object.keys(val[subKey])) {
                if (Array.isArray(val[subKey][subSubKey]) && val[subKey][subSubKey].length > 0) {
                  const first = val[subKey][subSubKey][0];
                  if (first && (first.docType || first.recordedDate || first.grantor || first.instrumentNumber)) {
                    return {
                      path: `${key}.${subKey}.${subSubKey}`,
                      count: val[subKey][subSubKey].length,
                      sample: val[subKey][subSubKey].slice(0, 3),
                      keys: Object.keys(first),
                    };
                  }
                }
              }
            }
          }
        }
      }

      // Just dump all keys at depth 2
      const dump: Record<string, string[]> = {};
      for (const key of keys) {
        if (data[key] && typeof data[key] === "object") {
          dump[key] = Object.keys(data[key]);
        }
      }
      return { dump };
    });

    if (records) {
      console.log("\n__data structure found:");
      console.log(JSON.stringify(records, null, 2).slice(0, 3000));
    }

    // Alternative: extract from DOM with correct column mapping
    const deedRecords = await page.evaluate(() => {
      const rows = document.querySelectorAll("table tbody tr");
      return Array.from(rows).map(row => {
        const tds = row.querySelectorAll("td");
        // Get all cell text, regardless of column header mapping
        return Array.from(tds).map(td => td.textContent?.trim() || "");
      });
    });

    if (deedRecords.length > 0) {
      console.log(`\n\nRaw table data (${deedRecords.length} rows):`);
      console.log(`Columns per row: ${deedRecords[0].length}`);
      console.log(`\nFirst 5 rows:`);
      for (let i = 0; i < Math.min(5, deedRecords.length); i++) {
        console.log(`  Row ${i}: ${deedRecords[i].map((c, j) => `[${j}]="${c.slice(0, 40)}"`).join(" | ")}`);
      }
    }

  } catch (err: any) {
    console.error("Error:", err.message);
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
