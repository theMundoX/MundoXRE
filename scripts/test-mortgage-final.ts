#!/usr/bin/env tsx
/**
 * Pull REAL Deed of Trust (mortgage) records from Dallas County TX.
 * Uses the correct table column mapping.
 */

import { chromium } from "playwright";

const BASE = "https://dallas.tx.publicsearch.us";

async function main() {
  console.log("Launching browser...\n");
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await context.newPage();

  try {
    // Try recent dates first
    const url = `${BASE}/results?department=RP&recordedDateRange=2026-03-01to2026-03-26&docType=DEED+OF+TRUST`;
    console.log(`Searching Dallas County for Deed of Trust records (March 2026)...`);
    await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
    await page.waitForTimeout(3000);

    // Check the documents data from __data
    const docData = await page.evaluate(() => {
      const data = (window as any).__data;
      if (!data?.documents?.workspaces) return null;
      const workspaces = data.documents.workspaces;
      // Find the search workspace
      for (const [id, ws] of Object.entries(workspaces) as any[]) {
        if (ws.results || ws.searchResults || ws.documents) {
          const results = ws.results || ws.searchResults || ws.documents;
          if (Array.isArray(results) && results.length > 0) {
            return {
              workspaceId: id,
              count: results.length,
              totalHits: ws.totalHits || ws.total || results.length,
              keys: Object.keys(results[0]),
              records: results.slice(0, 20),
            };
          }
        }
        // Check if results are nested differently
        const wsKeys = Object.keys(ws);
        for (const k of wsKeys) {
          if (Array.isArray(ws[k]) && ws[k].length > 0 && typeof ws[k][0] === "object") {
            return {
              workspaceId: id,
              key: k,
              count: ws[k].length,
              keys: Object.keys(ws[k][0]),
              records: ws[k].slice(0, 20),
            };
          }
        }
      }
      // Return workspace keys for debugging
      return { workspaceKeys: Object.keys(workspaces), debug: JSON.stringify(workspaces).slice(0, 2000) };
    });

    if (docData?.records) {
      console.log(`\nFound ${docData.count} records (total: ${docData.totalHits || "?"}).`);
      console.log(`Record fields: ${docData.keys.join(", ")}\n`);

      console.log("═══════════════════════════════════════════════════════════════════════");
      console.log("  REAL RECORDED MORTGAGE DATA — Dallas County, TX");
      console.log("═══════════════════════════════════════════════════════════════════════\n");

      for (let i = 0; i < docData.records.length; i++) {
        const r = docData.records[i];
        // Only show Deed of Trust
        const docType = r.docType || r.type || r.documentType || "";
        if (!docType.toUpperCase().includes("DEED OF TRUST") && !docType.toUpperCase().includes("DOT")) continue;

        console.log(`  Record #${i + 1}`);
        console.log(`  ─────────────────────────────────────────────────`);
        console.log(`  Borrower:        ${r.grantor || r.grantors || "?"}`);
        console.log(`  Lender:          ${r.grantee || r.grantees || "?"}`);
        console.log(`  Type:            ${docType}`);
        console.log(`  Recorded:        ${r.recordedDate || r.recordDate || r.date || "?"}`);
        console.log(`  Doc Number:      ${r.instrumentNumber || r.docNumber || r.number || "?"}`);
        console.log(`  Book/Page:       ${r.bookPage || r.bookVolumePage || "--"}`);
        console.log(`  Town:            ${r.town || r.city || "?"}`);
        console.log(`  Legal:           ${(r.legalDescription || r.legal || "?").slice(0, 80)}`);
        console.log();
      }
    } else if (docData) {
      console.log("Workspace data (debugging):");
      console.log(JSON.stringify(docData, null, 2).slice(0, 2000));
    }

    // Fallback: also extract from the table directly with correct column mapping
    const tableRecords = await page.evaluate(() => {
      const rows = document.querySelectorAll("table tbody tr");
      return Array.from(rows).map(row => {
        const tds = row.querySelectorAll("td");
        return {
          grantor: tds[3]?.textContent?.trim() || "",
          grantee: tds[4]?.textContent?.trim() || "",
          doc_type: tds[5]?.textContent?.trim() || "",
          recorded_date: tds[6]?.textContent?.trim() || "",
          doc_number: tds[7]?.textContent?.trim() || "",
          book_page: tds[8]?.textContent?.trim() || "",
          town: tds[9]?.textContent?.trim() || "",
          legal: tds[10]?.textContent?.trim() || "",
        };
      }).filter(r => r.doc_type.toUpperCase().includes("DEED OF TRUST"));
    });

    if (tableRecords.length > 0 && !docData?.records) {
      console.log("═══════════════════════════════════════════════════════════════════════");
      console.log("  REAL RECORDED MORTGAGE DATA — Dallas County, TX");
      console.log("═══════════════════════════════════════════════════════════════════════\n");

      for (let i = 0; i < Math.min(tableRecords.length, 15); i++) {
        const r = tableRecords[i];
        console.log(`  Record #${i + 1}`);
        console.log(`  ─────────────────────────────────────────────────`);
        console.log(`  Borrower:        ${r.grantor}`);
        console.log(`  Lender:          ${r.grantee}`);
        console.log(`  Type:            ${r.doc_type}`);
        console.log(`  Recorded:        ${r.recorded_date}`);
        console.log(`  Doc Number:      ${r.doc_number}`);
        console.log(`  Book/Page:       ${r.book_page}`);
        console.log(`  Town:            ${r.town}`);
        console.log(`  Legal:           ${r.legal.slice(0, 80)}`);
        console.log();
      }
      console.log(`  Total Deed of Trust records on page: ${tableRecords.length}`);
    }

    // Show page info
    const pageInfo = await page.evaluate(() => {
      const el = document.querySelector('.search-results__summary, [class*="result-count"], [class*="showing"]');
      return el?.textContent?.trim() || "";
    });
    if (pageInfo) console.log(`\nPage info: ${pageInfo}`);

    // Check current URL to see if date filter applied
    console.log(`\nFinal URL: ${page.url()}`);

  } catch (err: any) {
    console.error("Error:", err.message);
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
