#!/usr/bin/env tsx
/**
 * Fast Levy County recorder scraper — proven working approach.
 * Uses headed Playwright, clicks tabs, fills forms, reads rendered DOM.
 * No fancy response interception — just what works.
 */

import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const args = process.argv.slice(2);
const daysBack = parseInt(args.find(a => a.startsWith("--days="))?.split("=")[1] || "7", 10);

const endDate = new Date();
const startDate = new Date();
startDate.setDate(startDate.getDate() - daysBack);

function formatDate(d: Date): string {
  return `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")}/${d.getFullYear()}`;
}

function toISO(d: Date): string {
  return d.toISOString().split("T")[0];
}

function classifyDoc(rawType: string) {
  const u = rawType.toUpperCase();
  if (u.includes("MORTGAGE") && !u.includes("SATISFACTION") && !u.includes("RELEASE") && !u.includes("ASSIGNMENT")) return { document_type: "mortgage", loan_type: "purchase" };
  if (u.includes("SATISFACTION") || u.includes("RELEASE")) return { document_type: "satisfaction" };
  if (u.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (u.includes("WARRANTY DEED") || u === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (u.includes("QUIT CLAIM") || u === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (u.includes("DEED")) return { document_type: "deed" };
  return { document_type: u.toLowerCase() };
}

async function findProperty(name: string): Promise<number | null> {
  if (!name || name.length < 3) return null;
  const clean = name.replace(/\s+/g, " ").trim();
  const { data } = await db.from("properties").select("id").eq("state_code", "FL").ilike("owner_name", clean).limit(1);
  if (data?.length) return data[0].id;
  const last = clean.split(/[,\s]+/)[0];
  if (last.length >= 3) {
    const { data: p } = await db.from("properties").select("id").eq("state_code", "FL").ilike("owner_name", `${last}%`).limit(1);
    if (p?.length) return p[0].id;
  }
  return null;
}

async function main() {
  console.log(`\nLevy County Recorder Scraper`);
  console.log(`Date range: ${toISO(startDate)} to ${toISO(endDate)}\n`);

  const browser = await chromium.launch({
    headless: true,
    args: ["--disable-blink-features=AutomationControlled"],
  });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36",
    viewport: { width: 1920, height: 1080 },
  });
  await ctx.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
  });
  const page = await ctx.newPage();

  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => { if (typeof (window as any).SetDisclaimer === "function") (window as any).SetDisclaimer(); });
  await page.waitForTimeout(3000);

  let totalInserted = 0;
  let totalDupes = 0;
  const current = new Date(startDate);

  while (current <= endDate) {
    const dateStr = formatDate(current);
    const isoDate = toISO(current);

    try {
      // Click Record Date Search tab
      await page.click("text=Record Date Search");
      await page.waitForTimeout(500);

      // Fill dates
      await page.fill("#beginDate-RecordDate", dateStr);
      await page.fill("#endDate-RecordDate", dateStr);

      // Submit
      await page.click("#submit-RecordDate");

      // Wait for results
      try {
        await page.waitForFunction(() => {
          const t = document.getElementById("resultsTable");
          return t && t.querySelectorAll("tbody tr").length > 0;
        }, { timeout: 20000 });
        await page.waitForTimeout(2000);
      } catch {
        // No results for this day
        current.setDate(current.getDate() + 1);
        continue;
      }

      // Extract rows
      const rows = await page.evaluate(() => {
        const trs = document.querySelectorAll("#resultsTable tbody tr");
        return Array.from(trs).map(r => {
          const cells = Array.from(r.querySelectorAll("td"));
          return cells.map(c => c.textContent?.trim() || "");
        });
      });

      // Parse and insert
      const batch: any[] = [];
      for (const row of rows) {
        if (row.length < 6) continue;
        const dateIdx = row.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
        if (dateIdx < 0) continue;

        const grantor = (row[dateIdx - 2] || "").toUpperCase().trim();
        const grantee = (row[dateIdx - 1] || "").toUpperCase().trim();
        const dm = row[dateIdx].match(/(\d{2})\/(\d{2})\/(\d{4})/);
        const recDate = dm ? `${dm[3]}-${dm[1]}-${dm[2]}` : isoDate;
        const docType = row[dateIdx + 1] || "";
        const book = row[dateIdx + 3] || "";
        const pg = row[dateIdx + 4] || "";
        const cfn = row[dateIdx + 5] || "";
        const bookPage = book && pg ? `${book}/${pg}` : null;

        // Dedup
        if (cfn) {
          const { data: dup } = await db.from("mortgage_records").select("id").eq("document_number", cfn).eq("source_url", "https://online.levyclerk.com").limit(1);
          if (dup?.length) { totalDupes++; continue; }
        }

        const classified = classifyDoc(docType);
        const propId = await findProperty(grantor) || await findProperty(grantee);

        batch.push({
          property_id: propId || null,
          document_type: classified.document_type,
          recording_date: recDate,
          lender_name: grantee,
          borrower_name: grantor,
          document_number: cfn || null,
          book_page: bookPage,
          source_url: "https://online.levyclerk.com",
          loan_type: (classified as any).loan_type || null,
          deed_type: (classified as any).deed_type || null,
        });
      }

      if (batch.length > 0) {
        const { error } = await db.from("mortgage_records").insert(batch);
        if (error) console.error(`  Insert error ${isoDate}: ${error.message}`);
        else totalInserted += batch.length;
      }

      console.log(`  ${isoDate}: ${rows.length} found, ${batch.length} new, ${totalDupes} dupes total`);
    } catch (err) {
      console.error(`  ${isoDate}: ERROR - ${(err as Error).message.substring(0, 80)}`);
    }

    current.setDate(current.getDate() + 1);
  }

  await browser.close();
  console.log(`\nDone. Inserted: ${totalInserted}, Dupes skipped: ${totalDupes}`);
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
