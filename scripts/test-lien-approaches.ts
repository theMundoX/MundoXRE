#!/usr/bin/env tsx
/**
 * Test two approaches for linking liens to properties:
 * 1. Browser-based address search (Playwright UI)
 * 2. Legal description matching from existing data
 */
import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

// Test properties
const TEST_PROPERTIES = [
  { id: 21092652, parcel: "0410714300", address: "209 HARKLEROAD CT", city: "PICKERINGTON", owner: "LUCKETT CHRISTOPHER J" },
  { id: 21070204, parcel: "0240211600", address: "121 N HIGH ST", city: "LANCASTER", owner: "RICHARDSON ZACHARY S" },
  { id: 21085912, parcel: "0360121400", address: "8187 MEADOWMOORE BLVD NW", city: "PICKERINGTON", owner: "MORRIS KEVIN C" },
];

async function testBrowserSearch() {
  console.log("\n=== APPROACH 1: Browser-Based Address Search ===\n");

  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext(stealth);
  await context.addInitScript(STEALTH_INIT_SCRIPT);

  const page = await context.newPage();
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(3000);

  for (const prop of TEST_PROPERTIES) {
    console.log(`Property: ${prop.address}, ${prop.city} (owner: ${prop.owner})`);
    const start = Date.now();

    try {
      // Parse address into number and street
      const parts = prop.address.match(/^(\d+)\s+(.+)$/);
      if (!parts) { console.log("  Could not parse address\n"); continue; }
      const [, addrNum, streetFull] = parts;
      // Remove direction prefix and suffix for street name
      const street = streetFull.replace(/^(N|S|E|W|NE|NW|SE|SW)\s+/, "").replace(/\s+(ST|AVE|BLVD|DR|CT|LN|RD|WAY|PL|CIR|TER|LOOP)\s*$/i, "").trim();

      // Intercept search response
      const responsePromise = new Promise<string>((resolve) => {
        const handler = async (resp: any) => {
          if (resp.url().includes("breeze/Search")) {
            try { resolve(await resp.text()); } catch { resolve(""); }
            page.off("response", handler);
          }
        };
        page.on("response", handler);
        setTimeout(() => resolve(""), 20000);
      });

      // Fill address fields
      const addrNumInput = page.locator('input[placeholder="Address Number"]').first();
      const streetInput = page.locator('input[placeholder="Street Name"]').first();

      if (await addrNumInput.count() > 0) {
        await addrNumInput.click();
        await addrNumInput.fill(addrNum);
        await page.keyboard.press("Tab");
        await streetInput.click();
        await streetInput.fill(street);
        await page.keyboard.press("Tab");
      }

      // Also set a date range
      const dateInputs = page.locator('input[placeholder="MM/DD/YYYY"]');
      if (await dateInputs.count() >= 2) {
        await dateInputs.nth(0).click();
        await dateInputs.nth(0).fill("01/01/2000");
        await page.keyboard.press("Tab");
        await dateInputs.nth(1).click();
        await dateInputs.nth(1).fill("03/28/2026");
        await page.keyboard.press("Tab");
      }

      await page.waitForTimeout(500);
      await page.locator('button:has-text("Search")').first().click();
      await page.waitForTimeout(5000);

      const respText = await responsePromise;
      const elapsed = Date.now() - start;

      if (respText && respText.startsWith("{")) {
        const data = JSON.parse(respText);
        console.log(`  Found: ${data.TotalResults} documents (${elapsed}ms)`);
        for (const doc of (data.DocResults || []).slice(0, 3)) {
          const names = (doc.Names || []).map((n: any) => `${n.Type}: ${n.Name}`).join(" | ");
          console.log(`    ${doc.DocumentType} | ${doc.RecordedDateTime} | $${doc.ConsiderationAmount} | ${names}`);
          if (doc.Legals?.length) console.log(`    Legal: ${doc.Legals[0].Description}`);
        }
      } else {
        console.log(`  No results or timeout (${elapsed}ms)`);
      }

      // Clear fields for next search
      if (await addrNumInput.count() > 0) {
        await addrNumInput.click();
        await addrNumInput.fill("");
        await streetInput.click();
        await streetInput.fill("");
      }

    } catch (e: any) {
      console.log(`  Error: ${e.message?.substring(0, 100)}`);
    }
    console.log();
  }

  await browser.close();
}

async function main() {
  await testBrowserSearch();
  console.log("\nDone.");
}

main().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
