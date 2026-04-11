#!/usr/bin/env tsx
import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!);

async function main() {
  // Show sample DB parcels
  const { data: dbProps } = await db.from("properties").select("parcel_id").eq("county_id", 3).limit(5);
  console.log("DB parcel IDs:");
  for (const p of dbProps ?? []) console.log(`  "${p.parcel_id}"`);

  // Get sample ActDataScout parcels
  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ userAgent: stealth.userAgent, viewport: stealth.viewport });
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  await page.goto("https://www.actdatascout.com/RealProperty/Oklahoma/Comanche", { waitUntil: "networkidle", timeout: 30000 });
  await page.fill("#LastName", "A");
  await page.click("#RPNameSubmit");
  await page.waitForTimeout(4000);

  const rows = await page.evaluate(`
    (() => {
      const results = [];
      const trs = document.querySelectorAll("table tbody tr");
      for (const tr of trs) {
        const cells = tr.querySelectorAll("td");
        if (cells.length >= 6 && results.length < 5) {
          results.push({
            rpid: cells[1]?.textContent?.trim(),
            parcel: cells[2]?.textContent?.trim(),
            owner: cells[3]?.textContent?.trim(),
            address: cells[5]?.textContent?.trim(),
          });
        }
      }
      return results;
    })()
  `);

  console.log("\nActDataScout parcel IDs:");
  for (const r of rows as Array<Record<string, string>>) {
    console.log(`  parcel="${r.parcel}" rpid="${r.rpid}" owner="${r.owner}" addr="${r.address}"`);
  }

  await browser.close();
}

main().catch((err) => { console.error(err.message); process.exit(1); });
