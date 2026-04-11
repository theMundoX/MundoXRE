#!/usr/bin/env tsx
import "dotenv/config";
import { chromium } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";

async function main() {
  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ userAgent: stealth.userAgent, viewport: stealth.viewport });
  const page = await context.newPage();
  await page.addInitScript(STEALTH_INIT_SCRIPT);

  await page.goto("https://www.actdatascout.com/RealProperty/Oklahoma/Comanche", { waitUntil: "networkidle", timeout: 30000 });
  await page.fill("#LastName", "SMITH");
  await page.click("#RPNameSubmit");
  await page.waitForTimeout(5000);

  const data = await page.evaluate(`
    (() => {
      const headers = [];
      document.querySelectorAll("table thead th").forEach(th => headers.push(th.textContent.trim()));

      const rows = [];
      const trs = document.querySelectorAll("table tbody tr");
      for (let i = 0; i < Math.min(3, trs.length); i++) {
        const cells = [];
        trs[i].querySelectorAll("td").forEach(td => cells.push(td.textContent.trim().substring(0, 50)));
        rows.push(cells);
      }
      return { headers, rows, rowCount: trs.length };
    })()
  `);

  console.log("Headers:", JSON.stringify((data as any).headers));
  console.log("Row count:", (data as any).rowCount);
  console.log("\nSample rows:");
  for (const row of (data as any).rows) {
    row.forEach((cell: string, i: number) => {
      console.log(`  [${i}] ${(data as any).headers[i] || "?"}: "${cell}"`);
    });
    console.log("  ---");
  }

  await browser.close();
}

main().catch((err) => { console.error(err.message); process.exit(1); });
