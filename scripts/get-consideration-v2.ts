#!/usr/bin/env tsx
/**
 * Use LandmarkWeb Consideration search with correct field IDs.
 * Dump ALL columns to find where the dollar amount lives.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Martin County
  await page.goto("http://or.martinclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Click Consideration tab
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim()?.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(1000);

  // Fill correctly: lowerBound and upperBound
  await page.fill("#lowerBound", "100000");
  await page.fill("#upperBound", "900000");
  await page.fill("#beginDate-Consideration", "03/19/2026");
  await page.fill("#endDate-Consideration", "03/20/2026");

  // Capture response
  const resultPromise = new Promise<string>((resolve) => {
    page.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 25000);
  });

  await page.click("#submit-Consideration");
  const jsonStr = await resultPromise;

  if (!jsonStr || !jsonStr.startsWith("{")) {
    console.log("No results");
    // Try clicking submit via JS
    await page.evaluate(() => {
      const btn = document.querySelector("#submit-Consideration") as HTMLElement;
      if (btn) btn.click();
    });
    await page.waitForTimeout(5000);
    console.log("Retried click");
    await browser.close();
    return;
  }

  const data = JSON.parse(jsonStr);
  console.log(`Results: ${data.recordsTotal}\n`);

  // Dump the raw column data for first 3 records
  for (let r = 0; r < Math.min(3, data.data.length); r++) {
    const row = data.data[r];
    console.log(`═══ RAW Record ${r + 1} ═══`);
    for (let i = 0; i < 30; i++) {
      const raw = row[String(i)] || "";
      if (!raw) continue;
      // Strip HTML but keep dollar signs
      const stripped = raw
        .replace(/<[^>]+>/g, " ")
        .replace(/nobreak_\s*/g, "")
        .replace(/unclickable_/g, "")
        .replace(/\s+/g, " ")
        .trim();
      if (stripped && stripped !== "result" && !stripped.startsWith("hidden_") && stripped.length > 0) {
        console.log(`  [${String(i).padStart(2)}] = "${stripped.slice(0, 120)}"`);
      }
    }
    console.log();
  }

  // Now try Levy County — different portal, might have different columns
  console.log("\n\n═══ LEVY COUNTY ═══\n");
  const page2 = await ctx.newPage();
  await page2.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  await page2.evaluate(() => (window as any).SetDisclaimer());
  await page2.waitForTimeout(2000);

  // Check if Levy has Consideration tab
  const levyTabs = await page2.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    return Array.from(navs).map(n => n.textContent?.trim()).filter(Boolean);
  });
  console.log("Levy search tabs:", levyTabs.join(" | "));

  // Click Consideration if it exists
  const hasConsideration = levyTabs.some(t => t?.includes("Consideration"));
  if (hasConsideration) {
    await page2.evaluate(() => {
      const navs = document.querySelectorAll(".searchNav");
      for (const nav of navs) {
        if (nav.textContent?.trim()?.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
          (nav as HTMLElement).click();
          break;
        }
      }
    });
    await page2.waitForTimeout(1000);

    // Check field IDs
    const fields = await page2.evaluate(() => {
      const inputs = document.querySelectorAll("input, select");
      return Array.from(inputs)
        .filter(el => (el as HTMLElement).offsetHeight > 0 && el.id)
        .map(el => ({ id: el.id, type: (el as HTMLInputElement).type }));
    });
    console.log("Consideration form fields:", fields.map(f => f.id).join(", "));

    // Search
    try {
      await page2.fill("#lowerBound", "50000");
      await page2.fill("#upperBound", "500000");
      await page2.fill("#beginDate-Consideration", "03/19/2026");
      await page2.fill("#endDate-Consideration", "03/21/2026");
    } catch (e: any) {
      console.log("Fill error:", e.message.slice(0, 80));
    }

    const result2 = new Promise<string>((resolve) => {
      page2.on("response", async (resp) => {
        if (resp.url().includes("GetSearchResults")) {
          try { resolve(await resp.text()); } catch { resolve(""); }
        }
      });
      setTimeout(() => resolve(""), 25000);
    });

    await page2.click("#submit-Consideration").catch(() => {
      page2.evaluate(() => {
        const btn = document.querySelector("#submit-Consideration") as HTMLElement;
        if (btn) btn.click();
      });
    });

    const json2 = await result2;
    if (json2 && json2.startsWith("{")) {
      const data2 = JSON.parse(json2);
      console.log(`\nLevy results: ${data2.recordsTotal}\n`);
      for (let r = 0; r < Math.min(3, data2.data.length); r++) {
        const row = data2.data[r];
        console.log(`═══ Levy Record ${r + 1} ═══`);
        for (let i = 0; i < 30; i++) {
          const raw = row[String(i)] || "";
          if (!raw) continue;
          const stripped = raw.replace(/<[^>]+>/g, " ").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/\s+/g, " ").trim();
          if (stripped && stripped !== "result" && !stripped.startsWith("hidden_") && stripped.length > 0) {
            console.log(`  [${String(i).padStart(2)}] = "${stripped.slice(0, 120)}"`);
          }
        }
        console.log();
      }
    }
  }

  await browser.close();
}

main().catch(console.error);
