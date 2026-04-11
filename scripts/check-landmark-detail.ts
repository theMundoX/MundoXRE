#!/usr/bin/env tsx
/**
 * Check LandmarkWeb document detail for consideration/amount fields.
 * LandmarkWeb has a "Consideration" search tab — it stores this field.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Martin County — has mortgages
  await page.goto("http://or.martinclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);

  // Search by Consideration tab — this means the field EXISTS
  console.log("Checking Consideration search tab...\n");

  // Click Consideration tab
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      const text = nav.textContent?.trim();
      if (text && text.includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(1000);

  // Check what fields are on the Consideration search form
  const formFields = await page.evaluate(() => {
    const inputs = document.querySelectorAll("input, select");
    return Array.from(inputs)
      .filter(el => (el as HTMLElement).offsetHeight > 0)
      .map(el => ({
        id: el.id,
        name: (el as HTMLInputElement).name,
        type: (el as HTMLInputElement).type,
        placeholder: (el as HTMLInputElement).placeholder,
      }));
  });
  console.log("Consideration form fields:");
  for (const f of formFields) {
    console.log(`  id=${f.id} name=${f.name} type=${f.type} placeholder=${f.placeholder}`);
  }

  // Search for mortgages with consideration > $100,000
  try {
    await page.fill("#minConsideration", "100000");
    await page.fill("#maxConsideration", "999999999");

    // Set date range
    const beginDate = page.locator("#beginDate-Consideration");
    const endDate = page.locator("#endDate-Consideration");
    if (await beginDate.count() > 0) {
      await beginDate.fill("03/20/2026");
      await endDate.fill("03/20/2026");
    }
  } catch (err: any) {
    console.log(`Form fill error: ${err.message.slice(0, 100)}`);
  }

  // Capture the response
  const resultPromise = new Promise<string>((resolve) => {
    page.on("response", async (resp) => {
      if (resp.url().includes("GetSearchResults")) {
        try { resolve(await resp.text()); } catch { resolve(""); }
      }
    });
    setTimeout(() => resolve(""), 20000);
  });

  // Submit
  try {
    const submitBtn = page.locator("#submit-Consideration");
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
    }
  } catch {}

  const jsonStr = await resultPromise;

  if (jsonStr && jsonStr.startsWith("{")) {
    const data = JSON.parse(jsonStr);
    console.log(`\nResults: ${data.recordsTotal} documents with consideration\n`);

    // Parse and show records WITH amounts
    for (const row of (data.data || []).slice(0, 10)) {
      const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";

      const cols: string[] = [];
      for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");

      // Dump all non-empty columns to find the consideration
      console.log("─── Record ───");
      for (let i = 0; i < cols.length; i++) {
        if (cols[i] && cols[i] !== "result" && !cols[i].startsWith("doc_") && !cols[i].startsWith("eye_")) {
          console.log(`  [${i}] = "${cols[i].slice(0, 80)}"`);
        }
      }
      console.log();
    }
  } else {
    console.log("No results or error. Response:", jsonStr?.slice(0, 200));

    // Try the Record Date search instead and check if consideration shows in those results
    console.log("\nFalling back to Record Date search to check for consideration column...");
    await page.evaluate(() => {
      const navs = document.querySelectorAll(".searchNav");
      for (const nav of navs) {
        if (nav.textContent?.trim() === "Record Date Search" && (nav as HTMLElement).offsetHeight > 0) {
          (nav as HTMLElement).click();
          break;
        }
      }
    });
    await page.waitForTimeout(500);
    await page.fill("#beginDate-RecordDate", "03/20/2026");
    await page.fill("#endDate-RecordDate", "03/20/2026");

    const result2 = new Promise<string>((resolve) => {
      page.on("response", async (resp) => {
        if (resp.url().includes("GetSearchResults")) {
          try { resolve(await resp.text()); } catch { resolve(""); }
        }
      });
      setTimeout(() => resolve(""), 20000);
    });
    await page.click("#submit-RecordDate");
    const json2 = await result2;

    if (json2 && json2.startsWith("{")) {
      const data2 = JSON.parse(json2);
      // Find a mortgage and dump ALL columns
      for (const row of data2.data) {
        const strip = (v: string) => v?.replace(/<[^>]+>/g, "").replace(/nobreak_\s*/g, "").replace(/unclickable_/g, "").replace(/hidden_\S*/g, "").trim() || "";
        const cols: string[] = [];
        for (let i = 0; i < 30; i++) cols[i] = strip(row[String(i)] || "");
        const dateIdx = cols.findIndex(c => /^\d{2}\/\d{2}\/\d{4}$/.test(c));
        if (dateIdx < 0) continue;
        const docType = cols[dateIdx + 1] || "";
        if (!docType.toUpperCase().includes("MORTGAGE")) continue;

        console.log("\nMortgage record — ALL columns:");
        for (let i = 0; i < 30; i++) {
          console.log(`  [${i}] = "${(cols[i] || "").slice(0, 80)}"`);
        }
        break;
      }
    }
  }

  await browser.close();
}

main().catch(console.error);
