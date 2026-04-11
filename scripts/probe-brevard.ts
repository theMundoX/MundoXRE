#!/usr/bin/env tsx
/**
 * Quick network probe for Brevard County LandmarkWeb portal.
 * Logs all API calls made during a date search to identify the correct endpoint.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  const BASE = "https://officialrecords.brevardclerk.com";
  const PREFIX = "/LandmarkWeb";

  // Log all network requests
  const captured: string[] = [];
  page.on("request", req => {
    const url = req.url();
    if (url.includes(BASE) || url.includes("search") || url.includes("Search")) {
      captured.push(`REQ ${req.method()} ${url.replace(BASE, "")}`);
    }
  });
  page.on("response", async resp => {
    const url = resp.url();
    if (url.includes(BASE)) {
      const body = await resp.text().catch(() => "");
      const snippet = body.slice(0, 200).replace(/\s+/g, " ");
      captured.push(`RESP ${resp.status()} ${url.replace(BASE, "")} → ${snippet}`);
    }
  });

  console.log("=== Brevard County Portal Network Probe ===\n");

  // Step 1: Navigate to portal
  console.log("Step 1: Navigate to portal home...");
  await page.goto(`${BASE}${PREFIX}`, { waitUntil: "networkidle", timeout: 30000 });

  // Check page structure
  const pageInfo = await page.evaluate(() => ({
    title: document.title,
    hasSetDisclaimer: typeof (window as any).SetDisclaimer === "function",
    searchNavs: Array.from(document.querySelectorAll(".searchNav")).map(n => n.textContent?.trim()),
    forms: document.querySelectorAll("form").length,
    url: window.location.href,
  }));
  console.log("Page info:", JSON.stringify(pageInfo, null, 2));

  // Step 2: Accept disclaimer
  console.log("\nStep 2: Accept disclaimer...");
  if (pageInfo.hasSetDisclaimer) {
    await page.evaluate(() => (window as any).SetDisclaimer());
    console.log("  Called SetDisclaimer()");
  } else {
    const btn = page.locator('input[value*="Accept"], button:has-text("Accept"), a:has-text("Accept")');
    if (await btn.count() > 0) {
      await btn.first().click();
      console.log("  Clicked Accept button");
    } else {
      console.log("  No disclaimer found");
    }
  }
  await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});

  const afterDisclaimer = await page.evaluate(() => ({
    url: window.location.href,
    searchNavs: Array.from(document.querySelectorAll(".searchNav")).map(n => n.textContent?.trim()),
    hasConsiderationTab: Array.from(document.querySelectorAll(".searchNav")).some(
      n => n.textContent?.trim()?.includes("Consideration") && (n as HTMLElement).offsetHeight > 0
    ),
  }));
  console.log("After disclaimer:", JSON.stringify(afterDisclaimer, null, 2));

  // Step 3: Try Consideration search
  console.log("\nStep 3: Try Consideration search (2026-03-25)...");
  captured.length = 0;

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

  const considerationResult = await page.evaluate(() => {
    const lb = document.getElementById("lowerBound");
    const ub = document.getElementById("upperBound");
    const bd = document.getElementById("beginDate-Consideration");
    const ed = document.getElementById("endDate-Consideration");
    const btn = document.getElementById("submit-Consideration");
    return { lb: !!lb, ub: !!ub, bd: !!bd, ed: !!ed, btn: !!btn };
  });
  console.log("  Consideration form fields:", considerationResult);

  if (considerationResult.lb && considerationResult.bd) {
    await page.evaluate(() => {
      const lb = document.getElementById("lowerBound") as HTMLInputElement;
      const ub = document.getElementById("upperBound") as HTMLInputElement;
      const bd = document.getElementById("beginDate-Consideration") as HTMLInputElement;
      const ed = document.getElementById("endDate-Consideration") as HTMLInputElement;
      if (lb) lb.value = "1";
      if (ub) ub.value = "999999999";
      if (bd) bd.value = "03/25/2026";
      if (ed) ed.value = "03/25/2026";
    });

    const respPromise = new Promise<string>(resolve => {
      const handler = async (resp: any) => {
        const url = resp.url();
        const body = await resp.text().catch(() => "");
        if (body.length > 100) {
          console.log(`\n  API Response: ${url.replace(BASE, "")}`);
          console.log(`  Status: ${resp.status()}`);
          console.log(`  Body preview: ${body.slice(0, 300)}`);
          resolve(body);
          page.off("response", handler);
        }
      };
      page.on("response", handler);
      setTimeout(() => resolve(""), 20000);
    });

    const btn = document.getElementById("submit-Consideration");
    await page.evaluate(() => {
      const btn = document.getElementById("submit-Consideration");
      if (btn) (btn as HTMLElement).click();
    });
    const resp = await respPromise;
    if (!resp) console.log("  No API response captured (timeout)");
  }

  // Step 4: Try Record Date search
  console.log("\nStep 4: Try Record Date search...");
  captured.length = 0;

  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim()?.includes("Record Date") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(800);

  const rdFields = await page.evaluate(() => {
    const bd = document.getElementById("beginDate-RecordDate");
    const ed = document.getElementById("endDate-RecordDate");
    const btn = document.getElementById("submit-RecordDate");
    return { bd: !!bd, ed: !!ed, btn: !!btn };
  });
  console.log("  RecordDate form fields:", rdFields);

  if (rdFields.bd) {
    await page.evaluate(() => {
      const bd = document.getElementById("beginDate-RecordDate") as HTMLInputElement;
      const ed = document.getElementById("endDate-RecordDate") as HTMLInputElement;
      if (bd) bd.value = "03/25/2026";
      if (ed) ed.value = "03/25/2026";
    });

    const respPromise2 = new Promise<string>(resolve => {
      const handler = async (resp: any) => {
        const url = resp.url();
        const body = await resp.text().catch(() => "");
        if (body.length > 100 && !url.includes(".js") && !url.includes(".css")) {
          console.log(`\n  API Response: ${url.replace(BASE, "")}`);
          console.log(`  Status: ${resp.status()}`);
          console.log(`  Body preview: ${body.slice(0, 300)}`);
          resolve(body);
          page.off("response", handler);
        }
      };
      page.on("response", handler);
      setTimeout(() => resolve(""), 20000);
    });

    await page.evaluate(() => {
      const btn = document.getElementById("submit-RecordDate");
      if (btn) (btn as HTMLElement).click();
    });
    const resp2 = await respPromise2;
    if (!resp2) console.log("  No API response captured (timeout)");
  }

  // Log all captured requests
  console.log("\n=== All captured network requests ===");
  for (const r of captured.slice(0, 30)) console.log(r.slice(0, 200));

  await browser.close();
}

main().catch(console.error);
