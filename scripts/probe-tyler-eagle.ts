#!/usr/bin/env tsx
/**
 * Probe Tyler EagleWeb recorder portal to understand auth and search capabilities.
 */
import { chromium } from "playwright";

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  });
  const page = await ctx.newPage();

  // Elbert County CO — confirmed working earlier
  const url = "https://elbertcountyco-recorder.tylerhost.net/recorder/web/login.jsp";
  console.log(`Probing: ${url}\n`);

  // Monitor API calls
  page.on("response", async (resp) => {
    const u = resp.url();
    if (u.includes("search") || u.includes("Search") || u.includes("api")) {
      console.log(`  API: [${resp.status()}] ${u.slice(0, 100)}`);
    }
  });

  await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
  console.log("Title:", await page.title());
  console.log("URL:", page.url());

  // Check page content
  const pageInfo = await page.evaluate(() => {
    const body = document.body.innerText;
    const hasLogin = body.includes("Login") || body.includes("Sign In");
    const hasSearch = body.includes("Search") || body.includes("search");
    const hasGuest = body.includes("Guest") || body.includes("guest") || body.includes("Continue");
    const forms = document.querySelectorAll("form");
    const links = Array.from(document.querySelectorAll("a")).map(a => ({ text: a.textContent?.trim(), href: a.href })).filter(l => l.text);

    return { hasLogin, hasSearch, hasGuest, formCount: forms.length, links: links.slice(0, 15) };
  });

  console.log("\nPage info:");
  console.log("  Has login:", pageInfo.hasLogin);
  console.log("  Has search:", pageInfo.hasSearch);
  console.log("  Has guest:", pageInfo.hasGuest);
  console.log("  Forms:", pageInfo.formCount);
  console.log("  Links:");
  for (const l of pageInfo.links) {
    console.log(`    "${l.text?.slice(0, 30)}" -> ${l.href?.slice(0, 80)}`);
  }

  // Try clicking "Guest" or "Continue" if available
  const guestBtn = page.locator('a:has-text("Guest"), button:has-text("Guest"), a:has-text("Continue"), input[value*="Guest"]');
  if (await guestBtn.count() > 0) {
    console.log("\nClicking guest/continue button...");
    await guestBtn.first().click();
    await page.waitForTimeout(3000);
    console.log("URL after click:", page.url());
    console.log("Title:", await page.title());

    // Check for search form
    const searchInfo = await page.evaluate(() => {
      const inputs = document.querySelectorAll("input, select");
      return Array.from(inputs)
        .filter(el => (el as HTMLElement).offsetHeight > 0)
        .map(el => ({
          id: el.id,
          name: (el as HTMLInputElement).name,
          type: (el as HTMLInputElement).type,
          placeholder: (el as HTMLInputElement).placeholder,
        }))
        .slice(0, 15);
    });
    console.log("\nSearch form inputs:");
    for (const i of searchInfo) {
      console.log(`  id=${i.id} name=${i.name} type=${i.type} placeholder="${i.placeholder}"`);
    }
  }

  await browser.close();
}

main().catch(console.error);
