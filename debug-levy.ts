import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  let responses: string[] = [];
  page.on("response", async r => {
    const url = r.url();
    if (url.includes("GetDocumentList") || url.includes("GetSearchResults") || url.includes("Search")) {
      const status = r.status();
      const body = await r.text().catch(() => "");
      responses.push(`${status} ${url.slice(0, 80)}: ${body.slice(0, 200)}`);
    }
  });
  
  console.log("Navigating to Levy portal...");
  await page.goto("https://levyclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 }).catch(e => console.log("goto error:", e.message));
  console.log("URL:", page.url());
  console.log("Title:", await page.title());
  
  // Check for disclaimer elements
  const disclaimerElements = await page.evaluate(() => {
    const els = document.querySelectorAll('[id*="accept"], [id*="Accept"], [id*="disclaimer"], [class*="disclaimer"], button, input[type="button"]');
    return Array.from(els).map(el => ({ id: el.id, tag: el.tagName, txt: (el as HTMLElement).textContent?.trim().slice(0, 40) })).slice(0, 15);
  });
  console.log("Disclaimer elements:", JSON.stringify(disclaimerElements, null, 2));
  
  // Check for search nav
  const navItems = await page.evaluate(() => {
    return Array.from(document.querySelectorAll(".searchNav, nav a, [class*='nav'] a")).map(el => ({
      cls: el.className, txt: (el as HTMLElement).textContent?.trim().slice(0, 40), id: el.id
    })).slice(0, 15);
  });
  console.log("Nav items:", JSON.stringify(navItems, null, 2));
  
  console.log("\nResponses captured:", responses.length);
  responses.forEach(r => console.log(r));
  
  await browser.close();
}
inspect().catch(console.error);
