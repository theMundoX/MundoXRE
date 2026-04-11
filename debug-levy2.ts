import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  let networkReqs: string[] = [];
  page.on("response", async r => {
    const url = r.url();
    networkReqs.push(`${r.status()} ${url.slice(0, 100)}`);
  });
  
  console.log("Navigating to online.levyclerk.com/landmarkweb...");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 }).catch(e => console.log("goto error:", e.message));
  console.log("URL:", page.url());
  console.log("Title:", await page.title());
  
  // Check page content
  const bodyText = await page.evaluate(() => document.body?.textContent?.slice(0, 300));
  console.log("Body:", bodyText);
  
  // Look for disclaimer buttons
  const els = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("button, input[type='button'], a")).map(el => ({
      id: el.id, tag: el.tagName, txt: (el as HTMLElement).textContent?.trim().slice(0, 40), 
      href: (el as HTMLAnchorElement).href?.slice(0,60)
    })).filter(e => e.txt).slice(0, 20);
  });
  console.log("Clickable elements:", JSON.stringify(els, null, 2));
  
  console.log("\nAll network requests:");
  networkReqs.slice(-20).forEach(r => console.log(r));
  
  await browser.close();
}
inspect().catch(console.error);
