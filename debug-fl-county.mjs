import { chromium } from "playwright";

const url = "https://or.hernandoclerk.com/LandmarkWeb";

(async () => {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" });
  const page = await ctx.newPage();
  
  console.log("Navigating to", url);
  try {
    await page.goto(url, { waitUntil: "networkidle", timeout: 30000 });
  } catch(e) {
    console.log("goto error:", e.message);
  }
  
  const title = await page.title();
  console.log("Title:", title);
  
  const content = await page.content();
  console.log("Content length:", content.length);
  
  // Look for nav elements
  const navs = await page.evaluate(() => {
    const els = document.querySelectorAll(".searchNav, [class*=Nav], [class*=tab], button, a");
    return Array.from(els).slice(0, 20).map(e => ({
      tag: e.tagName,
      cls: e.className,
      txt: e.textContent?.trim()?.slice(0, 50)
    }));
  }).catch(e => ({ error: e.message }));
  console.log("Nav elements:", JSON.stringify(navs, null, 2));
  
  // Check for captcha
  const hasCaptcha = await page.evaluate(() => {
    return !!document.querySelector("[class*=captcha], #captcha, iframe[src*=captcha]");
  }).catch(() => false);
  console.log("Has captcha:", hasCaptcha);
  
  // Check URL after navigation
  console.log("Final URL:", page.url());
  
  await browser.close();
})();
