import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  console.log("Checking levyclerk.com main site...");
  await page.goto("https://levyclerk.com", { waitUntil: "networkidle", timeout: 30000 }).catch(e => console.log("goto error:", e.message));
  console.log("URL:", page.url());
  
  // Look for links containing LandmarkWeb, Official Records, Recording
  const links = await page.evaluate(() => {
    const results: any[] = [];
    document.querySelectorAll("a").forEach(a => {
      const href = a.href || "";
      const txt = a.textContent?.trim() || "";
      if (href.toLowerCase().includes("landmark") || href.toLowerCase().includes("official") || 
          href.toLowerCase().includes("record") || txt.toLowerCase().includes("official record") ||
          txt.toLowerCase().includes("landmark") || txt.toLowerCase().includes("document search")) {
        results.push({ href, txt: txt.slice(0, 60) });
      }
    });
    return results;
  });
  console.log("Relevant links:", JSON.stringify(links, null, 2));
  
  await browser.close();
}
inspect().catch(console.error);
