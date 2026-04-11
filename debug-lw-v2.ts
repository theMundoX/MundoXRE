/**
 * Deep inspect new LandmarkWeb version (Clark WA)
 */
import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  console.log("Inspecting Clark WA LandmarkWeb (new version)...");
  await page.goto("https://e-docs.clark.wa.gov/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  console.log("URL:", page.url());
  console.log("Title:", await page.title());
  
  // Get all clickable elements
  const links = await page.evaluate(() => {
    const results = [];
    document.querySelectorAll("a, button, [onclick], [role='button'], .search-type, .nav-link, nav *, [class*='search'], [id*='search'], [class*='nav']").forEach(el => {
      const txt = (el as HTMLElement).textContent?.trim().slice(0, 50);
      const cls = el.className;
      const id = el.id;
      const tag = el.tagName;
      const href = (el as HTMLAnchorElement).href;
      if (txt) results.push({ tag, id, cls: cls?.slice(0, 40), txt, href: href?.slice(0, 60) });
    });
    return results.slice(0, 40);
  });
  console.log("\nClickable elements:", JSON.stringify(links, null, 2));
  
  // Try clicking Accept/disclaimer
  const disclaimerBtn = page.locator('a:has-text("Accept"), button:has-text("Accept"), input[value*="Accept"]');
  if (await disclaimerBtn.count() > 0) {
    console.log("\nFound Accept button, clicking...");
    await disclaimerBtn.first().click().catch(() => {
      // Try JS
      return page.evaluate(() => {
        if (typeof (window as any).SetDisclaimer === "function") (window as any).SetDisclaimer();
      });
    });
    await page.waitForTimeout(2000);
    console.log("URL after accept:", page.url());
  }
  
  // Get page structure after disclaimer
  const structure = await page.evaluate(() => {
    const forms = Array.from(document.querySelectorAll("form")).map(f => ({
      id: f.id, action: f.action, inputs: Array.from(f.querySelectorAll("input, select, button")).map(i => ({ tag: i.tagName, id: i.id, name: (i as HTMLInputElement).name, type: (i as HTMLInputElement).type, value: (i as HTMLInputElement).value?.slice(0, 30) }))
    }));
    return { forms, bodyText: document.body?.innerText?.slice(0, 500) };
  });
  console.log("\nForms:", JSON.stringify(structure.forms, null, 2));
  console.log("\nBody text:", structure.bodyText);
  
  await browser.close();
}
inspect().catch(console.error);
