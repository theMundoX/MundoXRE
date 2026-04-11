import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  // Capture API calls
  let apiCalls: string[] = [];
  page.on("response", async r => {
    const url = r.url();
    if (url.includes("GetSearchResults") || url.includes("GetDocumentList")) {
      const body = await r.text().catch(() => "err");
      apiCalls.push(`${r.status()} ${url.slice(0, 80)}\nBody: ${body.slice(0, 300)}`);
    }
  });
  
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "networkidle", timeout: 30000 });
  console.log("Page title:", await page.title());
  
  // Accept disclaimer using #idAcceptYes
  await page.click("#idAcceptYes", { timeout: 5000 }).catch(e => console.log("Accept click failed:", e.message));
  await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});
  await page.waitForTimeout(2000);
  
  console.log("After disclaimer URL:", page.url());
  
  // Check for searchNav tabs
  const navs = await page.evaluate(() => {
    return Array.from(document.querySelectorAll(".searchNav")).map(n => ({
      cls: n.className, txt: (n as HTMLElement).textContent?.trim().slice(0, 30), 
      visible: (n as HTMLElement).offsetHeight > 0
    }));
  });
  console.log("SearchNav tabs:", JSON.stringify(navs, null, 2));
  
  // Fill a Record Date search for today
  const today = "03/31/2026";
  
  // Click Record Date Search tab
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim().includes("Record Date") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click();
        break;
      }
    }
  });
  await page.waitForTimeout(1000);
  
  // Check form inputs
  const inputs = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("input, select")).map(i => ({
      id: i.id, type: (i as HTMLInputElement).type, value: (i as HTMLInputElement).value
    })).filter(i => i.id).slice(0, 20);
  });
  console.log("Form inputs:", JSON.stringify(inputs, null, 2));
  
  // Try to fill dates and submit
  await page.fill("#beginDate-RecordDate", today).catch(e => console.log("fill beginDate:", e.message.slice(0,50)));
  await page.fill("#endDate-RecordDate", today).catch(e => console.log("fill endDate:", e.message.slice(0,50)));
  
  // Submit
  await page.click("#submit-RecordDate", { timeout: 5000 }).catch(e => console.log("submit click:", e.message.slice(0,50)));
  await page.waitForTimeout(5000);
  
  console.log("\nAPI calls captured:", apiCalls.length);
  apiCalls.forEach(c => console.log(c));
  
  await browser.close();
}
inspect().catch(console.error);
