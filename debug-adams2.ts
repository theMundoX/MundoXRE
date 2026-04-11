import { chromium } from "playwright";
async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await (await browser.newContext({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" })).newPage();
  let apis: string[] = [];
  page.on("response", async r => {
    const url = r.url();
    if (url.includes("GetSearchResults") || url.includes("GetDocumentList")) {
      const t = await r.text().catch(() => "");
      apis.push(`${url.slice(0,80)}\n${t.slice(0,400)}`);
    }
  });
  await page.goto("https://recording.adcogov.org/landmarkweb", { waitUntil: "networkidle", timeout: 25000 }).catch(e => console.log("goto:", e.message));
  await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});
  await page.waitForTimeout(1000);
  
  // Try Record Date search (not Consideration) for March 31
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim() === "Record Date Search" && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click(); break;
      }
    }
  });
  await page.waitForTimeout(1000);
  await page.fill("#beginDate-RecordDate", "03/31/2026").catch(e => console.log("fill:", e.message));
  await page.fill("#endDate-RecordDate", "03/31/2026").catch(e => {});
  await page.click("#submit-RecordDate", { timeout: 5000 }).catch(e => console.log("submit:", e.message));
  await page.waitForTimeout(6000);
  
  console.log("API responses:", apis.length);
  apis.forEach(a => console.log(a));
  
  // Also try Consideration search
  await page.evaluate(() => {
    const navs = document.querySelectorAll(".searchNav");
    for (const nav of navs) {
      if (nav.textContent?.trim().includes("Consideration") && (nav as HTMLElement).offsetHeight > 0) {
        (nav as HTMLElement).click(); break;
      }
    }
  });
  await page.waitForTimeout(500);
  await page.fill("#lowerBound", "1").catch(() => {});
  await page.fill("#upperBound", "999999999").catch(() => {});
  await page.fill("#beginDate-Consideration", "03/31/2026").catch(() => {});
  await page.fill("#endDate-Consideration", "03/31/2026").catch(() => {});
  await page.click("#submit-Consideration", { timeout: 5000 }).catch(e => console.log("consid submit:", e.message));
  await page.waitForTimeout(6000);
  
  console.log("Total API responses:", apis.length);
  apis.slice(-2).forEach(a => console.log(a));
  
  await browser.close();
}
inspect().catch(console.error);
