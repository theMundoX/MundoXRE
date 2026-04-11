import { chromium } from "playwright";
async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await (await browser.newContext({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" })).newPage();
  await page.goto("https://recording.adcogov.org/landmarkweb", { waitUntil: "networkidle", timeout: 25000 }).catch(e => console.log("goto:", e.message));
  console.log("URL:", page.url(), "Title:", await page.title());
  const hasSetDisclaimer = await page.evaluate(() => typeof (window as any).SetDisclaimer === "function");
  console.log("hasSetDisclaimer:", hasSetDisclaimer);
  if (hasSetDisclaimer) await page.evaluate(() => (window as any).SetDisclaimer());
  await page.waitForTimeout(2000);
  const navs = await page.evaluate(() => Array.from(document.querySelectorAll(".searchNav")).map(n => ({ txt: (n as HTMLElement).textContent?.trim().slice(0,30), vis: (n as HTMLElement).offsetHeight > 0 })));
  console.log("SearchNav:", JSON.stringify(navs));
  await browser.close();
}
inspect().catch(console.error);
