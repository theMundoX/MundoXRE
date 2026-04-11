/**
 * Debug: test Hernando (and similar) LandmarkWeb sites step by step.
 */
import { chromium } from "playwright";

const COUNTIES = [
  { name: "Hernando", url: "https://or.hernandoclerk.com/LandmarkWeb" },
  { name: "Lee",      url: "https://or.leeclerk.org/LandmarkWeb" },
  { name: "Manatee",  url: "https://records.manateeclerk.com/LandmarkWeb" },
];

async function testCounty(name: string, baseUrl: string) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  // Track network requests
  const requests: string[] = [];
  page.on("request", req => {
    if (req.url().includes("Search") || req.url().includes("search")) {
      requests.push(`→ ${req.method()} ${req.url()}`);
    }
  });
  page.on("response", resp => {
    if (resp.url().includes("Search") || resp.url().includes("search")) {
      requests.push(`← ${resp.status()} ${resp.url()}`);
    }
  });

  console.log(`\n═══ ${name} ═══`);
  console.log(`URL: ${baseUrl}`);

  try {
    await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30_000 });
    console.log(`Title: ${await page.title()}`);
    console.log(`URL after goto: ${page.url()}`);

    // Check for disclaimer / Accept button
    const acceptBtn = page.locator('a:has-text("Accept"), button:has-text("Accept"), input[value*="Accept"]');
    const btnCount = await acceptBtn.count();
    console.log(`Accept buttons: ${btnCount}`);

    if (btnCount > 0) {
      console.log("Clicking Accept...");
      await Promise.all([
        page.waitForNavigation({ waitUntil: "networkidle", timeout: 15_000 }).catch(() => {}),
        acceptBtn.first().click(),
      ]);
      await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
      console.log(`URL after accept: ${page.url()}`);
    }

    // Check search nav tabs
    const searchNavs = await page.evaluate(() => {
      const navs = document.querySelectorAll(".searchNav");
      return Array.from(navs).map(n => ({
        txt: n.textContent?.trim()?.slice(0, 40),
        visible: (n as HTMLElement).offsetHeight > 0,
        cls: n.className,
      }));
    }).catch(() => []);
    console.log(`Search nav tabs (${searchNavs.length}):`, JSON.stringify(searchNavs));

    // Check if search form exists
    const formExists = await page.evaluate(() => {
      return {
        beginDateRD: !!document.getElementById("beginDate-RecordDate"),
        endDateRD: !!document.getElementById("endDate-RecordDate"),
        beginDateCons: !!document.getElementById("beginDate-Consideration"),
        submitRD: !!document.getElementById("submit-RecordDate"),
        submitCons: !!document.getElementById("submit-Consideration"),
      };
    }).catch(() => ({}));
    console.log("Form fields:", JSON.stringify(formExists));

    // Try to fill and submit a Record Date search
    const today = "03/30/2026";
    await page.evaluate((d) => {
      const begin = document.getElementById("beginDate-RecordDate") as HTMLInputElement;
      const end = document.getElementById("endDate-RecordDate") as HTMLInputElement;
      if (begin) begin.value = d;
      if (end) end.value = d;
    }, today).catch(() => {});

    // Click submit
    await page.click("#submit-RecordDate", { timeout: 3000 }).catch(async () => {
      await page.evaluate(() => {
        const btn = document.getElementById("submit-RecordDate");
        if (btn) (btn as HTMLElement).click();
      }).catch(() => {});
    });

    // Wait briefly for response
    await page.waitForTimeout(8000);
    console.log(`URL after search: ${page.url()}`);
    console.log(`Network requests:`);
    requests.forEach(r => console.log(`  ${r}`));

    // Check for results table
    const hasResults = await page.evaluate(() => {
      const t = document.getElementById("resultsTable");
      if (!t) return { found: false, reason: "no resultsTable" };
      const rows = t.querySelectorAll("tbody tr");
      return { found: rows.length > 0, rows: rows.length };
    }).catch(() => ({ found: false, reason: "evaluate failed" }));
    console.log("Results:", JSON.stringify(hasResults));

  } catch (err: any) {
    console.log(`Error: ${err.message.slice(0, 120)}`);
  }

  await browser.close();
}

(async () => {
  for (const c of COUNTIES) {
    await testCounty(c.name, c.url);
  }
  console.log("\nDone.");
})();
