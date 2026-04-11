import { chromium } from "playwright";

async function inspect() {
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const ctx = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();
  
  // Capture GetDocumentList request body and response
  let docListReqs: Array<{body: string, resp: string}> = [];
  page.on("request", r => {
    if (r.url().includes("GetDocumentList") || r.url().includes("GetSearchResults")) {
      const body = r.postData() || "";
      docListReqs.push({ body, resp: "" });
    }
  });
  page.on("response", async r => {
    if (r.url().includes("GetDocumentList") || r.url().includes("GetSearchResults")) {
      const text = await r.text().catch(() => "");
      if (docListReqs.length > 0) {
        docListReqs[docListReqs.length - 1].resp = text.slice(0, 500);
      }
    }
  });
  
  await page.goto("https://e-docs.clark.wa.gov/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 });
  
  // Accept
  await page.evaluate(() => { 
    const b = document.getElementById('idAcceptYes'); 
    if (b) (b as HTMLElement).click();
  });
  await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => {});
  await page.waitForTimeout(2000);
  
  // Click Record Date tab
  await page.click('a:has-text("Record Date")');
  await page.waitForTimeout(2000);
  
  // Get Record Date form inputs
  const inputs = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("input, select")).map(i => ({
      id: i.id, name: (i as HTMLInputElement).name, type: (i as HTMLInputElement).type,
      value: (i as HTMLInputElement).value,
    })).filter(i => i.id || i.name);
  });
  console.log("Record Date inputs:", JSON.stringify(inputs, null, 2));
  
  // Fill in date and submit
  const today = "03/31/2026";
  
  // Try various date field IDs
  for (const id of ["beginDate-RecordDate", "beginDate", "StartDate", "BeginDate", "dtFrom", "dateFrom"]) {
    const el = page.locator(`#${id}`);
    if (await el.count() > 0) {
      console.log("Found date field:", id);
      await el.fill(today);
    }
  }
  
  // Click submit
  for (const sel of ["#submit-RecordDate", 'button:has-text("Submit")', 'input[value="Submit"]', 'a:has-text("Submit")', '.btn-search']) {
    const el = page.locator(sel);
    if (await el.count() > 0) {
      console.log("Clicking submit:", sel);
      await el.first().click();
      await page.waitForTimeout(3000);
      break;
    }
  }
  
  console.log("\nGetDocumentList requests captured:", docListReqs.length);
  docListReqs.forEach((r, i) => {
    console.log(`\nRequest ${i+1}:`);
    console.log("Body:", r.body.slice(0, 300));
    console.log("Response:", r.resp.slice(0, 300));
  });
  
  await browser.close();
}
inspect().catch(console.error);
