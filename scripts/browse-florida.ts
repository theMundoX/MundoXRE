#!/usr/bin/env tsx
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const DOWNLOAD_DIR = "C:\\Users\\msanc\\mxre-data\\florida";
if (!existsSync(DOWNLOAD_DIR)) mkdirSync(DOWNLOAD_DIR, { recursive: true });

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  // Navigate into NAL/2025F folder
  console.log("Navigating to NAL/2025F...");
  await page.goto(
    "https://floridarevenue.com/property/dataportal/Pages/default.aspx?path=/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2025F",
    { waitUntil: "networkidle", timeout: 60000 },
  );
  await page.waitForTimeout(8000);

  const text = await page.evaluate(() => document.body.innerText);
  console.log("Page text (first 4000):");
  console.log(text.substring(0, 4000));

  // Get ALL links
  const allLinks = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("a")).map((a) => ({
      text: (a.textContent || "").trim(),
      href: a.href,
    }));
  });

  const zipLinks = allLinks.filter(
    (l) => l.href.includes(".zip") || l.text.includes(".zip"),
  );
  console.log(`\nZIP links found: ${zipLinks.length}`);
  for (const l of zipLinks.slice(0, 70)) {
    console.log(`  ${l.text} -> ${l.href}`);
  }

  // If no zip links, look for any county-related links
  if (zipLinks.length === 0) {
    const countyLinks = allLinks.filter(
      (l) =>
        l.text.match(
          /Alachua|Baker|Bay|Bradford|Brevard|Broward|Calhoun|Charlotte|Citrus|Clay|Collier|Columbia|DeSoto|Dixie|Duval|Escambia|Flagler|Franklin|Gadsden|Gilchrist|Glades|Gulf|Hamilton|Hardee|Hendry|Hernando|Highlands|Hillsborough|Holmes|Indian|Jackson|Jefferson|Lafayette|Lake|Lee|Leon|Levy|Liberty|Madison|Manatee|Marion|Martin|Miami|Monroe|Nassau|Okaloosa|Okeechobee|Orange|Osceola|Palm|Pasco|Pinellas|Polk|Putnam|Santa|Sarasota|Seminole|St\.|Sumter|Suwannee|Taylor|Union|Volusia|Wakulla|Walton|Washington/i,
        ),
    );
    console.log(`\nCounty links found: ${countyLinks.length}`);
    for (const l of countyLinks.slice(0, 70)) {
      console.log(`  ${l.text} -> ${l.href}`);
    }
  }

  await browser.close();
}

main().catch((e) => {
  console.error("Error:", e.message);
  process.exit(1);
});
