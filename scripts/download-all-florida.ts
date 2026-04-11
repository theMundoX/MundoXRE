#!/usr/bin/env tsx
/**
 * Download all 67 Florida NAL files using discovered URLs.
 */
import { mkdirSync, existsSync, createWriteStream, unlinkSync } from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";
import https from "node:https";
import http from "node:http";

const DOWNLOAD_DIR = process.argv[2] || "C:\\Users\\msanc\\mxre-data\\florida";
if (!existsSync(DOWNLOAD_DIR)) mkdirSync(DOWNLOAD_DIR, { recursive: true });

const BASE = "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2025F";

const COUNTIES = [
  "Alachua 11", "Baker 12", "Bay 13", "Bradford 14", "Brevard 15",
  "Broward 16", "Calhoun 17", "Charlotte 18", "Citrus 19", "Clay 20",
  "Collier 21", "Columbia 22", "Dade 23", "Desoto 24", "Dixie 25",
  "Duval 26", "Escambia 27", "Flagler 28", "Franklin 29", "Gadsden 30",
  "Gilchrist 31", "Glades 32", "Gulf 33", "Hamilton 34", "Hardee 35",
  "Hendry 36", "Hernando 37", "Highlands 38", "Hillsborough 39", "Holmes 40",
  "Indian River 41", "Jackson 42", "Jefferson 43", "Lafayette 44", "Lake 45",
  "Lee 46", "Leon 47", "Levy 48", "Liberty 49", "Madison 50",
  "Manatee 51", "Marion 52", "Martin 53", "Monroe 54", "Nassau 55",
  "Okaloosa 56", "Okeechobee 57", "Orange 58", "Osceola 59", "Palm Beach 60",
  "Pasco 61", "Pinellas 62", "Polk 63", "Putnam 64", "Saint Johns 65",
  "Saint Lucie 66", "Santa Rosa 67", "Sarasota 68", "Seminole 69", "Sumter 70",
  "Suwannee 71", "Taylor 72", "Union 73", "Volusia 74", "Wakulla 75",
  "Walton 76", "Washington 77",
];

function download(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const file = createWriteStream(dest);
    const client = url.startsWith("https") ? https : http;
    client.get(url, { headers: { "User-Agent": "Mozilla/5.0" } }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        file.close();
        unlinkSync(dest);
        return download(res.headers.location!, dest).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        file.close();
        unlinkSync(dest);
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      res.pipe(file);
      file.on("finish", () => { file.close(); resolve(); });
      file.on("error", reject);
    }).on("error", (err) => { file.close(); reject(err); });
  });
}

async function main() {
  console.log(`Downloading ${COUNTIES.length} Florida NAL files to ${DOWNLOAD_DIR}\n`);

  let downloaded = 0;
  let skipped = 0;
  let errors = 0;

  for (const county of COUNTIES) {
    const zipName = `${county} Final NAL 2025.zip`;
    const csvName = `NAL${county.split(" ").pop()}F202501.csv`;
    const csvPath = join(DOWNLOAD_DIR, csvName);
    const zipPath = join(DOWNLOAD_DIR, zipName);

    // Skip if CSV already exists
    if (existsSync(csvPath)) {
      skipped++;
      continue;
    }

    const url = `${BASE}/${encodeURIComponent(zipName).replace(/%20/g, "%20")}`;
    const urlFixed = `${BASE}/${zipName.replace(/ /g, "%20")}`;

    try {
      process.stdout.write(`  ${county}... `);
      await download(urlFixed, zipPath);

      // Extract
      try {
        if (process.platform === "win32") {
          execSync(`powershell -Command "Expand-Archive -Path '${zipPath}' -DestinationPath '${DOWNLOAD_DIR}' -Force"`, { stdio: "pipe" });
        } else {
          execSync(`unzip -o "${zipPath}" -d "${DOWNLOAD_DIR}"`, { stdio: "pipe" });
        }
        // Clean up zip
        if (existsSync(zipPath)) unlinkSync(zipPath);
        downloaded++;
        console.log("OK");
      } catch (e) {
        console.log(`extract error: ${(e as Error).message.substring(0, 60)}`);
        errors++;
      }
    } catch (e) {
      console.log(`download error: ${(e as Error).message.substring(0, 60)}`);
      errors++;
    }
  }

  console.log(`\nDone: ${downloaded} downloaded, ${skipped} skipped (already exist), ${errors} errors`);
}

main();
