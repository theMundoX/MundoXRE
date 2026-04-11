#!/usr/bin/env tsx
/**
 * Quick test: Pull real mortgage records from Dallas County TX
 * using the publicsearch.us API directly (no browser needed).
 *
 * The publicsearch.us site is a React SPA backed by a JSON API.
 */

import "dotenv/config";

const BASE = "https://dallas.tx.publicsearch.us";

// Step 1: Discover the API structure
async function testApi() {
  console.log("Testing Dallas County PublicSearch API...\n");

  // The publicsearch.us API typically serves search results as JSON
  // Let's try common API patterns

  const headers: Record<string, string> = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": BASE + "/",
    "Origin": BASE,
  };

  // Try 1: Direct API search endpoint
  const endpoints = [
    `/api/v1/search?department=RP&recordedDateRange=2026-03-20to2026-03-26&docType=DEED%20OF%20TRUST`,
    `/api/v1/search?department=RP&recordedDateRange=2026-03-20to2026-03-26`,
    `/api/search?department=RP&recordedDateRange=2026-03-20to2026-03-26`,
    `/results?department=RP&recordedDateRange=2026-03-20to2026-03-26&docType=DEED+OF+TRUST`,
  ];

  for (const endpoint of endpoints) {
    const url = BASE + endpoint;
    console.log(`Trying: ${url}`);
    try {
      const resp = await fetch(url, { headers, redirect: "follow" });
      console.log(`  Status: ${resp.status} ${resp.statusText}`);
      console.log(`  Content-Type: ${resp.headers.get("content-type")}`);
      if (resp.ok) {
        const text = await resp.text();
        // Check if it's JSON
        if (text.startsWith("{") || text.startsWith("[")) {
          const data = JSON.parse(text);
          console.log(`  JSON response! Keys: ${Object.keys(data).join(", ")}`);
          if (Array.isArray(data)) {
            console.log(`  Array length: ${data.length}`);
            if (data.length > 0) console.log(`  First item keys: ${Object.keys(data[0]).join(", ")}`);
          }
          // Print first record if small enough
          const preview = JSON.stringify(data).slice(0, 500);
          console.log(`  Preview: ${preview}`);
          return data;
        } else {
          console.log(`  HTML response (${text.length} chars)`);
          // Look for API base URL or config in the HTML
          const apiMatch = text.match(/api[Bb]ase[Uu]rl["':\s]+(["'])(.*?)\1/);
          if (apiMatch) console.log(`  Found API base URL: ${apiMatch[2]}`);
          const configMatch = text.match(/window\.__CONFIG__\s*=\s*({.*?});/s);
          if (configMatch) {
            console.log(`  Found window config!`);
            try {
              const config = JSON.parse(configMatch[1]);
              console.log(`  Config keys: ${Object.keys(config).join(", ")}`);
            } catch {}
          }
        }
      }
    } catch (err: any) {
      console.log(`  Error: ${err.message}`);
    }
    console.log();
  }

  // Try 2: Get the main page and look for API clues
  console.log("\nFetching main page to find API endpoints...");
  try {
    const resp = await fetch(BASE + "/", { headers: { ...headers, "Accept": "text/html" } });
    const html = await resp.text();

    // Look for API URLs in the JavaScript bundles
    const scriptMatches = html.match(/src="([^"]*\.js[^"]*)"/g) || [];
    console.log(`Found ${scriptMatches.length} script tags`);

    // Look for any /api/ references in the HTML
    const apiRefs = html.match(/["'](\/api\/[^"']+)["']/g) || [];
    if (apiRefs.length > 0) {
      console.log(`API references in HTML:`);
      for (const ref of apiRefs.slice(0, 10)) console.log(`  ${ref}`);
    }

    // Look for common config patterns
    const envMatch = html.match(/window\.__ENV__\s*=\s*({.*?})/s);
    if (envMatch) console.log(`Window env: ${envMatch[1].slice(0, 200)}`);

    // Check for Next.js / React patterns
    const nextMatch = html.match(/__NEXT_DATA__.*?({.*?})<\/script>/s);
    if (nextMatch) console.log(`Next.js data found!`);

    // Look for any fetch/axios base URL patterns in inline scripts
    const inlineScripts = html.match(/<script[^>]*>([^<]+)<\/script>/g) || [];
    for (const script of inlineScripts) {
      if (script.includes("api") || script.includes("API") || script.includes("fetch")) {
        console.log(`Interesting inline script: ${script.slice(0, 300)}`);
      }
    }
  } catch (err: any) {
    console.log(`Error: ${err.message}`);
  }
}

testApi().catch(console.error);
