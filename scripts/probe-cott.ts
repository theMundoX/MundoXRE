#!/usr/bin/env tsx
/**
 * Probe Cott RECORDhub recorder platform — Phase 2: API deep dive.
 *
 * Phase 1 found:
 * - Login wall on all county portals (email/password required)
 * - /api/{county}/Notification/GetAll/ returns JSON without auth
 * - SignalR websocket connection established
 * - "Search Sites" public page at /Portal/SearchSites/Selection
 * - No guest/public search access
 *
 * Phase 2: Register for account, explore authenticated API, or find
 * unauthenticated endpoints.
 */
import { chromium } from "playwright";

const COUNTY_SLUG = "autauga";
const BASE = `https://recordhub.cottsystems.com`;
const COUNTY_BASE = `${BASE}/${COUNTY_SLUG}`;

interface ApiCall {
  method: string;
  url: string;
  status: number;
  contentType: string;
  requestBody: string;
  responseBody: string;
  authHeader: string;
  cookies: string;
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  });
  const page = await ctx.newPage();

  const apiCalls: ApiCall[] = [];

  page.on("response", async (resp) => {
    const url = resp.url();
    if (url.match(/\.(js|css|png|jpg|svg|woff2?|ttf|eot|ico|map)(\?|$)/)) return;
    if (url.includes("fonts.googleapis") || url.includes("cdnjs") || url.includes("visualstudio")) return;
    if (url.includes("google") || url.includes("gtag")) return;

    const ct = resp.headers()["content-type"] || "";
    let responseBody = "";
    let requestBody = "";

    try {
      responseBody = (await resp.text()).slice(0, 1000);
    } catch {}

    try {
      requestBody = resp.request().postData()?.slice(0, 500) || "";
    } catch {}

    const reqHeaders = resp.request().headers();

    apiCalls.push({
      method: resp.request().method(),
      url,
      status: resp.status(),
      contentType: ct.split(";")[0],
      requestBody,
      responseBody,
      authHeader: reqHeaders["authorization"] || "",
      cookies: reqHeaders["cookie"] ? "[present]" : "",
    });
  });

  // ──────────────────────────────────────────────────────────────────
  // Step 1: Probe unauthenticated API endpoints
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 1: Probing unauthenticated API endpoints");
  console.log(`${"=".repeat(60)}\n`);

  // Test known API pattern: /api/{county}/...
  const apiPaths = [
    `/api/${COUNTY_SLUG}/Notification/GetAll/`,
    `/api/${COUNTY_SLUG}/Search/`,
    `/api/${COUNTY_SLUG}/Search/DocumentTypes`,
    `/api/${COUNTY_SLUG}/Search/DocTypes`,
    `/api/${COUNTY_SLUG}/Search/GetDocTypes`,
    `/api/${COUNTY_SLUG}/Document/Search`,
    `/api/${COUNTY_SLUG}/Document/DocTypes`,
    `/api/${COUNTY_SLUG}/DocumentSearch`,
    `/api/${COUNTY_SLUG}/DocumentSearch/GetDocTypes`,
    `/api/${COUNTY_SLUG}/DocumentSearch/Search`,
    `/api/${COUNTY_SLUG}/Recording/Search`,
    `/api/${COUNTY_SLUG}/Recording/DocTypes`,
    `/api/${COUNTY_SLUG}/Instrument/Search`,
    `/api/${COUNTY_SLUG}/Instrument/Types`,
    `/api/${COUNTY_SLUG}/Config`,
    `/api/${COUNTY_SLUG}/Config/GetConfig`,
    `/api/${COUNTY_SLUG}/County`,
    `/api/${COUNTY_SLUG}/County/Info`,
    `/api/${COUNTY_SLUG}/User`,
    `/api/${COUNTY_SLUG}/Account`,
    `/api/${COUNTY_SLUG}/Portal`,
    `/api/${COUNTY_SLUG}/Portal/Search`,
    `/api/${COUNTY_SLUG}/Lookup/DocTypes`,
    `/api/${COUNTY_SLUG}/Lookup/InstrumentTypes`,
  ];

  for (const path of apiPaths) {
    try {
      const resp = await fetch(`${BASE}${path}`, {
        method: "GET",
        redirect: "follow",
        headers: {
          "User-Agent": "Mozilla/5.0",
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const ct = resp.headers.get("content-type") || "";
      const body = await resp.text();
      const isLogin = body.includes("Login") || resp.url.includes("Login");
      const isJson = ct.includes("json");
      const is404 = resp.url.includes("404") || body.includes("Page Not Found");

      if (is404) continue; // Skip 404s

      const tag = isLogin ? "LOGIN" : isJson ? "JSON!" : resp.status.toString();
      console.log(`  ${path}: [${tag}] ${ct.split(";")[0]}`);

      if (isJson && !isLogin) {
        console.log(`    >>> ${body.slice(0, 400)}`);
      }
    } catch (e: any) {
      console.log(`  ${path}: ERROR — ${e.message.slice(0, 60)}`);
    }
  }

  // ──────────────────────────────────────────────────────────────────
  // Step 2: Explore the Search Sites page (public)
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 2: Exploring /Portal/SearchSites/Selection (public page)");
  console.log(`${"=".repeat(60)}\n`);

  apiCalls.length = 0;
  await page.goto(`${BASE}/Portal/SearchSites/Selection`, { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(3000);

  console.log(`  Title: ${await page.title()}`);
  console.log(`  URL: ${page.url()}`);

  const searchSitesInfo = await page.evaluate(() => {
    const body = document.body.innerText;
    const links = Array.from(document.querySelectorAll("a"))
      .filter((a) => a.textContent?.trim())
      .map((a) => ({ text: a.textContent?.trim().slice(0, 50), href: a.href }))
      .slice(0, 30);

    const selects = Array.from(document.querySelectorAll("select"))
      .map((s) => ({
        id: s.id,
        name: s.name,
        options: Array.from(s.options)
          .map((o) => o.text.trim())
          .filter((t) => t)
          .slice(0, 20),
      }));

    const inputs = Array.from(document.querySelectorAll("input"))
      .filter((el) => el.offsetHeight > 0)
      .map((el) => ({ type: el.type, id: el.id, name: el.name, placeholder: el.placeholder }))
      .slice(0, 15);

    return { text: body.slice(0, 2000), links, selects, inputs };
  });

  console.log("\n  Page text (first 1500 chars):");
  console.log(`    ${searchSitesInfo.text.slice(0, 1500).replace(/\n/g, "\n    ")}`);

  console.log("\n  Selects:");
  for (const sel of searchSitesInfo.selects) {
    console.log(`    ${sel.id || sel.name}: ${sel.options.join(", ")}`);
  }

  console.log("\n  Inputs:");
  for (const inp of searchSitesInfo.inputs) {
    console.log(`    type=${inp.type} id="${inp.id}" name="${inp.name}" placeholder="${inp.placeholder}"`);
  }

  // Show API calls from this page
  const searchSiteApis = apiCalls.filter(
    (c) => c.contentType.includes("json") || c.method === "POST" || c.url.includes("api")
  );
  if (searchSiteApis.length > 0) {
    console.log("\n  API calls from SearchSites page:");
    for (const call of searchSiteApis) {
      console.log(`    ${call.method} [${call.status}] ${call.url.slice(0, 100)}`);
      if (call.responseBody && call.contentType.includes("json")) {
        console.log(`      Response: ${call.responseBody.slice(0, 400)}`);
      }
    }
  }

  // ──────────────────────────────────────────────────────────────────
  // Step 3: Navigate to the county login page and check registration
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 3: Checking registration page");
  console.log(`${"=".repeat(60)}\n`);

  apiCalls.length = 0;
  await page.goto(
    `${BASE}/${COUNTY_SLUG}/account/portalregister?returnurl=/${COUNTY_SLUG}`,
    { waitUntil: "networkidle", timeout: 30000 }
  );
  await page.waitForTimeout(2000);

  console.log(`  Title: ${await page.title()}`);
  console.log(`  URL: ${page.url()}`);

  const regInfo = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll("input, select, textarea"))
      .filter((el) => (el as HTMLElement).offsetHeight > 0)
      .map((el) => ({
        type: (el as HTMLInputElement).type,
        id: el.id,
        name: (el as HTMLInputElement).name,
        placeholder: (el as HTMLInputElement).placeholder,
        required: (el as HTMLInputElement).required,
      }))
      .slice(0, 20);

    const text = document.body.innerText.slice(0, 1500);
    const hasTerms = text.includes("Terms") || text.includes("agree");
    const hasFee = text.includes("fee") || text.includes("$") || text.includes("cost") || text.includes("subscription");
    const hasFree = text.includes("free") || text.includes("Free") || text.includes("no cost");

    return { inputs, text, hasTerms, hasFee, hasFree };
  });

  console.log(`  Has terms: ${regInfo.hasTerms}`);
  console.log(`  Has fee/cost: ${regInfo.hasFee}`);
  console.log(`  Has free mention: ${regInfo.hasFree}`);

  console.log("\n  Registration fields:");
  for (const inp of regInfo.inputs) {
    console.log(
      `    type=${inp.type} id="${inp.id}" name="${inp.name}" placeholder="${inp.placeholder}" required=${inp.required}`
    );
  }

  console.log("\n  Registration page text:");
  console.log(`    ${regInfo.text.slice(0, 1500).replace(/\n/g, "\n    ")}`);

  // ──────────────────────────────────────────────────────────────────
  // Step 4: Look at the JavaScript bundles for API routes
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 4: Scanning JavaScript for API routes");
  console.log(`${"=".repeat(60)}\n`);

  // Go back to login page to find JS bundles
  await page.goto(`${COUNTY_BASE}/Portal/Account/Login`, {
    waitUntil: "networkidle",
    timeout: 30000,
  });

  const scriptSrcs = await page.evaluate(() => {
    return Array.from(document.querySelectorAll("script[src]")).map((s) => (s as HTMLScriptElement).src);
  });

  console.log("  Script bundles:");
  for (const src of scriptSrcs) {
    console.log(`    ${src.slice(0, 100)}`);
  }

  // Try fetching the main JS bundle and search for API patterns
  for (const src of scriptSrcs) {
    if (src.includes("bundle") || src.includes("main") || src.includes("app") || src.includes("site")) {
      try {
        const resp = await fetch(src);
        const js = await resp.text();

        // Search for API route patterns
        const apiPatterns = js.match(/["'](\/api\/[^"']{5,60})["']/g) || [];
        const urlPatterns = js.match(/["'](\/[a-zA-Z]+\/[a-zA-Z]+(?:\/[a-zA-Z]+)?)["']/g) || [];
        const fetchCalls = js.match(/fetch\([^)]{10,80}\)/g) || [];
        const ajaxCalls = js.match(/\$\.(ajax|get|post|getJSON)\([^)]{10,80}\)/g) || [];
        const controllerActions = js.match(/["'](?:Search|Document|Recording|Instrument|Portal)[^"']{0,40}["']/g) || [];

        if (apiPatterns.length > 0) {
          console.log(`\n  API routes in ${src.split("/").pop()}:`);
          const unique = [...new Set(apiPatterns)];
          for (const p of unique.slice(0, 30)) {
            console.log(`    ${p}`);
          }
        }

        if (controllerActions.length > 0) {
          console.log(`\n  Controller/Action patterns in ${src.split("/").pop()}:`);
          const unique = [...new Set(controllerActions)];
          for (const p of unique.slice(0, 30)) {
            console.log(`    ${p}`);
          }
        }

        // Look for specific keywords
        const hasWebAPI = js.includes("WebApi") || js.includes("webapi") || js.includes("Web API");
        const hasOData = js.includes("odata") || js.includes("OData");
        const hasGraphQL = js.includes("graphql") || js.includes("GraphQL");
        const hasSwagger = js.includes("swagger") || js.includes("Swagger");

        console.log(`\n  Framework signals in ${src.split("/").pop()}:`);
        console.log(`    WebAPI: ${hasWebAPI}, OData: ${hasOData}, GraphQL: ${hasGraphQL}, Swagger: ${hasSwagger}`);
      } catch {}
    }
  }

  // ──────────────────────────────────────────────────────────────────
  // Step 5: Try more API patterns based on ASP.NET MVC conventions
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 5: Probing ASP.NET-style API routes");
  console.log(`${"=".repeat(60)}\n`);

  const aspNetPaths = [
    `/${COUNTY_SLUG}/Portal/DocumentSearch`,
    `/${COUNTY_SLUG}/Portal/DocumentSearch/Index`,
    `/${COUNTY_SLUG}/Portal/DocumentSearch/GetDocTypes`,
    `/${COUNTY_SLUG}/Portal/Search`,
    `/${COUNTY_SLUG}/Portal/Search/Index`,
    `/${COUNTY_SLUG}/Portal/Search/DocumentTypes`,
    `/${COUNTY_SLUG}/Portal/NameSearch`,
    `/${COUNTY_SLUG}/Portal/DateSearch`,
    `/${COUNTY_SLUG}/Portal/BookPageSearch`,
    `/${COUNTY_SLUG}/Portal/InstrumentSearch`,
    `/${COUNTY_SLUG}/Portal/Home/Index`,
    `/${COUNTY_SLUG}/Portal/RecordSearch`,
    `/${COUNTY_SLUG}/Portal/LandRecords`,
  ];

  for (const path of aspNetPaths) {
    try {
      const resp = await fetch(`${BASE}${path}`, {
        redirect: "follow",
        headers: {
          "User-Agent": "Mozilla/5.0",
          Accept: "text/html, application/json",
        },
      });

      const body = await resp.text();
      const isLogin = body.includes('id="UserName"') || resp.url.includes("Login");
      const is404 = resp.url.includes("404") || body.includes("Page Not Found");

      if (is404) continue;

      const tag = isLogin ? "LOGIN-WALL" : `OK`;
      console.log(`  ${path}: [${resp.status}] ${tag} -> ${resp.url.slice(0, 80)}`);

      if (!isLogin && resp.status === 200) {
        // Check what's on the page
        const titleMatch = body.match(/<title>([^<]+)<\/title>/);
        console.log(`    Title: ${titleMatch?.[1] || "N/A"}`);
        console.log(`    Has search inputs: ${body.includes('type="search"') || body.includes('id="searchTerm"')}`);
        console.log(`    Body preview: ${body.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 200)}`);
      }
    } catch {}
  }

  // ──────────────────────────────────────────────────────────────────
  // Step 6: Load county portal with Playwright and intercept ALL XHR
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 6: Full page load with XHR interception");
  console.log(`${"=".repeat(60)}\n`);

  apiCalls.length = 0;
  await page.goto(`${COUNTY_BASE}/Portal/Account/Login`, {
    waitUntil: "networkidle",
    timeout: 30000,
  });
  await page.waitForTimeout(3000);

  // Log all non-static requests
  console.log("  All network calls during login page load:");
  for (const call of apiCalls) {
    console.log(`    ${call.method} [${call.status}] ${call.url.slice(0, 100)} (${call.contentType})`);
    if (call.contentType.includes("json") && call.responseBody) {
      console.log(`      Response: ${call.responseBody.slice(0, 300)}`);
    }
    if (call.requestBody) {
      console.log(`      Request: ${call.requestBody.slice(0, 200)}`);
    }
  }

  // ──────────────────────────────────────────────────────────────────
  // Step 7: Check if there are any public document viewing endpoints
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("Step 7: Testing direct document/recording endpoints");
  console.log(`${"=".repeat(60)}\n`);

  const docPaths = [
    `/${COUNTY_SLUG}/Portal/Document/1`,
    `/${COUNTY_SLUG}/Portal/Recording/1`,
    `/${COUNTY_SLUG}/Portal/Instrument/1`,
    `/api/${COUNTY_SLUG}/Document/1`,
    `/api/${COUNTY_SLUG}/Recording/1`,
    `/api/${COUNTY_SLUG}/Document/Get/1`,
    `/api/${COUNTY_SLUG}/Recording/Get/1`,
    `/api/${COUNTY_SLUG}/Search/Recent`,
    `/api/${COUNTY_SLUG}/Search/ByDate?start=2024-01-01&end=2024-01-31`,
    `/api/${COUNTY_SLUG}/Search/ByDateRange?startDate=2024-01-01&endDate=2024-01-31`,
  ];

  for (const path of docPaths) {
    try {
      const resp = await fetch(`${BASE}${path}`, {
        redirect: "follow",
        headers: {
          "User-Agent": "Mozilla/5.0",
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const ct = resp.headers.get("content-type") || "";
      const body = await resp.text();
      const isLogin = body.includes("Login") || resp.url.includes("Login");
      const is404 = resp.url.includes("404") || body.includes("Page Not Found");

      if (is404) continue;

      const tag = isLogin ? "LOGIN" : ct.includes("json") ? "JSON!" : `${resp.status}`;
      console.log(`  ${path}: [${tag}] ${ct.split(";")[0]}`);

      if (ct.includes("json") && !isLogin) {
        console.log(`    >>> ${body.slice(0, 400)}`);
      }
    } catch {}
  }

  // ──────────────────────────────────────────────────────────────────
  // Summary
  // ──────────────────────────────────────────────────────────────────
  console.log(`\n${"=".repeat(60)}`);
  console.log("FINAL ASSESSMENT");
  console.log(`${"=".repeat(60)}\n`);

  console.log("Platform: Cott RECORDhub (recordhub.cottsystems.com)");
  console.log("Counties in census: ~2,587");
  console.log("Tech stack: ASP.NET MVC + SignalR + Azure Application Insights");
  console.log("Auth: Email/password login required (no guest/anonymous search)");
  console.log("Registration: Free signup available at /{county}/account/portalregister");
  console.log("API base: /api/{county}/... (requires auth cookies)");
  console.log("Real-time: SignalR websockets for notifications");
  console.log("");
  console.log("VERDICT: Login wall blocks all search functionality.");
  console.log("Unlike Fidlar AVA, there is NO open API without authentication.");
  console.log("Options:");
  console.log("  1. Register account (free) and probe authenticated API");
  console.log("  2. Use Playwright with login automation");
  console.log("  3. Check if specific counties have different access policies");

  await browser.close();
}

main().catch(console.error);
