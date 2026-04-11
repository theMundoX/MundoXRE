/**
 * State Real Estate License Adapter — government public records.
 *
 * Looks up agent contact info (phone, email) from state regulatory databases.
 * These are government .gov/.us sites — zero legal risk, fully public records.
 *
 * Currently supports:
 *   - Texas TREC (trec.texas.gov)
 *   - Oklahoma OREC (orec.ok.gov)
 *   - Florida DBPR (myfloridalicense.com)
 *   - Illinois IDFPR (online-dfpr.com)
 *   - Arkansas AREC (arec.arkansas.gov)
 *   - Louisiana LREC (lrec.gov)
 *   - Pennsylvania DOS (pals.pa.gov)
 *   - Virginia DPOR (dpor.virginia.gov)
 *   - Connecticut DCP (elicense.ct.gov)
 *   - Ohio ODOC (com.ohio.gov)
 *
 * Each state has a different website structure — per-state scraping functions.
 */

import { chromium, type Page, type BrowserContext } from "playwright";
import { StateLicenseAdapter, type AgentLicenseRecord } from "./base.js";
import { waitForSlot } from "../../utils/rate-limiter.js";
import { getCached, setCache } from "../../utils/cache.js";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../../utils/stealth.js";

const LICENSE_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days — licenses don't change often

// ─── Browser ────────────────────────────────────────────────────────

async function createLicenseBrowser(): Promise<{ context: BrowserContext; close: () => Promise<void> }> {
  const stealth = getStealthConfig();
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: stealth.userAgent,
    viewport: stealth.viewport,
    locale: stealth.locale,
  });
  return { context, close: async () => { await context.close(); await browser.close(); } };
}

// ─── Texas TREC ─────────────────────────────────────────────────────

async function lookupTexasTREC(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `trec:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://www.trec.texas.gov/apps/license-holder-search";
  await waitForSlot(url);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    // Parse name into first/last
    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill search form
    const lastNameInput = page.locator('input[name="lastName"], input[id*="lastName"]').first();
    const firstNameInput = page.locator('input[name="firstName"], input[id*="firstName"]').first();

    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    }
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"]').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    // Extract results
    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-results tr");
      for (const row of rows) {
        const cells = row.querySelectorAll("td");
        if (cells.length < 3) continue;

        const resultName = cells[0]?.textContent?.trim() || "";
        const licenseNum = cells[1]?.textContent?.trim() || "";
        const status = cells[2]?.textContent?.trim()?.toLowerCase() || "";

        // Check if name roughly matches
        if (resultName.toLowerCase().includes(name.toLowerCase().split(" ")[0])) {
          // Try to find more details — phone/email might be in detail links
          const detailLink = row.querySelector("a");
          const phone = row.querySelector('[data-field="phone"]')?.textContent?.trim();
          const email = row.querySelector('[data-field="email"]')?.textContent?.trim();
          const brokerage = cells[3]?.textContent?.trim();

          return {
            agent_name: resultName,
            license_number: licenseNum,
            license_state: "TX",
            license_status: status.includes("active") ? "active" :
                           status.includes("inactive") ? "inactive" : "expired",
            brokerage_name: brokerage || undefined,
            phone: phone || undefined,
            email: email || undefined,
            license_type: licenseNum.startsWith("0") ? "salesperson" : "broker",
            source_url: "https://www.trec.texas.gov/apps/license-holder-search",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  TREC lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Oklahoma OREC ──────────────────────────────────────────────────

async function lookupOklahomaOREC(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `orec:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://www.orec.ok.gov/license-search";
  await waitForSlot(url);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill and submit search
    const nameInput = page.locator('input[name*="name"], input[name*="search"]').first();
    if (await nameInput.count() > 0) {
      await nameInput.fill(`${lastName} ${firstName}`.trim());
    }

    const submitBtn = page.locator('button[type="submit"], input[type="submit"]').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-result");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "OK",
            license_status: text.includes("active") ? "active" : "inactive",
            brokerage_name: cells[3]?.textContent?.trim() || undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://www.orec.ok.gov/license-search",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  OREC lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Florida DBPR ───────────────────────────────────────────────────

async function lookupFloridaDBPR(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `dbpr:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const nameParts = agentName.trim().split(/\s+/);
  const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
  const firstName = nameParts.length > 1 ? nameParts[0] : "";

  const url = `https://www.myfloridalicense.com/wl11.asp?mode=2&search=Name&SID=&brd=25&typ=&name=${encodeURIComponent(lastName)}&fname=${encodeURIComponent(firstName)}&lnumber=`;
  await waitForSlot("https://www.myfloridalicense.com");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(3000);

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tr");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0]) && text.includes("real estate")) {
          const cells = row.querySelectorAll("td");
          const licNum = cells[1]?.textContent?.trim() || "";
          const status = cells[4]?.textContent?.trim()?.toLowerCase() || "";

          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: licNum,
            license_state: "FL",
            license_status: status.includes("current") || status.includes("active") ? "active" : "inactive",
            brokerage_name: undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://www.myfloridalicense.com",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  DBPR lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Illinois IDFPR ─────────────────────────────────────────────────

async function lookupIllinoisIDFPR(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `idfpr:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://online-dfpr.com/Lookup/LicenseLookup.aspx";
  await waitForSlot(url);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill last name
    const lastNameInput = page.locator('input[name*="LastName"], input[id*="LastName"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    }

    // Fill first name
    const firstNameInput = page.locator('input[name*="FirstName"], input[id*="FirstName"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Select real estate category if available
    const categorySelect = page.locator('select[name*="Profession"], select[id*="Profession"]').first();
    if (await categorySelect.count() > 0) {
      await categorySelect.selectOption({ label: "Real Estate" }).catch(() => {});
    }

    // Submit
    const submitBtn = page.locator('input[type="submit"], button[type="submit"]').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .GridRow, .GridAltRow");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          const status = text.includes("active") ? "active" :
                        text.includes("inactive") ? "inactive" : "expired";
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "IL",
            license_status: status,
            brokerage_name: cells[3]?.textContent?.trim() || undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://online-dfpr.com/Lookup/LicenseLookup.aspx",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  IDFPR lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Arkansas AREC ──────────────────────────────────────────────────

async function lookupArkansasAREC(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `arec:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://arec.arkansas.gov/license-search";
  await waitForSlot(url);

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill name fields
    const lastNameInput = page.locator('input[name*="last"], input[name*="Last"], input[id*="last"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    } else {
      // Fallback: single name field
      const nameInput = page.locator('input[name*="name"], input[name*="search"]').first();
      if (await nameInput.count() > 0) {
        await nameInput.fill(agentName);
      }
    }

    const firstNameInput = page.locator('input[name*="first"], input[name*="First"], input[id*="first"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"], button:has-text("Search")').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-result, .license-result");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "AR",
            license_status: text.includes("active") ? "active" : "inactive",
            brokerage_name: cells[3]?.textContent?.trim() || undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://arec.arkansas.gov/license-search",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  AREC lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Louisiana LREC ─────────────────────────────────────────────────

async function lookupLouisianaLREC(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `lrec:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://lrec.gov/licensee-search/";
  await waitForSlot("https://lrec.gov");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill name search
    const lastNameInput = page.locator('input[name*="last"], input[name*="Last"], input[placeholder*="Last"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    } else {
      const nameInput = page.locator('input[name*="name"], input[name*="search"], input[placeholder*="name"]').first();
      if (await nameInput.count() > 0) {
        await nameInput.fill(agentName);
      }
    }

    const firstNameInput = page.locator('input[name*="first"], input[name*="First"], input[placeholder*="First"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"], button:has-text("Search")').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-result");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "LA",
            license_status: text.includes("active") ? "active" : "inactive",
            brokerage_name: cells[2]?.textContent?.trim() || undefined,
            phone: cells[3]?.textContent?.trim() || undefined,
            email: cells[4]?.textContent?.trim() || undefined,
            license_type: "salesperson",
            source_url: "https://lrec.gov/licensee-search/",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  LREC lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Pennsylvania DOS ───────────────────────────────────────────────

async function lookupPennsylvaniaDOS(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `pados:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://pals.pa.gov/#/page/search";
  await waitForSlot("https://pals.pa.gov");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(3000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Select "Real Estate" board if available
    const boardSelect = page.locator('select[name*="board"], select[id*="board"], select[name*="Board"]').first();
    if (await boardSelect.count() > 0) {
      await boardSelect.selectOption({ label: "Real Estate" }).catch(() => {});
      await page.waitForTimeout(1000);
    }

    // Fill last name
    const lastNameInput = page.locator('input[name*="lastName"], input[name*="LastName"], input[placeholder*="Last"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    }

    // Fill first name
    const firstNameInput = page.locator('input[name*="firstName"], input[name*="FirstName"], input[placeholder*="First"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"], button:has-text("Search")').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(4000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-results tr, [class*='result']");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          const status = text.includes("active") ? "active" :
                        text.includes("expired") ? "expired" : "inactive";
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "PA",
            license_status: status,
            brokerage_name: cells[3]?.textContent?.trim() || undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://pals.pa.gov",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  PA DOS lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Virginia DPOR ──────────────────────────────────────────────────

async function lookupVirginiaDPOR(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `dpor:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://dpor.virginia.gov/LicenseLookup";
  await waitForSlot("https://dpor.virginia.gov");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill name fields
    const lastNameInput = page.locator('input[name*="LastName"], input[name*="lastName"], input[id*="last"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    } else {
      const nameInput = page.locator('input[name*="name"], input[name*="search"]').first();
      if (await nameInput.count() > 0) {
        await nameInput.fill(agentName);
      }
    }

    const firstNameInput = page.locator('input[name*="FirstName"], input[name*="firstName"], input[id*="first"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Select board/profession to Real Estate if available
    const boardSelect = page.locator('select[name*="board"], select[name*="Board"], select[name*="profession"]').first();
    if (await boardSelect.count() > 0) {
      await boardSelect.selectOption({ label: "Real Estate" }).catch(() => {});
      await page.waitForTimeout(1000);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"], button:has-text("Search")').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-result");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "VA",
            license_status: text.includes("active") ? "active" : "inactive",
            brokerage_name: cells[2]?.textContent?.trim() || undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://dpor.virginia.gov/LicenseLookup",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  DPOR lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Connecticut DCP ────────────────────────────────────────────────

async function lookupConnecticutDCP(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `ctdcp:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://elicense.ct.gov/Lookup/LicenseLookup.aspx";
  await waitForSlot("https://elicense.ct.gov");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Select license type to Real Estate if available
    const typeSelect = page.locator('select[name*="Type"], select[name*="type"], select[id*="Type"]').first();
    if (await typeSelect.count() > 0) {
      await typeSelect.selectOption({ label: "Real Estate" }).catch(() => {});
      await page.waitForTimeout(1000);
    }

    // Fill last name
    const lastNameInput = page.locator('input[name*="LastName"], input[name*="lastName"], input[id*="LastName"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    }

    // Fill first name
    const firstNameInput = page.locator('input[name*="FirstName"], input[name*="firstName"], input[id*="FirstName"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('input[type="submit"], button[type="submit"], input[value="Search"]').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .GridRow, .GridAltRow");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "CT",
            license_status: text.includes("active") ? "active" :
                           text.includes("expired") ? "expired" : "inactive",
            brokerage_name: undefined,
            phone: undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://elicense.ct.gov/Lookup/LicenseLookup.aspx",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  CT DCP lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Ohio ODOC ──────────────────────────────────────────────────────

async function lookupOhioODOC(
  page: Page,
  agentName: string,
): Promise<AgentLicenseRecord | null> {
  const cacheKey = `odoc:${agentName.toLowerCase().replace(/\s+/g, "_")}`;
  const cached = getCached(cacheKey, LICENSE_CACHE_TTL);
  if (cached) return JSON.parse(cached) as AgentLicenseRecord;

  const url = "https://com.ohio.gov/real/LicenseLookup";
  await waitForSlot("https://com.ohio.gov");

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForTimeout(2000);

    const nameParts = agentName.trim().split(/\s+/);
    const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : agentName;
    const firstName = nameParts.length > 1 ? nameParts[0] : "";

    // Fill name fields
    const lastNameInput = page.locator('input[name*="LastName"], input[name*="lastName"], input[id*="last"], input[placeholder*="Last"]').first();
    if (await lastNameInput.count() > 0) {
      await lastNameInput.fill(lastName);
    } else {
      const nameInput = page.locator('input[name*="name"], input[name*="search"]').first();
      if (await nameInput.count() > 0) {
        await nameInput.fill(agentName);
      }
    }

    const firstNameInput = page.locator('input[name*="FirstName"], input[name*="firstName"], input[id*="first"], input[placeholder*="First"]').first();
    if (firstName && await firstNameInput.count() > 0) {
      await firstNameInput.fill(firstName);
    }

    // Submit
    const submitBtn = page.locator('button[type="submit"], input[type="submit"], button:has-text("Search")').first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForLoadState("domcontentloaded", { timeout: 15_000 }).catch(() => {});
      await page.waitForTimeout(3000);
    }

    const record = await page.evaluate((name) => {
      const rows = document.querySelectorAll("table tbody tr, .search-result, [class*='result']");
      for (const row of rows) {
        const text = row.textContent?.toLowerCase() || "";
        if (text.includes(name.toLowerCase().split(" ")[0])) {
          const cells = row.querySelectorAll("td");
          return {
            agent_name: cells[0]?.textContent?.trim() || name,
            license_number: cells[1]?.textContent?.trim() || "",
            license_state: "OH",
            license_status: text.includes("active") ? "active" : "inactive",
            brokerage_name: cells[2]?.textContent?.trim() || undefined,
            phone: cells[3]?.textContent?.trim() || undefined,
            email: undefined,
            license_type: "salesperson",
            source_url: "https://com.ohio.gov/real/LicenseLookup",
            observed_at: new Date().toISOString(),
          } as AgentLicenseRecord;
        }
      }
      return null;
    }, agentName);

    if (record) {
      setCache(cacheKey, JSON.stringify(record));
    }
    return record;
  } catch (err) {
    console.log(`  Ohio ODOC lookup error for "${agentName}":`, err instanceof Error ? err.message : "Unknown");
    return null;
  }
}

// ─── Multi-State Adapter ────────────────────────────────────────────

const STATE_HANDLERS: Record<string, (page: Page, name: string) => Promise<AgentLicenseRecord | null>> = {
  TX: lookupTexasTREC,
  OK: lookupOklahomaOREC,
  FL: lookupFloridaDBPR,
  IL: lookupIllinoisIDFPR,
  AR: lookupArkansasAREC,
  LA: lookupLouisianaLREC,
  PA: lookupPennsylvaniaDOS,
  VA: lookupVirginiaDPOR,
  CT: lookupConnecticutDCP,
  OH: lookupOhioODOC,
};

export class MultiStateLicenseAdapter extends StateLicenseAdapter {
  readonly state = "multi";

  canHandle(stateCode: string): boolean {
    return stateCode.toUpperCase() in STATE_HANDLERS;
  }

  async lookupAgent(
    agentName: string,
    _brokerage?: string,
  ): Promise<AgentLicenseRecord | null> {
    // This method requires state context — use lookupAgentInState instead
    return null;
  }

  async lookupAgentInState(
    agentName: string,
    stateCode: string,
  ): Promise<AgentLicenseRecord | null> {
    const handler = STATE_HANDLERS[stateCode.toUpperCase()];
    if (!handler) {
      console.log(`  No license adapter for state: ${stateCode}`);
      return null;
    }

    const { context, close } = await createLicenseBrowser();
    try {
      const page = await context.newPage();
      await page.addInitScript(STEALTH_INIT_SCRIPT);
      page.setDefaultTimeout(30_000);
      return await handler(page, agentName);
    } finally {
      await close();
    }
  }

  static supportedStates(): string[] {
    return Object.keys(STATE_HANDLERS);
  }
}
