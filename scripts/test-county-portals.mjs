import { chromium } from 'playwright';

const counties = [
  { name: 'Levy', url: 'https://online.levyclerk.com/landmarkweb', note: 'KNOWN WORKING' },
  { name: 'Martin', url: 'http://or.martinclerk.com/' },
  { name: 'Lee', url: 'https://or.leeclerk.org/LandMarkWeb/' },
  { name: 'Walton', url: 'https://orsearch.clerkofcourts.co.walton.fl.us/' },
  { name: 'Citrus', url: 'https://search.citrusclerk.org/LandmarkWeb' },
  { name: 'Escambia', url: 'https://dory.escambiaclerk.com/LandmarkWeb' },
  { name: 'Clay', url: 'https://landmark.clayclerk.com/landmarkweb' },
  { name: 'Palm Beach', url: 'https://erec.mypalmbeachclerk.com', note: 'KNOWN CAPTCHA' },
];

const results = [];

async function testCounty(county) {
  const result = { name: county.name, url: county.url, status: 'UNKNOWN', details: '' };
  let browser;

  try {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Testing: ${county.name} — ${county.url}`);
    console.log(`${'='.repeat(60)}`);

    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    });
    const page = await context.newPage();

    // Step 1: Navigate to the URL
    console.log(`  [1] Navigating to ${county.url}...`);
    try {
      await page.goto(county.url, { waitUntil: 'domcontentloaded', timeout: 15000 });
    } catch (e) {
      result.status = 'CONNECTION_FAILED';
      result.details = `Navigation failed: ${e.message.split('\n')[0]}`;
      console.log(`  FAIL: ${result.details}`);
      return result;
    }
    console.log(`  [1] Page loaded. Title: "${await page.title()}"`);

    // Check for captcha indicators
    const pageContent = await page.content();
    const hasCaptcha = pageContent.includes('captcha') ||
                       pageContent.includes('CAPTCHA') ||
                       pageContent.includes('recaptcha') ||
                       pageContent.includes('hcaptcha') ||
                       pageContent.includes('cf-challenge') ||
                       pageContent.includes('challenge-platform');

    if (hasCaptcha) {
      console.log(`  WARNING: Captcha/challenge detected on page`);
    }

    // Step 2: Accept disclaimer via SetDisclaimer()
    console.log(`  [2] Calling SetDisclaimer()...`);
    try {
      await page.evaluate(() => {
        if (typeof SetDisclaimer === 'function') {
          SetDisclaimer();
        }
      });
      await page.waitForTimeout(2000);
      console.log(`  [2] Disclaimer accepted`);
    } catch (e) {
      console.log(`  [2] SetDisclaimer not available or failed: ${e.message.split('\n')[0]}`);
      // Try clicking accept/agree button as fallback
      try {
        const acceptBtn = await page.$('button:has-text("Accept"), button:has-text("Agree"), input[value="Accept"], input[value="Agree"], a:has-text("Accept"), a:has-text("Agree")');
        if (acceptBtn) {
          await acceptBtn.click();
          await page.waitForTimeout(2000);
          console.log(`  [2] Clicked accept/agree button instead`);
        }
      } catch (_) {}
    }

    // Step 3: Click "Record Date Search" tab
    console.log(`  [3] Looking for Record Date Search tab...`);
    let foundTab = false;

    // Try multiple selectors for the tab
    const tabSelectors = [
      'a:has-text("Record Date Search")',
      'text=Record Date Search',
      '#idAccordionRecordDateSearch',
      'a[href*="RecordDate"]',
      'a[href*="recorddate"]',
      'h4:has-text("Record Date Search")',
      '.accordion-toggle:has-text("Record Date")',
    ];

    for (const sel of tabSelectors) {
      try {
        const el = await page.$(sel);
        if (el) {
          await el.click();
          foundTab = true;
          console.log(`  [3] Clicked tab using selector: ${sel}`);
          await page.waitForTimeout(1500);
          break;
        }
      } catch (_) {}
    }

    if (!foundTab) {
      console.log(`  [3] Could not find Record Date Search tab`);
      // Log available links for debugging
      const links = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a, button, h4, .accordion-toggle'))
          .slice(0, 20)
          .map(el => `${el.tagName}: "${el.textContent.trim().substring(0, 60)}" href=${el.getAttribute('href') || 'none'}`);
      });
      console.log(`  Available elements:\n    ${links.join('\n    ')}`);
    }

    // Step 4: Fill dates
    console.log(`  [4] Filling date fields...`);
    let filledDates = false;

    // Try various date input selectors
    const beginSelectors = ['#beginDate', '#BeginDate', 'input[name="beginDate"]', 'input[name="BeginDate"]', '#txtBeginDate', 'input[placeholder*="Begin"]'];
    const endSelectors = ['#endDate', '#EndDate', 'input[name="endDate"]', 'input[name="EndDate"]', '#txtEndDate', 'input[placeholder*="End"]'];

    for (const bSel of beginSelectors) {
      try {
        const bEl = await page.$(bSel);
        if (bEl) {
          await bEl.fill('03/24/2026');
          console.log(`  [4] Filled begin date using: ${bSel}`);
          filledDates = true;
          break;
        }
      } catch (_) {}
    }

    for (const eSel of endSelectors) {
      try {
        const eEl = await page.$(eSel);
        if (eEl) {
          await eEl.fill('03/24/2026');
          console.log(`  [4] Filled end date using: ${eSel}`);
          break;
        }
      } catch (_) {}
    }

    if (!filledDates) {
      // Try filling any visible date inputs
      const dateInputs = await page.$$('input[type="text"]');
      console.log(`  [4] Found ${dateInputs.length} text inputs, trying to find date fields...`);
      for (const inp of dateInputs) {
        const placeholder = await inp.getAttribute('placeholder') || '';
        const id = await inp.getAttribute('id') || '';
        const name = await inp.getAttribute('name') || '';
        const isVisible = await inp.isVisible();
        if (isVisible && (placeholder.toLowerCase().includes('date') || id.toLowerCase().includes('date') || name.toLowerCase().includes('date'))) {
          console.log(`  [4] Found date-like input: id=${id} name=${name} placeholder=${placeholder}`);
        }
      }
    }

    // Step 5: Click submit
    console.log(`  [5] Clicking submit...`);
    let clicked = false;
    const submitSelectors = [
      '#btnRecordDateSearch',
      '#btnSubmit',
      'button:has-text("Submit")',
      'input[type="submit"]',
      'input[value="Submit"]',
      'button:has-text("Search")',
      'input[value="Search"]',
      'a:has-text("Submit")',
    ];

    for (const sel of submitSelectors) {
      try {
        const el = await page.$(sel);
        if (el && await el.isVisible()) {
          await el.click();
          clicked = true;
          console.log(`  [5] Clicked submit using: ${sel}`);
          break;
        }
      } catch (_) {}
    }

    if (!clicked) {
      console.log(`  [5] Could not find submit button`);
    }

    // Step 6: Wait 15 seconds for results
    console.log(`  [6] Waiting 15 seconds for results...`);
    await page.waitForTimeout(15000);

    // Step 7: Check for results
    console.log(`  [7] Checking for results...`);

    // Check page content again for captcha after interaction
    const postContent = await page.content();
    const postCaptcha = postContent.includes('captcha') ||
                        postContent.includes('CAPTCHA') ||
                        postContent.includes('recaptcha') ||
                        postContent.includes('hcaptcha') ||
                        postContent.includes('cf-challenge') ||
                        postContent.includes('challenge-platform');

    // Look for results table
    const resultSelectors = [
      '#resultsTable',
      '#searchResultsTable',
      'table.search-results',
      'table.display',
      '#GridView1',
      '.dataTables_wrapper',
      'table[id*="result"]',
      'table[id*="Result"]',
    ];

    let tableFound = false;
    let rowCount = 0;

    for (const sel of resultSelectors) {
      try {
        const table = await page.$(sel);
        if (table) {
          tableFound = true;
          // Count rows (skip header)
          const rows = await page.$$(`${sel} tbody tr`);
          rowCount = rows.length;
          console.log(`  [7] Found table "${sel}" with ${rowCount} rows`);
          break;
        }
      } catch (_) {}
    }

    if (!tableFound) {
      // Try any table with rows
      const allTables = await page.$$('table');
      console.log(`  [7] No result table found by ID. Total tables on page: ${allTables.length}`);
      for (let i = 0; i < allTables.length; i++) {
        const rows = await allTables[i].$$('tbody tr');
        if (rows.length > 0) {
          console.log(`  [7] Table[${i}] has ${rows.length} body rows`);
          if (rows.length > rowCount) {
            rowCount = rows.length;
            tableFound = true;
          }
        }
      }
    }

    // Determine final status
    if (postCaptcha && !tableFound) {
      result.status = 'CAPTCHA_BLOCKED';
      result.details = 'Captcha/challenge detected, no results returned';
    } else if (tableFound && rowCount > 0) {
      result.status = 'WORKING';
      result.details = `Found ${rowCount} result rows`;
    } else if (tableFound && rowCount === 0) {
      result.status = 'NO_RESULTS';
      result.details = 'Table found but no rows (may be no recordings for that date, or search failed)';
    } else if (filledDates && clicked) {
      result.status = 'NO_TABLE';
      result.details = 'Search submitted but no results table found';
    } else {
      result.status = 'SEARCH_FAILED';
      result.details = 'Could not complete the search workflow';
    }

    const currentUrl = page.url();
    const currentTitle = await page.title();
    console.log(`  Final URL: ${currentUrl}`);
    console.log(`  Final Title: "${currentTitle}"`);
    console.log(`  STATUS: ${result.status} — ${result.details}`);

  } catch (e) {
    result.status = 'ERROR';
    result.details = e.message.split('\n')[0];
    console.log(`  ERROR: ${result.details}`);
  } finally {
    if (browser) await browser.close();
  }

  return result;
}

async function main() {
  console.log('Florida Landmark Web County Portal Test');
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Testing ${counties.length} counties...\n`);

  for (const county of counties) {
    const result = await testCounty(county);
    results.push(result);
  }

  // Summary
  console.log(`\n\n${'='.repeat(60)}`);
  console.log('SUMMARY');
  console.log('='.repeat(60));

  const working = results.filter(r => r.status === 'WORKING');
  const captcha = results.filter(r => r.status === 'CAPTCHA_BLOCKED');
  const connFail = results.filter(r => r.status === 'CONNECTION_FAILED');
  const other = results.filter(r => !['WORKING', 'CAPTCHA_BLOCKED', 'CONNECTION_FAILED'].includes(r.status));

  console.log(`\nWORKING (${working.length}):`);
  for (const r of working) console.log(`  ✓ ${r.name}: ${r.details} — ${r.url}`);

  console.log(`\nCAPTCHA BLOCKED (${captcha.length}):`);
  for (const r of captcha) console.log(`  ✗ ${r.name}: ${r.details} — ${r.url}`);

  console.log(`\nCONNECTION FAILED (${connFail.length}):`);
  for (const r of connFail) console.log(`  ✗ ${r.name}: ${r.details} — ${r.url}`);

  console.log(`\nOTHER (${other.length}):`);
  for (const r of other) console.log(`  ? ${r.name}: [${r.status}] ${r.details} — ${r.url}`);

  console.log(`\n${'='.repeat(60)}`);
}

main().catch(console.error);
