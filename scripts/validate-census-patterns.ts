#!/usr/bin/env tsx
/**
 * Validate census URL patterns against known real counties.
 * Tests each platform probe with counties we KNOW use that platform
 * to confirm our URL construction is correct before running full census.
 */

const TIMEOUT_MS = 10000;

interface TestCase {
  platform: string;
  type: "assessor" | "recorder";
  county: string;
  state: string;
  urls: string[];
  known_good_url?: string; // URL we know works (for comparison)
}

// Test cases: counties we KNOW use specific platforms (verified by research)
const TEST_CASES: TestCase[] = [
  // ─── Assessor Platforms ───

  // Vision Government Solutions — hosts at gis.vgsi.com/[name]/
  {
    platform: "vgsi",
    type: "assessor",
    county: "Warren",
    state: "VA",
    urls: [
      "https://gis.vgsi.com/warrencountyva/",
      "https://gis.vgsi.com/warrenva/",
      "https://gis.vgsi.com/WarrenCountyVA/",
    ],
    known_good_url: "https://gis.vgsi.com/warrencountyva/",
  },
  {
    platform: "vgsi",
    type: "assessor",
    county: "Essex",
    state: "VA",
    urls: [
      "https://gis.vgsi.com/essexcountyva/",
      "https://gis.vgsi.com/essexva/",
      "https://gis.vgsi.com/EssexCountyVA/",
    ],
  },

  // Devnet wEdge — hosts at [county][state]-assessor.devnetwedge.com
  {
    platform: "devnet",
    type: "assessor",
    county: "Nye",
    state: "NV",
    urls: [
      "https://nyenv-assessor.devnetwedge.com/",
      "https://nyenv.devnetwedge.com/",
    ],
  },
  {
    platform: "devnet",
    type: "assessor",
    county: "Kane",
    state: "IL",
    urls: [
      "https://kaneil-assessor.devnetwedge.com/",
      "https://kaneil.devnetwedge.com/",
    ],
  },

  // qPublic / Schneider — use known-good county names
  {
    platform: "qpublic",
    type: "assessor",
    county: "Douglas",
    state: "GA",
    urls: [
      "https://qpublic.schneidercorp.com/Application.aspx?App=DouglasCountyGA&PageType=Search",
    ],
    known_good_url: "https://qpublic.schneidercorp.com/Application.aspx?App=DouglasCountyGA&PageType=Search",
  },
  {
    platform: "qpublic",
    type: "assessor",
    county: "Hall",
    state: "GA",
    urls: [
      "https://qpublic.schneidercorp.com/Application.aspx?App=HallCountyGA&PageType=Search",
    ],
  },

  // True Automation — propaccess.trueautomation.com
  {
    platform: "true_automation",
    type: "assessor",
    county: "Bexar",
    state: "TX",
    urls: [
      "https://propaccess.trueautomation.com/clientdb/?cid=bexar",
      "https://bexar.trueautomation.com/clientdb/propertysearch.aspx",
    ],
  },
  {
    platform: "true_automation",
    type: "assessor",
    county: "Travis",
    state: "TX",
    urls: [
      "https://propaccess.trueautomation.com/clientdb/?cid=travis",
      "https://travis.trueautomation.com/clientdb/propertysearch.aspx",
    ],
  },

  // TaxSifter / Aumentum
  {
    platform: "taxsifter",
    type: "assessor",
    county: "Okanogan",
    state: "WA",
    urls: [
      "https://okanoganwa-taxsifter.publicaccessnow.com/",
    ],
  },
  {
    platform: "taxsifter",
    type: "assessor",
    county: "Lincoln",
    state: "WA",
    urls: [
      "https://lincolnwa-taxsifter.publicaccessnow.com/",
    ],
  },

  // Tyler Web (self-service)
  {
    platform: "tyler_web",
    type: "assessor",
    county: "Williamson",
    state: "TX",
    urls: [
      "https://williamsoncountytx-web.tylerhost.net/web/",
    ],
    known_good_url: "https://williamsoncountytx-web.tylerhost.net/williamsonweb/",
  },
  {
    platform: "tyler_web",
    type: "assessor",
    county: "Ector",
    state: "TX",
    urls: [
      "https://ectorcountytx-web.tylerhost.net/web/",
    ],
  },
  {
    platform: "tyler_web",
    type: "assessor",
    county: "Hanover",
    state: "VA",
    urls: [
      "https://hanovercountyva-web.tylerhost.net/web/",
    ],
  },
  // Tyler Assessor variant
  {
    platform: "tyler_assessor",
    type: "assessor",
    county: "Roosevelt",
    state: "NM",
    urls: [
      "https://rooseveltcountynm-assessor.tylerhost.net/assessor/web/",
    ],
  },

  // Patriot Properties
  {
    platform: "patriot",
    type: "assessor",
    county: "Beverly",
    state: "MA",
    urls: [
      "https://beverly.patriotproperties.com/default.asp",
    ],
    known_good_url: "https://beverly.patriotproperties.com/default.asp",
  },

  // actDataScout
  {
    platform: "actdatascout",
    type: "assessor",
    county: "Comanche",
    state: "OK",
    urls: [
      "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
    ],
    known_good_url: "https://www.actdatascout.com/RealProperty/Oklahoma/Comanche",
  },

  // ISW Data — fixed: use webSearchName.aspx with lowercase dbkey
  {
    platform: "isw",
    type: "assessor",
    county: "Hopkins",
    state: "TX",
    urls: [
      "https://iswdataclient.azurewebsites.net/webSearchName.aspx?dbkey=hopkinscad",
    ],
  },
  {
    platform: "isw",
    type: "assessor",
    county: "Wise",
    state: "TX",
    urls: [
      "https://iswdataclient.azurewebsites.net/webSearchName.aspx?dbkey=wisecad",
    ],
  },

  // ─── Recorder Platforms ───

  // Fidlar AVA
  {
    platform: "fidlar_ava",
    type: "recorder",
    county: "Wyandot",
    state: "OH",
    urls: [
      "https://ava.fidlar.com/OHWyandot/AvaWeb/",
    ],
  },
  {
    platform: "fidlar_ava",
    type: "recorder",
    county: "Fairfield",
    state: "OH",
    urls: [
      "https://ava.fidlar.com/OHFairfield/AvaWeb/",
    ],
  },
  {
    platform: "fidlar_ava",
    type: "recorder",
    county: "Fannin",
    state: "TX",
    urls: [
      "https://ava.fidlar.com/TXFannin/AvaWeb/",
    ],
  },

  // Tyler EagleWeb
  {
    platform: "tyler_eagleweb",
    type: "recorder",
    county: "Elbert",
    state: "CO",
    urls: [
      "https://elbertcountyco-recorder.tylerhost.net/recorder/eagleweb/docSearch.jsp",
      "https://elbertco-recorder.tylerhost.net/recorder/eagleweb/docSearch.jsp",
    ],
  },
  {
    platform: "tyler_eagleweb",
    type: "recorder",
    county: "Yavapai",
    state: "AZ",
    urls: [
      "https://yavapaicountyaz-recorder.tylerhost.net/recorder/eagleweb/docSearch.jsp",
      "https://yavapaiaz-recorder.tylerhost.net/recorder/eagleweb/docSearch.jsp",
    ],
  },

  // Cott RECORDhub
  {
    platform: "cott_recordhub",
    type: "recorder",
    county: "Benton",
    state: "AR",
    urls: [
      "https://recordhub.cottsystems.com/benton/",
      "https://recordhub.cottsystems.com/BentonAR/",
    ],
  },
];

// ─── Run Tests ─────────────────────────────────────────────────────

async function testUrl(url: string): Promise<{ status: number | "error" | "timeout"; finalUrl?: string; redirected?: boolean }> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

    const resp = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      },
    });
    clearTimeout(timer);

    return {
      status: resp.status,
      finalUrl: resp.url !== url ? resp.url : undefined,
      redirected: resp.redirected,
    };
  } catch (err: any) {
    if (err.name === "AbortError") return { status: "timeout" };
    return { status: "error" };
  }
}

async function main() {
  console.log("═══════════════════════════════════════════════════════");
  console.log("  URL PATTERN VALIDATION");
  console.log("═══════════════════════════════════════════════════════\n");

  const platformResults = new Map<string, { success: number; fail: number; patterns: string[] }>();

  for (const test of TEST_CASES) {
    console.log(`\n[${test.platform}] ${test.county}, ${test.state} (${test.type})`);

    let anySuccess = false;
    for (const url of test.urls) {
      const result = await testUrl(url);
      const icon = result.status === 200 ? "✓" : result.status === 301 || result.status === 302 ? "→" : "✗";
      const statusStr = typeof result.status === "number" ? String(result.status) : result.status;

      console.log(`  ${icon} [${statusStr}] ${url}`);
      if (result.finalUrl) {
        console.log(`    → Redirected to: ${result.finalUrl}`);
      }

      if (result.status === 200 || (typeof result.status === "number" && result.status >= 200 && result.status < 400)) {
        anySuccess = true;
        // Track which URL pattern worked
        const stats = platformResults.get(test.platform) || { success: 0, fail: 0, patterns: [] };
        stats.success++;
        stats.patterns.push(url);
        platformResults.set(test.platform, stats);
        break; // First success is enough
      }
    }

    if (!anySuccess) {
      const stats = platformResults.get(test.platform) || { success: 0, fail: 0, patterns: [] };
      stats.fail++;
      platformResults.set(test.platform, stats);
    }
  }

  // Summary
  console.log("\n\n═══════════════════════════════════════════════════════");
  console.log("  VALIDATION SUMMARY");
  console.log("═══════════════════════════════════════════════════════\n");

  for (const [platform, stats] of [...platformResults.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const total = stats.success + stats.fail;
    const icon = stats.fail === 0 ? "✓" : stats.success > 0 ? "~" : "✗";
    console.log(`  ${icon} ${platform.padEnd(25)} ${stats.success}/${total} test cases passed`);
    if (stats.patterns.length > 0) {
      console.log(`    Working pattern: ${stats.patterns[0]}`);
    }
  }

  const totalSuccess = [...platformResults.values()].reduce((s, v) => s + v.success, 0);
  const totalTests = [...platformResults.values()].reduce((s, v) => s + v.success + v.fail, 0);
  console.log(`\n  Overall: ${totalSuccess}/${totalTests} test cases passed`);
}

main().catch(console.error);
