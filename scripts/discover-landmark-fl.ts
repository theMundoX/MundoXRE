#!/usr/bin/env tsx
/**
 * Discover all Florida counties using LandmarkWeb for official records.
 * Tests common URL patterns for each county.
 */
import "dotenv/config";

const FL_COUNTIES = [
  "Alachua","Baker","Bay","Bradford","Brevard","Broward","Calhoun","Charlotte",
  "Citrus","Clay","Collier","Columbia","DeSoto","Dixie","Duval","Escambia",
  "Flagler","Franklin","Gadsden","Gilchrist","Glades","Gulf","Hamilton","Hardee",
  "Hendry","Hernando","Highlands","Hillsborough","Holmes","Indian River","Jackson",
  "Jefferson","Lafayette","Lake","Lee","Leon","Levy","Liberty","Madison","Manatee",
  "Marion","Martin","Miami-Dade","Monroe","Nassau","Okaloosa","Okeechobee","Orange",
  "Osceola","Palm Beach","Pasco","Pinellas","Polk","Putnam","Santa Rosa","Sarasota",
  "Seminole","St. Johns","St. Lucie","Sumter","Suwannee","Taylor","Union","Volusia",
  "Wakulla","Walton","Washington"
];

// Common URL patterns for FL clerk LandmarkWeb portals
function generatePatterns(county: string): string[] {
  const clean = county.replace(/[.\s-]/g, "").toLowerCase();
  const hyphen = county.replace(/\s+/g, "-").toLowerCase();
  const dot = county.replace(/\s+/g, ".").toLowerCase();

  return [
    `https://officialrecords.${clean}clerk.com/LandmarkWeb`,
    `https://or.${clean}clerk.com/LandmarkWeb`,
    `https://or.${clean}clerk.org/LandmarkWeb`,
    `https://orsearch.${clean}clerk.com/LandmarkWeb`,
    `https://online.${clean}clerk.com/landmarkweb`,
    `https://search.${clean}clerk.com/LandmarkWeb`,
    `https://search.${clean}clerk.org/LandmarkWeb`,
    `https://landmark.${clean}clerk.com/LandmarkWeb`,
    `https://landmark.${clean}clerk.com/landmarkweb`,
    `https://records.${clean}clerk.com/LandmarkWeb`,
    `https://${clean}clerk.com/LandmarkWeb`,
    `https://www.${clean}clerk.com/LandmarkWeb`,
    `https://or.${clean}county.org/LandmarkWeb`,
    `https://officialrecords.${clean}county.org/LandmarkWeb`,
    `https://orsearch.clerkofcourts.co.${clean}.fl.us/LandmarkWeb`,
    // Specific known patterns
    `https://oncore-${clean}.miamidade.gov/LandmarkWeb`,
  ];
}

async function testUrl(url: string): Promise<{ works: boolean; redirectUrl?: string }> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

    const resp = await fetch(url, {
      method: "HEAD",
      signal: controller.signal,
      redirect: "follow",
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" },
    });
    clearTimeout(timeout);

    if (resp.ok || resp.status === 302 || resp.status === 301) {
      // Check if the final URL still contains Landmark
      const finalUrl = resp.url;
      if (finalUrl.toLowerCase().includes("landmark") || resp.ok) {
        return { works: true, redirectUrl: finalUrl !== url ? finalUrl : undefined };
      }
    }
    return { works: false };
  } catch {
    return { works: false };
  }
}

async function main() {
  console.log("MXRE — Discover Florida LandmarkWeb Portals\n");

  const discovered: Array<{ county: string; url: string; redirect?: string }> = [];
  const CONCURRENCY = 10;

  for (let i = 0; i < FL_COUNTIES.length; i += CONCURRENCY) {
    const batch = FL_COUNTIES.slice(i, i + CONCURRENCY);

    await Promise.all(batch.map(async (county) => {
      const patterns = generatePatterns(county);

      for (const url of patterns) {
        const result = await testUrl(url);
        if (result.works) {
          console.log(`  ✓ ${county}: ${url}`);
          discovered.push({ county, url, redirect: result.redirectUrl });
          return; // Found one, stop checking patterns for this county
        }
      }
      console.log(`  ✗ ${county}: no LandmarkWeb found`);
    }));
  }

  console.log(`\n═══════════════════════════════════`);
  console.log(`Discovered ${discovered.length} / ${FL_COUNTIES.length} FL counties with LandmarkWeb`);
  console.log(`═══════════════════════════════════\n`);

  for (const d of discovered) {
    console.log(`  { county_name: "${d.county}", state: "FL", base_url: "${d.url.replace('/LandmarkWeb', '').replace('/landmarkweb', '')}", path_prefix: "/LandmarkWeb", county_id: 0 },`);
  }

  // Save to file
  const { writeFileSync } = await import("node:fs");
  writeFileSync("data/landmark-fl-discovered.json", JSON.stringify(discovered, null, 2));
  console.log("\nSaved to data/landmark-fl-discovered.json");
}

main().catch(console.error);
