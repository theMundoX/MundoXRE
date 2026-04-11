#!/usr/bin/env tsx
/**
 * Test Dallas publicsearch.us — find the actual API and pull mortgage records.
 * The site embeds __data into the HTML. The /results page likely has the data inline.
 */

const BASE = "https://dallas.tx.publicsearch.us";

const headers: Record<string, string> = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml",
  "Referer": BASE + "/",
};

async function main() {
  // The results page embeds data in window.__data — fetch it and extract
  const url = `${BASE}/results?department=RP&recordedDateRange=2026-03-20to2026-03-25&docType=DEED+OF+TRUST`;
  console.log(`Fetching: ${url}\n`);

  const resp = await fetch(url, { headers });
  const html = await resp.text();

  // Extract window.__data
  const match = html.match(/window\.__data\s*=\s*({.*?})\s*<\/script>/s);
  if (!match) {
    console.log("No __data found. Trying to find data in HTML...");
    // Try looking for JSON in the page
    const jsonMatch = html.match(/"results"\s*:\s*(\[.*?\])/s);
    if (jsonMatch) {
      console.log("Found results array!");
      const results = JSON.parse(jsonMatch[1]);
      console.log(`Count: ${results.length}`);
      if (results.length > 0) {
        console.log("First result:", JSON.stringify(results[0], null, 2));
      }
    }

    // Also try extracting the full __data more aggressively
    const dataStart = html.indexOf('window.__data=');
    if (dataStart !== -1) {
      // Find the matching closing brace
      const jsonStart = html.indexOf('{', dataStart);
      let depth = 0;
      let jsonEnd = jsonStart;
      for (let i = jsonStart; i < html.length; i++) {
        if (html[i] === '{') depth++;
        if (html[i] === '}') depth--;
        if (depth === 0) { jsonEnd = i + 1; break; }
      }
      const jsonStr = html.slice(jsonStart, jsonEnd);
      try {
        const data = JSON.parse(jsonStr);
        const keys = Object.keys(data);
        console.log(`\n__data top-level keys: ${keys.join(", ")}`);

        // Explore the structure to find search results
        for (const key of keys) {
          const val = data[key];
          if (val && typeof val === "object") {
            const subkeys = Object.keys(val);
            console.log(`  ${key}: { ${subkeys.slice(0, 10).join(", ")} }`);
            if (subkeys.includes("results") || subkeys.includes("documents") || subkeys.includes("records")) {
              console.log(`\n  *** Found data under "${key}" ***`);
              const results = val.results || val.documents || val.records;
              if (Array.isArray(results)) {
                console.log(`  Count: ${results.length}`);
                if (results.length > 0) {
                  console.log(`  First record:`, JSON.stringify(results[0], null, 2).slice(0, 1000));
                }
                // Print a few records
                for (const r of results.slice(0, 3)) {
                  console.log(`\n  ---`);
                  console.log(`  Type: ${r.docType || r.type || r.documentType || "?"}`);
                  console.log(`  Date: ${r.recordedDate || r.date || r.recordingDate || "?"}`);
                  console.log(`  Grantor: ${r.grantor || r.grantors || "?"}`);
                  console.log(`  Grantee: ${r.grantee || r.grantees || "?"}`);
                  console.log(`  Amount: ${r.consideration || r.amount || r.loanAmount || "?"}`);
                  console.log(`  Instrument: ${r.instrumentNumber || r.instrument || r.docNumber || "?"}`);
                }
              }
            }
          }
        }
      } catch (e: any) {
        console.log(`JSON parse error: ${e.message}`);
        console.log(`JSON preview: ${jsonStr.slice(0, 500)}`);
      }
    }
    return;
  }

  const data = JSON.parse(match[1]);
  console.log("Top-level keys:", Object.keys(data).join(", "));
}

main().catch(console.error);
