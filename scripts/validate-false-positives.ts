#!/usr/bin/env tsx
/**
 * Validate the census results by checking if platforms actually serve
 * county-specific content vs. returning generic pages for any URL.
 *
 * Tests: true_automation and cott_recordhub (the two with 99% match rates).
 */

const TIMEOUT_MS = 15000;

async function fetchBody(url: string): Promise<string | null> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
      signal: controller.signal,
      redirect: "follow",
    });
    clearTimeout(timer);
    if (!resp.ok) return null;
    return await resp.text();
  } catch {
    return null;
  }
}

async function main() {
  console.log("Validating false positive rates...\n");

  // ─── true_automation ───
  console.log("═══ TRUE AUTOMATION (propaccess.trueautomation.com) ═══\n");

  // Test a REAL county we know exists
  const realTA = await fetchBody("https://propaccess.trueautomation.com/clientdb/?cid=bexar");
  console.log(`Real county (Bexar): ${realTA ? `${realTA.length} chars` : "FAILED"}`);
  if (realTA) {
    const hasBexar = realTA.toLowerCase().includes("bexar");
    console.log(`  Contains "bexar": ${hasBexar}`);
    console.log(`  Title: ${realTA.match(/<title>(.*?)<\/title>/)?.[1]}`);
  }

  // Test a FAKE county that shouldn't exist
  const fakeTA = await fetchBody("https://propaccess.trueautomation.com/clientdb/?cid=zzzzfakecounty");
  console.log(`\nFake county (zzzzfakecounty): ${fakeTA ? `${fakeTA.length} chars` : "FAILED"}`);
  if (fakeTA) {
    const hasFake = fakeTA.toLowerCase().includes("zzzzfakecounty");
    console.log(`  Contains "zzzzfakecounty": ${hasFake}`);
    console.log(`  Title: ${fakeTA.match(/<title>(.*?)<\/title>/)?.[1]}`);
    // Check if it's an error/redirect page
    const isError = fakeTA.toLowerCase().includes("error") || fakeTA.toLowerCase().includes("not found") || fakeTA.toLowerCase().includes("invalid");
    console.log(`  Contains error text: ${isError}`);
  }

  // Check if real and fake return same content
  if (realTA && fakeTA) {
    console.log(`  Same content length: ${realTA.length === fakeTA.length}`);
    console.log(`  Same response: ${realTA === fakeTA}`);
  }

  // Test a non-TX county
  const nonTX = await fetchBody("https://propaccess.trueautomation.com/clientdb/?cid=losangeles");
  console.log(`\nNon-TX county (losangeles): ${nonTX ? `${nonTX.length} chars` : "FAILED"}`);
  if (nonTX) {
    console.log(`  Title: ${nonTX.match(/<title>(.*?)<\/title>/)?.[1]}`);
    console.log(`  Same as fake: ${nonTX === fakeTA}`);
  }

  // ─── cott_recordhub ───
  console.log("\n\n═══ COTT RECORDHUB (recordhub.cottsystems.com) ═══\n");

  const realCott = await fetchBody("https://recordhub.cottsystems.com/benton/");
  console.log(`Real county (benton): ${realCott ? `${realCott.length} chars` : "FAILED"}`);
  if (realCott) {
    const hasBenton = realCott.toLowerCase().includes("benton");
    console.log(`  Contains "benton": ${hasBenton}`);
    console.log(`  Title: ${realCott.match(/<title>(.*?)<\/title>/)?.[1]}`);
    console.log(`  URL in content: ${realCott.includes("/benton/")}`);
  }

  const fakeCott = await fetchBody("https://recordhub.cottsystems.com/zzzzfakecounty/");
  console.log(`\nFake county (zzzzfakecounty): ${fakeCott ? `${fakeCott.length} chars` : "FAILED"}`);
  if (fakeCott) {
    console.log(`  Title: ${fakeCott.match(/<title>(.*?)<\/title>/)?.[1]}`);
    console.log(`  Same as real: ${fakeCott === realCott}`);
  }

  // Test another real county
  const realCott2 = await fetchBody("https://recordhub.cottsystems.com/washington/");
  console.log(`\nAnother real? (washington): ${realCott2 ? `${realCott2.length} chars` : "FAILED"}`);
  if (realCott2) {
    console.log(`  Title: ${realCott2.match(/<title>(.*?)<\/title>/)?.[1]}`);
  }

  console.log("\n\nConclusion: If fake counties return the same response as real ones,");
  console.log("the platform probe is generating false positives and needs content validation.");
}

main().catch(console.error);
