#!/usr/bin/env tsx
/**
 * Probe the actual Levy clerk landmarkweb site to find what pages/URLs
 * the search actually lives on now. Dumps HTML snippets and link targets
 * so we can rebuild the scraper against the current UI.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/levy-debug";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36",
    viewport: { width: 1280, height: 900 },
  });
  const page = await ctx.newPage();

  console.log("[1] landing");
  await page.goto("https://online.levyclerk.com/landmarkweb", { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1500);

  // Try the disclaimer function if it exists
  const disclaimerResult = await page.evaluate(() => {
    try {
      if ((window as any).SetDisclaimer) {
        (window as any).SetDisclaimer();
        return "SetDisclaimer called";
      }
      return "SetDisclaimer not found";
    } catch (e: any) {
      return `error: ${e.message}`;
    }
  });
  console.log(`  disclaimer: ${disclaimerResult}`);
  await page.waitForTimeout(1500);

  console.log(`[2] current URL after disclaimer: ${page.url()}`);

  // Dump all visible links and buttons
  const navigationLinks = await page.evaluate(() => {
    const items: any[] = [];
    // Collect every <a> with an href
    document.querySelectorAll("a[href]").forEach((a: any) => {
      const text = (a.textContent || "").trim();
      if (!text || text.length > 80) return;
      items.push({
        tag: "a",
        text,
        href: a.href,
        visible: a.offsetHeight > 0,
      });
    });
    // Collect buttons
    document.querySelectorAll("button, [role='button']").forEach((b: any) => {
      const text = (b.textContent || "").trim();
      if (!text) return;
      items.push({
        tag: "button",
        text: text.slice(0, 60),
        id: b.id,
        visible: b.offsetHeight > 0,
      });
    });
    return items;
  });

  const searchRelated = navigationLinks.filter(
    (n) =>
      n.visible &&
      /search|instrument|document|record|party|name|grantor|grantee/i.test(n.text),
  );

  console.log(`\n[3] Search-related visible links/buttons (${searchRelated.length}):`);
  for (const n of searchRelated.slice(0, 30)) {
    console.log(`  ${n.tag}: "${n.text}" ${n.href || n.id || ""}`);
  }

  // Dump the title + top-level HTML structure
  const title = await page.title();
  const bodySnippet = await page.evaluate(() =>
    (document.body?.innerText || "").slice(0, 2000),
  );
  console.log(`\n[4] Page title: ${title}`);
  console.log(`[4] First 2000 chars of body innertext:\n${bodySnippet}\n`);

  // Save the full HTML for offline inspection
  const html = await page.content();
  writeFileSync(join(OUT, "landing-post-disclaimer.html"), html);
  console.log(`[5] Saved full HTML (${html.length} bytes) to landing-post-disclaimer.html`);

  // Take a big screenshot of the post-disclaimer page
  await page.screenshot({
    path: join(OUT, "POST-DISCLAIMER-FULL.png"),
    fullPage: true,
  });
  console.log("[6] Saved POST-DISCLAIMER-FULL.png");

  // Try clicking any link that looks like "search" and capture where it goes
  console.log("\n[7] Clicking first search-related link if found...");
  const searchLinks = navigationLinks.filter(
    (n) =>
      n.tag === "a" &&
      n.visible &&
      /search/i.test(n.text) &&
      n.href &&
      !n.href.includes("#"),
  );
  if (searchLinks.length > 0) {
    const target = searchLinks[0];
    console.log(`  clicking: "${target.text}" → ${target.href}`);
    try {
      await page.goto(target.href, { waitUntil: "domcontentloaded", timeout: 15_000 });
      await page.waitForTimeout(1500);
      console.log(`  landed at: ${page.url()}`);
      await page.screenshot({ path: join(OUT, "SEARCH-PAGE.png"), fullPage: true });

      // What inputs and buttons are on THIS page?
      const searchPageElements = await page.evaluate(() => {
        const inputs: any[] = [];
        document.querySelectorAll("input, select").forEach((el: any) => {
          if (el.offsetHeight > 0) {
            inputs.push({
              tag: el.tagName.toLowerCase(),
              type: el.type || "",
              id: el.id || "",
              name: el.name || "",
              placeholder: el.placeholder || "",
            });
          }
        });
        const btns: any[] = [];
        document
          .querySelectorAll("button, input[type='submit'], [role='button']")
          .forEach((el: any) => {
            if (el.offsetHeight > 0) {
              btns.push({
                tag: el.tagName.toLowerCase(),
                id: el.id || "",
                text: (el.textContent || el.value || "").trim().slice(0, 40),
              });
            }
          });
        return { inputs, btns };
      });
      console.log("\n[8] Inputs on search page:");
      console.log(JSON.stringify(searchPageElements.inputs, null, 2));
      console.log("\n[9] Buttons on search page:");
      console.log(JSON.stringify(searchPageElements.btns, null, 2));
    } catch (e: any) {
      console.log(`  nav failed: ${e.message}`);
    }
  } else {
    console.log("  no search link found");
  }

  await browser.close();
  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
