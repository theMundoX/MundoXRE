#!/usr/bin/env tsx
/**
 * Simple lien lookup — uses the CJS pattern that's proven to work.
 * No stealth config, no complex error handling. Just fill and search.
 */
import "dotenv/config";
import { chromium } from "playwright";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

const args = process.argv.slice(2);
const limit = parseInt(args.find(a => a.startsWith("--limit="))?.split("=")[1] || "10");
const dryRun = args.includes("--dry-run");

async function main() {
  console.log(`Lien Lookup — Fairfield OH | limit=${limit} | dry=${dryRun}\n`);

  // Get properties needing verification
  const { data: props } = await db.from("properties")
    .select("id, parcel_id, address, owner_name, legal_description, subdivision, lot_number")
    .eq("county_id", 31) // Fairfield OH
    .is("lien_status", null)
    .not("owner_name", "is", null)
    .neq("owner_name", "")
    .order("id")
    .limit(limit);

  if (!props?.length) { console.log("No properties to search."); return; }
  console.log(`${props.length} properties to search.\n`);

  // Launch browser (NO stealth — just plain Chromium like the working test)
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(5000);

  let hasLiens = 0, freeClear = 0, errors = 0, docsLinked = 0;

  for (let i = 0; i < props.length; i++) {
    const prop = props[i];
    const ownerParts = prop.owner_name.trim().split(/\s+/);
    const lastName = ownerParts[0] || "";
    const firstName = ownerParts[1] || "";

    process.stdout.write(`[${i + 1}/${props.length}] ${lastName} ${firstName} (${prop.address || "no addr"})... `);

    try {
      // Fill last name
      const lastInput = page.locator('input[placeholder="Last Name / Business Name"]');
      await lastInput.click({ timeout: 5000 });
      await lastInput.fill(lastName);
      await page.keyboard.press("Tab");

      // Fill first name
      const firstInput = page.locator('input[placeholder="First Name"]');
      await firstInput.click({ timeout: 5000 });
      await firstInput.fill(firstName);
      await page.keyboard.press("Tab");

      // Intercept response
      const respPromise = new Promise<any>((resolve) => {
        const handler = async (resp: any) => {
          if (resp.url().includes("breeze/Search") && resp.request().method() === "POST") {
            try { resolve(await resp.json()); } catch { resolve(null); }
            page.off("response", handler);
          }
        };
        page.on("response", handler);
        setTimeout(() => { page.off("response", handler); resolve(null); }, 15000);
      });

      // Click search
      await page.locator('button:has-text("Search")').first().click();
      await page.waitForTimeout(3000);

      const data = await respPromise;

      if (data && data.TotalResults > 0) {
        // Too many results = ambiguous name (GREEN, SMITH, etc.) — skip
        if (data.TotalResults > 50) {
          console.log(`${data.TotalResults} docs — TOO MANY (ambiguous name, skipped)`);
          // Re-navigate for clean state
          await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
          await page.waitForTimeout(3000);
          continue;
        }

        // Check for mortgage docs
        const mortgageDocs = (data.DocResults || []).filter((d: any) =>
          /mortgage|lien|deed of trust|judgment/i.test(d.DocumentType) &&
          !/release|satisfaction|assignment/i.test(d.DocumentType)
        );

        if (mortgageDocs.length > 0) {
          console.log(`${data.TotalResults} docs, ${mortgageDocs.length} mortgages`);

          if (!dryRun) {
            // Upsert mortgage records
            for (const doc of mortgageDocs) {
              const grantors = (doc.Names || []).filter((n: any) => n.Type === "Grantor").map((n: any) => n.Name);
              const grantees = (doc.Names || []).filter((n: any) => n.Type === "Grantee").map((n: any) => n.Name);
              const dateMatch = doc.RecordedDateTime?.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
              const recDate = dateMatch ? `${dateMatch[3]}-${dateMatch[1].padStart(2, "0")}-${dateMatch[2].padStart(2, "0")}` : null;

              await db.from("mortgage_records").upsert({
                property_id: prop.id,
                document_type: doc.DocumentType.toLowerCase().includes("mortgage") ? "mortgage" : "lien",
                recording_date: recDate,
                loan_amount: doc.ConsiderationAmount > 0 ? Math.round(doc.ConsiderationAmount) : null,
                lender_name: grantees.join("; ").toUpperCase() || null,
                borrower_name: grantors.join("; ").toUpperCase() || null,
                document_number: doc.DocumentName || null,
                source_url: "https://ava.fidlar.com/OHFairfield/AvaWeb/",
              }, { onConflict: "document_number,source_url", ignoreDuplicates: true });
              docsLinked++;
            }
            await db.from("properties").update({ lien_status: "has_liens" }).eq("id", prop.id);
          }
          hasLiens++;
        } else {
          console.log(`${data.TotalResults} docs but no mortgages → free_clear`);
          if (!dryRun) await db.from("properties").update({ lien_status: "free_clear" }).eq("id", prop.id);
          freeClear++;
        }
      } else {
        console.log("0 results → free_clear");
        if (!dryRun) await db.from("properties").update({ lien_status: "free_clear" }).eq("id", prop.id);
        freeClear++;
      }

      // Re-navigate for clean state (Reset button is unreliable after search)
      await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
      await page.waitForTimeout(3000);

    } catch (e: any) {
      console.log(`ERROR: ${e.message?.substring(0, 80)}`);
      errors++;
      // Re-navigate on error
      try {
        await page.goto("https://ava.fidlar.com/OHFairfield/AvaWeb/", { waitUntil: "networkidle", timeout: 30000 });
        await page.waitForTimeout(3000);
      } catch { /* ignore */ }
    }
  }

  await browser.close();
  console.log(`\n=== Results ===`);
  console.log(`Has liens: ${hasLiens} | Free/clear: ${freeClear} | Errors: ${errors} | Docs linked: ${docsLinked}`);
}

main().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
