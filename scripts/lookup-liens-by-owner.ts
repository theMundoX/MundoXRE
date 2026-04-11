#!/usr/bin/env tsx
/**
 * Lookup liens by owner name via Fidlar AVA recorder.
 *
 * For each property where lien_status IS NULL, searches the Fidlar AVA
 * Angular SPA by owner name, matches results by legal description, and
 * links found documents to the property as mortgage_records.
 *
 * Uses Playwright to interact with the Fidlar UI:
 *   1. Fill "Last Name / Business Name" + "First Name"
 *   2. Click Search, intercept breeze/Search JSON response
 *   3. Match legal description to property's legal_description
 *   4. Upsert matched documents to mortgage_records
 *   5. Update property.lien_status to 'has_liens' or 'free_clear'
 *
 * Usage:
 *   npx tsx scripts/lookup-liens-by-owner.ts --county Fairfield --state OH
 *   npx tsx scripts/lookup-liens-by-owner.ts --county Fairfield --state OH --limit 50
 *   npx tsx scripts/lookup-liens-by-owner.ts --county Fairfield --state OH --dry-run
 *   npx tsx scripts/lookup-liens-by-owner.ts --county Fairfield --state OH --offset 200 --limit 100
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium, type Browser, type BrowserContext, type Page } from "playwright";
import { getStealthConfig, STEALTH_INIT_SCRIPT } from "../src/utils/stealth.js";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";
import { FIDLAR_AVA_COUNTIES, type FidlarCountyConfig } from "../src/discovery/adapters/fidlar-ava.js";

// ─── CLI Args ─────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  const flags: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
      flags[args[i].replace("--", "")] = args[i + 1];
      i++;
    } else if (args[i].startsWith("--")) {
      flags[args[i].replace("--", "")] = "true";
    }
  }
  return {
    county: flags.county || "Fairfield",
    state: flags.state || "OH",
    limit: parseInt(flags.limit || "0", 10) || 0,
    offset: parseInt(flags.offset || "0", 10) || 0,
    dryRun: flags["dry-run"] === "true",
  };
}

// ─── DB ───────────────────────────────────────────────────────────────

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

// ─── Name Parsing ─────────────────────────────────────────────────────

const CORPORATE_KEYWORDS = [
  "LLC", "INC", "CORP", "CORPORATION", "LP", "LTD", "LIMITED",
  "TRUST", "ESTATE", "BANK", "INVESTMENTS", "HOLDINGS", "PROPERTIES",
  "REALTY", "GROUP", "FUND", "CAPITAL", "ASSOCIATES", "PARTNERS",
  "COMPANY", "CO", "ENTERPRISES", "MANAGEMENT", "DEVELOPMENT",
  "VENTURES", "SERVICES", "FINANCIAL", "NATIONAL", "FEDERAL",
];

interface ParsedName {
  lastName: string;
  firstName: string;
  isCorporate: boolean;
}

function parseOwnerName(raw: string): ParsedName {
  if (!raw || !raw.trim()) return { lastName: "", firstName: "", isCorporate: false };

  const cleaned = raw.trim().replace(/\s+/g, " ");

  // Check if corporate/trust
  const upper = cleaned.toUpperCase();
  const isCorporate = CORPORATE_KEYWORDS.some(kw => {
    const regex = new RegExp(`\\b${kw}\\b`);
    return regex.test(upper);
  });

  if (isCorporate) {
    return { lastName: cleaned, firstName: "", isCorporate: true };
  }

  // Strip multi-owner: "SMITH JOHN A & JANE B" → "SMITH JOHN A"
  const ampersandIdx = cleaned.indexOf("&");
  const singleOwner = ampersandIdx > 0 ? cleaned.substring(0, ampersandIdx).trim() : cleaned;

  // Also handle "SMITH JOHN A/JANE B" pattern
  const slashIdx = singleOwner.indexOf("/");
  const finalOwner = slashIdx > 0 ? singleOwner.substring(0, slashIdx).trim() : singleOwner;

  // Standard assessor format: "LASTNAME FIRSTNAME MIDDLE"
  const parts = finalOwner.split(/\s+/);
  if (parts.length === 0) return { lastName: "", firstName: "", isCorporate: false };
  if (parts.length === 1) return { lastName: parts[0], firstName: "", isCorporate: false };

  return {
    lastName: parts[0],
    firstName: parts[1],
    isCorporate: false,
  };
}

// ─── Legal Description Matching ───────────────────────────────────────

/**
 * Check if a Fidlar legal description matches a property's legal description.
 *
 * Property legal_description examples (from shapefile):
 *   "LOT 9 THE LANDINGS PH 1"
 *   "LOT 2 CRESTWOOD ADDITION"
 *
 * Fidlar legal examples:
 *   "LANDINGS L: 9"
 *   "CRESTWOOD ADDN L: 2 S: 16 T: 15 R: 18"
 *
 * Strategy: extract subdivision keywords from both and compare.
 */
function legalsMatch(propertyLegal: string | null, fidlarLegal: string): boolean {
  if (!propertyLegal) return false;

  const pLegal = propertyLegal.toUpperCase().trim();
  const fLegal = fidlarLegal.toUpperCase().trim();

  // Extract lot numbers from both
  const pLotMatch = pLegal.match(/\bLOT\s+(\d+)/);
  const fLotMatch = fLegal.match(/\bL:\s*(\d+)/);
  const pLot = pLotMatch?.[1];
  const fLot = fLotMatch?.[1];

  // Extract words that might be subdivision names (strip common words)
  const STOP_WORDS = new Set([
    "LOT", "LOTS", "L:", "S:", "T:", "R:", "BLK", "BLOCK", "PH", "PHASE",
    "SECTION", "UNIT", "ADDN", "ADDITION", "SUBD", "SUBDIVISION",
    "PT", "PART", "OF", "THE", "IN", "A", "AN", "NO", "N", "S", "E", "W",
    "NE", "NW", "SE", "SW", "QTR", "QUARTER", "HALF",
  ]);

  function extractKeywords(text: string): string[] {
    return text
      .replace(/[^A-Z0-9\s]/g, " ")
      .split(/\s+/)
      .filter(w => w.length > 2 && !STOP_WORDS.has(w) && !/^\d+$/.test(w));
  }

  const pKeywords = extractKeywords(pLegal);
  const fKeywords = extractKeywords(fLegal);

  // If both have lot numbers and they differ, no match
  if (pLot && fLot && pLot !== fLot) return false;

  // Look for subdivision name overlap
  let subdivisionMatch = false;
  for (const pk of pKeywords) {
    for (const fk of fKeywords) {
      // Substring match: "LANDINGS" matches "LANDINGS"
      if (pk === fk || pk.includes(fk) || fk.includes(pk)) {
        subdivisionMatch = true;
        break;
      }
    }
    if (subdivisionMatch) break;
  }

  // Need at least subdivision name match. Lot match is bonus confirmation.
  if (!subdivisionMatch) return false;

  // If we have both lots and they match, strong match
  if (pLot && fLot && pLot === fLot) return true;

  // Subdivision matches but no lot info to confirm — accept if property
  // lot is in fidlar or vice versa
  if (pLot && fLegal.includes(pLot)) return true;
  if (fLot && pLegal.includes(fLot)) return true;

  // Subdivision match alone is enough
  return true;
}

// ─── Document Type Classification ─────────────────────────────────────

function classifyDocType(rawType: string): { document_type: string; loan_type?: string; deed_type?: string } {
  const upper = rawType.toUpperCase();
  if (upper.includes("MORTGAGE") && !upper.includes("SATISFACTION") && !upper.includes("RELEASE") && !upper.includes("ASSIGNMENT")) {
    return { document_type: "mortgage", loan_type: upper.includes("MODIFICATION") ? "refinance" : "purchase" };
  }
  if (upper.includes("SATISFACTION") || upper.includes("RELEASE")) return { document_type: "satisfaction" };
  if (upper.includes("ASSIGNMENT")) return { document_type: "assignment" };
  if (upper.includes("WARRANTY DEED") || upper === "WD") return { document_type: "deed", deed_type: "warranty" };
  if (upper.includes("QUIT CLAIM") || upper === "QCD") return { document_type: "deed", deed_type: "quitclaim" };
  if (upper.includes("DEED")) return { document_type: "deed" };
  if (upper.includes("LIEN") || upper.includes("JUDGMENT") || upper.includes("JUDGEMENT")) return { document_type: "lien" };
  return { document_type: rawType.toLowerCase().trim() };
}

// ─── Fidlar AVA Search Response Types ─────────────────────────────────

interface AvaDocResult {
  Id: number;
  DocumentType: string;
  RecordedDateTime: string;
  DocumentName: string;
  ConsiderationAmount: number;
  Book: string;
  Page?: string;
  DocumentDate: string;
  Legals: Array<{ Id: number; LegalType: string; Description: string; Notes: string | null }>;
  LegalSummary: string;
  Names: Array<{ Name: string; Type: string }>;
}

interface AvaSearchResponse {
  ResultAccessCode: string;
  ResultId: number;
  TotalResults: number;
  ViewableResults: number;
  DocResults: AvaDocResult[];
}

// ─── Browser Automation ───────────────────────────────────────────────

interface SearchResult {
  documents: AvaDocResult[];
  totalResults: number;
  error?: string;
}

async function setupBrowser(): Promise<{ browser: Browser; context: BrowserContext; page: Page }> {
  const stealth = getStealthConfig();
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const context = await browser.newContext(stealth);
  await context.addInitScript(STEALTH_INIT_SCRIPT);
  const page = await context.newPage();
  return { browser, context, page };
}

async function navigateToSearch(page: Page, baseUrl: string): Promise<void> {
  await page.goto(baseUrl, { waitUntil: "networkidle", timeout: 30000 });
  await page.waitForTimeout(5000);
  // Wait for the search form to be ready
  await page.waitForSelector('input[placeholder="Last Name / Business Name"]', { timeout: 15000 });
}

async function searchByName(
  page: Page,
  lastName: string,
  firstName: string,
): Promise<SearchResult> {
  // Set up response interception BEFORE clicking search
  let searchResponseData: AvaSearchResponse | null = null;
  let responseError: string | undefined;

  const responsePromise = new Promise<void>((resolve) => {
    const handler = async (resp: import("playwright").Response) => {
      if (resp.url().includes("breeze/Search") && resp.request().method() === "POST") {
        try {
          const data = await resp.json();
          searchResponseData = data as AvaSearchResponse;
        } catch (e: any) {
          responseError = e.message;
        }
        page.off("response", handler);
        resolve();
      }
    };
    page.on("response", handler);

    // Timeout after 15s
    setTimeout(() => {
      page.off("response", handler);
      if (!searchResponseData) responseError = "Search response timeout";
      resolve();
    }, 15000);
  });

  // Click Reset to clear any previous search
  try {
    const resetBtn = page.locator('button.red:has-text("Reset")');
    if (await resetBtn.count() > 0) {
      await resetBtn.first().click();
      await page.waitForTimeout(2000);
      // Wait for form to be ready again after reset
      await page.waitForSelector('input[placeholder="Last Name / Business Name"]', { timeout: 10000 });
    }
  } catch {
    // Reset button may not exist on first search
  }

  // Fill the name fields
  const lastNameInput = page.locator('input[placeholder="Last Name / Business Name"]').first();
  const firstNameInput = page.locator('input[placeholder="First Name"]').first();

  await lastNameInput.click();
  await lastNameInput.fill(lastName);

  if (firstName) {
    await firstNameInput.click();
    await firstNameInput.fill(firstName);
  }

  // Click Search
  const searchBtn = page.locator('button:has-text("Search")');
  await searchBtn.first().click();

  // Wait for the response
  await responsePromise;

  if (responseError) {
    return { documents: [], totalResults: 0, error: responseError };
  }

  if (!searchResponseData) {
    return { documents: [], totalResults: 0, error: "No response received" };
  }

  return {
    documents: searchResponseData.DocResults || [],
    totalResults: searchResponseData.TotalResults || 0,
  };
}

// ─── Main ─────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs();
  console.log(`MXRE — Lookup Liens by Owner Name via Fidlar AVA`);
  console.log(`  County: ${args.county}, ${args.state}`);
  console.log(`  Limit: ${args.limit || "all"} | Offset: ${args.offset} | Dry run: ${args.dryRun}\n`);

  // Find the Fidlar config for this county
  const fidlarConfig = FIDLAR_AVA_COUNTIES.find(
    c => c.county_name.toLowerCase() === args.county.toLowerCase() && c.state === args.state,
  );
  if (!fidlarConfig) {
    console.error(`No Fidlar AVA config found for ${args.county}, ${args.state}`);
    console.error("Available counties:", FIDLAR_AVA_COUNTIES.map(c => `${c.county_name}, ${c.state}`).join("; "));
    process.exit(1);
  }

  // Get county ID from DB
  const { data: county } = await db.from("counties")
    .select("id")
    .eq("county_name", args.county)
    .eq("state_code", args.state)
    .single();

  if (!county) {
    console.error(`County not found in DB: ${args.county}, ${args.state}`);
    process.exit(1);
  }
  console.log(`  County ID: ${county.id}`);

  // Query properties where lien_status IS NULL
  let query = db.from("properties")
    .select("id, owner_name, legal_description, parcel_id, address")
    .eq("county_id", county.id)
    .is("lien_status", null)
    .not("owner_name", "is", null)
    .neq("owner_name", "")
    .order("id", { ascending: true });

  if (args.offset > 0) {
    query = query.range(args.offset, args.offset + (args.limit || 1000) - 1);
  } else if (args.limit > 0) {
    query = query.limit(args.limit);
  } else {
    query = query.limit(10000);
  }

  const { data: properties, error: propError } = await query;
  if (propError) {
    console.error("Error fetching properties:", propError.message);
    process.exit(1);
  }
  if (!properties || properties.length === 0) {
    console.log("No properties with null lien_status found.");
    return;
  }
  console.log(`  Found ${properties.length} properties to search\n`);

  if (args.dryRun) {
    console.log("DRY RUN — showing first 10 parsed names:");
    for (const prop of properties.slice(0, 10)) {
      const parsed = parseOwnerName(prop.owner_name);
      console.log(`  ${prop.owner_name} → last="${parsed.lastName}" first="${parsed.firstName}" corp=${parsed.isCorporate}`);
    }
    return;
  }

  // Launch browser
  console.log("Launching browser...");
  const { browser, page } = await setupBrowser();

  try {
    // Navigate to the Fidlar AVA search page
    console.log(`Navigating to ${fidlarConfig.base_url}...`);
    await navigateToSearch(page, fidlarConfig.base_url);
    console.log("Search form ready.\n");

    // Stats
    let searched = 0;
    let hasLiens = 0;
    let freeClear = 0;
    let skippedAmbiguous = 0;
    let errors = 0;
    let documentsLinked = 0;

    const startTime = Date.now();

    for (const prop of properties) {
      searched++;
      const parsed = parseOwnerName(prop.owner_name);

      if (!parsed.lastName) {
        // No usable name — skip
        skippedAmbiguous++;
        continue;
      }

      try {
        const result = await searchByName(page, parsed.lastName, parsed.firstName);

        if (result.error) {
          // If we hit an error, try re-navigating
          console.error(`\n  Error for "${prop.owner_name}": ${result.error}`);
          errors++;
          try {
            await navigateToSearch(page, fidlarConfig.base_url);
          } catch {
            console.error("  Failed to re-navigate. Stopping.");
            break;
          }
          continue;
        }

        if (result.totalResults === 0 || result.documents.length === 0) {
          // No results → free and clear
          await db.from("properties")
            .update({ lien_status: "free_clear", updated_at: new Date().toISOString() })
            .eq("id", prop.id);
          freeClear++;
          continue;
        }

        // Filter documents that match this property's legal description
        const matchedDocs: AvaDocResult[] = [];

        for (const doc of result.documents) {
          const fidlarLegal = doc.Legals?.map(l => l.Description).join("; ") || doc.LegalSummary || "";

          if (prop.legal_description && fidlarLegal) {
            if (legalsMatch(prop.legal_description, fidlarLegal)) {
              matchedDocs.push(doc);
            }
          } else if (!prop.legal_description) {
            // No legal description on property — accept if exactly 1 property
            // with this owner name in the county (checked below)
            matchedDocs.push(doc);
          }
        }

        // If property has no legal description, only accept if this owner is unique
        if (!prop.legal_description && matchedDocs.length > 0) {
          const { count } = await db.from("properties")
            .select("*", { count: "exact", head: true })
            .eq("county_id", county.id)
            .ilike("owner_name", `${parsed.lastName}%`);

          if ((count ?? 0) > 1) {
            // Ambiguous — skip
            skippedAmbiguous++;
            continue;
          }
        }

        if (matchedDocs.length === 0) {
          // Results found but nothing matched our property — ambiguous, skip
          skippedAmbiguous++;
          continue;
        }

        // Upsert matched documents to mortgage_records
        for (const doc of matchedDocs) {
          const classified = classifyDocType(doc.DocumentType);
          const grantors = doc.Names?.filter(n => n.Type === "Grantor").map(n => n.Name) || [];
          const grantees = doc.Names?.filter(n => n.Type === "Grantee").map(n => n.Name) || [];

          const dateMatch = doc.RecordedDateTime.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
          const recordDate = dateMatch
            ? `${dateMatch[3]}-${dateMatch[1].padStart(2, "0")}-${dateMatch[2].padStart(2, "0")}`
            : null;

          const legal = doc.Legals?.map(l => l.Description).join("; ") || doc.LegalSummary || "";

          const record: Record<string, unknown> = {
            property_id: prop.id,
            document_type: classified.document_type,
            recording_date: recordDate,
            loan_amount: doc.ConsiderationAmount > 0 ? Math.round(doc.ConsiderationAmount) : null,
            original_amount: doc.ConsiderationAmount > 0 ? Math.round(doc.ConsiderationAmount) : null,
            lender_name: grantees.join("; ").toUpperCase().trim().slice(0, 500) || null,
            borrower_name: grantors.join("; ").toUpperCase().trim().slice(0, 500) || null,
            document_number: doc.DocumentName || null,
            book_page: doc.Book && doc.Page ? `${doc.Book}/${doc.Page}` : (doc.Book || null),
            source_url: fidlarConfig.base_url,
            loan_type: classified.loan_type || null,
            deed_type: classified.deed_type || null,
            legal_description: legal || null,
          };

          // Compute mortgage fields if applicable
          if (doc.ConsiderationAmount > 0 && recordDate && (classified.document_type === "mortgage" || classified.document_type === "lien")) {
            const fields = computeMortgageFields({
              originalAmount: Math.round(doc.ConsiderationAmount),
              recordingDate: recordDate,
            });
            record.interest_rate = fields.interest_rate;
            record.term_months = fields.term_months;
            record.estimated_monthly_payment = fields.estimated_monthly_payment;
            record.estimated_current_balance = fields.estimated_current_balance;
            record.balance_as_of = fields.balance_as_of;
            record.maturity_date = fields.maturity_date;
          }

          // Upsert by document_number + source_url
          if (doc.DocumentName) {
            const { data: existing } = await db.from("mortgage_records")
              .select("id")
              .eq("document_number", doc.DocumentName)
              .eq("source_url", fidlarConfig.base_url)
              .limit(1);

            if (existing && existing.length > 0) {
              // Update: link to this property
              await db.from("mortgage_records")
                .update({ property_id: prop.id })
                .eq("id", existing[0].id);
            } else {
              await db.from("mortgage_records").insert(record);
            }
          } else {
            await db.from("mortgage_records").insert(record);
          }

          documentsLinked++;
        }

        // Update property lien_status
        const hasMortgage = matchedDocs.some(d => {
          const t = d.DocumentType.toUpperCase();
          return t.includes("MORTGAGE") || t.includes("LIEN") || t.includes("JUDGMENT");
        });

        await db.from("properties")
          .update({
            lien_status: hasMortgage ? "has_liens" : "free_clear",
            updated_at: new Date().toISOString(),
          })
          .eq("id", prop.id);

        if (hasMortgage) hasLiens++;
        else freeClear++;

      } catch (err: any) {
        console.error(`\n  Error for "${prop.owner_name}" (ID ${prop.id}): ${err.message?.slice(0, 100)}`);
        errors++;

        // Try to recover by re-navigating
        try {
          await navigateToSearch(page, fidlarConfig.base_url);
        } catch {
          console.error("  Failed to re-navigate. Stopping.");
          break;
        }
      }

      // Progress logging
      if (searched % 100 === 0 || searched === properties.length) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const rate = (searched / (Date.now() - startTime) * 1000).toFixed(1);
        console.log(
          `  [${searched}/${properties.length}] ` +
          `liens=${hasLiens} free=${freeClear} skip=${skippedAmbiguous} err=${errors} ` +
          `docs=${documentsLinked} | ${elapsed}s (${rate}/s)`,
        );
      }

      // Small delay between searches to be respectful
      await page.waitForTimeout(1000 + Math.floor(Math.random() * 1000));
    }

    // Final stats
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`\n═══════════════════════════════════════`);
    console.log(`  Searched:   ${searched}`);
    console.log(`  Has liens:  ${hasLiens}`);
    console.log(`  Free/clear: ${freeClear}`);
    console.log(`  Ambiguous:  ${skippedAmbiguous}`);
    console.log(`  Errors:     ${errors}`);
    console.log(`  Docs linked: ${documentsLinked}`);
    console.log(`  Time:       ${elapsed}s`);

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
