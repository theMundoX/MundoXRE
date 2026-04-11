#!/usr/bin/env tsx
/**
 * Enrich Fidlar AVA mortgage records by fetching individual document detail pages.
 * Fills in borrower_name, lender_name, and loan_amount for records that are missing them.
 * Then links enriched records to properties by name matching.
 *
 * Usage:
 *   npx tsx scripts/enrich-fidlar-details.ts                    # All Fidlar counties
 *   npx tsx scripts/enrich-fidlar-details.ts --county=Fairfield # Single county
 */
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { FidlarAvaAdapter, FIDLAR_AVA_COUNTIES } from "../src/discovery/adapters/fidlar-ava.js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const RATE_LIMIT_MS = 350;
const BATCH_SIZE = 500;

const countyArg = process.argv.find(a => a.startsWith("--county="))?.split("=")[1];

function isCompany(name: string): boolean {
  return /\b(LLC|INC|CORP|BANK|MORTGAGE|CREDIT UNION|TRUST|NATIONAL|FEDERAL|LENDING|LOAN|FINANCIAL|SERVIC|ASSOC|INSURANCE|SAVINGS|AGENCY|SYSTEMS|ELECTRONIC|REGISTRATION|SECRETARY|TREASURER|COUNTY|STATE|CITY)\b/i.test(name);
}

async function buildOwnerIndex(countyId: number): Promise<Map<string, Array<{ id: number; name: string }>>> {
  const index = new Map<string, Array<{ id: number; name: string }>>();
  let offset = 0;
  while (true) {
    const { data } = await db.from("properties")
      .select("id, owner_name")
      .eq("county_id", countyId)
      .not("owner_name", "is", null)
      .neq("owner_name", "")
      .order("id")
      .range(offset, offset + 9999);
    if (!data || data.length === 0) break;
    for (const p of data) {
      const parts = (p.owner_name || "").toUpperCase().split(/[\s,;]+/);
      const lastName = parts[0];
      if (lastName && lastName.length > 2 && !isCompany(lastName)) {
        if (!index.has(lastName)) index.set(lastName, []);
        index.get(lastName)!.push({ id: p.id, name: p.owner_name.toUpperCase() });
      }
    }
    offset += 10000;
    if (data.length < 10000) break;
  }
  return index;
}

function matchToProperty(
  personName: string,
  ownerIndex: Map<string, Array<{ id: number; name: string }>>,
): number | null {
  if (!personName || personName.length < 3 || isCompany(personName)) return null;

  const parts = personName.toUpperCase().split(/[\s,;]+/).filter(p => p.length > 2);
  const lastName = parts[0];
  if (!lastName) return null;

  const candidates = ownerIndex.get(lastName);
  if (!candidates) return null;

  let bestId: number | null = null;
  let bestScore = 0;

  for (const c of candidates.slice(0, 50)) {
    let score = 0;
    for (const part of parts) {
      if (c.name.includes(part)) score++;
    }
    if (score >= Math.min(2, parts.length) && score > bestScore) {
      bestScore = score;
      bestId = c.id;
    }
  }

  return bestId;
}

async function main() {
  console.log("MXRE — Enrich Fidlar AVA Document Details\n");

  const adapter = new FidlarAvaAdapter();
  await adapter.init();

  let counties = FIDLAR_AVA_COUNTIES;
  if (countyArg) {
    counties = counties.filter(c => c.county_name.toLowerCase() === countyArg.toLowerCase());
  }

  for (const config of counties) {
    // Find county in DB
    const { data: county } = await db.from("counties")
      .select("id")
      .eq("county_name", config.county_name)
      .eq("state_code", config.state)
      .single();
    if (!county) continue;

    // Count unlinked records without names
    const { count: needsEnrichment } = await db.from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .is("property_id", null)
      .ilike("source_url", `%${config.state}${config.county_name.replace(/ /g, "")}%`)
      .or("borrower_name.is.null,borrower_name.eq.");

    if (!needsEnrichment || needsEnrichment === 0) {
      console.log(`  ${config.county_name} ${config.state}: no records need enrichment`);
      continue;
    }

    console.log(`\n━━━ ${config.county_name}, ${config.state} — ${needsEnrichment.toLocaleString()} records need detail fetch ━━━`);

    // Build owner index for linking
    const ownerIndex = await buildOwnerIndex(county.id);
    console.log(`  Owner index: ${ownerIndex.size} unique last names`);

    // Get token
    let token: string;
    try {
      token = await adapter.getToken(config);
      console.log(`  Token acquired`);
    } catch (err: any) {
      console.error(`  Token failed: ${err.message?.slice(0, 100)}`);
      continue;
    }

    const apiBase = `https://ava.fidlar.com/${config.state}${config.county_name.replace(/ /g, "")}/`;
    let enriched = 0, linked = 0, errors = 0;
    let offset = 0;

    while (true) {
      // Get batch of records needing enrichment
      const { data: records } = await db.from("mortgage_records")
        .select("id, document_number, document_type")
        .is("property_id", null)
        .ilike("source_url", `%${config.state}${config.county_name.replace(/ /g, "")}%`)
        .or("borrower_name.is.null,borrower_name.eq.")
        .order("id")
        .range(offset, offset + BATCH_SIZE - 1);

      if (!records || records.length === 0) break;

      for (const rec of records) {
        try {
          // Fetch document detail from Fidlar API
          const detailUrl = `${apiBase}breeze/DocumentDetail/${rec.document_number}`;
          const resp = await fetch(detailUrl, {
            headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
          });

          if (resp.status === 401) {
            // Refresh token
            token = await adapter.getToken(config);
            const retry = await fetch(detailUrl, {
              headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
            });
            if (!retry.ok) { errors++; continue; }
            const data = await retry.json();
            await processDetail(rec, data, ownerIndex);
          } else if (resp.ok) {
            const contentType = resp.headers.get("content-type") || "";
            if (contentType.includes("text/html")) {
              // Token expired, refresh
              token = await adapter.getToken(config);
              errors++;
              continue;
            }
            const data = await resp.json();
            await processDetail(rec, data, ownerIndex);
          } else {
            errors++;
          }

          enriched++;
          await new Promise(r => setTimeout(r, RATE_LIMIT_MS));
        } catch (err: any) {
          errors++;
          if (errors <= 3) console.error(`  Error on ${rec.document_number}: ${err.message?.slice(0, 80)}`);
          if (errors > 100) {
            console.error(`  Too many errors, stopping ${config.county_name}`);
            break;
          }
        }

        if (enriched % 100 === 0) {
          process.stdout.write(`\r  Enriched: ${enriched} | Linked: ${linked} | Errors: ${errors}`);
        }
      }

      offset += BATCH_SIZE;
      if (records.length < BATCH_SIZE) break;
      if (errors > 100) break;
    }

    console.log(`\n  ${config.county_name}: enriched ${enriched} | linked ${linked} | errors ${errors}`);
  }

  // Refresh MVs
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_KEY!;
  for (const v of ["county_lien_counts", "county_stats_mv"]) {
    await fetch(`${url}/pg/query`, {
      method: "POST",
      headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW ${v}` }),
    });
  }
  console.log("\nMVs refreshed");

  await adapter.close();

  async function processDetail(
    rec: { id: number; document_type: string },
    data: any,
    ownerIndex: Map<string, Array<{ id: number; name: string }>>,
  ) {
    const grantor = data.Grantor || data.grantor || data.GrantorName || "";
    const grantee = data.Grantee || data.grantee || data.GranteeName || "";
    const amount = parseFloat(data.ConsiderationAmount || data.consideration || "0");

    const update: Record<string, unknown> = {};
    if (grantor) update.borrower_name = String(grantor).slice(0, 500);
    if (grantee) update.lender_name = String(grantee).slice(0, 500);
    if (amount > 0 && amount < 2147483647) {
      update.loan_amount = Math.round(amount);
      update.original_amount = Math.round(amount);
    }

    // Try to link by name
    let personName = "";
    if (rec.document_type === "mortgage") {
      personName = grantor && !isCompany(grantor) ? grantor : "";
    } else if (rec.document_type === "satisfaction") {
      personName = grantee && !isCompany(grantee) ? grantee : "";
    } else {
      personName = grantee && !isCompany(grantee) ? grantee : grantor && !isCompany(grantor) ? grantor : "";
    }

    const propId = matchToProperty(personName, ownerIndex);
    if (propId) {
      update.property_id = propId;
      linked++;
    }

    if (Object.keys(update).length > 0) {
      await db.from("mortgage_records").update(update).eq("id", rec.id);
    }
  }
}

main().catch(console.error);
