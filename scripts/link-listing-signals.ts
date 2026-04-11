#!/usr/bin/env tsx
/**
 * MXRE — Listing Signals Linker
 *
 * Links listing_signals.property_id to properties by address+zip match.
 *
 * Strategy:
 *   1. Load all unlinked listing_signals grouped by state
 *   2. For each, try exact address match by zip+normalized address
 *   3. Update property_id where match found
 *
 * Usage:
 *   npx tsx scripts/link-listing-signals.ts
 *   npx tsx scripts/link-listing-signals.ts --state=OH
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PAGE = 1000;
const UPDATE_CHUNK = 200;

const stateArg = process.argv.find(a => a.startsWith("--state="))?.split("=")[1]?.toUpperCase();

// Normalize address for comparison: uppercase, collapse spaces, strip punctuation
function normAddr(addr: string): string {
  return addr
    .toUpperCase()
    .replace(/[.,#]/g, " ")
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bTERRACE\b/g, "TER")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bWAY\b/g, "WY")
    .replace(/\bTRAIL\b/g, "TRL")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/\s+/g, " ")
    .trim();
}

async function getStates(): Promise<string[]> {
  if (stateArg) return [stateArg];
  const { data } = await db.from("listing_signals")
    .select("state_code")
    .is("property_id", null)
    .neq("state_code", null);
  const states = [...new Set((data || []).map((r: any) => r.state_code as string).filter(Boolean))];
  return states.sort();
}

async function linkState(stateCode: string): Promise<{ matched: number; total: number }> {
  console.log(`\n── ${stateCode} ──`);

  // Load all unlinked listings for this state
  const listings: Array<{ id: number; address: string; zip: string }> = [];
  let offset = 0;
  while (true) {
    const { data, error } = await db.from("listing_signals")
      .select("id, address, zip")
      .eq("state_code", stateCode)
      .is("property_id", null)
      .not("address", "is", null)
      .not("zip", "is", null)
      .range(offset, offset + PAGE - 1);
    if (error) { console.error(`  Error loading listings: ${error.message}`); break; }
    if (!data || data.length === 0) break;
    for (const r of data as any[]) listings.push(r);
    offset += PAGE;
    if (data.length < PAGE) break;
  }

  console.log(`  ${listings.length.toLocaleString()} unlinked listings`);
  if (listings.length === 0) return { matched: 0, total: 0 };

  // Group by zip code for efficient property lookup
  const byZip = new Map<string, Array<{ id: number; address: string }>>();
  for (const l of listings) {
    if (!l.zip) continue;
    if (!byZip.has(l.zip)) byZip.set(l.zip, []);
    byZip.get(l.zip)!.push({ id: l.id, address: l.address });
  }

  let matched = 0;
  const updates: Array<{ listingId: number; propertyId: number }> = [];

  process.stdout.write(`  Processing ${byZip.size} zip codes...`);

  for (const [zip, zipListings] of byZip) {
    // Build address → property_id map for this zip
    const propMap = new Map<string, number>();
    let propOffset = 0;
    while (true) {
      const { data: props } = await db.from("properties")
        .select("id, address")
        .eq("zip", zip)
        .eq("state_code", stateCode)
        .range(propOffset, propOffset + PAGE - 1);
      if (!props || props.length === 0) break;
      for (const p of props as any[]) {
        if (p.address) {
          propMap.set(normAddr(p.address), p.id);
          propMap.set(p.address.toUpperCase().trim(), p.id); // also exact
        }
      }
      propOffset += PAGE;
      if (props.length < PAGE) break;
    }

    // Match each listing to a property
    for (const l of zipListings) {
      const norm = normAddr(l.address);
      const exact = l.address.toUpperCase().trim();
      const propertyId = propMap.get(exact) ?? propMap.get(norm) ?? null;
      if (propertyId) {
        updates.push({ listingId: l.id, propertyId });
        matched++;
      }
    }

    // Flush updates in chunks
    while (updates.length >= UPDATE_CHUNK) {
      const chunk = updates.splice(0, UPDATE_CHUNK);
      for (const u of chunk) {
        await db.from("listing_signals")
          .update({ property_id: u.propertyId })
          .eq("id", u.listingId);
      }
      process.stdout.write(`\r  Matched: ${matched.toLocaleString()}/${listings.length.toLocaleString()}   `);
    }
  }

  // Flush remaining
  for (const u of updates) {
    await db.from("listing_signals")
      .update({ property_id: u.propertyId })
      .eq("id", u.listingId);
  }

  console.log(`\r  Done: ${matched.toLocaleString()} matched / ${listings.length.toLocaleString()} total (${Math.round(matched / listings.length * 100)}%)`);
  return { matched, total: listings.length };
}

async function main() {
  console.log("MXRE — Listing Signals Linker");
  console.log("=".repeat(50));

  const states = await getStates();
  console.log(`States with unlinked listings: ${states.join(", ")}`);

  let grandMatched = 0;
  let grandTotal = 0;

  for (const state of states) {
    const { matched, total } = await linkState(state);
    grandMatched += matched;
    grandTotal += total;
  }

  console.log(`\n${"=".repeat(50)}`);
  console.log(`GRAND TOTAL: ${grandMatched.toLocaleString()} matched / ${grandTotal.toLocaleString()} (${grandTotal > 0 ? Math.round(grandMatched / grandTotal * 100) : 0}%)`);

  // Refresh materialized views
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_KEY!;
  for (const v of ["county_lien_counts", "county_stats_mv"]) {
    const res = await fetch(`${url}/pg/query`, {
      method: "POST",
      headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW ${v}` }),
    });
    console.log(`REFRESH ${v}: ${res.ok ? "OK" : "FAILED"}`);
  }

  console.log("Done.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
