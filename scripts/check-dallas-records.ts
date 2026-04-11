#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Get Dallas records with all fields
  const { data } = await db.from("mortgage_records")
    .select("*")
    .like("source_url", "%publicsearch%")
    .not("document_number", "is", null)
    .limit(10);

  for (const r of data || []) {
    console.log("─────────────────────────────────");
    for (const [k, v] of Object.entries(r)) {
      if (k === "raw") continue;
      console.log(`  ${k.padEnd(25)} ${v === null ? "NULL" : v === "" ? "(empty)" : String(v).slice(0, 60)}`);
    }
    if (r.raw) {
      console.log(`  ${"raw".padEnd(25)} ${JSON.stringify(r.raw).slice(0, 200)}`);
    }
    console.log();
  }

  // Count by document_type
  const { data: types } = await db.from("mortgage_records")
    .select("document_type")
    .like("source_url", "%publicsearch%");

  const counts = new Map<string, number>();
  for (const t of types || []) {
    counts.set(t.document_type || "(empty)", (counts.get(t.document_type || "(empty)") || 0) + 1);
  }
  console.log("\nDallas document types:");
  for (const [t, c] of [...counts.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`  ${t.padEnd(30)} ${c}`);
  }

  // How many matched to properties?
  const { count: matched } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .like("source_url", "%publicsearch%")
    .not("property_id", "is", null);
  const { count: total } = await db.from("mortgage_records")
    .select("*", { count: "exact", head: true })
    .like("source_url", "%publicsearch%");
  console.log(`\nMatched to properties: ${matched}/${total}`);
}
main().catch(console.error);
