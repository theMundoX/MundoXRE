#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  // Get distinct source_urls
  const { data } = await db.from("mortgage_records").select("source_url").limit(10000);
  const urls: Record<string, number> = {};
  for (const r of data || []) { urls[r.source_url] = (urls[r.source_url] || 0) + 1; }
  console.log("Source URLs:");
  for (const [u, c] of Object.entries(urls).sort((a, b) => b[1] - a[1]).slice(0, 15)) {
    console.log(`  ${c} — ${u}`);
  }
  // Check linked vs unlinked
  const { count: totalLinked } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  const { count: totalUnlinked } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).is("property_id", null);
  console.log(`\nLinked: ${totalLinked}`);
  console.log(`Unlinked: ${totalUnlinked}`);
}
main().catch(console.error);
