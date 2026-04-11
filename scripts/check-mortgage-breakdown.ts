#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  const { count: total } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log("Total mortgage_records:", total);

  const { count: linked } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  console.log("Linked to a property:", linked);

  const { count: withAmt } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("loan_amount", "is", null).gt("loan_amount", 0);
  console.log("With lien amounts:", withAmt);

  // By document_type
  for (const dt of ["mortgage", "deed", "lien", "satisfaction", "assignment", "transfer upon death", "notice of commencement", "power of attorney"]) {
    const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).eq("document_type", dt);
    if (count && count > 0) console.log("  " + dt + ":", count);
  }

  // By source
  const { data: sources } = await db.from("mortgage_records").select("source_url").limit(5000);
  const sourceCounts: Record<string, number> = {};
  for (const s of sources || []) {
    const key = new URL(s.source_url).hostname;
    sourceCounts[key] = (sourceCounts[key] || 0) + 1;
  }
  console.log("\nBy source (sample of 5000):");
  for (const [k, v] of Object.entries(sourceCounts).sort((a, b) => b[1] - a[1])) {
    console.log("  " + k + ": " + v);
  }
}
main().catch(console.error);
