#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { count: total } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  const { count: linked } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  const { count: withAmt } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("loan_amount", "is", null).gt("loan_amount", 0);
  const { count: withNames } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("borrower_name", "is", null).neq("borrower_name", "");
  const { count: props } = await db.from("properties").select("*", { count: "exact", head: true });
  const { count: propsWithOwner } = await db.from("properties").select("*", { count: "exact", head: true }).not("owner_name", "is", null);

  console.log("=== FULL STATUS ===");
  console.log("Properties:", props?.toLocaleString());
  console.log("Properties with owner_name:", propsWithOwner?.toLocaleString());
  console.log("Mortgage records:", total?.toLocaleString());
  console.log("  Linked to properties:", linked?.toLocaleString());
  console.log("  With lien amounts:", withAmt?.toLocaleString());
  console.log("  With borrower names:", withNames?.toLocaleString());

  // Count by county where we have BOTH properties and liens
  const { data: counties } = await db.from("counties").select("id, county_name, state_code").eq("active", true);
  console.log("\n=== COUNTIES WITH PROPERTIES ===");
  for (const c of counties || []) {
    const { count: propCount } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", c.id);
    if ((propCount || 0) > 0) {
      const { count: ownerCount } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", c.id).not("owner_name", "is", null);
      console.log(`  ${c.county_name}, ${c.state_code}: ${propCount?.toLocaleString()} props (${ownerCount?.toLocaleString()} with owners)`);
    }
  }
}
main().catch(console.error);
