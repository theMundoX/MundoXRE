#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { count: total } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  const { count: withAmt } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("loan_amount", "is", null).gt("loan_amount", 0);
  const { count: props } = await db.from("properties").select("*", { count: "exact", head: true });
  const { count: rents } = await db.from("rent_snapshots").select("*", { count: "exact", head: true });
  console.log("Properties:", props?.toLocaleString());
  console.log("Rent snapshots:", rents?.toLocaleString());
  console.log("Mortgage/lien records:", total?.toLocaleString());
  console.log("With actual lien amounts:", withAmt?.toLocaleString());
}
main().catch(console.error);
