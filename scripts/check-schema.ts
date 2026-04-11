#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });
async function main() {
  const { data } = await db.from("properties").select("*").limit(1);
  if (data?.[0]) console.log("properties columns:", Object.keys(data[0]).join(", "));
}
main().catch(console.error);
