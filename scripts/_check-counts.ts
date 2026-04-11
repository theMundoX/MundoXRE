#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const countyNames = ["Butler", "Lake", "Licking", "Franklin", "Cuyahoga", "Montgomery", "Summit", "Hamilton", "Delaware", "Lorain", "Medina", "Warren"];

for (const name of countyNames) {
  const { data } = await db.from("counties").select("id").eq("county_name", name).eq("state_code", "OH").single();
  if (data) {
    const { count } = await db.from("properties").select("*", { count: "exact", head: true }).eq("county_id", data.id);
    const { count: recCount } = await db.from("recorder_documents").select("*", { count: "exact", head: true }).eq("county_id", data.id);
    console.log(`${name} (id=${data.id}): ${count?.toLocaleString()} props, ${recCount?.toLocaleString()} recorder docs`);
  } else {
    console.log(`${name}: not in counties table`);
  }
}
