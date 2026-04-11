#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

const counties = [
  { county_name: "Paulding", state_code: "OH", state_fips: "39", county_fips: "125" },
  { county_name: "Wyandot", state_code: "OH", state_fips: "39", county_fips: "175" },
  { county_name: "Scott", state_code: "IA", state_fips: "19", county_fips: "163" },
  { county_name: "Black Hawk", state_code: "IA", state_fips: "19", county_fips: "013" },
  { county_name: "Hillsborough", state_code: "NH", state_fips: "33", county_fips: "011" },
  { county_name: "Rockingham", state_code: "NH", state_fips: "33", county_fips: "015" },
  { county_name: "Saline", state_code: "AR", state_fips: "05", county_fips: "125" },
  { county_name: "Yakima", state_code: "WA", state_fips: "53", county_fips: "077" },
];

for (const c of counties) {
  const { data, error } = await db.from("counties")
    .upsert({ ...c, active: true }, { onConflict: "state_fips,county_fips" })
    .select("id").single();
  if (data) console.log(`  ${c.county_name}, ${c.state_code}: id=${data.id}`);
  else console.log(`  ${c.county_name}, ${c.state_code}: ${error?.message}`);
}
