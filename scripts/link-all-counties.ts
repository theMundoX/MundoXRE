#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

const SOURCES: Record<string, { county_name: string; state_code: string }> = {
  "https://ava.fidlar.com/OHFairfield/AvaWeb/": { county_name: "Fairfield", state_code: "OH" },
  "https://ava.fidlar.com/OHGeauga/AvaWeb/": { county_name: "Geauga", state_code: "OH" },
  "https://ava.fidlar.com/OHPaulding/AvaWeb/": { county_name: "Paulding", state_code: "OH" },
  "https://ava.fidlar.com/OHWyandot/AvaWeb/": { county_name: "Wyandot", state_code: "OH" },
  "https://ava.fidlar.com/MIOakland/AvaWeb/": { county_name: "Oakland", state_code: "MI" },
};

async function main() {
  console.log("MXRE — Link Liens Across All New Counties\n");
  let totalLinked = 0;

  for (const [url, info] of Object.entries(SOURCES)) {
    const { data: county } = await db.from("counties").select("id").eq("county_name", info.county_name).eq("state_code", info.state_code).single();
    if (!county) { console.log(`  Skip ${info.county_name}, ${info.state_code} — no county`); continue; }

    const { data: records } = await db.from("mortgage_records").select("id, borrower_name")
      .is("property_id", null).eq("source_url", url)
      .not("borrower_name", "is", null).neq("borrower_name", "").limit(5000);

    if (!records?.length) { console.log(`  ${info.county_name}: no unlinked records`); continue; }
    console.log(`  ${info.county_name}, ${info.state_code}: ${records.length} unlinked`);

    let linked = 0;
    for (const rec of records) {
      const parts = rec.borrower_name.split(";")[0].trim().split(/\s+/).filter((p: string) => p.length > 1);
      if (parts.length === 0) continue;

      const { data: props } = await db.from("properties").select("id, owner_name")
        .eq("county_id", county.id).ilike("owner_name", `${parts[0]}%`).limit(20);
      if (!props?.length) continue;

      let best: { id: number; score: number } | null = null;
      for (const prop of props) {
        const owner = (prop.owner_name || "").toUpperCase();
        let score = 0;
        for (const part of parts) { if (owner.includes(part)) score++; }
        if (score >= Math.min(2, parts.length) && (!best || score > best.score)) best = { id: prop.id, score };
      }
      if (best) {
        await db.from("mortgage_records").update({ property_id: best.id }).eq("id", rec.id);
        linked++;
      }
    }
    console.log(`    Linked: ${linked}`);
    totalLinked += linked;
  }

  const { count } = await db.from("mortgage_records").select("*", { count: "exact", head: true }).not("property_id", "is", null);
  console.log(`\n  Total newly linked: ${totalLinked}`);
  console.log(`  Total linked in DB: ${count}`);
}
main().catch(console.error);
