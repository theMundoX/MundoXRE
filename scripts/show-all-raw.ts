#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, { auth: { persistSession: false } });

async function main() {
  // Get property
  const { data: prop } = await db.from("properties").select("*").eq("id", 123668).single();
  if (!prop) return;

  console.log("═══════════════════════════════════════════════════════════════════════════");
  console.log("  MXRE — RAW DATA DUMP — 5444 GASTON AVE, DALLAS TX 75214");
  console.log("═══════════════════════════════════════════════════════════════════════════\n");

  console.log("  ─── properties TABLE (every column) ────────────────────────────────\n");
  for (const [key, val] of Object.entries(prop)) {
    if (key === "raw" || key === "assessor_url") continue; // skip huge raw blob
    const display = val === null ? "NULL" : val === "" ? "(empty)" : String(val);
    console.log(`  ${key.padEnd(25)} ${display}`);
  }
  if (prop.assessor_url) {
    console.log(`  ${"assessor_url".padEnd(25)} ${prop.assessor_url}`);
  }

  // Rent snapshots
  const { data: rents } = await db.from("rent_snapshots").select("*").eq("property_id", prop.id);
  console.log("\n  ─── rent_snapshots TABLE (every column) ────────────────────────────\n");
  if (rents && rents.length > 0) {
    for (let i = 0; i < rents.length; i++) {
      console.log(`  --- Snapshot ${i + 1} ---`);
      for (const [key, val] of Object.entries(rents[i])) {
        if (key === "raw") {
          console.log(`  ${key.padEnd(25)} ${JSON.stringify(val)}`);
        } else {
          const display = val === null ? "NULL" : val === "" ? "(empty)" : String(val);
          console.log(`  ${key.padEnd(25)} ${display}`);
        }
      }
      console.log();
    }
  } else {
    console.log("  (no rent snapshots)");
  }

  // Mortgage records
  const { data: mortgages } = await db.from("mortgage_records").select("*").eq("property_id", prop.id);
  console.log("\n  ─── mortgage_records TABLE (every column) ──────────────────────────\n");
  if (mortgages && mortgages.length > 0) {
    for (let i = 0; i < mortgages.length; i++) {
      console.log(`  --- Record ${i + 1} ---`);
      for (const [key, val] of Object.entries(mortgages[i])) {
        const display = val === null ? "NULL" : val === "" ? "(empty)" : String(val);
        console.log(`  ${key.padEnd(25)} ${display}`);
      }
      console.log();
    }
  } else {
    console.log("  (no mortgage records — recorder ingestion needed)");
  }

  // Also show a DIFFERENT property that DOES have mortgage data
  const { data: withMortgage } = await db.from("mortgage_records")
    .select("property_id")
    .eq("document_type", "mortgage")
    .not("property_id", "is", null)
    .limit(1);

  if (withMortgage && withMortgage.length > 0) {
    const pid2 = withMortgage[0].property_id;
    const { data: prop2 } = await db.from("properties").select("*").eq("id", pid2).single();
    const { data: mort2 } = await db.from("mortgage_records").select("*").eq("property_id", pid2);

    if (prop2) {
      console.log("\n═══════════════════════════════════════════════════════════════════════════");
      console.log("  PROPERTY WITH ACTUAL MORTGAGE DATA");
      console.log("═══════════════════════════════════════════════════════════════════════════\n");

      console.log("  ─── Property ───\n");
      for (const [key, val] of Object.entries(prop2)) {
        if (key === "raw" || key === "assessor_url") continue;
        const display = val === null ? "NULL" : val === "" ? "(empty)" : String(val);
        console.log(`  ${key.padEnd(25)} ${display}`);
      }

      console.log("\n  ─── Mortgage Records ───\n");
      for (const m of mort2 || []) {
        for (const [key, val] of Object.entries(m)) {
          const display = val === null ? "NULL" : val === "" ? "(empty)" : String(val);
          console.log(`  ${key.padEnd(25)} ${display}`);
        }
        console.log();
      }
    }
  }
}

main().catch(console.error);
